//! End-to-end tests against an independent mock exchange.
//!
//! These tests drive `coordinator::run_with_shutdown` with the real Polymarket
//! discovery/session adapters, collector core, serialized SBE writer, and SBE
//! reader. The mock exchange owns an independent authoritative book and event
//! history.
//!
//! `verify` is only a capture self-consistency check. The oracle for capture
//! completeness under these scenarios is the mock exchange's full delivered
//! update history plus the snapshots it sent at recovery boundaries.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use engineering_challenge::{
    Side,
    config::{ExchangeKind, Settings},
    coordinator,
    fixed::format_micros,
    journal,
    report::RunReport,
    time::now_us,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
        sha256_hex,
    },
    verify::{self, VerifySummary},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::sleep,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const CHUNK_SIZE: usize = 16 * 1024;
const CHUNK_STR: &str = "16384";
const WAIT_LIMIT: Duration = Duration::from_secs(15);
const SYMBOL_A: &str = "polymarket:cond-a:0";

fn market(condition: &str, token: &str) -> MarketSpec {
    MarketSpec {
        condition_id: condition.to_owned(),
        token_id: token.to_owned(),
        question: format!("Mock market {condition}"),
    }
}

fn test_settings(mock: &MockExchange, output: &Path) -> Settings {
    let mut settings = Settings::for_exchange(ExchangeKind::Polymarket);
    settings.polymarket.gamma_url = format!("http://{}", mock.http_addr);
    settings.polymarket.ws_url = format!("ws://{}/ws", mock.ws_addr);
    settings.polymarket.batch_size = 500;
    settings.polymarket.discovery_page_size = 100;
    settings.polymarket.request_timeout_secs = 5;
    settings.polymarket.discovery_page_delay_ms = 0;
    settings.polymarket.require_accepting_orders = true;

    settings.collector.shards = 1;
    settings.collector.replicas = 2;
    settings.collector.snapshot_interval_secs = 60;
    settings.collector.discovery_interval_secs = 60;
    settings.collector.reconnect_after_secs = 1800;
    settings.collector.connection_idle_timeout_secs = 45;
    settings.collector.stale_probe_after_secs = 120;
    settings.collector.probe_interval_secs = 30;
    settings.collector.max_probes = 3;
    settings.collector.writer_queue = 4096;
    settings.collector.flush_interval_secs = 1;
    settings.collector.max_markets = 0;
    settings.collector.duration_secs = 0;
    settings.collector.final_snapshots = true;
    settings.collector.writer_send_timeout_secs = 5;

    settings.output.path = output.display().to_string();
    settings.output.chunk_size = CHUNK_SIZE;
    settings.output.metadata_path = None;
    settings.output.report_path = None;
    settings.output.sync_on_close = false;
    settings.metrics.listen = None;
    settings.network = Default::default();
    settings
}

fn spawn_collector(settings: Settings, done: Arc<Notify>) -> JoinHandle<anyhow::Result<RunReport>> {
    spawn_collector_with_limit(settings, done, Duration::from_secs(30))
}

fn spawn_collector_with_limit(
    settings: Settings,
    done: Arc<Notify>,
    safety: Duration,
) -> JoinHandle<anyhow::Result<RunReport>> {
    tokio::spawn(coordinator::run_with_shutdown(settings, async move {
        tokio::select! {
            _ = done.notified() => "test scenario complete".to_owned(),
            _ = sleep(safety) => "test scenario safety timeout".to_owned(),
        }
    }))
}

async fn finish_collector(handle: JoinHandle<anyhow::Result<RunReport>>) -> RunReport {
    finish_collector_with_limit(handle, Duration::from_secs(35)).await
}

async fn finish_collector_with_limit(
    handle: JoinHandle<anyhow::Result<RunReport>>,
    limit: Duration,
) -> RunReport {
    match tokio::time::timeout(limit, handle).await {
        Ok(Ok(Ok(report))) => report,
        Ok(Ok(Err(error))) => panic!("collector failed: {error:#}"),
        Ok(Err(error)) => panic!("collector task failed: {error}"),
        Err(_) => panic!("collector did not stop within {limit:?}"),
    }
}

/// Reserve an ephemeral local port for the collector's metrics listener.
fn unused_local_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binding an ephemeral port");
    listener
        .local_addr()
        .expect("reading the ephemeral address")
}

/// Read one unlabelled Prometheus counter from the collector's metrics
/// endpoint. A missing endpoint or metric is not an error: startup and the
/// one-second metrics refresh both race the first scrape.
async fn scrape_metric(address: SocketAddr, metric: &str) -> Option<f64> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    let body = response.split_once("\r\n\r\n")?.1;
    body.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        if name == metric {
            value.parse().ok()
        } else {
            None
        }
    })
}

async fn wait_until(what: &str, mut predicate: impl FnMut() -> bool) {
    wait_until_for(WAIT_LIMIT, what, &mut predicate).await;
}

async fn wait_for_metric_at_least(address: SocketAddr, metric: &str, expected: f64) {
    let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
    let mut last = None;
    loop {
        if let Some(value) = scrape_metric(address, metric).await {
            last = Some(value);
            if value >= expected {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {metric} >= {expected} (last value {last:?})");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_no_resyncing_symbols(address: SocketAddr) {
    let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
    let mut last = None;
    loop {
        if let Some(value) = scrape_metric(address, "collector_symbols_resyncing").await {
            last = Some(value);
            if value == 0.0 {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for recovery to reach Live (last resyncing value {last:?})");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_until_for(limit: Duration, what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if predicate() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {limit:?} waiting for {what}");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Independent exchange model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct MockBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl MockBook {
    fn set(&mut self, side: Side, price: i64, quantity: i64) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
    }

    fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    fn from_snapshot(snapshot: &SnapshotDomain) -> Self {
        let mut book = Self::default();
        for level in &snapshot.bids {
            if level.quantity != 0 {
                book.bids.insert(level.price_level, level.quantity);
            }
        }
        for level in &snapshot.asks {
            if level.quantity != 0 {
                book.asks.insert(level.price_level, level.quantity);
            }
        }
        book
    }

    fn book_json(&self, token: &str) -> Value {
        json!({
            "event_type": "book",
            "asset_id": token,
            "market": "mock",
            "bids": self.bids.iter().rev().map(|(price, quantity)| json!({
                "price": format_micros(*price),
                "size": format_micros(*quantity),
            })).collect::<Vec<_>>(),
            "asks": self.asks.iter().map(|(price, quantity)| json!({
                "price": format_micros(*price),
                "size": format_micros(*quantity),
            })).collect::<Vec<_>>(),
            "timestamp": mock_timestamp_ms().to_string(),
            "hash": "mock",
        })
    }
}

#[derive(Debug, Clone)]
struct MarketSpec {
    condition_id: String,
    token_id: String,
    question: String,
}

impl MarketSpec {
    fn gamma_json(&self) -> Value {
        json!({
            "conditionId": self.condition_id,
            "question": self.question,
            "active": true,
            "closed": false,
            "archived": false,
            "enableOrderBook": true,
            "acceptingOrders": true,
            "version": "v1",
            "clobTokenIds": serde_json::to_string(&[&self.token_id]).expect("token JSON"),
            "outcomes": serde_json::to_string(&["Yes"]).expect("outcome JSON"),
            "orderPriceMinTickSize": "0.01",
        })
    }
}

#[derive(Debug, Clone)]
struct MockUpdate {
    token: String,
    side: Side,
    price: i64,
    quantity: i64,
    delivered: bool,
    resulting: MockBook,
}

#[derive(Debug, Clone)]
struct SubscriptionFrame {
    connection: u64,
    operation: String,
    assets: usize,
    initial: bool,
}

#[derive(Clone)]
enum RawMockFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}

impl From<RawMockFrame> for Outbound {
    fn from(value: RawMockFrame) -> Self {
        match value {
            RawMockFrame::Text(text) => Self::Text(text),
            RawMockFrame::Binary(bytes) => Self::Binary(bytes),
            RawMockFrame::Ping(bytes) => Self::Ping(bytes),
            RawMockFrame::Pong(bytes) => Self::Pong(bytes),
        }
    }
}

struct Connection {
    subscribed: BTreeSet<String>,
    outbound: mpsc::UnboundedSender<Outbound>,
    close: Option<oneshot::Sender<()>>,
    initial_gate: Option<Arc<Notify>>,
    initial_dump_sent: bool,
}

enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

struct MockState {
    markets: Mutex<Vec<MarketSpec>>,
    truth: Mutex<BTreeMap<String, MockBook>>,
    updates: Mutex<Vec<MockUpdate>>,
    connections: Mutex<BTreeMap<u64, Connection>>,
    subscription_log: Mutex<Vec<SubscriptionFrame>>,
    next_id: AtomicU64,
    /// Application text/binary frames written to any socket. Every frame sent
    /// here must later appear in the raw-frame journal exactly once.
    frames_sent: AtomicU64,
    reject_connections: AtomicBool,
    suppress_deltas: AtomicBool,
    gate_second: bool,
    shutdown: broadcast::Sender<()>,
}

impl MockState {
    fn new(markets: Vec<MarketSpec>, gate_second: bool) -> Self {
        let (shutdown, _) = broadcast::channel(1);
        Self {
            markets: Mutex::new(markets),
            truth: Mutex::new(BTreeMap::new()),
            updates: Mutex::new(Vec::new()),
            connections: Mutex::new(BTreeMap::new()),
            subscription_log: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            reject_connections: AtomicBool::new(false),
            suppress_deltas: AtomicBool::new(false),
            gate_second,
            shutdown,
        }
    }

    fn frames_sent(&self) -> u64 {
        self.frames_sent.load(Ordering::SeqCst)
    }

    /// Push one raw application payload to every connection that completed
    /// its initial dump. Used to deliver malformed, unknown, and control
    /// payloads the collector must still journal.
    fn publish_raw(&self, payload: RawMockFrame) {
        let targets: Vec<mpsc::UnboundedSender<Outbound>> = self
            .connections
            .lock()
            .expect("connections lock")
            .values()
            .filter(|connection| connection.initial_dump_sent)
            .map(|connection| connection.outbound.clone())
            .collect();
        for target in targets {
            let _ = target.send(Outbound::from(payload.clone()));
        }
    }

    fn set_book(&self, token: &str, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        let mut book = MockBook::default();
        for (price, quantity) in bids {
            book.bids.insert(*price, *quantity);
        }
        for (price, quantity) in asks {
            book.asks.insert(*price, *quantity);
        }
        self.truth
            .lock()
            .expect("truth lock")
            .insert(token.to_owned(), book);
    }

    fn book(&self, token: &str) -> MockBook {
        self.truth
            .lock()
            .expect("truth lock")
            .get(token)
            .cloned()
            .unwrap_or_default()
    }

    fn set_markets(&self, markets: Vec<MarketSpec>) {
        *self.markets.lock().expect("markets lock") = markets;
    }

    fn register(
        &self,
        id: u64,
        outbound: mpsc::UnboundedSender<Outbound>,
        close: oneshot::Sender<()>,
    ) {
        let initial_gate = (self.gate_second && id == 1).then(|| Arc::new(Notify::new()));
        self.connections.lock().expect("connections lock").insert(
            id,
            Connection {
                subscribed: BTreeSet::new(),
                outbound,
                close: Some(close),
                initial_gate,
                initial_dump_sent: false,
            },
        );
    }

    fn unregister(&self, id: u64) {
        self.connections
            .lock()
            .expect("connections lock")
            .remove(&id);
    }

    fn release_initial(&self, id: u64) {
        if let Some(gate) = self
            .connections
            .lock()
            .expect("connections lock")
            .get(&id)
            .and_then(|connection| connection.initial_gate.clone())
        {
            gate.notify_one();
        }
    }

    fn close_connection(&self, id: u64) {
        if let Some(mut connection) = self
            .connections
            .lock()
            .expect("connections lock")
            .remove(&id)
        {
            if let Some(close) = connection.close.take() {
                let _ = close.send(());
            }
            let _ = connection.outbound.send(Outbound::Close);
        }
    }

    fn close_all(&self) {
        let connections = std::mem::take(&mut *self.connections.lock().expect("connections lock"));
        for (_, mut connection) in connections {
            if let Some(close) = connection.close.take() {
                let _ = close.send(());
            }
            let _ = connection.outbound.send(Outbound::Close);
        }
    }

    fn connection_count(&self) -> usize {
        self.connections.lock().expect("connections lock").len()
    }

    fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    fn max_connection_id(&self) -> Option<u64> {
        self.connections
            .lock()
            .expect("connections lock")
            .keys()
            .next_back()
            .copied()
    }

    fn subscription_count(&self, token: &str) -> usize {
        self.connections
            .lock()
            .expect("connections lock")
            .values()
            .filter(|connection| connection.subscribed.contains(token))
            .count()
    }

    fn total_subscriptions(&self) -> usize {
        self.connections
            .lock()
            .expect("connections lock")
            .values()
            .map(|connection| connection.subscribed.len())
            .sum()
    }

    fn max_subscriptions_per_connection(&self) -> usize {
        self.connections
            .lock()
            .expect("connections lock")
            .values()
            .map(|connection| connection.subscribed.len())
            .max()
            .unwrap_or(0)
    }

    fn set_reject_connections(&self, reject: bool) {
        self.reject_connections.store(reject, Ordering::SeqCst);
    }

    fn set_suppress_deltas(&self, suppress: bool) {
        self.suppress_deltas.store(suppress, Ordering::SeqCst);
    }

    fn publish_delta(&self, token: &str, side: Side, price: i64, quantity: i64) -> bool {
        let resulting = {
            let mut truth = self.truth.lock().expect("truth lock");
            let book = truth.entry(token.to_owned()).or_default();
            book.set(side, price, quantity);
            book.clone()
        };
        let suppressed = self.suppress_deltas.load(Ordering::SeqCst);
        let frame = (!suppressed).then(|| delta_json(token, side, price, quantity));
        let mut delivered = false;
        if let Some(frame) = frame {
            let targets: Vec<mpsc::UnboundedSender<Outbound>> = self
                .connections
                .lock()
                .expect("connections lock")
                .values()
                .filter(|connection| {
                    connection.subscribed.contains(token) && connection.initial_dump_sent
                })
                .map(|connection| connection.outbound.clone())
                .collect();
            for target in targets {
                if target.send(Outbound::Text(frame.clone())).is_ok() {
                    delivered = true;
                }
            }
        }
        self.updates.lock().expect("updates lock").push(MockUpdate {
            token: token.to_owned(),
            side,
            price,
            quantity,
            delivered,
            resulting,
        });
        delivered
    }

    fn delivered_updates_for(&self, token: &str) -> Vec<MockUpdate> {
        self.updates
            .lock()
            .expect("updates lock")
            .iter()
            .filter(|update| update.token == token && update.delivered)
            .cloned()
            .collect()
    }

    fn all_updates_for(&self, token: &str) -> Vec<MockUpdate> {
        self.updates
            .lock()
            .expect("updates lock")
            .iter()
            .filter(|update| update.token == token)
            .cloned()
            .collect()
    }

    fn book_frames(&self, assets: &[String], as_array: bool) -> Vec<String> {
        let truth = self.truth.lock().expect("truth lock");
        let books: Vec<Value> = assets
            .iter()
            .filter_map(|asset| truth.get(asset).map(|book| book.book_json(asset)))
            .collect();
        if books.is_empty() {
            return Vec::new();
        }
        if as_array {
            vec![Value::Array(books).to_string()]
        } else {
            books.into_iter().map(|book| book.to_string()).collect()
        }
    }

    async fn handle_client_message(&self, id: u64, text: &str) -> Vec<String> {
        let trimmed = text.trim();
        if trimmed.eq_ignore_ascii_case("PING") {
            return vec!["PONG".to_owned()];
        }
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("PONG") {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            return Vec::new();
        };
        if value.get("type").and_then(Value::as_str) == Some("market") {
            let assets = string_array(value.get("assets_ids"));
            let initial = value
                .get("initial_dump")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let gate = {
                let mut connections = self.connections.lock().expect("connections lock");
                let Some(connection) = connections.get_mut(&id) else {
                    return Vec::new();
                };
                connection.subscribed = assets.iter().cloned().collect();
                connection.initial_gate.clone()
            };
            self.subscription_log
                .lock()
                .expect("subscription log")
                .push(SubscriptionFrame {
                    connection: id,
                    operation: "initial_dump".to_owned(),
                    assets: assets.len(),
                    initial,
                });
            if initial {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                if let Some(connection) = self
                    .connections
                    .lock()
                    .expect("connections lock")
                    .get_mut(&id)
                {
                    connection.initial_dump_sent = true;
                }
                return self.book_frames(&assets, true);
            }
            return Vec::new();
        }
        if let Some(operation) = value.get("operation").and_then(Value::as_str) {
            let assets = string_array(value.get("assets_ids"));
            {
                let mut connections = self.connections.lock().expect("connections lock");
                let Some(connection) = connections.get_mut(&id) else {
                    return Vec::new();
                };
                match operation {
                    "subscribe" => connection.subscribed.extend(assets.iter().cloned()),
                    "unsubscribe" => {
                        for asset in &assets {
                            connection.subscribed.remove(asset);
                        }
                    }
                    _ => {}
                }
            }
            self.subscription_log
                .lock()
                .expect("subscription log")
                .push(SubscriptionFrame {
                    connection: id,
                    operation: operation.to_owned(),
                    assets: assets.len(),
                    initial: false,
                });
            if operation == "subscribe" {
                return self.book_frames(&assets, false);
            }
        }
        Vec::new()
    }

    fn gamma_response(&self, target: &str) -> (u16, String) {
        let path = target.split('?').next().unwrap_or_default();
        if path != "/markets/keyset" {
            return (404, json!({"error": "not found"}).to_string());
        }
        let query = target.split_once('?').map_or("", |(_, query)| query);
        let mut limit = 100usize;
        let mut offset = 0usize;
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "limit" => limit = value.parse().unwrap_or(100),
                "after_cursor" => offset = value.parse().unwrap_or(0),
                _ => {}
            }
        }
        let markets = self.markets.lock().expect("markets lock");
        let start = offset.min(markets.len());
        let end = start.saturating_add(limit.max(1)).min(markets.len());
        let page: Vec<Value> = markets[start..end]
            .iter()
            .map(MarketSpec::gamma_json)
            .collect();
        let mut body = json!({"markets": page});
        if end < markets.len() {
            body["next_cursor"] = json!(end.to_string());
        }
        (200, body.to_string())
    }
}

struct MockExchange {
    http_addr: SocketAddr,
    ws_addr: SocketAddr,
    state: Arc<MockState>,
    http_task: JoinHandle<()>,
    ws_task: JoinHandle<()>,
}

impl MockExchange {
    async fn start(markets: Vec<MarketSpec>, gate_second: bool) -> Self {
        let state = Arc::new(MockState::new(markets, gate_second));
        let http_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind Gamma");
        let ws_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind websocket");
        let http_addr = http_listener.local_addr().expect("Gamma address");
        let ws_addr = ws_listener.local_addr().expect("websocket address");
        let http_task = tokio::spawn(run_http(
            http_listener,
            Arc::clone(&state),
            state.shutdown.subscribe(),
        ));
        let ws_task = tokio::spawn(run_websocket(
            ws_listener,
            Arc::clone(&state),
            state.shutdown.subscribe(),
        ));
        Self {
            http_addr,
            ws_addr,
            state,
            http_task,
            ws_task,
        }
    }

    fn set_book(&self, token: &str, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        self.state.set_book(token, bids, asks);
    }

    fn book(&self, token: &str) -> MockBook {
        self.state.book(token)
    }

    fn set_markets(&self, markets: Vec<MarketSpec>) {
        self.state.set_markets(markets);
    }

    fn publish_delta(&self, token: &str, side: Side, price: i64, quantity: i64) -> bool {
        self.state.publish_delta(token, side, price, quantity)
    }

    fn frames_sent(&self) -> u64 {
        self.state.frames_sent()
    }

    fn publish_text(&self, text: &str) {
        self.state.publish_raw(RawMockFrame::Text(text.to_owned()));
    }

    fn publish_binary(&self, bytes: &[u8]) {
        self.state.publish_raw(RawMockFrame::Binary(bytes.to_vec()));
    }

    fn publish_ping(&self, payload: &[u8]) {
        self.state.publish_raw(RawMockFrame::Ping(payload.to_vec()));
    }

    fn publish_pong(&self, payload: &[u8]) {
        self.state.publish_raw(RawMockFrame::Pong(payload.to_vec()));
    }

    fn delivered_updates_for(&self, token: &str) -> Vec<MockUpdate> {
        self.state.delivered_updates_for(token)
    }

    fn all_updates_for(&self, token: &str) -> Vec<MockUpdate> {
        self.state.all_updates_for(token)
    }

    fn release_initial(&self, id: u64) {
        self.state.release_initial(id);
    }

    fn close_connection(&self, id: u64) {
        self.state.close_connection(id);
    }

    fn close_all(&self) {
        self.state.close_all();
    }

    fn set_reject_connections(&self, reject: bool) {
        self.state.set_reject_connections(reject);
    }

    fn set_suppress_deltas(&self, suppress: bool) {
        self.state.set_suppress_deltas(suppress);
    }

    fn next_id(&self) -> u64 {
        self.state.next_id()
    }

    fn total_subscriptions(&self) -> usize {
        self.state.total_subscriptions()
    }

    fn max_subscriptions_per_connection(&self) -> usize {
        self.state.max_subscriptions_per_connection()
    }

    fn subscription_log(&self) -> Vec<SubscriptionFrame> {
        self.state
            .subscription_log
            .lock()
            .expect("subscription log")
            .clone()
    }

    async fn wait_for_connections(&self, count: usize) {
        wait_until(&format!("{count} websocket connections"), || {
            self.state.connection_count() >= count
        })
        .await;
    }

    async fn wait_for_subscriptions(&self, token: &str, count: usize) {
        wait_until(&format!("{count} subscriptions to {token}"), || {
            self.state.subscription_count(token) >= count
        })
        .await;
    }

    async fn wait_for_total_subscriptions(&self, count: usize) {
        wait_until(&format!("{count} total subscriptions"), || {
            self.state.total_subscriptions() >= count
        })
        .await;
    }

    async fn wait_for_new_connection(&self, previous_next_id: u64) {
        wait_until_for(
            Duration::from_secs(20),
            "a reconnected websocket",
            &mut || {
                self.state.next_id() > previous_next_id
                    && self
                        .state
                        .max_connection_id()
                        .is_some_and(|id| id >= previous_next_id)
            },
        )
        .await;
    }

    async fn wait_for_new_connections(&self, previous_next_id: u64, count: usize) {
        wait_until_for(
            Duration::from_secs(20),
            &format!("{count} reconnected websockets"),
            &mut || {
                self.state.next_id() >= previous_next_id.saturating_add(count as u64)
                    && self.state.connection_count() >= count
            },
        )
        .await;
    }

    /// Block until the connection set holds steady: `count` live connections
    /// and no replacement for 400ms. Publishing a frame while a replacement
    /// is landing would write it into a doomed socket, so payload-publishing
    /// tests must wait for stability first.
    async fn wait_for_stable_connections(&self, count: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut stable_since: Option<std::time::Instant> = None;
        let mut last_id = self.state.next_id();
        while std::time::Instant::now() < deadline {
            sleep(Duration::from_millis(50)).await;
            let id = self.state.next_id();
            if id != last_id {
                last_id = id;
                stable_since = None;
                continue;
            }
            if self.state.connection_count() < count || self.state.total_subscriptions() < count {
                stable_since = None;
                continue;
            }
            let since = stable_since.get_or_insert(std::time::Instant::now());
            if since.elapsed() >= Duration::from_millis(400) {
                return;
            }
        }
        panic!("collector connections never stabilized");
    }
}

impl Drop for MockExchange {
    fn drop(&mut self) {
        let _ = self.state.shutdown.send(());
        self.http_task.abort();
        self.ws_task.abort();
    }
}

async fn run_http(
    listener: TcpListener,
    state: Arc<MockState>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => {
                let Ok((mut socket, _)) = accepted else { continue };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buffer = [0u8; 4096];
                    loop {
                        match socket.read(&mut buffer).await {
                            Ok(0) => break,
                            Ok(read) => {
                                request.extend_from_slice(&buffer[..read]);
                                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => return,
                        }
                        if request.len() > 64 * 1024 {
                            return;
                        }
                    }
                    let text = String::from_utf8_lossy(&request);
                    let target = text
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let (status, body) = state.gamma_response(target);
                    let reason = if status == 200 { "OK" } else { "Not Found" };
                    let head = format!(
                        "HTTP/1.1 {status} {reason}\r\n\
                         Content-Type: application/json\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\
                         \r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(body.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        }
    }
}

async fn run_websocket(
    listener: TcpListener,
    state: Arc<MockState>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else { continue };
                if state.reject_connections.load(Ordering::SeqCst) {
                    drop(socket);
                    continue;
                }
                let id = state.next_id.fetch_add(1, Ordering::SeqCst);
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    handle_connection(id, socket, state).await;
                });
            }
        }
    }
}

async fn handle_connection(id: u64, socket: TcpStream, state: Arc<MockState>) {
    let Ok(websocket) = accept_async(socket).await else {
        return;
    };
    let (mut sink, mut stream) = websocket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    let (close_tx, mut close_rx) = oneshot::channel();
    state.register(id, outbound_tx, close_tx);
    let mut shutdown = state.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = &mut close_rx => break,
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let outgoing = state.handle_client_message(id, text.as_str()).await;
                        for frame in outgoing {
                            if sink.send(Message::Text(frame.into())).await.is_err() {
                                state.unregister(id);
                                return;
                            }
                            state.frames_sent.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(Outbound::Text(frame)) => {
                        if sink.send(Message::Text(frame.into())).await.is_err() {
                            break;
                        }
                        state.frames_sent.fetch_add(1, Ordering::SeqCst);
                    }
                    Some(Outbound::Binary(bytes)) => {
                        if sink.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                        state.frames_sent.fetch_add(1, Ordering::SeqCst);
                    }
                    Some(Outbound::Ping(bytes)) => {
                        if sink.send(Message::Ping(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outbound::Pong(bytes)) => {
                        if sink.send(Message::Pong(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outbound::Close) | None => break,
                }
            }
        }
    }
    let _ = sink.close().await;
    state.unregister(id);
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn mock_timestamp_ms() -> i64 {
    now_us().unwrap_or(0) / 1_000
}

fn delta_json(token: &str, side: Side, price: i64, quantity: i64) -> String {
    json!({
        "event_type": "price_change",
        "market": "mock",
        "timestamp": mock_timestamp_ms().to_string(),
        "price_changes": [{
            "asset_id": token,
            "price": format_micros(price),
            "size": format_micros(quantity),
            "side": match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            },
            "hash": "mock",
        }],
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Capture reader and assertions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum CapturedMessage {
    Snapshot {
        symbol: String,
        received_at_us: i64,
        book: MockBook,
    },
    Delta {
        symbol: String,
        received_at_us: i64,
        side: Side,
        price: i64,
        quantity: i64,
    },
}

impl CapturedMessage {
    fn symbol(&self) -> &str {
        match self {
            Self::Snapshot { symbol, .. } | Self::Delta { symbol, .. } => symbol,
        }
    }

    fn received_at_us(&self) -> i64 {
        match self {
            Self::Snapshot { received_at_us, .. } | Self::Delta { received_at_us, .. } => {
                *received_at_us
            }
        }
    }

    fn delta_fields(&self) -> Option<(Side, i64, i64)> {
        match self {
            Self::Delta {
                side,
                price,
                quantity,
                ..
            } => Some((*side, *price, *quantity)),
            Self::Snapshot { .. } => None,
        }
    }
}

fn read_capture(path: &Path) -> Vec<CapturedMessage> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(path, CHUNK_SIZE)
        .expect("opening capture");
    let mut messages = Vec::new();
    while let Some(message) = reader.read_msg().expect("decoding capture message") {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot =
                    SnapshotDomain::try_from_decoder(decoder).expect("decoding SBE snapshot");
                messages.push(CapturedMessage::Snapshot {
                    symbol: snapshot.symbol.clone(),
                    received_at_us: snapshot.received_at.timestamp_micros(),
                    book: MockBook::from_snapshot(&snapshot),
                });
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)
                    .expect("decoding SBE price-level update");
                messages.push(CapturedMessage::Delta {
                    symbol: update.symbol.clone(),
                    received_at_us: update.backtest_timestamp.timestamp_micros(),
                    side: update.side,
                    price: update.price_level,
                    quantity: update.quantity,
                });
            }
            _ => panic!("unexpected SBE message variant"),
        }
    }
    messages
}

fn verify_capture(path: &Path) -> VerifySummary {
    verify::run(path, CHUNK_STR).expect("running verify")
}

fn first_snapshot<'a>(capture: &'a [CapturedMessage], symbol: &str) -> &'a MockBook {
    capture
        .iter()
        .find_map(|message| match message {
            CapturedMessage::Snapshot {
                symbol: found,
                book,
                ..
            } if found == symbol => Some(book),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no snapshot for {symbol}"))
}

fn last_snapshot<'a>(capture: &'a [CapturedMessage], symbol: &str) -> &'a MockBook {
    capture
        .iter()
        .rev()
        .find_map(|message| match message {
            CapturedMessage::Snapshot {
                symbol: found,
                book,
                ..
            } if found == symbol => Some(book),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no snapshot for {symbol}"))
}

fn reconstructed_book(capture: &[CapturedMessage], symbol: &str) -> MockBook {
    let mut book = MockBook::default();
    for message in capture {
        match message {
            CapturedMessage::Snapshot {
                symbol: found,
                book: snapshot,
                ..
            } if found == symbol => book = snapshot.clone(),
            CapturedMessage::Delta {
                symbol: found,
                side,
                price,
                quantity,
                ..
            } if found == symbol => book.set(*side, *price, *quantity),
            _ => {}
        }
    }
    book
}

fn captured_deltas<'a>(capture: &'a [CapturedMessage], symbol: &str) -> Vec<&'a CapturedMessage> {
    capture
        .iter()
        .filter(|message| matches!(message, CapturedMessage::Delta { symbol: found, .. } if found == symbol))
        .collect()
}

fn assert_deltas_match_updates(capture: &[CapturedMessage], updates: &[MockUpdate], symbol: &str) {
    let deltas = captured_deltas(capture, symbol);
    assert_eq!(
        deltas.len(),
        updates.len(),
        "captured delta count for {symbol}"
    );
    for (message, update) in deltas.into_iter().zip(updates) {
        let (side, price, quantity) = message.delta_fields().expect("filtered to deltas");
        assert_eq!(side, update.side, "side for {symbol}");
        assert_eq!(price, update.price, "price for {symbol}");
        assert_eq!(quantity, update.quantity, "quantity for {symbol}");
    }
}

fn recovery_snapshot<'a>(capture: &'a [CapturedMessage], symbol: &str) -> &'a MockBook {
    let mut saw_delta = false;
    for message in capture {
        match message {
            CapturedMessage::Delta { symbol: found, .. } if found == symbol => saw_delta = true,
            CapturedMessage::Snapshot {
                symbol: found,
                book,
                ..
            } if found == symbol && saw_delta => return book,
            _ => {}
        }
    }
    panic!("no recovery snapshot after deltas for {symbol}");
}

fn snapshot_at_or_after_matching(
    capture: &[CapturedMessage],
    symbol: &str,
    received_at_us: i64,
    expected: &MockBook,
) {
    let found = capture.iter().any(|message| {
        matches!(
            message,
            CapturedMessage::Snapshot {
                symbol: found,
                received_at_us: at,
                book,
            } if found == symbol && *at >= received_at_us && book == expected
        )
    });
    assert!(
        found,
        "no snapshot for {symbol} at or after {received_at_us} matched the expected recovery truth {expected:?}"
    );
}

fn assert_per_symbol_anchor(capture: &[CapturedMessage], symbol: &str) {
    let mut seen_snapshot = false;
    for message in capture.iter().filter(|message| message.symbol() == symbol) {
        match message {
            CapturedMessage::Snapshot { .. } => seen_snapshot = true,
            CapturedMessage::Delta { .. } => {
                assert!(
                    seen_snapshot,
                    "delta for {symbol} appeared before its first snapshot"
                );
            }
        }
    }
}

fn assert_delta_receive_times_monotonic(capture: &[CapturedMessage], symbol: &str) {
    let mut previous_received_at = i64::MIN;
    for message in capture {
        if message.symbol() != symbol || message.delta_fields().is_none() {
            continue;
        }
        let received_at = message.received_at_us();
        assert!(
            received_at >= previous_received_at,
            "delta receive times for {symbol} regressed from {previous_received_at} to {received_at}"
        );
        previous_received_at = received_at;
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steady_state_history_matches_mock_exchange_truth() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("steady.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let initial = mock.book("asset-a");
    let settings = test_settings(&mock, &output);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;

    for quantity in [11, 12, 13, 14, 15] {
        assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, quantity * 1_000_000));
        sleep(Duration::from_millis(20)).await;
    }
    for quantity in [6, 7] {
        assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, quantity * 1_000_000));
        sleep(Duration::from_millis(20)).await;
    }
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let updates = mock.delivered_updates_for("asset-a");
    assert_eq!(updates.len(), 7);
    assert_eq!(first_snapshot(&capture, SYMBOL_A), &initial);
    assert_deltas_match_updates(&capture, &updates, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    assert_delta_receive_times_monotonic(&capture, SYMBOL_A);

    let final_truth = mock.book("asset-a");
    assert_eq!(reconstructed_book(&capture, SYMBOL_A), final_truth);
    assert_eq!(last_snapshot(&capture, SYMBOL_A), &final_truth);

    // Compare each intermediate captured state with the mock's state after the
    // corresponding delivered update, not only the final book.
    let mut running = initial;
    for (index, update) in updates.iter().enumerate() {
        running.set(update.side, update.price, update.quantity);
        assert_eq!(running, update.resulting, "mock truth after update {index}");
    }

    let summary = verify_capture(&output);
    assert!(
        summary.valid,
        "verify rejected a clean capture: {summary:?}"
    );
    assert_eq!(summary.deltas_before_snapshot, 0);
    assert_eq!(summary.snapshot_mismatches, 0);
    assert_eq!(report.core.deltas_emitted, 7);
    assert_eq!(report.core.final_snapshots, 1);
    assert_eq!(report.writer.messages as usize, capture.len());
    assert_eq!(report.shutdown_reason, "test scenario complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_socket_loss_fails_over_and_emits_anchoring_snapshot() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("socket-loss.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], true).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    // Connection 0 is the active source. Connection 1 is gated so it cannot
    // win the initial snapshot race, then receives a snapshot after batch 1.
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 12_000_000));
    sleep(Duration::from_millis(100)).await;
    mock.release_initial(1);
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 13_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 14_000_000));
    sleep(Duration::from_millis(200)).await;

    let truth_at_loss = mock.book("asset-a");
    let previous_next_id = mock.next_id();
    mock.close_connection(0);
    // The surviving replica must reconnect with a new connection epoch,
    // resubscribe, and deliver a fresh exchange snapshot. The coordinator may
    // only promote it after acknowledging the pending recovery generation.
    mock.wait_for_new_connection(previous_next_id).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    wait_for_metric_at_least(metrics_address, "collector_failover_snapshots_total", 1.0).await;
    wait_for_no_resyncing_symbols(metrics_address).await;
    wait_for_metric_at_least(metrics_address, "collector_symbols_anchored", 1.0).await;
    sleep(Duration::from_millis(100)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 15_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let updates = mock.delivered_updates_for("asset-a");

    assert_eq!(updates.len(), 6);
    assert_deltas_match_updates(&capture, &updates, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    assert_delta_receive_times_monotonic(&capture, SYMBOL_A);
    assert_eq!(
        recovery_snapshot(&capture, SYMBOL_A),
        &truth_at_loss,
        "failover snapshot must contain the shadow's current truth"
    );
    assert_eq!(reconstructed_book(&capture, SYMBOL_A), mock.book("asset-a"));
    let summary = verify_capture(&output);
    assert!(summary.valid, "verify rejected clean failover: {summary:?}");
    assert_eq!(report.core.failover_snapshots, 1);
    assert_eq!(report.core.source_switches, 1);
    assert_eq!(report.core.discontinuity_incidents, 1);
    assert_eq!(report.core.unquantifiable_gaps, 1);
    assert_eq!(report.core.deltas_emitted, 6);
    assert!(
        report.sources.iter().any(|source| source.reconnects >= 1),
        "the recovery path must reconnect at least one source"
    );

    let delta_positions: Vec<usize> = capture
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            matches!(message, CapturedMessage::Delta { symbol, .. } if symbol == SYMBOL_A)
                .then_some(index)
        })
        .collect();
    assert_eq!(delta_positions.len(), 6);
    let replacement_snapshots = capture[delta_positions[3] + 1..delta_positions[4]]
        .iter()
        .filter(|message| {
            matches!(message, CapturedMessage::Snapshot { symbol, .. } if symbol == SYMBOL_A)
        })
        .count();
    assert_eq!(
        replacement_snapshots, 1,
        "exactly one fresh snapshot may replace the lost active source"
    );
    assert!(
        mock.subscription_log().iter().any(|frame| {
            frame.connection >= previous_next_id
                && frame.operation == "initial_dump"
                && frame.assets > 0
        }),
        "the reconnected source must resubscribe before recovering"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_outage_recovery_exposes_the_uncovered_interval() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("outage.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(200)).await;
    let pre_outage = mock.book("asset-a");

    let previous_next_id = mock.next_id();
    mock.set_suppress_deltas(true);
    mock.set_reject_connections(true);
    mock.close_all();
    sleep(Duration::from_millis(200)).await;

    // These updates occur during the uncovered interval. They change the
    // authoritative book but are not delivered.
    assert!(!mock.publish_delta("asset-a", Side::Buy, 400_000, 20_000_000));
    assert!(!mock.publish_delta("asset-a", Side::Sell, 600_000, 1_000_000));
    let truth_after_outage = mock.book("asset-a");
    assert_ne!(pre_outage, truth_after_outage);

    sleep(Duration::from_millis(700)).await;
    let recovery_started_us = now_us().expect("wall clock");
    mock.set_reject_connections(false);
    mock.set_suppress_deltas(false);
    mock.wait_for_new_connections(previous_next_id, 2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    // Recovery is only complete after a fresh exchange snapshot has been
    // accepted against the acknowledged recovery fence and continuity is Live.
    wait_for_metric_at_least(metrics_address, "collector_failover_snapshots_total", 1.0).await;
    wait_for_no_resyncing_symbols(metrics_address).await;
    wait_for_metric_at_least(metrics_address, "collector_symbols_anchored", 1.0).await;
    sleep(Duration::from_millis(100)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 21_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let delivered = mock.delivered_updates_for("asset-a");
    let all = mock.all_updates_for("asset-a");

    assert_eq!(delivered.len(), 3);
    assert_eq!(all.len(), 5);
    assert_deltas_match_updates(&capture, &delivered, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    snapshot_at_or_after_matching(&capture, SYMBOL_A, recovery_started_us, &truth_after_outage);
    assert_eq!(reconstructed_book(&capture, SYMBOL_A), mock.book("asset-a"));

    // The missed interval is not silently represented as complete: verify
    // reports the pre-outage deltas disagreeing with the recovery snapshot.
    let summary = verify_capture(&output);
    assert!(
        !summary.valid,
        "a capture with an uncovered interval must not verify as complete"
    );
    assert!(summary.snapshot_mismatches >= 1);
    assert_eq!(summary.mismatch_samples[0]["symbol"], "polymarket:cond-a:0");
    assert!(report.core.failover_snapshots >= 1);
    assert_eq!(report.core.deltas_emitted, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_churn_retires_and_adds_symbols() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("churn.sbe");
    let a = market("cond-a", "asset-a");
    let b = market("cond-b", "asset-b");
    let c = market("cond-c", "asset-c");
    let mock = MockExchange::start(vec![a.clone(), b.clone()], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    mock.set_book("asset-b", &[(410_000, 8_000_000)], &[(590_000, 7_000_000)]);
    mock.set_book("asset-c", &[(420_000, 6_000_000)], &[(580_000, 9_000_000)]);
    let mut settings = test_settings(&mock, &output);
    settings.collector.discovery_interval_secs = 1;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    mock.wait_for_subscriptions("asset-b", 2).await;
    sleep(Duration::from_millis(300)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    assert!(mock.publish_delta("asset-b", Side::Sell, 590_000, 6_000_000));
    sleep(Duration::from_millis(300)).await;

    mock.set_markets(vec![a, c]);
    mock.wait_for_subscriptions("asset-c", 2).await;
    sleep(Duration::from_millis(400)).await;
    assert!(mock.publish_delta("asset-c", Side::Buy, 420_000, 7_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);

    let b_messages: Vec<&CapturedMessage> = capture
        .iter()
        .filter(|message| message.symbol() == "polymarket:cond-b:0")
        .collect();
    let last_b = b_messages.last().expect("asset-b history");
    let CapturedMessage::Snapshot { book, .. } = last_b else {
        panic!("asset-b must end with a retirement snapshot");
    };
    assert!(book.is_empty(), "retirement snapshot must be empty");
    let retirement = b_messages
        .iter()
        .position(|message| {
            matches!(
                message,
                CapturedMessage::Snapshot { book, .. } if book.is_empty()
            )
        })
        .expect("asset-b must have an empty retirement snapshot");
    assert!(
        b_messages[retirement..]
            .iter()
            .all(|message| matches!(message, CapturedMessage::Snapshot { .. })),
        "no delta may follow asset-b's retirement boundary"
    );

    assert_eq!(
        first_snapshot(&capture, "polymarket:cond-c:0"),
        &MockBook {
            bids: [(420_000, 6_000_000)].into(),
            asks: [(580_000, 9_000_000)].into(),
        }
    );
    assert_deltas_match_updates(
        &capture,
        &mock.delivered_updates_for("asset-c"),
        "polymarket:cond-c:0",
    );
    assert_eq!(
        reconstructed_book(&capture, "polymarket:cond-a:0"),
        mock.book("asset-a")
    );
    assert_eq!(
        reconstructed_book(&capture, "polymarket:cond-c:0"),
        mock.book("asset-c")
    );
    assert_eq!(
        reconstructed_book(&capture, "polymarket:cond-b:0"),
        MockBook::default()
    );

    let summary = verify_capture(&output);
    assert!(
        summary.valid,
        "retirement snapshot must not be a verify mismatch: {summary:?}"
    );
    assert!(summary.empty_snapshots >= 1);
    assert_eq!(report.core.removed_symbols, 1);
    assert_eq!(report.core.added_symbols, 3);
    assert!(report.core.discovery_rounds >= 2);
    assert_eq!(report.final_symbols, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_subscription_is_detected_and_repaired() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("silent.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    settings.collector.stale_probe_after_secs = 10;
    settings.collector.probe_interval_secs = 1;
    settings.collector.max_probes = 1;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(200)).await;
    let quiet_truth = mock.book("asset-a");

    let connections_before = mock.next_id();
    mock.set_suppress_deltas(true);
    let quiet_started = tokio::time::Instant::now();
    // The stale probe must repair the quiet symbol by cycling its
    // subscription on the live connection. No new websocket may be opened.
    wait_until_for(
        Duration::from_secs(20),
        "a quiet-symbol resubscribe",
        &mut || {
            mock.subscription_log().iter().any(|frame| {
                frame.operation == "unsubscribe" && frame.connection < connections_before
            })
        },
    )
    .await;
    assert!(
        quiet_started.elapsed() >= Duration::from_secs(9),
        "resubscribe happened before the stale-probe threshold"
    );
    assert_eq!(
        mock.next_id(),
        connections_before,
        "the repair must stay on the existing connection"
    );
    mock.set_suppress_deltas(false);
    sleep(Duration::from_millis(300)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 12_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let updates = mock.delivered_updates_for("asset-a");

    assert_deltas_match_updates(&capture, &updates, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    assert_delta_receive_times_monotonic(&capture, SYMBOL_A);
    assert_eq!(
        recovery_snapshot(&capture, SYMBOL_A),
        &quiet_truth,
        "repair snapshot must contain the unchanged truth"
    );
    let summary = verify_capture(&output);
    assert!(
        summary.valid,
        "silent repair should leave a self-consistent capture: {summary:?}"
    );
    assert!(report.core.stale_probes >= 1);
    // The repair stays on the live connection: the probe cycles the
    // subscription, so no source switch, failover, or reconnect is involved.
    assert_eq!(report.core.source_switches, 0);
    assert_eq!(report.core.failover_snapshots, 0);
    assert!(report.sources.iter().all(|source| source.reconnects == 0));
    let log = mock.subscription_log();
    assert!(
        log.iter()
            .any(|frame| frame.operation == "subscribe" && !frame.initial),
        "the resubscribe must re-add the asset"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_control_missing_update_is_detected_by_verify() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("negative.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(200)).await;
    let captured_truth = mock.book("asset-a");

    let previous_next_id = mock.next_id();
    mock.set_suppress_deltas(true);
    assert!(!mock.publish_delta("asset-a", Side::Buy, 400_000, 20_000_000));
    assert!(!mock.publish_delta("asset-a", Side::Sell, 600_000, 1_000_000));
    let authoritative_truth = mock.book("asset-a");
    assert_ne!(captured_truth, authoritative_truth);
    let recovery_started_us = now_us().expect("wall clock");
    sleep(Duration::from_millis(200)).await;

    // Force a fresh exchange snapshot. The collector's capture is
    // self-consistent before this point but is now shown to be incomplete.
    mock.close_all();
    mock.wait_for_new_connections(previous_next_id, 2).await;
    mock.set_suppress_deltas(false);
    mock.wait_for_subscriptions("asset-a", 2).await;
    wait_for_metric_at_least(metrics_address, "collector_failover_snapshots_total", 1.0).await;
    wait_for_no_resyncing_symbols(metrics_address).await;
    wait_for_metric_at_least(metrics_address, "collector_symbols_anchored", 1.0).await;
    sleep(Duration::from_millis(100)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 21_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let delivered = mock.delivered_updates_for("asset-a");

    assert_deltas_match_updates(&capture, &delivered, SYMBOL_A);
    snapshot_at_or_after_matching(
        &capture,
        SYMBOL_A,
        recovery_started_us,
        &authoritative_truth,
    );
    let summary = verify_capture(&output);
    assert!(
        !summary.valid,
        "negative control must fail verification when the missing update is exposed"
    );
    assert!(summary.snapshot_mismatches >= 1);
    assert_eq!(summary.mismatch_samples[0]["symbol"], "polymarket:cond-a:0");
    assert_eq!(report.core.deltas_emitted, delivered.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_subscription_scale_across_shards() {
    const SYMBOLS: usize = 1_000;
    const SHARDS: usize = 4;
    const REPLICAS: usize = 2;

    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("scale.sbe");
    let markets: Vec<MarketSpec> = (0..SYMBOLS)
        .map(|index| market(&format!("cond-{index:03}"), &format!("asset-{index:03}")))
        .collect();
    let mock = MockExchange::start(markets, false).await;
    for index in 0..SYMBOLS {
        mock.set_book(
            &format!("asset-{index:03}"),
            &[(400_000, 10_000_000)],
            &[(600_000, 5_000_000)],
        );
    }
    let mut settings = test_settings(&mock, &output);
    settings.collector.shards = SHARDS;
    settings.collector.replicas = REPLICAS;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    let connection_count = SHARDS * REPLICAS;
    let subscription_count = SYMBOLS * REPLICAS;
    mock.wait_for_connections(connection_count).await;
    mock.wait_for_total_subscriptions(subscription_count).await;
    assert_eq!(mock.total_subscriptions(), subscription_count);
    let max_subscriptions = mock.max_subscriptions_per_connection();
    sleep(Duration::from_secs(1)).await;

    for index in 0..SYMBOLS {
        let token = format!("asset-{index:03}");
        assert!(mock.publish_delta(&token, Side::Buy, 400_000, (11 + index as i64) * 1_000_000));
    }
    sleep(Duration::from_secs(1)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);

    assert_eq!(report.shard_count, SHARDS);
    assert_eq!(report.replica_count, REPLICAS);
    assert_eq!(report.discovered_symbols, SYMBOLS);
    assert_eq!(report.final_symbols, SYMBOLS);
    assert_eq!(report.core.deltas_emitted, SYMBOLS as u64);
    assert_eq!(
        max_subscriptions,
        SYMBOLS / SHARDS,
        "each shard replica should carry one shard's symbols"
    );

    for index in 0..SYMBOLS {
        let token = format!("asset-{index:03}");
        let symbol = format!("polymarket:cond-{index:03}:0");
        assert_eq!(
            reconstructed_book(&capture, &symbol),
            mock.book(&token),
            "final book for {symbol}"
        );
        assert_per_symbol_anchor(&capture, &symbol);
        assert_deltas_match_updates(&capture, &mock.delivered_updates_for(&token), &symbol);
    }

    let summary = verify_capture(&output);
    assert!(
        summary.valid,
        "scale capture must be self-consistent: {summary:?}"
    );
    assert_eq!(summary.symbols, SYMBOLS);

    let subscription_log = mock.subscription_log();
    for frame in &subscription_log {
        assert!(
            (frame.connection as usize) < connection_count,
            "subscription frame came from an unknown connection: {frame:?}"
        );
        assert!(
            matches!(
                frame.operation.as_str(),
                "initial_dump" | "subscribe" | "unsubscribe"
            ),
            "unexpected subscription operation: {frame:?}"
        );
    }
    let initial_frames: Vec<_> = subscription_log
        .into_iter()
        .filter(|frame| frame.initial)
        .collect();
    assert_eq!(initial_frames.len(), connection_count);
    let initial_connections: BTreeSet<u64> = initial_frames
        .iter()
        .map(|frame| frame.connection)
        .collect();
    assert_eq!(initial_connections.len(), connection_count);
    assert!(
        initial_frames
            .iter()
            .all(|frame| frame.assets == SYMBOLS / SHARDS),
        "each initial dump should contain exactly one shard's symbols: {initial_frames:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_produces_a_complete_manifest_and_readable_segments() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("rotated.sbe");
    let mock = MockExchange::start(vec![market("cond-rot", "asset-rot")], false).await;
    mock.set_book(
        "asset-rot",
        &[(400_000, 10_000_000)],
        &[(600_000, 5_000_000)],
    );
    let mut settings = test_settings(&mock, &output);
    settings.output.rotate_bytes = CHUNK_SIZE as u64;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_total_subscriptions(2).await;
    sleep(Duration::from_millis(200)).await;
    for index in 0..400i64 {
        assert!(mock.publish_delta("asset-rot", Side::Buy, 400_000, (11 + index) * 1_000_000));
    }
    sleep(Duration::from_secs(1)).await;
    done.notify_one();
    let report = finish_collector(handle).await;

    let manifest_path = report
        .output_manifest
        .as_ref()
        .expect("rotation must record its manifest");
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(manifest_path).expect("reading manifest"))
            .expect("parsing manifest");
    assert_eq!(manifest["complete"], true);
    assert_eq!(manifest["chunk_size"], CHUNK_SIZE);
    let segments = manifest["segments"].as_array().expect("segment list");
    assert!(
        segments.len() >= 2,
        "expected at least two rotated segments, got {}",
        segments.len()
    );
    assert_eq!(report.writer.segments as usize, segments.len());

    let mut messages = Vec::new();
    for entry in segments {
        let file = entry["file"].as_str().expect("segment file name");
        let path = directory.path().join(file);
        let bytes = std::fs::read(&path).expect("reading segment");
        assert_eq!(bytes.len() as u64, entry["bytes"].as_u64().unwrap());
        assert_eq!(sha256_hex(&bytes), entry["sha256"].as_str().unwrap());
        assert_eq!(bytes.len() % CHUNK_SIZE, 0, "segments stay chunk-aligned");
        messages.extend(read_capture(&path));
    }
    assert_eq!(
        reconstructed_book(&messages, "polymarket:cond-rot:0"),
        mock.book("asset-rot"),
        "reassembling every segment must reproduce the final book"
    );
    for entry in std::fs::read_dir(directory.path()).unwrap().flatten() {
        assert!(
            !entry.file_name().to_string_lossy().ends_with(".partial"),
            "unfinalized segment left behind: {}",
            entry.path().display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_finalizes_and_verifies_on_graceful_shutdown() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("journal.sbe");
    let journal_root = directory.path().join("journal");
    let mock = MockExchange::start(vec![market("cond-journal", "asset-journal")], false).await;
    mock.set_book(
        "asset-journal",
        &[(400_000, 10_000_000)],
        &[(600_000, 5_000_000)],
    );
    let mut settings = test_settings(&mock, &output);
    settings.journal.directory = Some(journal_root.clone());
    settings.journal.segment_max_bytes = 4096;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_total_subscriptions(2).await;
    sleep(Duration::from_millis(200)).await;
    for index in 0..120i64 {
        assert!(mock.publish_delta(
            "asset-journal",
            Side::Buy,
            400_000,
            (11 + index) * 1_000_000
        ));
    }
    sleep(Duration::from_secs(1)).await;
    done.notify_one();
    let report = finish_collector(handle).await;

    let status = report.journal.expect("journal status in the run report");
    assert_eq!(status.state, journal::RunState::Finalized);
    assert!(status.last_error.is_none());
    assert!(status.counters.finalized >= 120);
    assert!(status.counters.segments >= 2);
    assert_eq!(status.counters.finalized, status.counters.written);

    let run_directory = std::path::PathBuf::from(&status.run_directory);
    let verification = journal::verify_run(&run_directory).expect("verifying the run");
    assert!(verification.complete, "{:?}", verification.errors);
    assert_eq!(verification.record_count, status.counters.finalized);
    assert!(verification.segments.len() >= 2);

    let records = journal::read_records(&run_directory).expect("reading journal records");
    assert_eq!(records.len() as u64, verification.record_count);
    let mut last_seq = BTreeMap::<String, u64>::new();
    let mut price_change_payloads = 0usize;
    for record in records {
        assert_eq!(record.schema_version, journal::JOURNAL_SCHEMA_VERSION);
        assert!(record.received_at_us > 0);
        let previous = last_seq
            .insert(record.source.clone(), record.receive_seq)
            .unwrap_or(0);
        assert!(
            record.receive_seq > previous,
            "sequence must advance for {}",
            record.source
        );
        let text = String::from_utf8(record.payload).expect("Polymarket text payload");
        if text.contains("\"price_change\"") {
            price_change_payloads += 1;
        }
    }
    assert!(price_change_payloads >= 120);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_accounts_for_every_delivered_payload() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("journal-ingress.sbe");
    let journal_root = directory.path().join("journal");
    let mock = MockExchange::start(vec![market("cond-ingress", "asset-ingress")], false).await;
    mock.set_book(
        "asset-ingress",
        &[(410_000, 7_000_000)],
        &[(590_000, 3_000_000)],
    );
    let mut settings = test_settings(&mock, &output);
    settings.journal.directory = Some(journal_root.clone());
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_total_subscriptions(2).await;
    mock.wait_for_stable_connections(2).await;
    let connection_baseline = mock.next_id();
    let baseline = mock.frames_sent();
    for index in 0..10i64 {
        assert!(mock.publish_delta(
            "asset-ingress",
            Side::Buy,
            410_000,
            (21 + index) * 1_000_000
        ));
    }

    // Payloads the adapter accepts but does not act on must still be
    // journaled with an explicit ignored outcome. None of these disconnect.
    let unknown = r#"{"event_type":"future_event","asset_id":"asset-ingress"}"#.to_owned();
    let control = "PONG".to_owned();
    let empty = "   ".to_owned();
    mock.publish_text(&unknown);
    mock.publish_text(&control);
    mock.publish_text(&empty);
    let send_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while mock.frames_sent() <= baseline && std::time::Instant::now() < send_deadline {
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        mock.frames_sent() > baseline,
        "mock did not send the published frames before the deadline"
    );

    // Let the worker drain every delivered frame before finalization. No
    // payload here triggers a reconnect, so the connection generation must be
    // unchanged: that is what makes the strict sent-versus-journaled equality
    // valid.
    sleep(Duration::from_secs(1)).await;
    assert_eq!(
        mock.next_id(),
        connection_baseline,
        "an unexpected reconnect invalidates the exact-count scenario"
    );
    done.notify_one();
    let report = finish_collector(handle).await;

    let status = report.journal.expect("journal status in the run report");
    assert_eq!(status.state, journal::RunState::Finalized);
    assert!(status.last_error.is_none());

    let run_directory = std::path::PathBuf::from(&status.run_directory);
    let verification = journal::verify_run(&run_directory).expect("verifying the run");
    assert!(verification.complete, "{:?}", verification.errors);

    let records = journal::read_records(&run_directory).expect("reading journal records");
    assert_eq!(
        records.len() as u64,
        mock.frames_sent(),
        "every application payload the mock delivered must be journaled exactly once"
    );
    assert_eq!(verification.record_count, status.counters.finalized);

    // Exactly one fence advance per payload: sequences are contiguous from 1
    // within every (source, connection epoch).
    let mut seen: BTreeMap<(String, u64), Vec<u64>> = BTreeMap::new();
    for record in &records {
        seen.entry((record.source.clone(), record.connection_epoch))
            .or_default()
            .push(record.receive_seq);
    }
    assert!(!seen.is_empty());
    for sequences in seen.values() {
        let mut expected: Vec<u64> = (1..=sequences.len() as u64).collect();
        expected.sort_unstable();
        let mut actual = sequences.clone();
        actual.sort_unstable();
        assert_eq!(actual, expected, "sequences must advance exactly once");
    }

    let find = |needle: &[u8]| {
        records
            .iter()
            .find(|record| record.payload.as_slice() == needle)
            .unwrap_or_else(|| panic!("missing journaled payload {needle:?}"))
    };
    assert!(matches!(
        find(unknown.as_bytes()).outcome,
        journal::DecodeOutcome::Ignored { .. }
    ));
    let control_record = find(control.as_bytes());
    assert!(
        matches!(
            control_record.outcome,
            journal::DecodeOutcome::Ignored { ref reason } if reason == "control_frame"
        ),
        "control text must be recorded as ignored: {:?}",
        control_record.outcome
    );
    let empty_record = find(empty.as_bytes());
    assert!(
        matches!(
            empty_record.outcome,
            journal::DecodeOutcome::Ignored { ref reason } if reason == "empty_frame"
        ),
        "an empty frame must be recorded as ignored: {:?}",
        empty_record.outcome
    );

    let price_changes = records
        .iter()
        .filter(|record| {
            serde_json::from_slice::<Value>(&record.payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("event_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some("price_change")
        })
        .count();
    assert_eq!(
        price_changes, 20,
        "both replicas must journal every delivered delta exactly once"
    );
}

/// A payload that decoding rejects is journaled with its exact bytes before
/// the reconnect it triggers, and the receive fence advances exactly once per
/// read payload across connection generations. A frame the mock writes at the
/// moment a replica tears its socket down is a transport loss, not a journal
/// accounting failure, so this scenario asserts per-payload guarantees rather
/// than a strict sent-versus-journaled count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_payloads_are_journaled_across_reconnects() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("journal-reject.sbe");
    let journal_root = directory.path().join("journal");
    let mock = MockExchange::start(vec![market("cond-reject", "asset-reject")], false).await;
    mock.set_book(
        "asset-reject",
        &[(420_000, 6_000_000)],
        &[(580_000, 4_000_000)],
    );
    let mut settings = test_settings(&mock, &output);
    settings.journal.directory = Some(journal_root.clone());
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_total_subscriptions(2).await;
    sleep(Duration::from_millis(200)).await;

    // A malformed payload is journaled as rejected before the reconnect it
    // triggers.
    let malformed = "{not json".to_owned();
    mock.publish_text(&malformed);
    let first_generation = mock.next_id();
    mock.wait_for_new_connections(first_generation, 2).await;
    mock.wait_for_total_subscriptions(2).await;
    sleep(Duration::from_millis(300)).await;

    // A binary payload is rejected by the UTF-8 decode boundary.
    let binary = vec![0u8, 159, 146, 150, 255];
    mock.publish_binary(&binary);
    let second_generation = mock.next_id();
    mock.wait_for_new_connections(second_generation, 2).await;
    mock.wait_for_total_subscriptions(2).await;
    sleep(Duration::from_millis(300)).await;

    // Deltas on the settled final generation: every one of them must be
    // journaled by both replicas.
    mock.wait_for_stable_connections(2).await;
    for index in 0..5i64 {
        assert!(mock.publish_delta("asset-reject", Side::Buy, 420_000, (31 + index) * 1_000_000));
    }

    // Let the worker drain the settled generation before finalization.
    sleep(Duration::from_secs(1)).await;

    done.notify_one();
    let report = finish_collector(handle).await;

    let status = report.journal.expect("journal status in the run report");
    assert_eq!(status.state, journal::RunState::Finalized);
    let run_directory = std::path::PathBuf::from(&status.run_directory);
    let verification = journal::verify_run(&run_directory).expect("verifying the run");
    assert!(verification.complete, "{:?}", verification.errors);
    let records = journal::read_records(&run_directory).expect("reading journal records");

    let malformed_records: Vec<_> = records
        .iter()
        .filter(|record| record.payload.as_slice() == malformed.as_bytes())
        .collect();
    assert!(
        !malformed_records.is_empty(),
        "the malformed payload must be journaled"
    );
    for record in &malformed_records {
        assert!(matches!(
            record.outcome,
            journal::DecodeOutcome::Rejected { .. }
        ));
    }
    let binary_records: Vec<_> = records
        .iter()
        .filter(|record| record.payload == binary)
        .collect();
    assert!(
        !binary_records.is_empty(),
        "the binary payload must be journaled"
    );
    for record in &binary_records {
        assert_eq!(record.frame_kind, journal::FrameKind::Binary);
        assert!(matches!(
            record.outcome,
            journal::DecodeOutcome::Rejected { .. }
        ));
    }

    // Every payload that was read advanced the fence exactly once, across
    // every connection generation.
    let mut seen: BTreeMap<(String, u64), Vec<u64>> = BTreeMap::new();
    for record in &records {
        seen.entry((record.source.clone(), record.connection_epoch))
            .or_default()
            .push(record.receive_seq);
    }
    assert!(
        seen.iter().any(|((_, epoch), _)| *epoch > 1),
        "a reconnect generation must appear in the journal"
    );
    for sequences in seen.values() {
        let mut expected: Vec<u64> = (1..=sequences.len() as u64).collect();
        expected.sort_unstable();
        let mut actual = sequences.clone();
        actual.sort_unstable();
        assert_eq!(actual, expected, "sequences must advance exactly once");
    }

    let price_changes = records
        .iter()
        .filter(|record| {
            serde_json::from_slice::<Value>(&record.payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("event_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some("price_change")
        })
        .count();
    assert_eq!(
        price_changes, 10,
        "both replicas must journal every delta delivered before the reconnects"
    );
}

/// Transport-level ping/pong frames carry no application payload. They must
/// neither create journal records nor consume receive sequence numbers; only
/// the data frames around them advance the fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_control_frames_do_not_advance_the_journal_fence() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("journal-control.sbe");
    let journal_root = directory.path().join("journal");
    let mock = MockExchange::start(vec![market("cond-control", "asset-control")], false).await;
    mock.set_book(
        "asset-control",
        &[(400_000, 8_000_000)],
        &[(600_000, 2_000_000)],
    );
    let mut settings = test_settings(&mock, &output);
    settings.journal.directory = Some(journal_root.clone());
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_total_subscriptions(2).await;
    mock.wait_for_stable_connections(2).await;
    let connection_baseline = mock.next_id();
    let data_baseline = mock.frames_sent();

    // Real websocket control frames, delivered between application payloads.
    for _ in 0..4 {
        mock.publish_ping(b"journal-ping");
        mock.publish_pong(b"journal-pong");
    }
    sleep(Duration::from_millis(300)).await;
    for index in 0..6i64 {
        assert!(mock.publish_delta(
            "asset-control",
            Side::Buy,
            400_000,
            (41 + index) * 1_000_000
        ));
    }
    let expected_data_frames = data_baseline + 12;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while mock.frames_sent() < expected_data_frames && std::time::Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.frames_sent(),
        expected_data_frames,
        "control frames must not count as application payloads"
    );

    sleep(Duration::from_secs(1)).await;
    assert_eq!(
        mock.next_id(),
        connection_baseline,
        "transport controls must not force a reconnect"
    );
    done.notify_one();
    let report = finish_collector(handle).await;

    let status = report.journal.expect("journal status in the run report");
    assert_eq!(status.state, journal::RunState::Finalized);
    let run_directory = std::path::PathBuf::from(&status.run_directory);
    let verification = journal::verify_run(&run_directory).expect("verifying the run");
    assert!(verification.complete, "{:?}", verification.errors);
    let records = journal::read_records(&run_directory).expect("reading journal records");
    assert_eq!(
        records.len() as u64,
        mock.frames_sent(),
        "only application payloads may be journaled"
    );

    let mut seen: BTreeMap<(String, u64), Vec<u64>> = BTreeMap::new();
    for record in &records {
        seen.entry((record.source.clone(), record.connection_epoch))
            .or_default()
            .push(record.receive_seq);
    }
    assert!(!seen.is_empty());
    for sequences in seen.values() {
        let mut expected: Vec<u64> = (1..=sequences.len() as u64).collect();
        expected.sort_unstable();
        let mut actual = sequences.clone();
        actual.sort_unstable();
        assert_eq!(
            actual, expected,
            "idle control frames must not leave sequence gaps"
        );
    }
}

/// Opt-in scale harness. Run one size at a time, for example:
/// `SCALE_SYMBOLS=5000 SCALE_REPORT=measurements/scale-5000.json \
///  cargo test --release --test e2e_mock_exchange opt_in_scale_run -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in scale run; set SCALE_SYMBOLS and use --ignored"]
async fn opt_in_scale_run() {
    let symbols: usize = std::env::var("SCALE_SYMBOLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);
    let shards: usize = std::env::var("SCALE_SHARDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| symbols.div_ceil(5_000).max(1));
    let replicas: usize = std::env::var("SCALE_REPLICAS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2);
    let wait_secs: u64 = std::env::var("SCALE_WAIT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(180);
    let stop_secs: u64 = std::env::var("SCALE_STOP_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(180);
    let settle_secs: u64 = std::env::var("SCALE_SETTLE_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    let report_path = std::env::var("SCALE_REPORT").ok();

    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("scale.sbe");
    let started = std::time::Instant::now();
    let markets: Vec<MarketSpec> = (0..symbols)
        .map(|index| market(&format!("cond-{index:06}"), &format!("asset-{index:06}")))
        .collect();
    let mock = MockExchange::start(markets, false).await;
    for index in 0..symbols {
        mock.set_book(
            &format!("asset-{index:06}"),
            &[(400_000, 10_000_000)],
            &[(600_000, 5_000_000)],
        );
    }
    let mut settings = test_settings(&mock, &output);
    settings.collector.shards = shards;
    settings.collector.replicas = replicas;
    settings.collector.max_symbols_per_socket = 5_000;
    settings.collector.max_shards = 256;
    settings.collector.max_total_connections = 512;
    settings.collector.max_initial_subscription_bytes = 1 << 20;
    settings.collector.max_estimated_book_memory_mb = 4_096;
    settings.collector.max_snapshot_bytes_per_sec = 64 << 20;
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector_with_limit(
        settings,
        Arc::clone(&done),
        Duration::from_secs(stop_secs.saturating_mul(3).max(60)),
    );

    let connection_count = shards * replicas;
    let subscription_count = symbols * replicas;
    let wait = Duration::from_secs(wait_secs);
    wait_until_for(wait, "scale connections", &mut || {
        mock.state.connection_count() >= connection_count
    })
    .await;
    wait_until_for(wait, "scale subscriptions", &mut || {
        mock.state.total_subscriptions() >= subscription_count
    })
    .await;
    let connected_at = started.elapsed();
    let max_per_socket = mock.state.max_subscriptions_per_connection();
    println!(
        "SCALE_PHASE connected symbols={symbols} shards={shards} replicas={replicas} \
         subscriptions={} elapsed_ms={}",
        mock.state.total_subscriptions(),
        connected_at.as_millis()
    );

    // Wait for the collector to process initial snapshots and anchor all symbols.
    // At large scale this can take significantly longer than the subscription handshake.
    let anchor_deadline = std::time::Instant::now() + Duration::from_secs(wait_secs);
    let mut anchored = 0.0;
    loop {
        if let Some(value) = scrape_metric(metrics_address, "collector_symbols_anchored").await {
            anchored = value;
            if anchored >= symbols as f64 {
                break;
            }
        }
        if std::time::Instant::now() >= anchor_deadline {
            panic!("collector anchored {anchored} of {symbols} symbols before the anchor deadline");
        }
        sleep(Duration::from_millis(500)).await;
    }
    println!(
        "SCALE_PHASE anchored symbols={symbols} elapsed_ms={}",
        started.elapsed().as_millis()
    );

    for index in 0..symbols {
        let token = format!("asset-{index:06}");
        assert!(mock.publish_delta(&token, Side::Buy, 400_000, (11 + index as i64) * 1_000_000));
    }
    sleep(Duration::from_secs(2)).await;
    // The published deltas can sit behind a large initial-snapshot backlog.
    // Poll the collector's own counter instead of inferring completion from
    // output-file size, which can be stable while shadow snapshots are being
    // processed without producing output.
    let settle_deadline = std::time::Instant::now() + Duration::from_secs(settle_secs);
    let mut emitted = 0.0;
    loop {
        if let Some(value) = scrape_metric(metrics_address, "collector_deltas_emitted_total").await
        {
            emitted = value;
            if emitted >= symbols as f64 {
                break;
            }
        }
        if std::time::Instant::now() >= settle_deadline {
            panic!("collector emitted {emitted} of {symbols} deltas before the settle deadline");
        }
        sleep(Duration::from_millis(500)).await;
    }
    println!("SCALE_PHASE settled symbols={symbols} emitted={emitted}");
    done.notify_one();
    let report = finish_collector_with_limit(handle, Duration::from_secs(stop_secs)).await;
    let elapsed = started.elapsed();
    println!(
        "SCALE_PHASE stopped symbols={symbols} total_ms={}",
        elapsed.as_millis()
    );

    assert_eq!(report.discovered_symbols, symbols);
    assert_eq!(report.final_symbols, symbols);
    assert_eq!(report.core.deltas_emitted, symbols as u64);
    assert_eq!(max_per_socket, symbols / shards);

    let rss_kb = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let evidence = json!({
        "symbols": symbols,
        "shards": shards,
        "replicas": replicas,
        "connections": connection_count,
        "subscriptions": subscription_count,
        "max_subscriptions_per_socket": max_per_socket,
        "deltas": symbols,
        "output_bytes": report.writer.bytes,
        "output_segments": report.writer.segments,
        "connect_and_subscribe_ms": connected_at.as_millis() as u64,
        "total_ms": elapsed.as_millis() as u64,
        "process_rss_kb": rss_kb,
        "capacity": report.capacity,
    });
    println!("SCALE_RESULT {}", serde_json::to_string(&evidence).unwrap());
    if let Some(path) = report_path {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap())
            .expect("writing scale evidence");
    }
}
