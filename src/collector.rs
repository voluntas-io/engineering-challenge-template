//! Core collection engine.
//!
//! [`Collector`] manages the full lifecycle of the orderbook collection:
//!
//! 1. **Bootstrap**: fetch open markets via exchange REST API, retrying with
//!    backoff until the exchange answers.
//! 2. **WebSocket pool**: maintain `SOCKETS_PER_BUCKET` × `NUM_BUCKETS` sockets,
//!    with markets load-balanced across buckets.
//!    - Each bucket has a **primary** socket (the source of truth) and a
//!      **redundant** shadow socket receiving the same feed (Extension 1).
//! 3. **State**: a shared [`DashMap`] of per-symbol [`Orderbook`] instances.
//! 4. **Output**: a [`Mutex<Output>`] for serialized SBE writes to stdout.
//! 5. **Snapshot timer**: every 60 s emit a full snapshot for every symbol.
//! 6. **Market discovery**: every 30 s poll REST for new/closed markets.
//! 7. **Subscription monitor** (Extension 2):
//!    - Per-symbol: if no update received for `SYMBOL_IDLE_THRESHOLD`, request a
//!      fresh snapshot.
//!    - Per-connection: a `seq` gap requests fresh snapshots for the whole bucket.
//!    - Per-socket: periodic full reconnect every `SOCKET_RECONNECT_INTERVAL`.
//! 8. **Failover** (Extension 1): if the primary socket goes dark or is recycled,
//!    promote the redundant socket, emit the in-memory book, and ignore that
//!    bucket's deltas until a fresh exchange snapshot replaces each book.
//! 9. **Generic adapter** (Extension 3): the collector interacts only with
//!    [`ExchangeAdapter`]. Exchange-specific JSON encoding/decoding is isolated.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio::time;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::Side;

use crate::adapter::{ExchangeAdapter, InboundMessage, SequencePoint};
use crate::kalshi;
use crate::orderbook::Orderbook;
use crate::output::Output;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// Maximum markets per WebSocket connection. Kalshi's docs warn that one socket
/// for all symbols likely won't work; empirically 100–200 is safe.
const MAX_MARKETS_PER_SOCKET: usize = 200;

/// For each bucket we open this many redundant connections (Extension 1).
const SOCKETS_PER_BUCKET: usize = 2;

/// Emit a full snapshot for every symbol at this interval.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);

/// Poll REST for new/closed markets at this interval.
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// A symbol that hasn't sent any update in this window is considered stale.
/// Request a fresh snapshot for a market that has been quiet this long (Extension 2).
const SYMBOL_IDLE_THRESHOLD: Duration = Duration::from_secs(90);

/// Fully reconnect a socket after this interval to clear exchange-side drift (Extension 2).
const SOCKET_RECONNECT_INTERVAL: Duration = Duration::from_secs(600);

/// Minimum time between snapshot requests for the same market.
const SNAPSHOT_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);

/// First and maximum delays between attempts at the initial market discovery.
const INITIAL_DISCOVERY_RETRY: Duration = Duration::from_secs(1);
const MAX_INITIAL_DISCOVERY_RETRY: Duration = Duration::from_secs(30);

// ── Message sent from a WebSocket task back to the collector ──────────────────

#[derive(Debug)]
enum WsEvent {
    /// A fresh snapshot for a market arrived on a given socket.
    Snapshot {
        socket_id: usize,
        ticker: String,
        clock: u64,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        received_at: chrono::DateTime<Utc>,
    },
    /// An incremental delta arrived on a given socket.
    Delta {
        socket_id: usize,
        ticker: String,
        clock: u64,
        price: i64,
        qty_delta: i64,
        side: Side,
        exchange_ts_ms: Option<i64>,
        received_at: chrono::DateTime<Utc>,
    },
    /// The WebSocket connection was closed or errored.
    Disconnected { socket_id: usize },
    /// The subscription on this socket has been assigned a server sid.
    Subscribed { socket_id: usize, sid: u64 },
    /// This connection skipped at least one message on a subscription.
    SequenceGap {
        socket_id: usize,
        sid: u64,
        expected: u64,
        received: u64,
    },
}

/// Tracks contiguous `seq` values per subscription on one connection.
#[derive(Debug, Default)]
struct SequenceTracker {
    last_seen: HashMap<u64, u64>,
}

impl SequenceTracker {
    /// Record a frame's position. Returns the expected `seq` if this frame skipped ahead
    /// or repeated an earlier position.
    fn observe(&mut self, point: SequencePoint) -> Option<u64> {
        let previous = self.last_seen.insert(point.sid, point.seq)?;
        let expected = previous.wrapping_add(1);
        (point.seq != expected).then_some(expected)
    }
}

// ── Per-socket control handle ─────────────────────────────────────────────────

#[derive(Debug)]
enum SocketCmd {
    AddMarkets(Vec<String>),
    RemoveMarkets(Vec<String>),
    GetSnapshot(Vec<String>),
    Close,
}

// ── Collector ─────────────────────────────────────────────────────────────────

/// Central collector engine, exchange-agnostic via [`ExchangeAdapter`].
pub struct Collector {
    adapter: Arc<dyn ExchangeAdapter>,
    http: reqwest::Client,
    /// Live orderbooks, keyed by ticker.
    books: Arc<DashMap<String, Orderbook>>,
    /// Output sink (mutex-protected so both snapshot timer and event loop can write).
    output: Arc<Mutex<Output>>,
    /// Shutdown flag.
    shutdown: Arc<AtomicBool>,
    /// Notify woken when shutdown is set.
    shutdown_notify: Arc<Notify>,
    /// Monotonic atomic socket ID allocator.
    next_socket_id: Arc<AtomicUsize>,
}

impl Collector {
    pub async fn new(adapter: Arc<dyn ExchangeAdapter>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("building HTTP client")?;
        let output = Arc::new(Mutex::new(Output::new(None).context("opening output")?));

        Ok(Self {
            adapter,
            http,
            books: Arc::new(DashMap::new()),
            output,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            next_socket_id: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Allocate the next monotonic atomic socket identifier.
    pub fn next_socket_id(&self) -> usize {
        self.next_socket_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Signal a clean shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }

    /// Main collection loop. Returns when shutdown is signalled.
    pub async fn run(&self) -> Result<()> {
        info!("Starting collector with adapter: {}", self.adapter.name());

        // Initial market discovery
        let mut known_tickers: HashSet<String> = HashSet::new();
        let initial = if self.adapter.name() == "mock" {
            vec![
                "KXBTCD-26NOV05".to_string(),
                "KXHIGHNY-26NOV05".to_string(),
                "KXFED-26NOV05".to_string(),
            ]
        } else {
            let mut retry = INITIAL_DISCOVERY_RETRY;
            loop {
                match kalshi::fetch_open_markets(&self.http).await {
                    Ok(mkts) if !mkts.is_empty() => {
                        info!("Discovered {} open markets via REST", mkts.len());
                        break mkts;
                    }
                    Ok(_) => {
                        warn!("REST market discovery returned no markets; retrying in {retry:?}")
                    }
                    Err(e) => warn!("REST market discovery failed: {e:#}; retrying in {retry:?}"),
                }

                tokio::select! {
                    _ = self.shutdown_notify.notified() => return Ok(()),
                    _ = time::sleep(retry) => {}
                }
                if self.shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                retry = (retry * 2).min(MAX_INITIAL_DISCOVERY_RETRY);
            }
        };

        known_tickers.extend(initial.iter().cloned());

        // Channel for WebSocket tasks to send events back to collector
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<WsEvent>();

        // Assign markets to buckets and spawn WebSocket tasks
        let mut buckets: Vec<BucketState> = Vec::new();
        self.allocate_buckets(initial, &mut buckets, &event_tx);

        // Timers
        let mut snapshot_tick = time::interval(SNAPSHOT_INTERVAL);
        snapshot_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut discovery_tick = time::interval(DISCOVERY_INTERVAL);
        discovery_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        // Per-symbol last-update timestamps (for idle detection, Extension 2)
        let mut last_update: HashMap<String, Instant> = HashMap::new();
        let mut monitor_tick = time::interval(Duration::from_secs(30));
        monitor_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        // Per-socket reconnect schedule (Extension 2)
        let mut reconnect_tick = time::interval(Duration::from_secs(60));
        reconnect_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        if self.shutdown.load(Ordering::SeqCst) {
            return Ok(());
        }

        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                info!("Shutting down WebSocket tasks");
                for b in &mut buckets {
                    for s in &mut b.sockets {
                        let _ = s.cmd_tx.send(SocketCmd::Close);
                    }
                }
                let _ = self.output.lock().flush();
                break;
            }

            tokio::select! {
                biased;

                _ = self.shutdown_notify.notified() => {
                    info!("Shutting down WebSocket tasks");
                    for b in &mut buckets {
                        for s in &mut b.sockets {
                            let _ = s.cmd_tx.send(SocketCmd::Close);
                        }
                    }
                    let _ = self.output.lock().flush();
                    break;
                }

                Some(event) = event_rx.recv() => {
                    self.handle_event(event, &mut buckets, &event_tx, &mut last_update).await?;
                }

                _ = snapshot_tick.tick() => {
                    self.emit_all_snapshots()?;
                }

                _ = discovery_tick.tick() => {
                    self.discover_markets(&mut known_tickers, &mut buckets, &event_tx).await?;
                }

                _ = monitor_tick.tick() => {
                    self.monitor_subscriptions(&mut buckets, &last_update).await?;
                }

                _ = reconnect_tick.tick() => {
                    self.maybe_reconnect_sockets(&mut buckets, &event_tx).await?;
                }
            }
        }

        Ok(())
    }

    // ── Bucket & socket allocation ────────────────────────────────────────────

    fn allocate_buckets(
        &self,
        tickers: Vec<String>,
        buckets: &mut Vec<BucketState>,
        event_tx: &mpsc::UnboundedSender<WsEvent>,
    ) {
        let now = Instant::now();
        for chunk in tickers.chunks(MAX_MARKETS_PER_SOCKET) {
            let markets: Vec<String> = chunk.to_vec();
            let mut bucket = BucketState {
                markets: markets.iter().cloned().collect(),
                sockets: Vec::new(),
                primary_idx: 0,
                socket_reconnect_at: Vec::new(),
                reconnect_attempts: vec![0; SOCKETS_PER_BUCKET],
                awaiting_snapshot: HashSet::new(),
                last_snapshot_request: HashMap::new(),
            };
            for socket_role in 0..SOCKETS_PER_BUCKET {
                let socket = self.spawn_socket(
                    self.next_socket_id(),
                    markets.clone(),
                    event_tx.clone(),
                    Duration::ZERO,
                );
                bucket.sockets.push(socket);
                // Stagger initial reconnect schedules: socket 0 at full interval, socket 1 at half
                let stagger_offset = if socket_role == 0 {
                    SOCKET_RECONNECT_INTERVAL
                } else {
                    SOCKET_RECONNECT_INTERVAL / (SOCKETS_PER_BUCKET as u32)
                };
                bucket.socket_reconnect_at.push(now + stagger_offset);
            }
            buckets.push(bucket);
        }
    }

    fn spawn_socket(
        &self,
        socket_id: usize,
        markets: Vec<String>,
        event_tx: mpsc::UnboundedSender<WsEvent>,
        backoff: Duration,
    ) -> SocketHandle {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SocketCmd>();
        let adapter = Arc::clone(&self.adapter);

        tokio::spawn(ws_task(
            socket_id, adapter, markets, cmd_rx, event_tx, backoff,
        ));

        SocketHandle {
            socket_id,
            cmd_tx,
            sid: None,
            _spawned_at: Instant::now(),
        }
    }

    // ── Event handling ────────────────────────────────────────────────────────

    async fn handle_event(
        &self,
        event: WsEvent,
        buckets: &mut [BucketState],
        event_tx: &mpsc::UnboundedSender<WsEvent>,
        last_update: &mut HashMap<String, Instant>,
    ) -> Result<()> {
        match event {
            WsEvent::Subscribed { socket_id, sid } => {
                if let Some(bidx) = find_bucket_for_socket(buckets, socket_id) {
                    let bucket = &mut buckets[bidx];
                    if let Some(pos) = bucket.sockets.iter().position(|s| s.socket_id == socket_id)
                    {
                        bucket.sockets[pos].sid = Some(sid);
                        bucket.reconnect_attempts[pos] = 0;
                        debug!("Socket {socket_id} subscribed with sid={sid}");
                    }
                }
            }

            WsEvent::Snapshot {
                socket_id,
                ticker,
                clock,
                bids,
                asks,
                received_at,
            } => {
                if !is_primary(buckets, socket_id) {
                    return Ok(());
                }
                if let Some(bidx) = find_bucket_for_socket(buckets, socket_id) {
                    buckets[bidx].awaiting_snapshot.remove(&ticker);
                }
                last_update.insert(ticker.clone(), Instant::now());

                // A snapshot from the primary replaces the book, even after a clock reset.
                self.books
                    .entry(ticker.clone())
                    .or_default()
                    .replace_from_snapshot(&bids, &asks, clock);

                // Emit snapshot to SBE output
                let bids_levels: Vec<_> = bids
                    .iter()
                    .map(|(p, q)| crate::orderbook::PriceLevel { price: *p, qty: *q })
                    .collect();
                let asks_levels: Vec<_> = asks
                    .iter()
                    .map(|(p, q)| crate::orderbook::PriceLevel { price: *p, qty: *q })
                    .collect();

                self.output.lock().write_snapshot(
                    &ticker,
                    received_at,
                    received_at,
                    &bids_levels,
                    &asks_levels,
                )?;
            }

            WsEvent::Delta {
                socket_id,
                ticker,
                clock,
                price,
                qty_delta,
                side,
                exchange_ts_ms,
                received_at,
            } => {
                if !is_primary(buckets, socket_id) {
                    return Ok(());
                }

                let Some(bidx) = find_bucket_for_socket(buckets, socket_id) else {
                    return Ok(());
                };
                let bucket = &mut buckets[bidx];

                // The coming snapshot replaces this book and includes this change.
                if bucket.awaiting_snapshot.contains(&ticker) {
                    return Ok(());
                }

                // Gate output with the same clock check used by the book.
                let previous = {
                    let mut book = self.books.entry(ticker.clone()).or_default();
                    if clock < book.clock {
                        let book_clock = book.clock;
                        drop(book);
                        // One connection delivers in order, so a lower clock means the
                        // market's clock was reset, as Kalshi documents for maintenance.
                        if bucket.request_snapshots(vec![ticker.clone()]) > 0 {
                            warn!(
                                "Rejected {ticker} delta with clock {clock} below book clock {book_clock}; requested a snapshot"
                            );
                        }
                        return Ok(());
                    }
                    book.apply_delta(side, price, qty_delta, clock)
                };
                last_update.insert(ticker.clone(), Instant::now());

                // The SBE schema defines this field as the absolute quantity at the level.
                let quantity = previous.saturating_add(qty_delta).max(0);

                // Emit delta to SBE output
                let exchange_ts = exchange_ts_ms
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .unwrap_or(received_at);

                self.output.lock().write_delta(
                    &ticker,
                    received_at,
                    exchange_ts,
                    price,
                    quantity,
                    side,
                )?;
            }

            WsEvent::SequenceGap {
                socket_id,
                sid,
                expected,
                received,
            } => {
                if !is_primary(buckets, socket_id) {
                    debug!("Shadow socket {socket_id} skipped seq {expected}..{received}");
                    return Ok(());
                }
                warn!(
                    "Primary socket {socket_id} sid {sid} expected seq {expected}, received {received}; resynchronizing bucket"
                );
                if let Some(bidx) = find_bucket_for_socket(buckets, socket_id) {
                    let bucket = &mut buckets[bidx];
                    let markets: Vec<String> = bucket.markets.iter().cloned().collect();
                    bucket.awaiting_snapshot.extend(markets.iter().cloned());
                    bucket.request_snapshots(markets);
                }
            }

            WsEvent::Disconnected { socket_id } => {
                warn!("Socket {socket_id} disconnected");

                let bucket_idx = find_bucket_for_socket(buckets, socket_id);

                if let Some(bidx) = bucket_idx {
                    let bucket = &mut buckets[bidx];
                    let socket_pos = bucket
                        .sockets
                        .iter()
                        .position(|s| s.socket_id == socket_id)
                        .unwrap();

                    if socket_pos == bucket.primary_idx {
                        info!("Primary socket {socket_id} lost; initiating failover (Extension 1)");
                        let new_primary = (0..SOCKETS_PER_BUCKET)
                            .find(|&i| i != socket_pos)
                            .unwrap_or(0);
                        self.promote_primary(bucket, new_primary)?;
                    }

                    // Increment reconnect attempts and calculate backoff
                    let attempts = bucket.reconnect_attempts[socket_pos];
                    bucket.reconnect_attempts[socket_pos] = attempts.saturating_add(1);
                    let backoff = calculate_backoff(attempts);
                    if !backoff.is_zero() {
                        warn!(
                            "Applying reconnect backoff of {backoff:?} to replacement for socket {socket_id}"
                        );
                    }

                    // Respawn the failed socket with monotonic atomic ID and backoff
                    let next_id = self.next_socket_id();
                    let markets: Vec<String> = bucket.markets.iter().cloned().collect();
                    let new_handle = self.spawn_socket(next_id, markets, event_tx.clone(), backoff);
                    bucket.sockets[socket_pos] = new_handle;
                }
            }
        }

        Ok(())
    }

    // ── Failover ──────────────────────────────────────────────────────────────

    /// Make another socket the primary and resynchronize the bucket from it.
    ///
    /// The two feeds are not aligned at the moment of the swap: a delta may have been
    /// ignored on the old shadow and never read from the old primary, or may be seen
    /// again on the new primary. Deltas are therefore ignored until a fresh snapshot
    /// from the new primary replaces each book.
    fn promote_primary(&self, bucket: &mut BucketState, new_primary: usize) -> Result<()> {
        bucket.primary_idx = new_primary;
        bucket.last_snapshot_request.clear();

        let markets: Vec<String> = bucket.markets.iter().cloned().collect();
        bucket.awaiting_snapshot.extend(markets.iter().cloned());
        bucket.request_snapshots(markets.clone());

        // Emit snapshots from current in-memory book for output continuity
        for ticker in &markets {
            if let Some(book) = self.books.get(ticker) {
                let now = Utc::now();
                let b = book.bids_snapshot();
                let a = book.asks_snapshot();
                drop(book);
                self.output
                    .lock()
                    .write_snapshot(ticker, now, now, &b, &a)?;
            }
        }
        Ok(())
    }

    // ── Snapshot emission ─────────────────────────────────────────────────────

    fn emit_all_snapshots(&self) -> Result<()> {
        let now = Utc::now();
        let mut out = self.output.lock();
        for entry in self.books.iter() {
            let ticker = entry.key();
            let book = entry.value();
            let bids = book.bids_snapshot();
            let asks = book.asks_snapshot();
            out.write_snapshot(ticker, now, now, &bids, &asks)?;
        }
        out.flush()?;
        Ok(())
    }

    // ── Market discovery ──────────────────────────────────────────────────────

    async fn discover_markets(
        &self,
        known: &mut HashSet<String>,
        buckets: &mut Vec<BucketState>,
        event_tx: &mpsc::UnboundedSender<WsEvent>,
    ) -> Result<()> {
        if self.adapter.name() == "mock" {
            return Ok(());
        }

        let current = match kalshi::fetch_open_markets(&self.http).await {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        let current_set: HashSet<String> = current.into_iter().collect();

        let new_tickers: Vec<String> = current_set.difference(&*known).cloned().collect();

        let gone_tickers: Vec<String> = known.difference(&current_set).cloned().collect();

        if !gone_tickers.is_empty() {
            info!("Removing {} closed market(s)", gone_tickers.len());
            for ticker in &gone_tickers {
                self.books.remove(ticker);
            }
            self.remove_markets_from_buckets(gone_tickers, buckets);
        }

        if !new_tickers.is_empty() {
            info!("Adding {} newly opened market(s)", new_tickers.len());
            self.add_markets_to_buckets(new_tickers.clone(), buckets, event_tx);
        }

        *known = current_set;
        Ok(())
    }

    fn remove_markets_from_buckets(&self, tickers: Vec<String>, buckets: &mut [BucketState]) {
        for ticker in &tickers {
            for bucket in buckets.iter_mut() {
                if bucket.markets.remove(ticker) {
                    for s in &bucket.sockets {
                        let _ = s
                            .cmd_tx
                            .send(SocketCmd::RemoveMarkets(vec![ticker.clone()]));
                    }
                    break;
                }
            }
        }
    }

    fn add_markets_to_buckets(
        &self,
        tickers: Vec<String>,
        buckets: &mut Vec<BucketState>,
        event_tx: &mpsc::UnboundedSender<WsEvent>,
    ) {
        let mut remaining = tickers;
        loop {
            if remaining.is_empty() {
                break;
            }

            let target = buckets
                .iter_mut()
                .find(|b| b.markets.len() < MAX_MARKETS_PER_SOCKET);

            if let Some(bucket) = target {
                let space = MAX_MARKETS_PER_SOCKET - bucket.markets.len();
                let to_add: Vec<String> = remaining.drain(..space.min(remaining.len())).collect();
                for t in &to_add {
                    bucket.markets.insert(t.clone());
                }
                for s in &bucket.sockets {
                    let _ = s.cmd_tx.send(SocketCmd::AddMarkets(to_add.clone()));
                }
            } else {
                let chunk: Vec<String> = remaining
                    .drain(..MAX_MARKETS_PER_SOCKET.min(remaining.len()))
                    .collect();
                self.allocate_buckets(chunk, buckets, event_tx);
            }
        }
    }

    // ── Subscription monitor (Extension 2) ───────────────────────────────────

    async fn monitor_subscriptions(
        &self,
        buckets: &mut [BucketState],
        last_update: &HashMap<String, Instant>,
    ) -> Result<()> {
        let now = Instant::now();

        for bucket in buckets.iter_mut() {
            let unsynchronized: Vec<String> = bucket.awaiting_snapshot.iter().cloned().collect();
            if !unsynchronized.is_empty() {
                warn!(
                    "{} market(s) still awaiting a fresh snapshot; their deltas are held back",
                    unsynchronized.len()
                );
                bucket.request_snapshots(unsynchronized);
            }

            // A quiet market is often just illiquid, so keep its subscription in place
            // and confirm its book with a snapshot rather than removing and re-adding it.
            let stale: Vec<String> = bucket
                .markets
                .iter()
                .filter(|t| {
                    last_update
                        .get(*t)
                        .map(|&last| now.duration_since(last) > SYMBOL_IDLE_THRESHOLD)
                        .unwrap_or(false)
                })
                .cloned()
                .collect();

            if !stale.is_empty() {
                let sample = stale[..stale.len().min(5)].to_vec();
                let requested = bucket.request_snapshots(stale);
                if requested > 0 {
                    debug!("Requested snapshots for {requested} quiet market(s): {sample:?}");
                }
            }
        }

        Ok(())
    }

    // ── Periodic socket reconnect (Extension 2) ───────────────────────────────

    async fn maybe_reconnect_sockets(
        &self,
        buckets: &mut [BucketState],
        event_tx: &mpsc::UnboundedSender<WsEvent>,
    ) -> Result<()> {
        let now = Instant::now();

        for (bidx, bucket) in buckets.iter_mut().enumerate() {
            let markets: Vec<String> = bucket.markets.iter().cloned().collect();

            for i in 0..bucket.sockets.len() {
                if now < bucket.socket_reconnect_at[i] {
                    continue;
                }
                bucket.socket_reconnect_at[i] = now + SOCKET_RECONNECT_INTERVAL;

                // If this is the primary socket, promote healthy shadow before closing
                if i == bucket.primary_idx {
                    let other_idx = (0..bucket.sockets.len())
                        .find(|&idx| idx != i && bucket.sockets[idx].sid.is_some());
                    if let Some(new_primary) = other_idx {
                        info!(
                            "Promoting shadow socket {} to primary before recycling primary socket {}",
                            bucket.sockets[new_primary].socket_id, bucket.sockets[i].socket_id
                        );
                        self.promote_primary(bucket, new_primary)?;
                    }
                }

                let old_id = bucket.sockets[i].socket_id;
                info!(
                    "Periodic staggered reconnect for bucket {bidx}, socket pos {i} (old ID {old_id})"
                );

                // Close the old socket
                let _ = bucket.sockets[i].cmd_tx.send(SocketCmd::Close);

                // Spawn replacement with fresh monotonic socket ID
                let new_id = self.next_socket_id();
                bucket.reconnect_attempts[i] = 0;
                let new_handle =
                    self.spawn_socket(new_id, markets.clone(), event_tx.clone(), Duration::ZERO);
                bucket.sockets[i] = new_handle;
            }
        }

        Ok(())
    }
}

// ── Helper: bucket/socket lookup ──────────────────────────────────────────────

fn find_bucket_for_socket(buckets: &[BucketState], socket_id: usize) -> Option<usize> {
    buckets
        .iter()
        .position(|b| b.sockets.iter().any(|s| s.socket_id == socket_id))
}

fn is_primary(buckets: &[BucketState], socket_id: usize) -> bool {
    for bucket in buckets {
        if let Some(pos) = bucket.sockets.iter().position(|s| s.socket_id == socket_id) {
            return pos == bucket.primary_idx;
        }
    }
    false
}

fn calculate_backoff(attempts: u32) -> Duration {
    if attempts == 0 {
        return Duration::ZERO;
    }
    let base_ms = 100u64;
    let max_ms = 10_000u64;
    let shift = (attempts - 1).min(7);
    let ms = (base_ms * (1u64 << shift)).min(max_ms);
    Duration::from_millis(ms)
}

struct BucketState {
    markets: HashSet<String>,
    sockets: Vec<SocketHandle>,
    primary_idx: usize,
    socket_reconnect_at: Vec<Instant>,
    reconnect_attempts: Vec<u32>,
    /// Markets whose book is known to be incomplete until a fresh snapshot arrives.
    awaiting_snapshot: HashSet<String>,
    /// When each market's snapshot was last requested on the current primary.
    last_snapshot_request: HashMap<String, Instant>,
}

impl BucketState {
    /// Ask the primary for snapshots, skipping markets requested very recently.
    ///
    /// A primary without a sid is still subscribing, and its initial snapshots
    /// will arrive without a request. Returns the number of markets requested.
    fn request_snapshots(&mut self, tickers: Vec<String>) -> usize {
        let primary = &self.sockets[self.primary_idx];
        if primary.sid.is_none() {
            return 0;
        }

        let now = Instant::now();
        let due: Vec<String> = tickers
            .into_iter()
            .filter(|ticker| {
                self.last_snapshot_request
                    .get(ticker)
                    .is_none_or(|&last| now.duration_since(last) >= SNAPSHOT_REQUEST_COOLDOWN)
            })
            .collect();
        if due.is_empty() {
            return 0;
        }

        for ticker in &due {
            self.last_snapshot_request.insert(ticker.clone(), now);
        }
        let requested = due.len();
        let _ = primary.cmd_tx.send(SocketCmd::GetSnapshot(due));
        requested
    }
}

struct SocketHandle {
    socket_id: usize,
    cmd_tx: mpsc::UnboundedSender<SocketCmd>,
    sid: Option<u64>,
    _spawned_at: Instant,
}

type WsError = tokio_tungstenite::tungstenite::Error;
type WsSink = std::pin::Pin<Box<dyn futures_util::Sink<Message, Error = WsError> + Send>>;
type WsStream = std::pin::Pin<
    Box<dyn futures_util::Stream<Item = std::result::Result<Message, WsError>> + Send>,
>;

// ── WebSocket task ─────────────────────────────────────────────────────────────

async fn ws_task(
    socket_id: usize,
    adapter: Arc<dyn ExchangeAdapter>,
    markets: Vec<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<SocketCmd>,
    event_tx: mpsc::UnboundedSender<WsEvent>,
    backoff: Duration,
) {
    if !backoff.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            cmd = cmd_rx.recv() => {
                if let Some(SocketCmd::Close) | None = cmd {
                    let _ = event_tx.send(WsEvent::Disconnected { socket_id });
                    return;
                }
            }
        }
    }

    info!(
        "Socket {socket_id}: connecting to {} with {} markets",
        adapter.name(),
        markets.len()
    );

    // If running with mock adapter, run synthetic stream generator
    if adapter.name() == "mock" {
        mock_task(socket_id, adapter, markets, cmd_rx, event_tx).await;
        return;
    }

    if let Err(e) = ws_task_inner(socket_id, adapter, markets, &mut cmd_rx, &event_tx).await {
        error!("Socket {socket_id} error: {e:#}");
    }

    let _ = event_tx.send(WsEvent::Disconnected { socket_id });
}

async fn ws_task_inner(
    socket_id: usize,
    adapter: Arc<dyn ExchangeAdapter>,
    markets: Vec<String>,
    cmd_rx: &mut mpsc::UnboundedReceiver<SocketCmd>,
    event_tx: &mpsc::UnboundedSender<WsEvent>,
) -> Result<()> {
    let ws_url = adapter.ws_url();
    let auth_headers = adapter.auth_headers();

    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(ws_url)
            .context("parsing WS URL")?;

    for (name, value) in &auth_headers {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::HeaderName::from_bytes(name.as_bytes())
                .context("header name")?,
            tokio_tungstenite::tungstenite::http::header::HeaderValue::from_str(value)
                .context("header value")?,
        );
    }

    let (mut write, mut read): (WsSink, WsStream) =
        if let Some(stream) = crate::mock::take_mock_stream(ws_url, socket_id) {
            let (ws, _) = tokio_tungstenite::client_async(request, stream)
                .await
                .context("WebSocket mock connect")?;
            let (w, r) = ws.split();
            (Box::pin(w), Box::pin(r))
        } else {
            let (ws, _) = tokio_tungstenite::connect_async(request)
                .await
                .context("WebSocket connect")?;
            let (w, r) = ws.split();
            (Box::pin(w), Box::pin(r))
        };

    info!("Socket {socket_id}: connected");

    let mut msg_id: u64 = 1;
    let sub_cmd = adapter.encode_subscribe(msg_id, &markets)?;
    write
        .send(Message::Text(sub_cmd.into()))
        .await
        .context("sending subscribe")?;
    msg_id += 1;

    let mut sid: Option<u64> = None;

    let read_timeout = std::env::var("WS_READ_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(30));

    let ping_period = std::env::var("WS_PING_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(15));

    let mut ping_interval = time::interval(ping_period);
    // Skip initial immediate tick
    ping_interval.tick().await;

    // One deadline, extended only by frames from the server, so that sending our
    // own pings can never hide a silent connection.
    let read_deadline = time::sleep(read_timeout);
    tokio::pin!(read_deadline);

    let mut sequences = SequenceTracker::default();

    loop {
        tokio::select! {
            biased;

            cmd = cmd_rx.recv() => {
                match cmd {
                    None | Some(SocketCmd::Close) => {
                        info!("Socket {socket_id}: closing");
                        let _ = write.send(Message::Close(None)).await;
                        break;
                    }
                    Some(SocketCmd::AddMarkets(tickers)) => {
                        if tickers.is_empty() { continue; }
                        let s = sid.unwrap_or(0);
                        if let Ok(json) = adapter.encode_add_markets(msg_id, s, &tickers) {
                            let _ = write.send(Message::Text(json.into())).await;
                            msg_id += 1;
                        }
                    }
                    Some(SocketCmd::RemoveMarkets(tickers)) => {
                        if tickers.is_empty() { continue; }
                        let s = sid.unwrap_or(0);
                        if let Ok(json) = adapter.encode_remove_markets(msg_id, s, &tickers) {
                            let _ = write.send(Message::Text(json.into())).await;
                            msg_id += 1;
                        }
                    }
                    Some(SocketCmd::GetSnapshot(tickers)) => {
                        if tickers.is_empty() { continue; }
                        let s = sid.unwrap_or(0);
                        if let Ok(json) = adapter.encode_get_snapshot(msg_id, s, &tickers) {
                            let _ = write.send(Message::Text(json.into())).await;
                            msg_id += 1;
                        }
                    }
                }
            }

            _ = ping_interval.tick() => {
                if let Err(e) = write.send(Message::Ping(Default::default())).await {
                    warn!("Socket {socket_id}: failed to send keepalive ping: {e}");
                    break;
                }
            }

            msg = read.next() => {
                read_deadline.as_mut().reset(time::Instant::now() + read_timeout);

                match msg {
                    None => {
                        info!("Socket {socket_id}: stream ended");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("Socket {socket_id}: read error: {e}");
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        let received_at = Utc::now();
                        let frame = match adapter.decode_frame(&text) {
                            Ok(frame) => frame,
                            Err(e) => {
                                debug!("Socket {socket_id}: decode error: {e} in {text:.200}");
                                continue;
                            }
                        };
                        if let Some(point) = frame.sequence
                            && let Some(expected) = sequences.observe(point)
                        {
                            let _ = event_tx.send(WsEvent::SequenceGap {
                                socket_id,
                                sid: point.sid,
                                expected,
                                received: point.seq,
                            });
                        }
                        match frame.message {
                            Some(InboundMessage::Subscribed { sid: s }) => {
                                sid = Some(s);
                                let _ = event_tx.send(WsEvent::Subscribed {
                                    socket_id,
                                    sid: s,
                                });
                            }
                            Some(InboundMessage::Snapshot { ticker, clock, bids, asks }) => {
                                let _ = event_tx.send(WsEvent::Snapshot {
                                    socket_id,
                                    ticker,
                                    clock,
                                    bids,
                                    asks,
                                    received_at,
                                });
                            }
                            Some(InboundMessage::Delta { ticker, clock, price, qty_delta, side, exchange_ts_ms }) => {
                                let _ = event_tx.send(WsEvent::Delta {
                                    socket_id,
                                    ticker,
                                    clock,
                                    price,
                                    qty_delta,
                                    side,
                                    exchange_ts_ms,
                                    received_at,
                                });
                            }
                            Some(InboundMessage::Error { code, message }) => {
                                warn!("Socket {socket_id}: exchange error code={:?}: {message}", code);
                            }
                            None => {}
                        }
                    }
                    Some(Ok(Message::Ping(d))) => {
                        let _ = write.send(Message::Pong(d)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        debug!("Socket {socket_id}: received pong");
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("Socket {socket_id}: server closed connection");
                        break;
                    }
                    Some(Ok(_)) => {}
                }
            }

            _ = &mut read_deadline => {
                warn!("Socket {socket_id}: no frames for {read_timeout:?} (silent socket detected)");
                break;
            }
        }
    }

    Ok(())
}

/// Standalone synthetic stream generator for the mock adapter.
async fn mock_task(
    socket_id: usize,
    _adapter: Arc<dyn ExchangeAdapter>,
    markets: Vec<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<SocketCmd>,
    event_tx: mpsc::UnboundedSender<WsEvent>,
) {
    let sid = (socket_id as u64) + 100;
    let _ = event_tx.send(WsEvent::Subscribed { socket_id, sid });

    // Initial snapshots
    let mut clock = 100u64;
    for ticker in &markets {
        clock += 1;
        let _ = event_tx.send(WsEvent::Snapshot {
            socket_id,
            ticker: ticker.clone(),
            clock,
            bids: vec![
                (450_000, 10_000_000),
                (440_000, 5_000_000),
                (430_000, 3_000_000),
            ],
            asks: vec![
                (550_000, 12_000_000),
                (560_000, 8_000_000),
                (570_000, 4_000_000),
            ],
            received_at: Utc::now(),
        });
    }

    let mut ticker_idx = 0;
    let mut interval = time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            biased;

            cmd = cmd_rx.recv() => {
                match cmd {
                    None | Some(SocketCmd::Close) => break,
                    Some(SocketCmd::GetSnapshot(ts)) => {
                        for t in ts {
                            clock += 1;
                            let _ = event_tx.send(WsEvent::Snapshot {
                                socket_id,
                                ticker: t,
                                clock,
                                bids: vec![(450_000, 10_000_000)],
                                asks: vec![(550_000, 12_000_000)],
                                received_at: Utc::now(),
                            });
                        }
                    }
                    _ => {}
                }
            }

            _ = interval.tick() => {
                if markets.is_empty() { continue; }
                let ticker = &markets[ticker_idx % markets.len()];
                ticker_idx += 1;
                clock += 1;

                let is_buy = clock.is_multiple_of(2);
                let price = if is_buy { 450_000 } else { 550_000 };
                let qty_delta = if clock.is_multiple_of(5) {
                    -500_000
                } else {
                    500_000
                };

                let _ = event_tx.send(WsEvent::Delta {
                    socket_id,
                    ticker: ticker.clone(),
                    clock,
                    price,
                    qty_delta,
                    side: if is_buy { Side::Buy } else { Side::Sell },
                    exchange_ts_ms: Some(Utc::now().timestamp_millis()),
                    received_at: Utc::now(),
                });
            }
        }
    }

    let _ = event_tx.send(WsEvent::Disconnected { socket_id });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockAdapter;
    use crate::util::dtos::challenge_orderbook::AnyMessage;
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    type CmdRx = mpsc::UnboundedReceiver<SocketCmd>;

    fn test_collector(file: &tempfile::NamedTempFile) -> Result<Collector> {
        Ok(Collector {
            adapter: Arc::new(MockAdapter::new()),
            http: reqwest::Client::new(),
            books: Arc::new(DashMap::new()),
            output: Arc::new(Mutex::new(Output::new(file.path().to_str())?)),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            next_socket_id: Arc::new(AtomicUsize::new(100)),
        })
    }

    /// A subscribed bucket for market TEST: socket 1 is primary, socket 2 is shadow.
    fn test_bucket() -> (BucketState, CmdRx, CmdRx) {
        let (primary_tx, primary_rx) = mpsc::unbounded_channel();
        let (shadow_tx, shadow_rx) = mpsc::unbounded_channel();
        let socket = |socket_id, cmd_tx| SocketHandle {
            socket_id,
            cmd_tx,
            sid: Some(socket_id as u64),
            _spawned_at: Instant::now(),
        };
        let later = Instant::now() + SOCKET_RECONNECT_INTERVAL;
        let bucket = BucketState {
            markets: HashSet::from(["TEST".to_owned()]),
            sockets: vec![socket(1, primary_tx), socket(2, shadow_tx)],
            primary_idx: 0,
            socket_reconnect_at: vec![later, later],
            reconnect_attempts: vec![0, 0],
            awaiting_snapshot: HashSet::new(),
            last_snapshot_request: HashMap::new(),
        };
        (bucket, primary_rx, shadow_rx)
    }

    fn delta(socket_id: usize, clock: u64, qty_delta: i64) -> WsEvent {
        WsEvent::Delta {
            socket_id,
            ticker: "TEST".to_owned(),
            clock,
            price: 500_000,
            qty_delta,
            side: Side::Buy,
            exchange_ts_ms: None,
            received_at: Utc::now(),
        }
    }

    fn snapshot(socket_id: usize, clock: u64, qty: i64) -> WsEvent {
        WsEvent::Snapshot {
            socket_id,
            ticker: "TEST".to_owned(),
            clock,
            bids: vec![(500_000, qty)],
            asks: vec![],
            received_at: Utc::now(),
        }
    }

    async fn feed(
        collector: &Collector,
        buckets: &mut [BucketState],
        last_update: &mut HashMap<String, Instant>,
        events: Vec<WsEvent>,
    ) -> Result<()> {
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        for event in events {
            collector
                .handle_event(event, buckets, &event_tx, last_update)
                .await?;
        }
        Ok(())
    }

    /// Every message in the output, in order: `Ok(qty)` for an update, `Err(())` for a snapshot.
    fn written(
        collector: &Collector,
        file: &tempfile::NamedTempFile,
    ) -> Result<Vec<std::result::Result<i64, ()>>> {
        collector.output.lock().flush()?;
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
        let mut messages = Vec::new();
        while let Some(message) = reader.read_msg()? {
            messages.push(match message {
                AnyMessage::PriceLevelUpdate(update) => Ok(update.quantity()),
                _ => Err(()),
            });
        }
        Ok(messages)
    }

    fn drain(rx: &mut CmdRx) -> Vec<SocketCmd> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn is_snapshot_request(commands: &[SocketCmd]) -> bool {
        matches!(commands, [SocketCmd::GetSnapshot(tickers)] if tickers == &["TEST".to_owned()])
    }

    #[test]
    fn sequence_tracker_reports_skips_and_repeats_per_subscription() {
        let mut tracker = SequenceTracker::default();
        let point = |sid, seq| SequencePoint { sid, seq };

        assert_eq!(
            tracker.observe(point(1, 5)),
            None,
            "first frame sets baseline"
        );
        assert_eq!(tracker.observe(point(1, 6)), None);
        assert_eq!(
            tracker.observe(point(2, 40)),
            None,
            "subscriptions are separate"
        );
        assert_eq!(tracker.observe(point(1, 8)), Some(7), "skipped seq 7");
        assert_eq!(tracker.observe(point(1, 8)), Some(9), "repeated seq 8");
        assert_eq!(tracker.observe(point(1, 9)), None);
    }

    #[tokio::test]
    async fn stale_events_are_absent_and_quantities_are_absolute() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, mut primary_rx, _shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        let events = vec![
            delta(1, 10, 100),
            delta(1, 9, 900),
            delta(2, 11, 900),
            delta(1, 10, 20),
            delta(1, 11, 30),
        ];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert_eq!(
            collector.books.get("TEST").unwrap().best_bid(),
            Some((500_000, 150))
        );
        assert_eq!(
            written(&collector, &file)?,
            vec![Ok(100), Ok(120), Ok(150)],
            "updates carry the absolute size at the level, as the schema requires"
        );
        assert!(
            is_snapshot_request(&drain(&mut primary_rx)),
            "a lower clock on the primary requests a snapshot"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reductions_write_remaining_size_and_removal_writes_zero() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, _primary_rx, _shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        let events = vec![delta(1, 1, 100), delta(1, 2, -40), delta(1, 3, -60)];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert_eq!(written(&collector, &file)?, vec![Ok(100), Ok(60), Ok(0)]);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_replaces_book_after_clock_reset() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, _primary_rx, _shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        // Maintenance resets the clock from 1000 to 5.
        let events = vec![
            delta(1, 1000, 100),
            delta(1, 6, 900),
            snapshot(1, 5, 70),
            delta(1, 6, 30),
        ];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        let book = collector.books.get("TEST").unwrap();
        assert_eq!(book.clock, 6);
        assert_eq!(book.best_bid(), Some((500_000, 100)));
        drop(book);
        assert_eq!(
            written(&collector, &file)?,
            vec![Ok(100), Err(()), Ok(100)],
            "the rejected delta is absent and the book resumes from the snapshot"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sequence_gap_holds_deltas_until_fresh_snapshot() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, mut primary_rx, _shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        let gap = WsEvent::SequenceGap {
            socket_id: 1,
            sid: 1,
            expected: 7,
            received: 9,
        };
        let events = vec![
            delta(1, 10, 100),
            gap,
            delta(1, 11, 5),
            snapshot(1, 12, 500),
            delta(1, 13, 1),
        ];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert!(is_snapshot_request(&drain(&mut primary_rx)));
        assert!(buckets[0].awaiting_snapshot.is_empty());
        assert_eq!(
            written(&collector, &file)?,
            vec![Ok(100), Err(()), Ok(501)],
            "the delta after the gap is held back and the snapshot restores the book"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shadow_sequence_gap_does_not_interrupt_primary() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, mut primary_rx, mut shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        let gap = WsEvent::SequenceGap {
            socket_id: 2,
            sid: 2,
            expected: 7,
            received: 9,
        };
        let events = vec![gap, delta(1, 10, 100)];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert!(drain(&mut primary_rx).is_empty());
        assert!(drain(&mut shadow_rx).is_empty());
        assert_eq!(written(&collector, &file)?, vec![Ok(100)]);
        Ok(())
    }

    #[tokio::test]
    async fn recycling_primary_resynchronizes_from_new_primary() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (mut bucket, mut primary_rx, mut shadow_rx) = test_bucket();
        bucket.socket_reconnect_at[0] = Instant::now();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        // Clock 11 reaches the shadow first and is ignored there, then the swap happens.
        let events = vec![delta(1, 10, 100), delta(2, 11, 1)];
        feed(&collector, &mut buckets, &mut last_update, events).await?;
        collector
            .maybe_reconnect_sockets(&mut buckets, &event_tx)
            .await?;

        assert_eq!(buckets[0].primary_idx, 1);
        assert!(matches!(
            drain(&mut primary_rx).as_slice(),
            [SocketCmd::Close]
        ));
        assert!(
            is_snapshot_request(&drain(&mut shadow_rx)),
            "the new primary must be asked for a fresh snapshot"
        );

        // Clock 11 is lost to both feeds, so clock 12 alone must not extend the book.
        let events = vec![delta(1, 11, 1), delta(2, 12, 1), snapshot(2, 12, 102)];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert_eq!(
            collector.books.get("TEST").unwrap().best_bid(),
            Some((500_000, 102))
        );
        assert_eq!(
            written(&collector, &file)?,
            vec![Ok(100), Err(()), Err(())],
            "first the update, then the continuity snapshot, then the fresh snapshot"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failover_does_not_double_count_equal_clock_deltas() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, _primary_rx, mut shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let mut last_update = HashMap::new();

        let events = vec![
            delta(1, 10, 100),
            WsEvent::Disconnected { socket_id: 1 },
            // The new primary replays the same exchange action at the same clock.
            delta(2, 10, 100),
            snapshot(2, 10, 100),
            delta(2, 11, 5),
        ];
        feed(&collector, &mut buckets, &mut last_update, events).await?;

        assert!(is_snapshot_request(&drain(&mut shadow_rx)));
        assert_eq!(
            collector.books.get("TEST").unwrap().best_bid(),
            Some((500_000, 105))
        );
        Ok(())
    }

    #[tokio::test]
    async fn quiet_market_gets_snapshot_without_losing_its_subscription() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let collector = test_collector(&file)?;
        let (bucket, mut primary_rx, mut shadow_rx) = test_bucket();
        let mut buckets = vec![bucket];
        let last_update = HashMap::from([(
            "TEST".to_owned(),
            Instant::now() - SYMBOL_IDLE_THRESHOLD - Duration::from_secs(1),
        )]);

        collector
            .monitor_subscriptions(&mut buckets, &last_update)
            .await?;
        assert!(is_snapshot_request(&drain(&mut primary_rx)));
        assert!(drain(&mut shadow_rx).is_empty());

        collector
            .monitor_subscriptions(&mut buckets, &last_update)
            .await?;
        assert!(
            drain(&mut primary_rx).is_empty(),
            "repeat requests wait for the cooldown"
        );
        Ok(())
    }
}
