//! Kalshi websocket session: authentication, chunked subscriptions, sequence
//! validation, lifecycle subscription, and snapshot requests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{Value, json};
use tracing::warn;

use crate::{
    config::{KalshiSettings, NetworkSettings},
    exchange::kalshi::{
        KalshiAuth,
        auth::{HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP},
        codec::{KalshiMessage, decode_kalshi_message},
    },
    journal::{DecodeOutcome, FrameKind, RawFrame},
    model::SourceId,
    session::{MarketDataSession, SessionFactory, SessionFailure, SessionFrame},
    time::now_us,
    ws::{self, ConnectOptions, WsPayload, WsStream},
};

/// Maximum consecutive classification-driven recoveries before the session
/// gives up and asks the worker to reconnect. This bounds loops caused by a
/// server that persists in returning a retryable error (for example code 26
/// with a one-market limit).
const MAX_TRANSIENT_RECOVERIES: u32 = 8;
/// Initial command-rate-limit backoff; doubles up to the cap.
const RATE_LIMIT_BACKOFF_START: Duration = Duration::from_millis(250);
const RATE_LIMIT_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Bound for sequence numbers attributed to control acknowledgements without a
/// sequence number. Prevents an old ack from masking an arbitrarily large gap.
const MAX_UNNUMBERED_ACKS: u32 = 64;

/// Tracks Kalshi's per-subscription `seq`.
///
/// `seq` counts every message sent on one subscription, including control
/// responses (`ok`) for `add_markets`, `delete_markets`, and `get_snapshot`.
/// The official `ok` example carries `sid` and `seq`, but the tracker also
/// tolerates an ack without a sequence number: one such ack can account for
/// one skipped delta sequence. A snapshot re-baselines the stream and clears
/// pending acks, because it supersedes any missed deltas.
#[derive(Default)]
struct SequenceTracker {
    last: BTreeMap<i64, u64>,
    unnumbered_acks: BTreeMap<i64, u32>,
}

impl SequenceTracker {
    fn clear(&mut self) {
        self.last.clear();
        self.unnumbered_acks.clear();
    }

    fn forget(&mut self, sid: i64) {
        self.last.remove(&sid);
        self.unnumbered_acks.remove(&sid);
    }

    /// Returns `false` for a snapshot older than the established baseline.
    fn on_snapshot(&mut self, sid: i64, seq: u64) -> bool {
        match self.last.get(&sid).copied() {
            Some(previous) if seq < previous => false,
            _ => {
                self.last.insert(sid, seq);
                self.unnumbered_acks.remove(&sid);
                true
            }
        }
    }

    /// Observe an `ok` or other control response for a subscription.
    fn on_control(&mut self, sid: i64, seq: Option<u64>) {
        if let Some(seq) = seq {
            if let Some(previous) = self.last.get_mut(&sid)
                && seq > *previous
            {
                *previous = seq;
            }
            return;
        }
        if self.last.contains_key(&sid) {
            let count = self.unnumbered_acks.entry(sid).or_insert(0);
            *count = count.saturating_add(1).min(MAX_UNNUMBERED_ACKS);
        }
    }

    fn on_delta(&mut self, sid: i64, seq: u64) -> Result<(), String> {
        let Some(previous) = self.last.get(&sid).copied() else {
            return Err(format!(
                "Kalshi delta on sid {sid} arrived before a snapshot"
            ));
        };
        let expected = previous.saturating_add(1);
        if seq == expected {
            self.last.insert(sid, seq);
            return Ok(());
        }
        if seq > expected {
            let gap = seq - expected;
            let acks = self.unnumbered_acks.entry(sid).or_insert(0);
            if u64::from(*acks) >= gap {
                *acks -= u32::try_from(gap).unwrap_or(u32::MAX);
                self.last.insert(sid, seq);
                return Ok(());
            }
        }
        Err(format!(
            "Kalshi sequence gap on sid {sid}: expected {expected}, received {seq}"
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateAction {
    AddMarkets,
    DeleteMarkets,
    GetSnapshot,
}

impl UpdateAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::AddMarkets => "add_markets",
            Self::DeleteMarkets => "delete_markets",
            Self::GetSnapshot => "get_snapshot",
        }
    }
}

struct PendingUpdate {
    sid: i64,
    action: UpdateAction,
    markets: Vec<String>,
}

/// Kalshi adapter handle. It owns credentials and the REST client; websocket
/// sessions are created per source by [`SessionFactory`].
pub struct KalshiExchange {
    pub(crate) settings: KalshiSettings,
    pub(crate) auth: Option<Arc<KalshiAuth>>,
    pub(crate) client: Client,
    pub(crate) network: NetworkSettings,
}

pub type KalshiSessionFactory = KalshiExchange;

#[async_trait]
impl SessionFactory for KalshiExchange {
    async fn create_session(&self, source: SourceId) -> anyhow::Result<Box<dyn MarketDataSession>> {
        let auth = self.auth.as_ref().with_context(|| {
            format!(
                "Kalshi credentials are required for source {source}; provide key_id and a private key"
            )
        })?;
        let connect_options = self
            .network
            .connect_options()
            .for_ordinal(source_ordinal(source))
            .with_context(|| format!("deriving connect options for source {source}"))?;
        Ok(Box::new(KalshiSession::new(
            self.settings.clone(),
            Arc::clone(auth),
            connect_options,
        )))
    }
}

struct OrderbookSubscription {
    sid: i64,
    markets: BTreeSet<String>,
}

/// Unique per-connection ordinal used to derive distinct pinned source ports.
///
/// The replica bound is validated to at most four, and shards to at most 256,
/// so `shard * 256 + replica` is injective across all live connections of a
/// run.
fn source_ordinal(source: SourceId) -> u32 {
    u32::from(source.shard) * 256 + u32::from(source.replica)
}

pub struct KalshiSession {
    settings: KalshiSettings,
    auth: Arc<KalshiAuth>,
    stream: Option<WsStream>,
    next_id: u64,
    desired: BTreeSet<String>,
    orderbook_subs: Vec<OrderbookSubscription>,
    pending_subscribes: BTreeMap<u64, BTreeSet<String>>,
    pending_updates: BTreeMap<u64, PendingUpdate>,
    pending_since: Option<Instant>,
    seq: SequenceTracker,
    lifecycle_sid: Option<i64>,
    lifecycle_subscribed: bool,
    /// Adaptive per-subscription market limit. Starts at the configured batch
    /// size and halves when Kalshi reports code 26.
    batch_size: usize,
    rate_limit_backoff: Duration,
    transient_recoveries: u32,
    ws_url: String,
    connect_options: ConnectOptions,
}

impl KalshiSession {
    fn new(
        settings: KalshiSettings,
        auth: Arc<KalshiAuth>,
        connect_options: ConnectOptions,
    ) -> Self {
        let batch_size = settings.batch_size.max(1);
        let ws_url = settings.environment.ws_url().to_owned();
        Self {
            settings,
            auth,
            stream: None,
            next_id: 1,
            desired: BTreeSet::new(),
            orderbook_subs: Vec::new(),
            pending_subscribes: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            pending_since: None,
            seq: SequenceTracker::default(),
            lifecycle_sid: None,
            lifecycle_subscribed: false,
            batch_size,
            rate_limit_backoff: RATE_LIMIT_BACKOFF_START,
            transient_recoveries: 0,
            ws_url,
            connect_options,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut WsStream, SessionFailure> {
        self.stream.as_mut().ok_or_else(|| {
            SessionFailure::Reconnect(anyhow::anyhow!("Kalshi session is not connected"))
        })
    }

    fn next_command_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1).max(1);
        id
    }

    async fn send_json(&mut self, mut value: Value) -> Result<u64, SessionFailure> {
        let id = self.next_command_id();
        value["id"] = json!(id);
        let text = serde_json::to_string(&value)
            .context("serializing Kalshi command")
            .map_err(SessionFailure::Reconnect)?;
        ws::send_text(self.stream_mut()?, text)
            .await
            .map_err(SessionFailure::Reconnect)?;
        Ok(id)
    }

    async fn send_lifecycle_subscribe(&mut self) -> Result<(), SessionFailure> {
        self.send_json(json!({
            "cmd": "subscribe",
            "params": {"channels": ["market_lifecycle_v2"]}
        }))
        .await?;
        self.lifecycle_subscribed = true;
        Ok(())
    }

    async fn send_orderbook_subscribe(
        &mut self,
        markets: &[String],
    ) -> Result<u64, SessionFailure> {
        let id = self
            .send_json(json!({
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": markets,
                    "use_yes_price": false
                }
            }))
            .await?;
        self.pending_subscribes
            .insert(id, markets.iter().cloned().collect());
        self.pending_since.get_or_insert_with(Instant::now);
        Ok(id)
    }

    async fn send_update(
        &mut self,
        action: UpdateAction,
        sid: i64,
        markets: &[String],
    ) -> Result<(), SessionFailure> {
        if markets.is_empty() {
            return Ok(());
        }
        let id = self
            .send_json(json!({
                "cmd": "update_subscription",
                "params": {
                    "sids": [sid],
                    "market_tickers": markets,
                    "action": action.as_str()
                }
            }))
            .await?;
        self.pending_updates.insert(
            id,
            PendingUpdate {
                sid,
                action,
                markets: markets.to_vec(),
            },
        );
        self.pending_since.get_or_insert_with(Instant::now);
        Ok(())
    }

    async fn reconcile(&mut self) -> Result<(), SessionFailure> {
        if !self.pending_subscribes.is_empty() || !self.pending_updates.is_empty() {
            return Ok(());
        }
        if !self.desired.is_empty() && self.orderbook_subs.is_empty() {
            let markets: Vec<String> = self.desired.iter().cloned().collect();
            for chunk in markets.chunks(self.batch_size) {
                self.send_orderbook_subscribe(chunk).await?;
            }
            return Ok(());
        }

        let batch_size = self.batch_size;
        let current: BTreeSet<String> = self
            .orderbook_subs
            .iter()
            .flat_map(|subscription| subscription.markets.iter().cloned())
            .collect();
        let additions: Vec<String> = self.desired.difference(&current).cloned().collect();
        let deletions: Vec<String> = current.difference(&self.desired).cloned().collect();

        // Deletions are acknowledged before the local market set changes, so a
        // failed command can be retried from the unchanged state.
        for index in 0..self.orderbook_subs.len() {
            let (sid, remove) = {
                let subscription = &self.orderbook_subs[index];
                let remove: Vec<String> = deletions
                    .iter()
                    .filter(|symbol| subscription.markets.contains(*symbol))
                    .cloned()
                    .collect();
                (subscription.sid, remove)
            };
            if !remove.is_empty() {
                self.send_update(UpdateAction::DeleteMarkets, sid, &remove)
                    .await?;
            }
        }

        let mut remaining = additions;
        for index in 0..self.orderbook_subs.len() {
            if remaining.is_empty() {
                break;
            }
            let (sid, capacity) = {
                let subscription = &self.orderbook_subs[index];
                (
                    subscription.sid,
                    batch_size.saturating_sub(subscription.markets.len()),
                )
            };
            if capacity == 0 {
                continue;
            }
            let take = capacity.min(remaining.len());
            let chunk: Vec<String> = remaining.drain(..take).collect();
            self.send_update(UpdateAction::AddMarkets, sid, &chunk)
                .await?;
        }
        for chunk in remaining.chunks(batch_size) {
            self.send_orderbook_subscribe(chunk).await?;
        }
        Ok(())
    }

    /// Acknowledge a `subscribe` command the venue folded into an existing
    /// subscription. Kalshi keeps one `orderbook_delta` subscription per
    /// connection: a repeated `subscribe` adds its markets to the existing
    /// sid and is acknowledged with `ok`, not a fresh `subscribed`. Fold the
    /// markets into the tracked subscription for that sid, creating it when
    /// the venue has not acknowledged it yet. `full_tickers`, when present,
    /// is the venue's authoritative list of markets on the subscription and
    /// replaces the tracked set.
    fn merge_subscription(
        &mut self,
        sid: Option<i64>,
        markets: BTreeSet<String>,
        full_tickers: Option<Vec<String>>,
    ) -> Result<(), SessionFailure> {
        let target =
            sid.or_else(|| (self.orderbook_subs.len() == 1).then_some(self.orderbook_subs[0].sid));
        let Some(target) = target else {
            return Err(SessionFailure::protocol(anyhow::anyhow!(
                "Kalshi subscribe acknowledgement named no sid and cannot be assigned unambiguously"
            )));
        };
        match self
            .orderbook_subs
            .iter_mut()
            .find(|subscription| subscription.sid == target)
        {
            Some(subscription) => {
                if let Some(full) = full_tickers {
                    subscription.markets = full.into_iter().collect();
                } else {
                    subscription.markets.extend(markets);
                }
            }
            None => {
                let markets: BTreeSet<String> = match full_tickers {
                    Some(full) => full.into_iter().collect(),
                    None => markets,
                };
                self.orderbook_subs.push(OrderbookSubscription {
                    sid: target,
                    markets,
                });
            }
        }
        Ok(())
    }

    fn complete_update(&mut self, id: u64) -> Option<PendingUpdate> {
        let pending = self.pending_updates.remove(&id)?;
        match pending.action {
            UpdateAction::AddMarkets => {
                if let Some(subscription) = self
                    .orderbook_subs
                    .iter_mut()
                    .find(|subscription| subscription.sid == pending.sid)
                {
                    subscription.markets.extend(pending.markets.iter().cloned());
                }
            }
            UpdateAction::DeleteMarkets => {
                if let Some(subscription) = self
                    .orderbook_subs
                    .iter_mut()
                    .find(|subscription| subscription.sid == pending.sid)
                {
                    for market in &pending.markets {
                        subscription.markets.remove(market);
                    }
                }
            }
            UpdateAction::GetSnapshot => {}
        }
        Some(pending)
    }

    /// Drop one server subscription; its markets stay in `desired`, so the
    /// next reconcile re-subscribes them (possibly split into new sids).
    fn drop_subscription(&mut self, sid: i64) {
        self.orderbook_subs
            .retain(|subscription| subscription.sid != sid);
        self.pending_updates.retain(|_, pending| pending.sid != sid);
        self.seq.forget(sid);
    }

    fn note_recovery(&mut self) -> Result<(), SessionFailure> {
        self.transient_recoveries = self.transient_recoveries.saturating_add(1);
        if self.transient_recoveries > MAX_TRANSIENT_RECOVERIES {
            return Err(SessionFailure::protocol(anyhow::anyhow!(
                "Kalshi command recovery exceeded {MAX_TRANSIENT_RECOVERIES} consecutive attempts"
            )));
        }
        Ok(())
    }
}

/// An application payload the adapter read but did not act on. The exact bytes
/// are preserved so the journal records every received frame with an explicit
/// ignored outcome instead of dropping it after the read.
fn ignored_payload_frame(
    kind: FrameKind,
    text: &str,
    received_at_us: i64,
    reason: &str,
) -> SessionFrame {
    SessionFrame::data(
        Vec::new(),
        text.len(),
        RawFrame {
            kind,
            payload: text.as_bytes().to_vec(),
        },
        DecodeOutcome::ignored(reason),
        received_at_us,
    )
}

#[async_trait]
impl MarketDataSession for KalshiSession {
    async fn connect(&mut self) -> Result<(), SessionFailure> {
        self.close().await;
        let headers = self
            .auth
            .headers("GET", "/trade-api/ws/v2")
            .context("signing Kalshi websocket handshake")
            .map_err(SessionFailure::Fatal)?;
        let handshake_headers = vec![
            (HEADER_KEY, headers.key),
            (HEADER_SIGNATURE, headers.signature),
            (HEADER_TIMESTAMP, headers.timestamp),
        ];
        let stream = ws::connect(&self.ws_url, &handshake_headers, &self.connect_options)
            .await
            .map_err(|error| {
                if matches!(ws::http_status(&error), Some(401 | 403)) {
                    SessionFailure::Fatal(error.context("Kalshi websocket authentication rejected"))
                } else {
                    SessionFailure::Reconnect(error.context("connecting to Kalshi websocket"))
                }
            })?;
        self.stream = Some(stream);
        self.next_id = 1;
        self.orderbook_subs.clear();
        self.pending_subscribes.clear();
        self.pending_updates.clear();
        self.pending_since = None;
        self.seq.clear();
        self.lifecycle_sid = None;
        self.lifecycle_subscribed = false;
        self.batch_size = self.settings.batch_size.max(1);
        self.rate_limit_backoff = RATE_LIMIT_BACKOFF_START;
        self.transient_recoveries = 0;
        Ok(())
    }

    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure> {
        self.desired = desired.clone();
        if !self.lifecycle_subscribed {
            self.send_lifecycle_subscribe().await?;
        }
        self.reconcile().await
    }

    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
        if self.pending_subscribes.is_empty() && self.pending_updates.is_empty() {
            self.pending_since = None;
        } else {
            let since = *self.pending_since.get_or_insert_with(Instant::now);
            if since.elapsed() > Duration::from_secs(15) {
                return Err(SessionFailure::protocol(anyhow::anyhow!(
                    "Kalshi orderbook command acknowledgement timed out"
                )));
            }
        }
        let payload = ws::next_payload(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)?;
        let received_at_us = now_us().unwrap_or(0);
        let (text, kind) = match payload {
            WsPayload::Idle => return Ok(SessionFrame::idle(0)),
            WsPayload::Text(text) => (text, FrameKind::Text),
            WsPayload::Binary(bytes) => match String::from_utf8(bytes.clone()) {
                Ok(text) => (text, FrameKind::Binary),
                Err(error) => {
                    return Err(SessionFailure::Decode {
                        error: anyhow::Error::new(error)
                            .context("Kalshi binary frame was not UTF-8 JSON"),
                        frame: Some(RawFrame {
                            kind: FrameKind::Binary,
                            payload: bytes,
                        }),
                        received_at_us,
                    });
                }
            },
        };
        let bytes = text.len();
        let message = decode_kalshi_message(&text, received_at_us).map_err(|error| {
            SessionFailure::Decode {
                error: error.context("decoding Kalshi frame"),
                frame: Some(RawFrame {
                    kind,
                    payload: text.clone().into_bytes(),
                }),
                received_at_us,
            }
        })?;
        match message {
            KalshiMessage::Snapshot { sid, seq, event } => {
                if !self.seq.on_snapshot(sid, seq) {
                    // A stale or duplicate snapshot is valid JSON the venue
                    // sent; keep the exact bytes in the journal and move on.
                    return Ok(ignored_payload_frame(
                        kind,
                        &text,
                        received_at_us,
                        "stale_snapshot",
                    ));
                }
                Ok(SessionFrame::data(
                    vec![event],
                    bytes,
                    RawFrame {
                        kind,
                        payload: text.into_bytes(),
                    },
                    DecodeOutcome::decoded(1),
                    received_at_us,
                ))
            }
            KalshiMessage::Delta { sid, seq, event } => {
                self.seq.on_delta(sid, seq).map_err(|error| {
                    SessionFailure::protocol_with_frame(
                        anyhow::anyhow!(error),
                        RawFrame {
                            kind,
                            payload: text.as_bytes().to_vec(),
                        },
                        received_at_us,
                    )
                })?;
                Ok(SessionFrame::data(
                    vec![event],
                    bytes,
                    RawFrame {
                        kind,
                        payload: text.into_bytes(),
                    },
                    DecodeOutcome::decoded(1),
                    received_at_us,
                ))
            }
            KalshiMessage::Lifecycle { sid, event, .. } => {
                if self
                    .lifecycle_sid
                    .is_none_or(|lifecycle_sid| lifecycle_sid == sid)
                {
                    Ok(SessionFrame::data(
                        vec![event],
                        bytes,
                        RawFrame {
                            kind,
                            payload: text.into_bytes(),
                        },
                        DecodeOutcome::decoded(1),
                        received_at_us,
                    ))
                } else {
                    Ok(ignored_payload_frame(
                        kind,
                        &text,
                        received_at_us,
                        "foreign_lifecycle",
                    ))
                }
            }
            KalshiMessage::Subscribed { id, channel, sid } => {
                if channel == "orderbook_delta" {
                    let id = id.context(
                        "Kalshi orderbook subscription acknowledgement is missing its request id",
                    )?;
                    let markets = self.pending_subscribes.remove(&id).context(
                        "Kalshi orderbook subscription acknowledgement referenced an unknown id",
                    )?;
                    if markets.is_empty() {
                        return Err(SessionFailure::protocol_with_frame(
                            anyhow::anyhow!(
                                "Kalshi orderbook subscription acknowledgement had an empty market set"
                            ),
                            RawFrame {
                                kind,
                                payload: text.as_bytes().to_vec(),
                            },
                            received_at_us,
                        ));
                    }
                    if let Some(existing) = self
                        .orderbook_subs
                        .iter_mut()
                        .find(|subscription| subscription.sid == sid)
                    {
                        // One sid per connection: a repeated `subscribe`
                        // merged here is folded into the tracked set rather
                        // than rejected.
                        existing.markets.extend(markets);
                    } else {
                        self.orderbook_subs
                            .push(OrderbookSubscription { sid, markets });
                    }
                } else if channel == "market_lifecycle_v2" {
                    self.lifecycle_sid = Some(sid);
                }
                self.transient_recoveries = 0;
                self.rate_limit_backoff = RATE_LIMIT_BACKOFF_START;
                if self.pending_subscribes.is_empty() && self.pending_updates.is_empty() {
                    self.pending_since = None;
                    self.reconcile().await?;
                }
                Ok(ignored_payload_frame(
                    kind,
                    &text,
                    received_at_us,
                    "subscription_ack",
                ))
            }
            KalshiMessage::Ok {
                id,
                sid,
                seq,
                full_tickers,
            } => {
                let completed = id.and_then(|id| self.complete_update(id));
                // The venue folds a repeated `subscribe` into the
                // connection's existing orderbook subscription and answers
                // with `ok`, not a fresh `subscribed`. Resolve those
                // acknowledgements here so the tracked set, the pending map
                // and the 15 s acknowledgement deadline stay consistent
                // with what the venue actually holds.
                if let Some(id) = id
                    && let Some(markets) = self.pending_subscribes.remove(&id)
                {
                    self.merge_subscription(sid, markets, full_tickers)?;
                }
                // The official `ok` example for update_subscription carries
                // `sid` and `seq`. Observing that sequence number prevents a
                // false gap on the next delta. An ack without a sequence
                // number is also tolerated for one skipped message.
                if let Some(sid) = sid.or_else(|| completed.as_ref().map(|pending| pending.sid)) {
                    self.seq.on_control(sid, seq);
                }
                if completed.is_some() {
                    self.transient_recoveries = 0;
                    self.rate_limit_backoff = RATE_LIMIT_BACKOFF_START;
                }
                if self.pending_subscribes.is_empty() && self.pending_updates.is_empty() {
                    self.pending_since = None;
                    self.reconcile().await?;
                }
                Ok(ignored_payload_frame(
                    kind,
                    &text,
                    received_at_us,
                    "command_ack",
                ))
            }
            KalshiMessage::Error { id, code, message } => {
                let pending_update = id.and_then(|id| self.pending_updates.remove(&id));
                if let Some(id) = id {
                    self.pending_subscribes.remove(&id);
                }
                if self.pending_subscribes.is_empty() && self.pending_updates.is_empty() {
                    self.pending_since = None;
                }

                match code {
                    // Per-subscription market limit. The server does not
                    // publish the limit, so shrink the batch and retry. The
                    // pending command was not applied to local state.
                    Some(26) => {
                        warn!(
                            code = 26,
                            batch_size = self.batch_size,
                            "Kalshi subscription market limit reached; shrinking batch"
                        );
                        self.note_recovery()?;
                        if self.batch_size <= 1 {
                            return Err(SessionFailure::protocol_with_frame(
                                anyhow::anyhow!(
                                    "Kalshi rejected a one-market subscription with code 26: {message}"
                                ),
                                RawFrame {
                                    kind,
                                    payload: text.as_bytes().to_vec(),
                                },
                                received_at_us,
                            ));
                        }
                        self.batch_size = (self.batch_size / 2).max(1);
                        self.reconcile().await?;
                        Ok(ignored_payload_frame(
                            kind,
                            &text,
                            received_at_us,
                            "handled_server_error",
                        ))
                    }
                    // Command rate limit: back off, then let reconcile retry
                    // the commands that are still required.
                    Some(27) => {
                        warn!(
                            code = 27,
                            backoff_ms = self.rate_limit_backoff.as_millis(),
                            "Kalshi command rate limit; backing off"
                        );
                        self.note_recovery()?;
                        tokio::time::sleep(self.rate_limit_backoff).await;
                        self.rate_limit_backoff =
                            (self.rate_limit_backoff * 2).min(RATE_LIMIT_BACKOFF_MAX);
                        self.reconcile().await?;
                        Ok(ignored_payload_frame(
                            kind,
                            &text,
                            received_at_us,
                            "handled_server_error",
                        ))
                    }
                    // Unknown sid, channel failure, or subscription buffer
                    // overflow are terminal for one subscription. Drop it and
                    // re-subscribe its markets under a fresh sid.
                    Some(7 | 10 | 25) => {
                        warn!(
                            code = code.unwrap_or_default(),
                            sid = ?pending_update.as_ref().map(|pending| pending.sid),
                            "Kalshi subscription failure; resubscribing that sid"
                        );
                        self.note_recovery()?;
                        if let Some(pending) = pending_update {
                            self.drop_subscription(pending.sid);
                        }
                        self.reconcile().await?;
                        Ok(ignored_payload_frame(
                            kind,
                            &text,
                            received_at_us,
                            "handled_server_error",
                        ))
                    }
                    _ => Err(SessionFailure::protocol_with_frame(
                        anyhow::anyhow!(
                            "Kalshi websocket error (code {}): {message}",
                            code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                        ),
                        RawFrame {
                            kind,
                            payload: text.as_bytes().to_vec(),
                        },
                        received_at_us,
                    )),
                }
            }
            KalshiMessage::Unsubscribed { sid, .. } => {
                if let Some(sid) = sid {
                    self.orderbook_subs
                        .retain(|subscription| subscription.sid != sid);
                    self.seq.forget(sid);
                }
                Ok(ignored_payload_frame(
                    kind,
                    &text,
                    received_at_us,
                    "unsubscribed",
                ))
            }
            KalshiMessage::Ignore { .. } => Ok(ignored_payload_frame(
                kind,
                &text,
                received_at_us,
                "ignored_event",
            )),
        }
    }

    async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
        ws::send_ping(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn request_snapshot(&mut self, symbols: &[String]) -> Result<(), SessionFailure> {
        let wanted: BTreeSet<&str> = symbols.iter().map(String::as_str).collect();
        let requests: Vec<(i64, Vec<String>)> = self
            .orderbook_subs
            .iter()
            .filter_map(|subscription| {
                let markets: Vec<String> = subscription
                    .markets
                    .iter()
                    .filter(|symbol| wanted.contains(symbol.as_str()))
                    .cloned()
                    .collect();
                (!markets.is_empty()).then_some((subscription.sid, markets))
            })
            .collect();
        for (sid, markets) in requests {
            self.send_update(UpdateAction::GetSnapshot, sid, &markets)
                .await?;
        }
        Ok(())
    }

    async fn close(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = ws::close(stream).await;
        }
        self.orderbook_subs.clear();
        self.pending_subscribes.clear();
        self.pending_updates.clear();
        self.pending_since = None;
        self.seq.clear();
        self.lifecycle_subscribed = false;
        self.lifecycle_sid = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey as _};
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

    use super::*;
    use crate::model::ExchangeEvent;

    type ServerWs = WebSocketStream<tokio::net::TcpStream>;

    fn test_auth() -> Arc<KalshiAuth> {
        // Deterministic test key: signing is exercised, but the mock server
        // does not verify the signature.
        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let pem = signing
            .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .unwrap();
        Arc::new(KalshiAuth::from_pem("test-key".to_owned(), &pem).unwrap())
    }

    async fn read_json(ws: &mut ServerWs) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("server read timed out")
                .expect("server stream ended")
                .expect("server websocket error");
            match message {
                Message::Text(text) => {
                    return serde_json::from_str(&text).expect("client sent non-JSON");
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected client frame: {other:?}"),
            }
        }
    }

    async fn send_json(ws: &mut ServerWs, value: Value) {
        ws.send(Message::Text(value.to_string().into()))
            .await
            .unwrap();
    }

    async fn accept(listener: TcpListener) -> ServerWs {
        let (stream, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_async(stream).await.unwrap()
    }

    /// Acknowledge the lifecycle and orderbook subscribe commands sent by
    /// `apply_subscriptions`, returning the orderbook sid.
    async fn acknowledge_initial_subscriptions(
        ws: &mut ServerWs,
        orderbook_sid: i64,
    ) -> (i64, Vec<String>) {
        let mut orderbook = None;
        for _ in 0..2 {
            let command = read_json(ws).await;
            assert_eq!(command["cmd"], "subscribe");
            let channel = command["params"]["channels"][0].as_str().unwrap();
            let id = command["id"].as_u64().unwrap();
            if channel == "orderbook_delta" {
                let markets: Vec<String> = command["params"]["market_tickers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap().to_owned())
                    .collect();
                orderbook = Some(markets);
                send_json(
                    ws,
                    json!({
                        "id": id,
                        "type": "subscribed",
                        "msg": {"channel": "orderbook_delta", "sid": orderbook_sid}
                    }),
                )
                .await;
            } else {
                send_json(
                    ws,
                    json!({
                        "id": id,
                        "type": "subscribed",
                        "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                    }),
                )
                .await;
            }
        }
        (orderbook_sid, orderbook.expect("orderbook subscribe"))
    }

    async fn test_session(address: std::net::SocketAddr) -> KalshiSession {
        let settings = KalshiSettings {
            batch_size: 100,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
    }

    async fn next_event(session: &mut KalshiSession) -> ExchangeEvent {
        for _ in 0..20 {
            let frame = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("client frame timed out")
                .expect("client frame failed");
            if let Some(event) = frame.events.into_iter().next() {
                return event;
            }
        }
        panic!("no data event arrived");
    }

    #[test]
    fn ok_sequence_bridges_a_control_command() {
        let mut tracker = SequenceTracker::default();
        assert!(tracker.on_snapshot(2, 1));
        tracker.on_control(2, Some(2));
        assert!(tracker.on_delta(2, 3).is_ok());
    }

    #[test]
    fn unnumbered_ack_bridges_one_skipped_delta() {
        let mut tracker = SequenceTracker::default();
        assert!(tracker.on_snapshot(2, 1));
        tracker.on_control(2, None);
        assert!(tracker.on_delta(2, 3).is_ok());
        // Only one message was accounted for.
        assert!(tracker.on_delta(2, 5).is_err());
    }

    #[test]
    fn real_sequence_gap_is_still_rejected() {
        let mut tracker = SequenceTracker::default();
        assert!(tracker.on_snapshot(2, 1));
        let error = tracker.on_delta(2, 3).unwrap_err();
        assert!(error.contains("expected 2, received 3"), "{error}");
    }

    #[test]
    fn snapshot_rebaselines_and_clears_stale_acks() {
        let mut tracker = SequenceTracker::default();
        assert!(tracker.on_snapshot(2, 1));
        tracker.on_control(2, None);
        // A fresh snapshot supersedes deltas that were missed before it.
        assert!(tracker.on_snapshot(2, 9));
        assert!(tracker.on_delta(2, 11).is_err());
        assert!(tracker.on_delta(2, 10).is_ok());
    }

    #[test]
    fn control_before_snapshot_does_not_create_a_baseline() {
        let mut tracker = SequenceTracker::default();
        tracker.on_control(2, Some(7));
        let error = tracker.on_delta(2, 8).unwrap_err();
        assert!(error.contains("before a snapshot"), "{error}");
    }

    #[tokio::test]
    async fn ok_frame_with_seq_does_not_create_a_false_gap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let (sid, _) = acknowledge_initial_subscriptions(&mut ws, 2).await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_snapshot", "sid": sid, "seq": 1,
                    "msg": {
                        "market_ticker": "M1", "clock": 1,
                        "yes_dollars_fp": [["0.5", "1"]]
                    }
                }),
            )
            .await;

            let update = read_json(&mut ws).await;
            assert_eq!(update["cmd"], "update_subscription");
            let id = update["id"].as_u64().unwrap();
            send_json(
                &mut ws,
                json!({
                    "id": id, "type": "ok", "sid": sid, "seq": 2,
                    "msg": {"market_tickers": ["M2"]}
                }),
            )
            .await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_delta", "sid": sid, "seq": 3,
                    "msg": {
                        "market_ticker": "M2", "clock": 2, "side": "yes",
                        "price_dollars": "0.5", "delta_fp": "1"
                    }
                }),
            )
            .await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        assert!(matches!(
            next_event(&mut session).await,
            ExchangeEvent::Snapshot { .. }
        ));
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned(), "M2".to_owned()]))
            .await
            .unwrap();
        let event = next_event(&mut session).await;
        assert!(
            matches!(event, ExchangeEvent::LevelChange { ref native_symbol, .. } if native_symbol == "M2"),
            "unexpected event: {event:?}"
        );
        assert_eq!(session.batch_size, 100);
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn code_7_resubscribes_the_affected_sid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let (sid, _) = acknowledge_initial_subscriptions(&mut ws, 2).await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_snapshot", "sid": sid, "seq": 1,
                    "msg": {"market_ticker": "M1", "clock": 1}
                }),
            )
            .await;

            let update = read_json(&mut ws).await;
            assert_eq!(update["cmd"], "update_subscription");
            send_json(
                &mut ws,
                json!({
                    "id": update["id"], "type": "error",
                    "msg": {"code": 7, "msg": "Unknown subscription ID"}
                }),
            )
            .await;

            // The session must create a replacement subscription containing
            // the original plus the added market.
            let retry = read_json(&mut ws).await;
            assert_eq!(retry["cmd"], "subscribe");
            let markets: Vec<String> = retry["params"]["market_tickers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(markets, vec!["M1".to_owned(), "M2".to_owned()]);
            send_json(
                &mut ws,
                json!({
                    "id": retry["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 3}
                }),
            )
            .await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_snapshot", "sid": 3, "seq": 1,
                    "msg": {"market_ticker": "M1", "clock": 1}
                }),
            )
            .await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        assert!(matches!(
            next_event(&mut session).await,
            ExchangeEvent::Snapshot { .. }
        ));
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned(), "M2".to_owned()]))
            .await
            .unwrap();
        // The error is handled internally; the next data event is a snapshot
        // from the replacement subscription.
        assert!(matches!(
            next_event(&mut session).await,
            ExchangeEvent::Snapshot { .. }
        ));
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn code_26_halves_the_batch_and_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            // Lifecycle subscribe first.
            let lifecycle = read_json(&mut ws).await;
            assert_eq!(lifecycle["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;

            // Initial four-market subscription is rejected by the market
            // limit, so the session must split it.
            let first = read_json(&mut ws).await;
            assert_eq!(first["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": first["id"], "type": "error",
                    "msg": {"code": 26, "msg": "Subscription market limit exceeded"}
                }),
            )
            .await;

            let mut chunks = Vec::new();
            for sid in [2i64, 3] {
                let retry = read_json(&mut ws).await;
                assert_eq!(retry["cmd"], "subscribe");
                let markets: Vec<String> = retry["params"]["market_tickers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap().to_owned())
                    .collect();
                chunks.push(markets.clone());
                send_json(
                    &mut ws,
                    json!({
                        "id": retry["id"], "type": "subscribed",
                        "msg": {"channel": "orderbook_delta", "sid": sid}
                    }),
                )
                .await;
            }
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].len(), 2);
            assert_eq!(chunks[1].len(), 2);
        });

        let settings = KalshiSettings {
            batch_size: 4,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from([
                "M1".to_owned(),
                "M2".to_owned(),
                "M3".to_owned(),
                "M4".to_owned(),
            ]))
            .await
            .unwrap();
        // Process the error and the two acknowledgements.
        for _ in 0..4 {
            let frame = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("frame timeout")
                .expect("frame failed");
            if frame.events.is_empty() {
                continue;
            }
        }
        assert_eq!(session.batch_size, 2);
        assert_eq!(session.orderbook_subs.len(), 2);
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn code_27_backs_off_then_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            let first = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": first["id"], "type": "error",
                    "msg": {"code": 27, "msg": "Too many requests"}
                }),
            )
            .await;
            let retry = read_json(&mut ws).await;
            assert_eq!(retry["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": retry["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 2}
                }),
            )
            .await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        // Skip the lifecycle acknowledgement, then observe the rate-limit
        // handler's backoff. It sleeps before returning the idle frame.
        let mut backed_off = false;
        for _ in 0..5 {
            let _ = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("backoff exceeded timeout")
                .expect("rate-limit error should not fail the session");
            if session.rate_limit_backoff > RATE_LIMIT_BACKOFF_START {
                backed_off = true;
                break;
            }
        }
        assert!(backed_off, "expected a rate-limit backoff");
        assert_eq!(session.rate_limit_backoff, RATE_LIMIT_BACKOFF_START * 2);
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn unknown_error_still_requires_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            let subscribe = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": subscribe["id"], "type": "error",
                    "msg": {"code": 99, "msg": "unexpected"}
                }),
            )
            .await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        let mut failure = None;
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_secs(5), session.next_frame()).await {
                Ok(Ok(_)) => continue,
                Ok(Err(error)) => {
                    failure = Some(error);
                    break;
                }
                Err(_) => panic!("frame timeout"),
            }
        }
        assert!(
            matches!(failure, Some(SessionFailure::Protocol { .. })),
            "unexpected failure: {failure:?}"
        );
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn server_disconnect_maps_to_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            ws.close(None).await.unwrap();
        });

        let mut session = test_session(address).await;
        let failure = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
            .await
            .expect("disconnect was not observed")
            .unwrap_err();
        assert!(
            matches!(failure, SessionFailure::Reconnect(_)),
            "unexpected failure: {failure:?}"
        );
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn protocol_rejection_preserves_raw_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gap_delta = json!({
            "type": "orderbook_delta", "sid": 7, "seq": 9,
            "msg": {"market_ticker": "M1", "side": "yes", "price": 55, "delta": 1}
        });
        let server_delta = gap_delta.clone();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            acknowledge_initial_subscriptions(&mut ws, 7).await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_snapshot", "sid": 7, "seq": 5,
                    "msg": {
                        "market_ticker": "M1", "clock": 1,
                        "yes_dollars_fp": [["0.5", "1"]]
                    }
                }),
            )
            .await;
            send_json(&mut ws, server_delta).await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        let snapshot = next_event(&mut session).await;
        assert!(matches!(snapshot, ExchangeEvent::Snapshot { .. }));
        let failure = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
            .await
            .expect("gap was not observed")
            .expect_err("a sequence gap must fail the session");
        match failure {
            SessionFailure::Protocol {
                frame: Some(raw), ..
            } => {
                assert_eq!(raw.kind, FrameKind::Text);
                assert_eq!(raw.payload, gap_delta.to_string().into_bytes());
            }
            other => panic!("unexpected failure: {other:?}"),
        }
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn repeated_subscribe_acked_with_ok_merges_into_the_connection_sid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            assert_eq!(lifecycle["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            // The first orderbook subscribe creates the sid; every repeated
            // subscribe folds into it and is acknowledged with `ok`, carrying
            // the venue's cumulative market list.
            let first = read_json(&mut ws).await;
            assert_eq!(first["cmd"], "subscribe");
            let first_markets: Vec<String> = first["params"]["market_tickers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(first_markets, vec!["M1".to_owned(), "M2".to_owned()]);
            send_json(
                &mut ws,
                json!({
                    "id": first["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 2}
                }),
            )
            .await;
            let second = read_json(&mut ws).await;
            assert_eq!(second["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": second["id"], "type": "ok", "sid": 2, "seq": 1,
                    "msg": {"market_tickers": ["M1", "M2", "M3", "M4"]}
                }),
            )
            .await;
            let third = read_json(&mut ws).await;
            assert_eq!(third["cmd"], "subscribe");
            send_json(
                &mut ws,
                json!({
                    "id": third["id"], "type": "ok", "sid": 2, "seq": 2,
                    "msg": {"market_tickers": ["M1", "M2", "M3", "M4", "M5"]}
                }),
            )
            .await;
        });

        let settings = KalshiSettings {
            batch_size: 2,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from([
                "M1".to_owned(),
                "M2".to_owned(),
                "M3".to_owned(),
                "M4".to_owned(),
                "M5".to_owned(),
            ]))
            .await
            .unwrap();

        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("frame timeout")
                .expect("acknowledgement handling failed");
        }

        assert_eq!(session.orderbook_subs.len(), 1);
        let tracked: Vec<String> = session.orderbook_subs[0].markets.iter().cloned().collect();
        assert_eq!(
            tracked,
            vec![
                "M1".to_owned(),
                "M2".to_owned(),
                "M3".to_owned(),
                "M4".to_owned(),
                "M5".to_owned(),
            ]
        );
        assert!(session.pending_subscribes.is_empty());
        assert!(session.pending_since.is_none());
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn merged_ok_without_sid_uses_the_single_orderbook_subscription() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            let first = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": first["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 2}
                }),
            )
            .await;
            // A venue that answers the repeated subscribe with an `ok`
            // carrying no sid at all still must not trip the acknowledgement
            // deadline or strand the pending command.
            let second = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": second["id"], "type": "ok", "seq": 9
                }),
            )
            .await;
        });

        let settings = KalshiSettings {
            batch_size: 1,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned(), "M2".to_owned()]))
            .await
            .unwrap();

        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("frame timeout")
                .expect("acknowledgement handling failed");
        }

        assert_eq!(session.orderbook_subs.len(), 1);
        let tracked: Vec<String> = session.orderbook_subs[0].markets.iter().cloned().collect();
        assert_eq!(tracked, vec!["M1".to_owned(), "M2".to_owned()]);
        assert!(session.pending_subscribes.is_empty());
        assert!(session.pending_since.is_none());
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn merged_ok_market_list_replaces_the_tracked_set_authoritatively() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            let first = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": first["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 2}
                }),
            )
            .await;
            let second = read_json(&mut ws).await;
            // The venue's list is the source of truth: it omits a market the
            // client believed was on the subscription.
            send_json(
                &mut ws,
                json!({
                    "id": second["id"], "type": "ok", "sid": 2, "seq": 1,
                    "msg": {"market_tickers": ["M2"]}
                }),
            )
            .await;
            // Losing M1 from the tracked set makes the next reconcile send a
            // fresh subscribe for it; the venue folds it into the same sid.
            let third = read_json(&mut ws).await;
            assert_eq!(third["cmd"], "subscribe");
            let resync: Vec<String> = third["params"]["market_tickers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(resync, vec!["M1".to_owned()]);
            send_json(
                &mut ws,
                json!({
                    "id": third["id"], "type": "ok", "sid": 2, "seq": 2,
                    "msg": {"market_tickers": ["M1", "M2"]}
                }),
            )
            .await;
        });

        let settings = KalshiSettings {
            batch_size: 1,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned(), "M2".to_owned()]))
            .await
            .unwrap();

        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("frame timeout")
                .expect("acknowledgement handling failed");
        }

        assert_eq!(session.orderbook_subs.len(), 1);
        let tracked: Vec<String> = session.orderbook_subs[0].markets.iter().cloned().collect();
        assert_eq!(tracked, vec!["M1".to_owned(), "M2".to_owned()]);
        assert!(session.pending_subscribes.is_empty());
        assert!(session.pending_since.is_none());
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn subscribed_with_an_existing_sid_merges_instead_of_failing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            let lifecycle = read_json(&mut ws).await;
            send_json(
                &mut ws,
                json!({
                    "id": lifecycle["id"], "type": "subscribed",
                    "msg": {"channel": "market_lifecycle_v2", "sid": 1}
                }),
            )
            .await;
            for _ in 0..2 {
                let command = read_json(&mut ws).await;
                send_json(
                    &mut ws,
                    json!({
                        "id": command["id"], "type": "subscribed",
                        "msg": {"channel": "orderbook_delta", "sid": 2}
                    }),
                )
                .await;
            }
        });

        let settings = KalshiSettings {
            batch_size: 1,
            ..KalshiSettings::default()
        };
        let mut session = KalshiSession::new(settings, test_auth(), ConnectOptions::default());
        session.ws_url = format!("ws://{address}");
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned(), "M2".to_owned()]))
            .await
            .unwrap();

        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("frame timeout")
                .expect("a repeated `subscribed` on an existing sid must not fail the session");
        }

        assert_eq!(session.orderbook_subs.len(), 1);
        let tracked: Vec<String> = session.orderbook_subs[0].markets.iter().cloned().collect();
        assert_eq!(tracked, vec!["M1".to_owned(), "M2".to_owned()]);
        assert!(session.pending_subscribes.is_empty());
        server.await.unwrap();
        session.close().await;
    }

    #[tokio::test]
    async fn stale_snapshot_is_reported_as_ignored_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stale = json!({
            "type": "orderbook_snapshot", "sid": 7, "seq": 5,
            "msg": {
                "market_ticker": "M1", "clock": 1,
                "yes_dollars_fp": [["0.5", "1"]]
            }
        });
        let server_stale = stale.clone();
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            acknowledge_initial_subscriptions(&mut ws, 7).await;
            send_json(
                &mut ws,
                json!({
                    "type": "orderbook_snapshot", "sid": 7, "seq": 10,
                    "msg": {
                        "market_ticker": "M1", "clock": 1,
                        "yes_dollars_fp": [["0.5", "1"]]
                    }
                }),
            )
            .await;
            send_json(&mut ws, server_stale).await;
        });

        let mut session = test_session(address).await;
        session
            .apply_subscriptions(&BTreeSet::from(["M1".to_owned()]))
            .await
            .unwrap();
        let snapshot = next_event(&mut session).await;
        assert!(matches!(snapshot, ExchangeEvent::Snapshot { .. }));
        let frame = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
            .await
            .expect("stale snapshot was not observed")
            .expect("a stale snapshot must not fail the session");
        assert!(frame.events.is_empty());
        assert_eq!(
            frame.decode,
            DecodeOutcome::ignored("stale_snapshot"),
            "the stale snapshot must carry an explicit ignored outcome"
        );
        let raw = frame.raw.expect("stale snapshot must preserve its payload");
        assert_eq!(raw.kind, FrameKind::Text);
        assert_eq!(raw.payload, stale.to_string().into_bytes());
        assert!(frame.received_at_us.is_some());
        server.await.unwrap();
        session.close().await;
    }
}
