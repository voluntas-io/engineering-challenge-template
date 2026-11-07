//! Measured discovery and websocket timestamp-latency experiment.
//!
//! If no timestamped frames arrive, percentiles are absent. Exchange
//! timestamps are compared with the local receive clock, so the result
//! includes any exchange/local clock offset. When more than one replica is
//! configured, identical events are matched across connections and their
//! arrival skew is reported separately. The methodology is documented in
//! `docs/latency.md`.

use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use serde::Serialize;
use tokio::{
    sync::{broadcast, mpsc},
    time::{MissedTickBehavior, interval, sleep_until, timeout},
};
use tracing::{info, warn};

use crate::{
    Side,
    config::Settings,
    coordinator::build_exchange,
    model::{EventMeta, ExchangeEvent, QuantityChange, SourceId},
    report::write_json_atomic,
    session::{MarketDataSession, SessionFailure, SessionFrame},
    time::now_us,
};

/// Message from a replica task to the measurement loop.
enum ReplicaEvent {
    Frame(usize, Result<SessionFrame, String>),
    /// Wall-clock milliseconds from the replica task start (immediately after
    /// connect and subscribe) to the first frame; sent once per replica,
    /// immediately before the frame itself.
    FirstFrame(usize, f64),
}

/// Startup period excluded from skew measurement. Initial snapshots are large,
/// arrive at different times on each connection, and would otherwise dominate
/// the distribution with connection-setup artefacts rather than steady-state
/// feed behaviour.
const SETTLE_SECS: u64 = 2;
/// Bound on pending cross-replica matches. A replica that never sees an event
/// must not grow the map without limit.
const MAX_PENDING_MATCHES: usize = 100_000;
/// Bound on recorded skew samples, matching the latency sample cap.
const MAX_SKEW_SAMPLES: usize = 1_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct LatencyReport {
    pub exchange: String,
    pub environment: String,
    pub measured_at_us: i64,
    pub discovery_rtt_ms: f64,
    pub discovered_markets: usize,
    pub subscribed_markets: usize,
    pub duration_secs: u64,
    pub settle_secs: u64,
    pub replicas: usize,
    pub tcp_nodelay: bool,
    pub source_ip: Option<String>,
    pub source_port: Option<u16>,
    pub connect_race: usize,
    pub frames: u64,
    pub events: u64,
    pub bytes: u64,
    pub replica_connect_ms: Vec<f64>,
    pub replica_first_frame_ms: Vec<Option<f64>>,
    pub latency_samples: u64,
    pub negative_latency_samples: u64,
    pub latency_p50_us: Option<i64>,
    pub latency_p95_us: Option<i64>,
    pub latency_p99_us: Option<i64>,
    pub latency_min_us: Option<i64>,
    pub latency_max_us: Option<i64>,
    pub latency_mean_us: Option<i64>,
    pub replica_events: Vec<u64>,
    pub replica_first_arrivals: Vec<u64>,
    pub replica_matched_events: u64,
    pub replica_abandoned_events: u64,
    pub replica_skew_samples: u64,
    pub replica_skew_p50_us: Option<i64>,
    pub replica_skew_p95_us: Option<i64>,
    pub replica_skew_p99_us: Option<i64>,
    pub replica_skew_min_us: Option<i64>,
    pub replica_skew_max_us: Option<i64>,
    pub replica_skew_mean_us: Option<i64>,
    pub error: Option<String>,
    pub errors: Vec<String>,
    pub notes: Vec<String>,
}

/// Measure discovery RTT and exchange-to-local timestamp latency for a short
/// live session. When `settings.collector.replicas > 1`, every replica opens
/// its own connection and the report includes cross-connection arrival skew.
pub async fn run(
    settings: Settings,
    max_markets: usize,
    duration_secs: u64,
    output: Option<&Path>,
) -> anyhow::Result<LatencyReport> {
    settings.validate()?;
    let network = settings.network.clone();
    let replica_count = settings.collector.replicas;
    let exchange = build_exchange(&settings, true)?;
    let discovery_started = Instant::now();
    let markets = exchange
        .discover_markets(max_markets)
        .await
        .with_context(|| format!("discovering {} markets for latency run", exchange.name()))?;
    let discovery_rtt_ms = discovery_started.elapsed().as_secs_f64() * 1_000.0;
    if markets.is_empty() {
        bail!("{} returned no markets for latency run", exchange.name());
    }
    let wanted = if max_markets == 0 {
        markets.len()
    } else {
        max_markets.min(markets.len())
    };
    let desired: std::collections::BTreeSet<String> = markets
        .iter()
        .take(wanted)
        .map(|market| market.native_symbol.clone())
        .collect();
    let factory = Arc::clone(&exchange).session_factory();

    let mut sessions: Vec<Box<dyn MarketDataSession>> = Vec::with_capacity(replica_count);
    let mut replica_connect_ms: Vec<f64> = Vec::with_capacity(replica_count);
    for replica in 0..replica_count {
        let mut session = factory
            .create_session(SourceId::new(0, replica as u16))
            .await
            .with_context(|| format!("creating latency session {replica}"))?;
        let connect_started = Instant::now();
        connect_with_retry(&mut session, replica).await?;
        replica_connect_ms.push(connect_started.elapsed().as_secs_f64() * 1_000.0);
        session
            .apply_subscriptions(&desired)
            .await
            .map_err(|failure| {
                anyhow::anyhow!(
                    "subscribing latency session {replica}: {}",
                    failure_text(&failure)
                )
            })?;
        sessions.push(session);
    }
    info!(
        exchange = exchange.name(),
        markets = wanted,
        replicas = replica_count,
        duration_secs,
        "latency sessions started"
    );

    let (frame_tx, mut frame_rx) = mpsc::channel::<ReplicaEvent>(4096);
    let (stop_tx, _) = broadcast::channel::<()>(1);
    let mut handles = Vec::with_capacity(replica_count);
    for (replica, session) in sessions.into_iter().enumerate() {
        handles.push(tokio::spawn(run_replica(
            replica,
            session,
            frame_tx.clone(),
            stop_tx.subscribe(),
        )));
    }
    drop(frame_tx);

    let mut frames = 0u64;
    let mut events = 0u64;
    let mut bytes = 0u64;
    let mut negative_latency_samples = 0u64;
    let mut samples: Vec<i64> = Vec::new();
    let mut replica_events = vec![0u64; replica_count];
    let mut first_frames: Vec<Option<f64>> = vec![None; replica_count];
    let mut errors: Vec<String> = Vec::new();
    let mut skew = SkewTracker::new(replica_count);
    let settle_secs = if replica_count > 1 { SETTLE_SECS } else { 0 };
    // The settle window discards startup frames, but the
    // first-frame timing is setup evidence and must survive it. Replica
    // transport errors during the window are retained.
    errors.extend(discard_startup_frames(&mut frame_rx, settle_secs, &mut first_frames).await);
    let measurement_started_us =
        now_us().context("reading wall clock at the start of latency measurement")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs.max(1));

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            received = frame_rx.recv() => {
                match received {
                    Some(ReplicaEvent::FirstFrame(replica, elapsed_ms)) => {
                        if let Some(slot) = first_frames.get_mut(replica) {
                            slot.get_or_insert(elapsed_ms);
                        }
                    }
                    Some(ReplicaEvent::Frame(replica, Ok(frame))) => {
                        if frame.bytes > 0 {
                            frames = frames.saturating_add(1);
                            bytes = bytes.saturating_add(frame.bytes as u64);
                        }
                        for event in &frame.events {
                            let Some(meta) = event_meta(event) else {
                                continue;
                            };
                            if meta.received_at_us < measurement_started_us {
                                continue;
                            }
                            events = events.saturating_add(1);
                            replica_events[replica] = replica_events[replica].saturating_add(1);
                            let Some(exchange_ts_us) = meta.exchange_ts_us else {
                                continue;
                            };
                            let latency = meta.received_at_us.saturating_sub(exchange_ts_us);
                            if latency < 0 {
                                negative_latency_samples =
                                    negative_latency_samples.saturating_add(1);
                            } else if latency <= 60_000_000 && samples.len() < 1_000_000 {
                                samples.push(latency);
                            }
                            if let Some(key) = event_key(event) {
                                skew.observe(key, replica, meta.received_at_us);
                            }
                        }
                    }
                    Some(ReplicaEvent::Frame(_, Err(error))) => {
                        errors.push(error);
                    }
                    None => break,
                }
            }
        }
    }

    let _ = stop_tx.send(());
    for handle in handles {
        if timeout(Duration::from_secs(5), handle).await.is_err() {
            warn!("latency replica task did not stop within five seconds");
        }
    }

    samples.sort_unstable();
    let latency_samples = samples.len() as u64;
    let mut skew_samples = std::mem::take(&mut skew.samples);
    skew_samples.sort_unstable();
    let skew_sample_count = skew_samples.len() as u64;
    let abandoned = skew
        .pending
        .len()
        .saturating_add(usize::try_from(skew.dropped).unwrap_or(usize::MAX));
    let report = LatencyReport {
        exchange: exchange.name().to_owned(),
        environment: exchange.environment().to_owned(),
        measured_at_us: now_us().unwrap_or(0),
        discovery_rtt_ms,
        discovered_markets: markets.len(),
        subscribed_markets: wanted,
        duration_secs: duration_secs.max(1),
        settle_secs,
        replicas: replica_count,
        tcp_nodelay: network.tcp_nodelay,
        source_ip: network.source_ip.map(|ip| ip.to_string()),
        source_port: network.source_port,
        connect_race: network.connect_race,
        frames,
        events,
        bytes,
        replica_connect_ms,
        replica_first_frame_ms: first_frames,
        latency_samples,
        negative_latency_samples,
        latency_p50_us: percentile(&samples, 50),
        latency_p95_us: percentile(&samples, 95),
        latency_p99_us: percentile(&samples, 99),
        latency_min_us: samples.first().copied(),
        latency_max_us: samples.last().copied(),
        latency_mean_us: mean(&samples),
        replica_events,
        replica_first_arrivals: skew.first_arrivals,
        replica_matched_events: skew.matched,
        replica_abandoned_events: u64::try_from(abandoned).unwrap_or(u64::MAX),
        replica_skew_samples: skew_sample_count,
        replica_skew_p50_us: percentile(&skew_samples, 50),
        replica_skew_p95_us: percentile(&skew_samples, 95),
        replica_skew_p99_us: percentile(&skew_samples, 99),
        replica_skew_min_us: skew_samples.first().copied(),
        replica_skew_max_us: skew_samples.last().copied(),
        replica_skew_mean_us: mean(&skew_samples),
        error: errors.first().cloned(),
        errors,
        notes: vec![
            "discovery_rtt_ms is a local wall-clock measurement around one discovery call".to_owned(),
            "latency is local_receive_us - exchange_timestamp_us and includes exchange/local clock offset".to_owned(),
            "negative values are counted separately and excluded from percentiles".to_owned(),
            "no latency value is reported when no timestamped events arrived".to_owned(),
            "replica_connect_ms and replica_first_frame_ms are local wall-clock measurements of session setup and subscription-to-first-frame latency".to_owned(),
            format!(
                "replica skew compares the same symbol, exchange timestamp, clock and event shape across {replica_count} connection(s); each delta is relative to the earliest arrival"
            ),
            format!(
                "the first {settle_secs} second(s) after all subscriptions are established are excluded so initial snapshots do not dominate the skew distribution"
            ),
            "identical duplicate events at the same exchange timestamp and shape can be coalesced; unmatched events are counted as abandoned".to_owned(),
            "a one-replica run reports no skew samples; skew requires at least two independent connections".to_owned(),
        ],
    };
    if let Some(path) = output {
        write_json_atomic(path, &report).context("writing latency report")?;
    }
    if let Some(error) = &report.error {
        warn!(error, "latency run ended with a replica transport error");
    }
    Ok(report)
}

/// Connect one latency session with the same bounded retry the live collector
/// uses, because the venue edge intermittently rejects individual handshakes
/// (HTTP 525). A fatal failure such as an authentication rejection is never
/// retried. The measurement window only starts after every session is
/// connected, so a retried handshake cannot bias any reported value.
async fn connect_with_retry(
    session: &mut Box<dyn MarketDataSession>,
    replica: usize,
) -> anyhow::Result<()> {
    let mut last_failure = None;
    for attempt in 0..3u32 {
        match session.connect().await {
            Ok(()) => return Ok(()),
            Err(SessionFailure::Fatal(error)) => {
                return Err(anyhow::anyhow!(
                    "connecting latency session {replica}: {error:#}"
                ));
            }
            Err(failure) => {
                warn!(
                    replica,
                    attempt = attempt + 1,
                    error = %failure_text(&failure),
                    "latency session connect failed; retrying"
                );
                last_failure = Some(failure_text(&failure));
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(1_000 << attempt)).await;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "connecting latency session {replica} failed after three attempts: {}",
        last_failure.unwrap_or_else(|| "unknown failure".to_owned())
    ))
}

async fn discard_startup_frames(
    frames: &mut mpsc::Receiver<ReplicaEvent>,
    settle_secs: u64,
    first_frames: &mut [Option<f64>],
) -> Vec<String> {
    let mut errors = Vec::new();
    if settle_secs == 0 {
        return errors;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(settle_secs);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, frames.recv()).await {
        match event {
            ReplicaEvent::FirstFrame(replica, elapsed_ms) => {
                if let Some(slot) = first_frames.get_mut(replica) {
                    slot.get_or_insert(elapsed_ms);
                }
            }
            ReplicaEvent::Frame(_, Err(error)) => errors.push(error),
            ReplicaEvent::Frame(_, Ok(_)) => {}
        }
    }
    while let Ok(event) = frames.try_recv() {
        match event {
            ReplicaEvent::FirstFrame(replica, elapsed_ms) => {
                if let Some(slot) = first_frames.get_mut(replica) {
                    slot.get_or_insert(elapsed_ms);
                }
            }
            ReplicaEvent::Frame(_, Err(error)) => errors.push(error),
            ReplicaEvent::Frame(_, Ok(_)) => {}
        }
    }
    errors
}

async fn send_frame(
    frames: &mpsc::Sender<ReplicaEvent>,
    stop: &mut broadcast::Receiver<()>,
    value: ReplicaEvent,
) -> bool {
    tokio::select! {
        _ = stop.recv() => false,
        result = frames.send(value) => result.is_ok(),
    }
}

async fn run_replica(
    replica: usize,
    mut session: Box<dyn MarketDataSession>,
    frames: mpsc::Sender<ReplicaEvent>,
    mut stop: broadcast::Receiver<()>,
) {
    let started = Instant::now();
    let mut first_frame_sent = false;
    let mut keepalive = interval(Duration::from_secs(10));
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    keepalive.tick().await;
    loop {
        tokio::select! {
            _ = stop.recv() => break,
            _ = keepalive.tick() => {
                if let Err(failure) = session.send_keepalive().await {
                    let _ = send_frame(
                        &frames,
                        &mut stop,
                        ReplicaEvent::Frame(
                            replica,
                            Err(format!("keepalive failed: {}", failure_text(&failure))),
                        ),
                    )
                    .await;
                    break;
                }
            }
            frame = session.next_frame() => {
                let message = match frame {
                    Ok(frame) => Ok(frame),
                    Err(failure) => Err(format!("websocket read failed: {}", failure_text(&failure))),
                };
                let terminal = message.is_err();
                if !first_frame_sent && message.is_ok() {
                    first_frame_sent = true;
                    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
                    if !send_frame(&frames, &mut stop, ReplicaEvent::FirstFrame(replica, elapsed_ms))
                        .await
                    {
                        break;
                    }
                }
                let sent = send_frame(&frames, &mut stop, ReplicaEvent::Frame(replica, message)).await;
                if terminal || !sent {
                    break;
                }
            }
        }
    }
    session.close().await;
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct EventKey {
    symbol: String,
    exchange_ts_us: i64,
    clock: Option<i64>,
    detail: EventDetail,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum EventDetail {
    Snapshot,
    Level {
        side: Side,
        price: i64,
        change: (u8, i64),
    },
    Lifecycle,
}

fn event_key(event: &ExchangeEvent) -> Option<EventKey> {
    let (native_symbol, meta, detail) = match event {
        ExchangeEvent::Snapshot {
            native_symbol,
            meta,
            ..
        } => (native_symbol, meta, EventDetail::Snapshot),
        ExchangeEvent::LevelChange {
            native_symbol,
            side,
            price,
            change,
            meta,
        } => (
            native_symbol,
            meta,
            EventDetail::Level {
                side: *side,
                price: price.raw(),
                change: match change {
                    QuantityChange::Absolute(quantity) => (0, quantity.raw()),
                    QuantityChange::Delta(quantity) => (1, quantity.raw()),
                },
            },
        ),
        ExchangeEvent::Lifecycle {
            native_symbol,
            meta,
            ..
        } => (native_symbol, meta, EventDetail::Lifecycle),
        ExchangeEvent::StreamReset { .. } => return None,
    };
    Some(EventKey {
        symbol: native_symbol.clone(),
        exchange_ts_us: meta.exchange_ts_us?,
        clock: meta.clock,
        detail,
    })
}

struct SkewTracker {
    replicas: usize,
    pending: HashMap<EventKey, Vec<Option<i64>>>,
    order: VecDeque<EventKey>,
    first_arrivals: Vec<u64>,
    matched: u64,
    dropped: u64,
    samples: Vec<i64>,
}

impl SkewTracker {
    fn new(replicas: usize) -> Self {
        Self {
            replicas,
            pending: HashMap::new(),
            order: VecDeque::new(),
            first_arrivals: vec![0; replicas],
            matched: 0,
            dropped: 0,
            samples: Vec::new(),
        }
    }

    fn observe(&mut self, key: EventKey, replica: usize, received_at_us: i64) {
        if self.replicas < 2 || replica >= self.replicas {
            return;
        }
        if !self.pending.contains_key(&key) {
            while self.pending.len() >= MAX_PENDING_MATCHES {
                let Some(oldest) = self.order.pop_front() else {
                    break;
                };
                if self.pending.remove(&oldest).is_some() {
                    self.dropped = self.dropped.saturating_add(1);
                }
            }
            self.pending.insert(key.clone(), vec![None; self.replicas]);
            self.order.push_back(key.clone());
            if self.order.len() > MAX_PENDING_MATCHES.saturating_mul(2) {
                self.order
                    .retain(|candidate| self.pending.contains_key(candidate));
            }
        }
        let Some(arrivals) = self.pending.get_mut(&key) else {
            return;
        };
        if arrivals[replica].is_some() {
            return;
        }
        arrivals[replica] = Some(received_at_us);
        if !arrivals.iter().all(Option::is_some) {
            return;
        }
        let times: Vec<i64> = arrivals.iter().filter_map(|value| *value).collect();
        let earliest = times.iter().copied().min().unwrap_or(received_at_us);
        let winner = times.iter().position(|value| *value == earliest);
        if let Some(winner) = winner {
            self.first_arrivals[winner] = self.first_arrivals[winner].saturating_add(1);
        }
        for (index, time) in times.iter().enumerate() {
            if Some(index) == winner || self.samples.len() >= MAX_SKEW_SAMPLES {
                continue;
            }
            self.samples.push(time.saturating_sub(earliest));
        }
        self.matched = self.matched.saturating_add(1);
        self.pending.remove(&key);
    }
}

fn event_meta(event: &ExchangeEvent) -> Option<&EventMeta> {
    match event {
        ExchangeEvent::Snapshot { meta, .. }
        | ExchangeEvent::LevelChange { meta, .. }
        | ExchangeEvent::Lifecycle { meta, .. } => Some(meta),
        ExchangeEvent::StreamReset { .. } => None,
    }
}

fn percentile(sorted: &[i64], percentile: u32) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let len = sorted.len() as u64;
    let rank = len.saturating_mul(u64::from(percentile)).saturating_add(99) / 100;
    let index = rank.saturating_sub(1).min(len.saturating_sub(1));
    sorted.get(usize::try_from(index).ok()?).copied()
}

fn mean(samples: &[i64]) -> Option<i64> {
    if samples.is_empty() {
        return None;
    }
    let sum: i128 = samples.iter().map(|value| i128::from(*value)).sum();
    i64::try_from(sum / samples.len() as i128).ok()
}

fn failure_text(failure: &SessionFailure) -> String {
    match failure {
        SessionFailure::Reconnect(error)
        | SessionFailure::Protocol { error, .. }
        | SessionFailure::Fatal(error) => format!("{error:#}"),
        SessionFailure::Decode { error, .. } => format!("{error:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventMeta, Price, Quantity};

    #[test]
    fn percentile_is_nearest_rank_without_extrapolation() {
        assert_eq!(percentile(&[], 50), None);
        assert_eq!(percentile(&[5], 99), Some(5));
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), Some(3));
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 95), Some(5));
    }

    fn level_event(symbol: &str, timestamp: i64, price: i64, quantity: i64) -> ExchangeEvent {
        ExchangeEvent::LevelChange {
            native_symbol: symbol.to_owned(),
            side: Side::Buy,
            price: Price::from_raw(price),
            change: QuantityChange::Absolute(Quantity::new(quantity).unwrap()),
            meta: EventMeta {
                received_at_us: timestamp,
                exchange_ts_us: Some(timestamp),
                seq: None,
                clock: Some(timestamp),
                connection_epoch: 0,
                receive_seq: 0,
            },
        }
    }

    #[test]
    fn matches_identical_events_and_reports_arrival_skew() {
        let mut tracker = SkewTracker::new(2);
        let key = event_key(&level_event("A", 10, 100, 5)).unwrap();
        tracker.observe(key.clone(), 0, 1_000);
        assert_eq!(tracker.matched, 0);
        tracker.observe(key, 1, 1_250);
        assert_eq!(tracker.matched, 1);
        assert_eq!(tracker.first_arrivals, vec![1, 0]);
        assert_eq!(tracker.samples, vec![250]);
        assert!(tracker.pending.is_empty());
    }

    #[test]
    fn different_event_shapes_do_not_match() {
        let mut tracker = SkewTracker::new(2);
        let first = event_key(&level_event("A", 10, 100, 5)).unwrap();
        let second = event_key(&level_event("A", 10, 101, 5)).unwrap();
        tracker.observe(first, 0, 1_000);
        tracker.observe(second, 1, 1_250);
        assert_eq!(tracker.matched, 0);
        assert_eq!(tracker.pending.len(), 2);
    }

    #[test]
    fn duplicate_arrivals_do_not_double_count() {
        let mut tracker = SkewTracker::new(2);
        let key = event_key(&level_event("A", 10, 100, 5)).unwrap();
        tracker.observe(key.clone(), 0, 1_000);
        tracker.observe(key.clone(), 0, 1_100);
        tracker.observe(key, 1, 1_250);
        assert_eq!(tracker.matched, 1);
        assert_eq!(tracker.samples, vec![250]);
    }

    #[test]
    fn one_replica_never_produces_skew_samples() {
        let mut tracker = SkewTracker::new(1);
        let key = event_key(&level_event("A", 10, 100, 5)).unwrap();
        tracker.observe(key, 0, 1_000);
        assert_eq!(tracker.matched, 0);
        assert!(tracker.samples.is_empty());
        assert!(tracker.pending.is_empty());
    }

    #[tokio::test]
    async fn discard_startup_frames_retains_replica_errors() {
        let (tx, mut rx) = mpsc::channel::<ReplicaEvent>(16);
        tx.send(ReplicaEvent::FirstFrame(0, 42.0)).await.unwrap();
        tx.send(ReplicaEvent::Frame(0, Ok(SessionFrame::idle(10))))
            .await
            .unwrap();
        tx.send(ReplicaEvent::Frame(
            0,
            Err("websocket read failed: boom".to_owned()),
        ))
        .await
        .unwrap();
        drop(tx);

        let mut first_frames = vec![None; 1];
        let errors = discard_startup_frames(&mut rx, 1, &mut first_frames).await;

        assert_eq!(first_frames, vec![Some(42.0)]);
        assert_eq!(errors, vec!["websocket read failed: boom".to_owned()]);
        // Startup measurement frames are discarded.
        assert!(rx.recv().await.is_none());
    }
}
