//! Generic websocket supervisor. Exchange adapters implement
//! [`MarketDataSession`]; this module owns reconnects, heartbeats, status
//! reporting, and command handling.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tracing::{info, warn};

use crate::{
    config::CollectorSettings,
    journal::{DecodeOutcome, JournalHandle, RawFrame},
    model::{Control, ExchangeEvent, ResetReason, SourceId, SourceStatus},
    time::now_us,
};

/// A batch of normalized events decoded from one websocket payload.
#[derive(Debug, Default)]
pub struct SessionFrame {
    pub events: Vec<ExchangeEvent>,
    pub bytes: usize,
    /// Exact payload, retained for the raw-frame journal. Transport heartbeat
    /// frames that carry no application payload leave this empty.
    pub raw: Option<RawFrame>,
    pub decode: DecodeOutcome,
    pub received_at_us: Option<i64>,
}

impl SessionFrame {
    #[must_use]
    pub fn idle(bytes: usize) -> Self {
        Self {
            events: Vec::new(),
            bytes,
            raw: None,
            decode: DecodeOutcome::default(),
            received_at_us: None,
        }
    }

    #[must_use]
    pub fn data(
        events: Vec<ExchangeEvent>,
        bytes: usize,
        raw: RawFrame,
        decode: DecodeOutcome,
        received_at_us: i64,
    ) -> Self {
        Self {
            events,
            bytes,
            raw: Some(raw),
            decode,
            received_at_us: Some(received_at_us),
        }
    }
}

/// Failure classification for an exchange session.
#[derive(Debug)]
pub enum SessionFailure {
    /// Transient transport failure: drop this socket, reconnect with backoff.
    Reconnect(anyhow::Error),
    /// The exchange sent data that could not be decoded. The raw payload is
    /// retained when the adapter reached the payload bytes.
    Decode {
        error: anyhow::Error,
        frame: Option<RawFrame>,
        received_at_us: i64,
    },
    /// A decoded frame violated the exchange protocol (sequence gap, server
    /// error). The raw payload is retained when one was read, so protocol
    /// rejects are journaled exactly like decode failures.
    Protocol {
        error: anyhow::Error,
        frame: Option<RawFrame>,
        received_at_us: i64,
    },
    /// Configuration or authentication failure: retrying cannot help.
    Fatal(anyhow::Error),
}

impl SessionFailure {
    /// A protocol violation observed before any payload was read (for
    /// example, a pending-command acknowledgement timeout).
    #[must_use]
    pub fn protocol(error: anyhow::Error) -> Self {
        Self::Protocol {
            error,
            frame: None,
            received_at_us: crate::time::now_us().unwrap_or(0),
        }
    }

    /// A protocol violation with the exact payload that triggered it.
    #[must_use]
    pub fn protocol_with_frame(error: anyhow::Error, frame: RawFrame, received_at_us: i64) -> Self {
        Self::Protocol {
            error,
            frame: Some(frame),
            received_at_us,
        }
    }
}

impl From<anyhow::Error> for SessionFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Reconnect(error)
    }
}

/// One websocket connection implementing an exchange protocol.
#[async_trait]
pub trait MarketDataSession: Send {
    async fn connect(&mut self) -> Result<(), SessionFailure>;
    /// Reconcile the server-side subscription with `desired`. Called after each
    /// successful connect and whenever the desired set changes.
    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure>;
    /// Read exactly one websocket payload (or a control frame) and decode it.
    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure>;
    async fn send_keepalive(&mut self) -> Result<(), SessionFailure>;
    /// Ask the exchange for fresh snapshots without changing membership when
    /// supported; otherwise resubscribe the symbols.
    async fn request_snapshot(&mut self, symbols: &[String]) -> Result<(), SessionFailure>;
    async fn close(&mut self);
}

/// Creates sessions for a source. Implemented by each exchange adapter.
#[async_trait]
pub trait SessionFactory: Send + Sync + 'static {
    async fn create_session(&self, source: SourceId) -> anyhow::Result<Box<dyn MarketDataSession>>;
}

/// Events sent from a source worker to the coordinator.
#[derive(Debug)]
pub enum WorkerEvent {
    Events {
        source: SourceId,
        events: Vec<ExchangeEvent>,
    },
    Status(SourceStatus),
    Fatal {
        source: SourceId,
        error: String,
    },
    /// Confirmation that a recovery control was applied. The receive fence is
    /// the worker's last read `(connection_epoch, receive_seq)` at that
    /// moment. Snapshot events sent later by this worker carry a strictly
    /// greater fence, so the core can reject frames read before the request.
    RecoveryReady {
        source: SourceId,
        generation: u64,
        connection_epoch: u64,
        receive_seq: u64,
    },
    Stopped {
        source: SourceId,
    },
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub keepalive_interval: Duration,
    pub status_interval: Duration,
    pub idle_timeout: Duration,
    pub reconnect_after: Duration,
    pub reconnect_base_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    /// Shared across every worker so the process never exceeds the configured
    /// number of simultaneous websocket handshakes.
    pub connect_permits: Arc<Semaphore>,
    /// Raw-frame journal shared by every source worker.
    pub journal: Option<JournalHandle>,
}

impl WorkerConfig {
    #[must_use]
    pub fn from_settings(settings: &CollectorSettings) -> Self {
        Self {
            keepalive_interval: Duration::from_secs(10),
            status_interval: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(settings.connection_idle_timeout_secs),
            reconnect_after: Duration::from_secs(settings.reconnect_after_secs),
            reconnect_base_backoff: Duration::from_millis(500),
            reconnect_max_backoff: Duration::from_secs(30),
            connect_permits: Arc::new(Semaphore::new(settings.connect_concurrency.max(1))),
            journal: None,
        }
    }
}

/// Supervisor task for one source.
pub async fn run_worker(
    factory: Arc<dyn SessionFactory>,
    source: SourceId,
    mut commands: mpsc::Receiver<Control>,
    events: mpsc::Sender<WorkerEvent>,
    config: WorkerConfig,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut desired = BTreeSet::new();
    let mut status = SourceStatus::new(source);
    let mut session: Option<Box<dyn MarketDataSession>> = None;
    let mut backoff = config.reconnect_base_backoff;
    let mut attempts = 0u64;
    // Local receive fence. The epoch changes on every successful connection;
    // the sequence is reset and then advanced for every frame read, before any
    // decoding or filtering can hide a frame from recovery ordering.
    let mut connection_epoch = 0u64;
    let mut receive_seq = 0u64;

    loop {
        if session.is_none() {
            match factory.create_session(source).await {
                Ok(new_session) => session = Some(new_session),
                Err(error) => {
                    let message = format!("{error:#}");
                    warn!(%source, error = %message, "failed to construct session");
                    let _ = events
                        .send(WorkerEvent::Fatal {
                            source,
                            error: message,
                        })
                        .await;
                    break;
                }
            }
        }

        let Some(active) = session.as_mut() else {
            break;
        };
        let permit = tokio::select! {
            permit = config.connect_permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => {
                    close_and_stop(session.take(), &mut status, &events).await;
                    return;
                }
            },
            _ = shutdown.recv() => {
                close_and_stop(session.take(), &mut status, &events).await;
                return;
            }
        };
        let connect_result = active.connect().await;
        drop(permit);
        if let Err(failure) = connect_result {
            match failure {
                SessionFailure::Fatal(error) => {
                    let message = format!("{error:#}");
                    warn!(%source, error = %message, "fatal session error");
                    let _ = events
                        .send(WorkerEvent::Fatal {
                            source,
                            error: message,
                        })
                        .await;
                    break;
                }
                SessionFailure::Reconnect(error) | SessionFailure::Protocol { error, .. } => {
                    warn!(%source, error = %format!("{error:#}"), "connect failed");
                    status.last_error = Some(truncate_error(&error));
                    status.connected = false;
                    let _ = events.send(WorkerEvent::Status(status.clone())).await;
                }
                SessionFailure::Decode { error, .. } => {
                    warn!(%source, error = %format!("{error:#}"), "connect failed");
                    status.last_error = Some(truncate_error(&error));
                    status.connected = false;
                    let _ = events.send(WorkerEvent::Status(status.clone())).await;
                }
            }
            if let Some(mut failed) = session.take() {
                failed.close().await;
            }
            attempts = attempts.saturating_add(1);
            backoff = next_backoff(backoff, config.reconnect_max_backoff);
            if !backoff_or_stop(
                &mut commands,
                &mut desired,
                &events,
                &mut status,
                &mut shutdown,
                backoff,
                connection_epoch,
                receive_seq,
            )
            .await
            {
                close_and_stop(session.take(), &mut status, &events).await;
                return;
            }
            continue;
        }

        attempts = attempts.saturating_add(1);
        if attempts > 1 {
            status.reconnects = status.reconnects.saturating_add(1);
        }
        connection_epoch = connection_epoch.saturating_add(1);
        receive_seq = 0;
        status.connected = true;
        status.subscriptions = desired.len();
        status.last_error = None;
        let now = Instant::now();
        let next_periodic_reconnect =
            now + config.reconnect_after + Duration::from_millis(rand::random::<u64>() % 60_000);
        let mut idle_deadline = now + config.idle_timeout;
        let mut next_keepalive = now + config.keepalive_interval;
        let mut next_status = now + config.status_interval;

        if !desired.is_empty() {
            let reset = ExchangeEvent::StreamReset {
                native_symbols: desired.iter().cloned().collect(),
                reason: ResetReason::Connected,
                received_at_us: now_us().unwrap_or(0),
            };
            if events
                .send(WorkerEvent::Events {
                    source,
                    events: vec![reset],
                })
                .await
                .is_err()
            {
                break;
            }
        }
        if let Err(failure) = active.apply_subscriptions(&desired).await {
            warn!(%source, error = %format!("{failure:?}"), "subscription reconciliation failed");
            status.connected = false;
            status.subscriptions = 0;
            status.last_error = Some(failure_message(&failure));
            let _ = events.send(WorkerEvent::Status(status.clone())).await;
            if let Some(mut failed) = session.take() {
                failed.close().await;
            }
            attempts = attempts.saturating_add(1);
            backoff = next_backoff(backoff, config.reconnect_max_backoff);
            if !backoff_or_stop(
                &mut commands,
                &mut desired,
                &events,
                &mut status,
                &mut shutdown,
                backoff,
                connection_epoch,
                receive_seq,
            )
            .await
            {
                close_and_stop(session.take(), &mut status, &events).await;
                return;
            }
            continue;
        }
        status.subscriptions = desired.len();
        status.reconnects = status.reconnects.max(attempts.saturating_sub(1));
        backoff = config.reconnect_base_backoff;
        let _ = events.send(WorkerEvent::Status(status.clone())).await;
        info!(%source, subscriptions = desired.len(), "source connected");

        let mut must_reconnect = false;
        let mut should_stop = false;
        while !must_reconnect && !should_stop {
            // Drain already-queued controls before polling the socket. This
            // keeps subscription changes responsive even when frames arrive
            // continuously, without letting a ready frame be discarded.
            loop {
                match commands.try_recv() {
                    Ok(Control::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) => {
                        should_stop = true;
                        break;
                    }
                    Ok(Control::Reconnect(reason)) => {
                        warn!(%source, reason, "reconnect requested");
                        must_reconnect = true;
                        break;
                    }
                    Ok(action) => {
                        must_reconnect = handle_control(
                            action,
                            &mut desired,
                            &mut status,
                            &mut session,
                            &events,
                            connection_epoch,
                            receive_seq,
                        )
                        .await;
                        if must_reconnect {
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                }
            }
            if must_reconnect || should_stop {
                break;
            }

            let now = Instant::now();
            if now >= next_keepalive {
                if let Some(active) = session.as_mut()
                    && let Err(failure) = active.send_keepalive().await
                {
                    warn!(%source, error = %format!("{failure:?}"), "keepalive failed");
                    status.last_error = Some(failure_message(&failure));
                    must_reconnect = true;
                }
                status.last_keepalive_us = now_us().ok();
                next_keepalive = now + config.keepalive_interval;
            }
            if now >= next_status {
                let _ = events.send(WorkerEvent::Status(status.clone())).await;
                next_status = now + config.status_interval;
            }
            if now >= next_periodic_reconnect {
                info!(%source, "periodic websocket refresh");
                must_reconnect = true;
            }
            if now >= idle_deadline {
                warn!(%source, "websocket idle timeout");
                status.last_error = Some("idle timeout".to_owned());
                must_reconnect = true;
            }
            if must_reconnect || should_stop {
                break;
            }

            let wake = next_keepalive
                .min(next_status)
                .min(next_periodic_reconnect)
                .min(idle_deadline);
            let mut pending_control: Option<Control> = None;
            tokio::select! {
                // Prefer a ready frame. A dropped poll loses no data: the
                // read state lives in the session's transport, and a decoded
                // data frame is returned without further await points.
                biased;
                frame = read_next_frame(&mut session) => {
                    match frame {
                        Ok(mut frame) => {
                            let received_at_us = frame.received_at_us.unwrap_or_else(|| {
                                now_us().unwrap_or(0)
                            });
                            // One payload read, one fence advance, one journal
                            // record — before any decode or protocol decision.
                            if !journal_ingress(
                                source,
                                connection_epoch,
                                &mut receive_seq,
                                &mut status,
                                config.journal.as_ref(),
                                &events,
                                frame.raw.take(),
                                received_at_us,
                                frame.decode.clone(),
                            )
                            .await
                            {
                                should_stop = true;
                                break;
                            }
                            stamp_frame(&mut frame, connection_epoch, receive_seq);
                            status.messages = status.messages.saturating_add(1);
                            status.last_frame_us = now_us().ok();
                            idle_deadline = Instant::now() + config.idle_timeout;
                            update_latency(&mut status, &frame.events);
                            if !frame.events.is_empty()
                                && events
                                    .send(WorkerEvent::Events {
                                        source,
                                        events: frame.events,
                                    })
                                    .await
                                    .is_err()
                            {
                                should_stop = true;
                            }
                        }
                        Err(failure) => {
                            match failure {
                                SessionFailure::Fatal(error) => {
                                    let message = format!("{error:#}");
                                    warn!(%source, error = %message, "fatal session error");
                                    let _ = events.send(WorkerEvent::Fatal {
                                        source,
                                        error: message,
                                    }).await;
                                    should_stop = true;
                                }
                                SessionFailure::Decode {
                                    error,
                                    frame,
                                    received_at_us,
                                } => {
                                    warn!(%source, error = %format!("{error:#}"), "websocket decode failed");
                                    status.messages = status.messages.saturating_add(1);
                                    status.decode_errors = status.decode_errors.saturating_add(1);
                                    status.last_error = Some(truncate_error(&error));
                                    // The payload is journaled with its stable
                                    // rejection category even though decoding
                                    // failed.
                                    if !journal_ingress(
                                        source,
                                        connection_epoch,
                                        &mut receive_seq,
                                        &mut status,
                                        config.journal.as_ref(),
                                        &events,
                                        frame,
                                        received_at_us,
                                        DecodeOutcome::rejected(decode_error_category(&error)),
                                    )
                                    .await
                                    {
                                        should_stop = true;
                                        break;
                                    }
                                    must_reconnect = true;
                                }
                                SessionFailure::Protocol {
                                    error,
                                    frame,
                                    received_at_us,
                                } => {
                                    warn!(%source, error = %format!("{error:#}"), "websocket protocol error");
                                    status.protocol_errors = status.protocol_errors.saturating_add(1);
                                    status.last_error = Some(truncate_error(&error));
                                    // A protocol reject is still a payload the
                                    // exchange sent: it advances the fence and
                                    // is journaled with its exact bytes.
                                    if !journal_ingress(
                                        source,
                                        connection_epoch,
                                        &mut receive_seq,
                                        &mut status,
                                        config.journal.as_ref(),
                                        &events,
                                        frame,
                                        received_at_us,
                                        DecodeOutcome::rejected("protocol_violation"),
                                    )
                                    .await
                                    {
                                        should_stop = true;
                                        break;
                                    }
                                    must_reconnect = true;
                                }
                                SessionFailure::Reconnect(error) => {
                                    warn!(%source, error = %format!("{error:#}"), "websocket read failed");
                                    status.last_error = Some(truncate_error(&error));
                                    must_reconnect = true;
                                }
                            }
                        }
                    }
                }
                _ = shutdown.recv() => {
                    should_stop = true;
                }
                command = commands.recv() => {
                    match command {
                        None | Some(Control::Shutdown) => should_stop = true,
                        Some(Control::Reconnect(reason)) => {
                            warn!(%source, reason, "reconnect requested");
                            must_reconnect = true;
                        }
                        Some(action) => pending_control = Some(action),
                    }
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {}
            }

            if let Some(action) = pending_control {
                must_reconnect = handle_control(
                    action,
                    &mut desired,
                    &mut status,
                    &mut session,
                    &events,
                    connection_epoch,
                    receive_seq,
                )
                .await;
            }
        }

        if should_stop {
            close_and_stop(session.take(), &mut status, &events).await;
            return;
        }

        status.connected = false;
        status.subscriptions = 0;
        let _ = events.send(WorkerEvent::Status(status.clone())).await;
        if let Some(mut old) = session.take() {
            old.close().await;
        }
        backoff = next_backoff(backoff, config.reconnect_max_backoff);
        if !backoff_or_stop(
            &mut commands,
            &mut desired,
            &events,
            &mut status,
            &mut shutdown,
            backoff,
            connection_epoch,
            receive_seq,
        )
        .await
        {
            close_and_stop(session.take(), &mut status, &events).await;
            return;
        }
    }

    status.connected = false;
    status.subscriptions = 0;
    let _ = events.send(WorkerEvent::Status(status)).await;
    let _ = events.send(WorkerEvent::Stopped { source }).await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_control(
    action: Control,
    desired: &mut BTreeSet<String>,
    status: &mut SourceStatus,
    session: &mut Option<Box<dyn MarketDataSession>>,
    events: &mpsc::Sender<WorkerEvent>,
    connection_epoch: u64,
    receive_seq: u64,
) -> bool {
    match action {
        Control::SetSubscriptions(next) => {
            *desired = next;
            status.subscriptions = desired.len();
            let mut reconnect = false;
            if let Some(active) = session.as_mut()
                && let Err(failure) = active.apply_subscriptions(desired).await
            {
                warn!(
                    source = %status.source,
                    error = %format!("{failure:?}"),
                    "failed to update subscriptions"
                );
                status.last_error = Some(failure_message(&failure));
                reconnect = true;
            }
            let _ = events.send(WorkerEvent::Status(status.clone())).await;
            reconnect
        }
        Control::RequestSnapshot(symbols) => {
            if let Some(active) = session.as_mut()
                && let Err(failure) = active.request_snapshot(&symbols).await
            {
                warn!(
                    source = %status.source,
                    error = %format!("{failure:?}"),
                    "snapshot request failed"
                );
                status.last_error = Some(failure_message(&failure));
                return true;
            }
            false
        }
        Control::Recover {
            generation,
            symbols,
            reconnect,
        } => {
            // Acknowledge before any action can push frames back through the
            // same event channel. The fence is the last frame read before this
            // control was applied; any later recovery snapshot must be newer.
            let ack = WorkerEvent::RecoveryReady {
                source: status.source,
                generation,
                connection_epoch,
                receive_seq,
            };
            if reconnect {
                let _ = events.send(ack).await;
                warn!(source = %status.source, generation, "recovery reconnect requested");
                return true;
            }
            let mut failed = false;
            if let Some(active) = session.as_mut()
                && let Err(failure) = active.request_snapshot(&symbols).await
            {
                warn!(
                    source = %status.source,
                    error = %format!("{failure:?}"),
                    "recovery snapshot request failed"
                );
                status.last_error = Some(failure_message(&failure));
                failed = true;
            }
            let _ = events.send(ack).await;
            failed
        }
        Control::Reconnect(reason) => {
            warn!(source = %status.source, reason, "reconnect requested");
            true
        }
        Control::Shutdown => false,
    }
}

/// Read one frame for the supervisor's `select!`.
///
/// Cancellation safety lives in the session contract, not in a buffer here: a
/// poll dropped while awaiting a read loses no data because the transport
/// keeps read state inside the session, and a decoded data frame is returned
/// without further await points. Control-frame handling that does await
/// (Kalshi's reconcile and rate-limit backoff) is re-driven by its
/// acknowledgement timeout, which self-heals through a reconnect.
async fn read_next_frame(
    session: &mut Option<Box<dyn MarketDataSession>>,
) -> Result<SessionFrame, SessionFailure> {
    match session.as_mut() {
        Some(active) => active.next_frame().await,
        None => Err(SessionFailure::Reconnect(anyhow::anyhow!(
            "missing session"
        ))),
    }
}

async fn close_and_stop(
    session: Option<Box<dyn MarketDataSession>>,
    status: &mut SourceStatus,
    events: &mpsc::Sender<WorkerEvent>,
) {
    if let Some(mut session) = session {
        session.close().await;
    }
    status.connected = false;
    status.subscriptions = 0;
    let _ = events.send(WorkerEvent::Status(status.clone())).await;
    let _ = events
        .send(WorkerEvent::Stopped {
            source: status.source,
        })
        .await;
}

/// During backoff, keep accepting control messages so subscription changes and
/// shutdown do not have to wait for the retry timer.
#[allow(clippy::too_many_arguments)]
async fn backoff_or_stop(
    commands: &mut mpsc::Receiver<Control>,
    desired: &mut BTreeSet<String>,
    events: &mpsc::Sender<WorkerEvent>,
    status: &mut SourceStatus,
    shutdown: &mut broadcast::Receiver<()>,
    duration: Duration,
    connection_epoch: u64,
    receive_seq: u64,
) -> bool {
    let sleep = tokio::time::sleep(duration);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            _ = shutdown.recv() => return false,
            command = commands.recv() => match command {
                None | Some(Control::Shutdown) => return false,
                Some(Control::SetSubscriptions(next)) => {
                    *desired = next;
                    status.subscriptions = desired.len();
                    let _ = events.send(WorkerEvent::Status(status.clone())).await;
                }
                Some(Control::Recover {
                    generation,
                    reconnect: _,
                    symbols: _,
                }) => {
                    // No frame can be read while backing off. The next
                    // connection increments the epoch, which makes any
                    // subsequent exchange snapshot strictly newer than this
                    // acknowledgement.
                    let _ = events
                        .send(WorkerEvent::RecoveryReady {
                            source: status.source,
                            generation,
                            connection_epoch,
                            receive_seq,
                        })
                        .await;
                }
                Some(Control::Reconnect(_)) | Some(Control::RequestSnapshot(_)) => {}
            },
        }
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    let doubled = current.saturating_mul(2).min(maximum);
    let jitter = 0.8 + rand::random::<f64>() * 0.4;
    doubled.mul_f64(jitter).min(maximum)
}

/// Attach the supervisor's receive fence to every decoded event.
fn stamp_frame(frame: &mut SessionFrame, connection_epoch: u64, receive_seq: u64) {
    for event in &mut frame.events {
        match event {
            ExchangeEvent::Snapshot { meta, .. }
            | ExchangeEvent::LevelChange { meta, .. }
            | ExchangeEvent::Lifecycle { meta, .. } => {
                meta.connection_epoch = connection_epoch;
                meta.receive_seq = receive_seq;
            }
            ExchangeEvent::StreamReset { .. } => {}
        }
    }
}

fn update_latency(status: &mut SourceStatus, events: &[ExchangeEvent]) {
    for event in events {
        let meta = match event {
            ExchangeEvent::Snapshot { meta, .. }
            | ExchangeEvent::LevelChange { meta, .. }
            | ExchangeEvent::Lifecycle { meta, .. } => Some(meta),
            ExchangeEvent::StreamReset { .. } => None,
        };
        let Some(meta) = meta else {
            continue;
        };
        let Some(exchange_ts) = meta.exchange_ts_us else {
            continue;
        };
        let latency = meta.received_at_us.saturating_sub(exchange_ts);
        if !(0..=60_000_000).contains(&latency) {
            continue;
        }
        status.latency_ewma_us = Some(match status.latency_ewma_us {
            Some(previous) => (previous * 4 + latency) / 5,
            None => latency,
        });
    }
}

fn failure_message(failure: &SessionFailure) -> String {
    match failure {
        SessionFailure::Reconnect(error)
        | SessionFailure::Protocol { error, .. }
        | SessionFailure::Fatal(error) => truncate_error(error),
        SessionFailure::Decode { error, .. } => truncate_error(error),
    }
}

/// Account for one transport payload at the single ingress boundary.
///
/// Advances the receive fence exactly once per payload read — before any
/// decode or protocol decision is recorded — and enqueues the exact bytes with
/// their decode outcome. Transport-level idle events carry no payload: they
/// neither advance the fence nor produce a journal record. Returns `false`
/// after a fatal journal error that must stop the worker.
#[allow(clippy::too_many_arguments)]
async fn journal_ingress(
    source: SourceId,
    connection_epoch: u64,
    receive_seq: &mut u64,
    status: &mut SourceStatus,
    journal: Option<&JournalHandle>,
    events: &mpsc::Sender<WorkerEvent>,
    payload: Option<RawFrame>,
    received_at_us: i64,
    decode: DecodeOutcome,
) -> bool {
    let Some(payload) = payload else {
        return true;
    };
    *receive_seq = receive_seq.saturating_add(1);
    status.bytes_received = status
        .bytes_received
        .saturating_add(payload.payload.len() as u64);
    let Some(journal) = journal else {
        return true;
    };
    if let Err(error) = journal.record(
        source,
        connection_epoch,
        *receive_seq,
        received_at_us,
        payload,
        decode,
    ) {
        let message = format!("raw-frame journal rejected a frame: {error:#}");
        warn!(%source, error = %message, "journal write failed");
        status.last_error = Some(truncate_error(&error));
        let _ = events
            .send(WorkerEvent::Fatal {
                source,
                error: message,
            })
            .await;
        return false;
    }
    true
}

/// Stable classification for a rejected frame, recorded in the journal.
fn decode_error_category(error: &anyhow::Error) -> &'static str {
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<serde_json::Error>().is_some())
    {
        "invalid_json"
    } else {
        "invalid_payload"
    }
}

fn truncate_error(error: &anyhow::Error) -> String {
    let mut message = format!("{error:#}");
    const LIMIT: usize = 512;
    if message.len() > LIMIT {
        let mut end = LIMIT;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push('…');
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::Exchange as _;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn truncates_errors_on_utf8_boundaries() {
        let error = anyhow::anyhow!("x{}", "é".repeat(300));
        let message = truncate_error(&error);
        assert!(message.ends_with('…'));
        assert!(message.len() <= 512 + '…'.len_utf8());
    }

    fn test_journal(root: &std::path::Path) -> JournalHandle {
        use crate::journal::{JournalConfig, JournalMeta, SyncMode};

        JournalHandle::start(
            JournalConfig {
                directory: root.to_path_buf(),
                queue_capacity: 16,
                enqueue_timeout: Duration::from_millis(250),
                flush_interval: Duration::from_millis(10),
                sync_interval: Duration::from_millis(20),
                segment_max_bytes: 0,
                segment_max_seconds: 0,
                sync_mode: SyncMode::Interval,
            },
            JournalMeta {
                run_id: "run".to_owned(),
                venue: "test".to_owned(),
                code_commit: "test".to_owned(),
                config_sha256: "test".to_owned(),
            },
        )
        .expect("journal start")
    }

    #[tokio::test]
    async fn transport_idle_and_payloadless_protocol_failures_do_not_advance_the_fence() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let handle = test_journal(&temp.path().join("journal"));
        let source = SourceId::new(0, 0);
        let (events, _events_rx) = mpsc::channel(4);
        let mut status = SourceStatus::new(source);
        let mut receive_seq = 0u64;

        // A transport ping/pong produces an idle session frame with no raw
        // payload. It must not consume a journal sequence number.
        assert!(
            journal_ingress(
                source,
                1,
                &mut receive_seq,
                &mut status,
                Some(&handle),
                &events,
                None,
                1,
                DecodeOutcome::default(),
            )
            .await
        );
        assert_eq!(receive_seq, 0);

        // A protocol failure observed before any payload was read has no
        // associated record and must not advance the fence either.
        assert!(
            journal_ingress(
                source,
                1,
                &mut receive_seq,
                &mut status,
                Some(&handle),
                &events,
                None,
                2,
                DecodeOutcome::rejected("protocol_violation"),
            )
            .await
        );
        assert_eq!(receive_seq, 0);

        // The first actual payload takes sequence 1, not 3.
        assert!(
            journal_ingress(
                source,
                1,
                &mut receive_seq,
                &mut status,
                Some(&handle),
                &events,
                Some(RawFrame::text("payload".to_owned())),
                3,
                DecodeOutcome::decoded(1),
            )
            .await
        );
        assert_eq!(receive_seq, 1);

        // An idle frame after a payload leaves the fence where it was.
        assert!(
            journal_ingress(
                source,
                1,
                &mut receive_seq,
                &mut status,
                Some(&handle),
                &events,
                None,
                4,
                DecodeOutcome::default(),
            )
            .await
        );
        assert_eq!(receive_seq, 1);

        let summary = handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        assert_eq!(summary.record_count, 1);
        let records = crate::journal::read_records(std::path::Path::new(&summary.run_directory))
            .expect("read records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].receive_seq, 1);
        assert_eq!(records[0].payload, b"payload");
    }

    #[tokio::test]
    async fn stalled_connect_reports_status_and_stops_cleanly() {
        // A TCP listener that accepts and never upgrades the handshake. The
        // connect deadline must turn this into a bounded, reported failure
        // rather than a wedge.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let mut settings =
            crate::config::Settings::for_exchange(crate::config::ExchangeKind::Polymarket);
        settings.polymarket.ws_url = format!("ws://{address}");
        settings.network.connect_timeout_secs = 1;
        let exchange = std::sync::Arc::new(
            crate::exchange::polymarket::PolymarketExchange::new(
                settings.polymarket.clone(),
                settings.network.clone(),
            )
            .unwrap(),
        );
        let factory = exchange.session_factory();

        let (event_tx, mut event_rx) = mpsc::channel(64);
        let (_command_tx, command_rx) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let config = WorkerConfig::from_settings(&settings.collector);
        let worker = tokio::spawn(run_worker(
            factory,
            SourceId::new(0, 0),
            command_rx,
            event_tx,
            config,
            shutdown_rx,
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut saw_failure = false;
        while !saw_failure && tokio::time::Instant::now() < deadline {
            let event = match tokio::time::timeout_at(deadline, event_rx.recv()).await {
                Ok(Some(event)) => event,
                _ => break,
            };
            if let WorkerEvent::Status(status) = event
                && !status.connected
                && status
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("timed out"))
            {
                saw_failure = true;
            }
        }
        assert!(
            saw_failure,
            "the stalled handshake must fail within the connect deadline and be reported"
        );

        // Shutdown still stops the worker promptly, even after a stalled
        // attempt.
        let _ = shutdown_tx.send(());
        let stopped = tokio::time::timeout(Duration::from_secs(10), worker).await;
        assert!(stopped.is_ok(), "worker did not stop after shutdown");
    }

    struct ConnectGauge {
        current: AtomicUsize,
        maximum: AtomicUsize,
    }

    struct GaugeSession {
        gauge: Arc<ConnectGauge>,
    }

    #[async_trait]
    impl MarketDataSession for GaugeSession {
        async fn connect(&mut self) -> Result<(), SessionFailure> {
            let current = self.gauge.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.gauge.maximum.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.gauge.current.fetch_sub(1, Ordering::SeqCst);
            Err(SessionFailure::Fatal(anyhow::anyhow!(
                "connect attempt finished"
            )))
        }

        async fn apply_subscriptions(
            &mut self,
            _desired: &BTreeSet<String>,
        ) -> Result<(), SessionFailure> {
            Ok(())
        }

        async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
            Err(SessionFailure::Fatal(anyhow::anyhow!("unused")))
        }

        async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
            Ok(())
        }

        async fn request_snapshot(&mut self, _symbols: &[String]) -> Result<(), SessionFailure> {
            Ok(())
        }

        async fn close(&mut self) {}
    }

    struct GaugeFactory {
        gauge: Arc<ConnectGauge>,
    }

    #[async_trait]
    impl SessionFactory for GaugeFactory {
        async fn create_session(
            &self,
            _source: SourceId,
        ) -> anyhow::Result<Box<dyn MarketDataSession>> {
            Ok(Box::new(GaugeSession {
                gauge: Arc::clone(&self.gauge),
            }))
        }
    }

    #[tokio::test]
    async fn connect_concurrency_is_bounded_across_workers() {
        let mut settings =
            crate::config::Settings::for_exchange(crate::config::ExchangeKind::Polymarket);
        settings.collector.connect_concurrency = 2;
        let gauge = Arc::new(ConnectGauge {
            current: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let factory: Arc<dyn SessionFactory> = Arc::new(GaugeFactory {
            gauge: Arc::clone(&gauge),
        });
        let config = WorkerConfig::from_settings(&settings.collector);
        let mut workers = Vec::new();
        let mut shutdown_senders = Vec::new();
        for index in 0..8u16 {
            let (event_tx, mut event_rx) = mpsc::channel(16);
            let (_command_tx, command_rx) = mpsc::channel(8);
            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            shutdown_senders.push(shutdown_tx);
            let factory = Arc::clone(&factory);
            let config = config.clone();
            workers.push(tokio::spawn(async move {
                run_worker(
                    factory,
                    SourceId::new(0, index),
                    command_rx,
                    event_tx,
                    config,
                    shutdown_rx,
                )
                .await;
                // Drain until the channel closes so status/fatal sends never
                // block a worker on a full test channel.
                while event_rx.recv().await.is_some() {}
            }));
        }
        for worker in workers {
            tokio::time::timeout(Duration::from_secs(10), worker)
                .await
                .expect("worker did not stop")
                .unwrap();
        }
        let maximum = gauge.maximum.load(Ordering::SeqCst);
        assert_eq!(
            maximum, 2,
            "connect concurrency should reach but never exceed the limit"
        );
    }
}
