//! Append-only journal for raw public market-data frames.
//!
//! The journal preserves the exact bytes read from a public websocket before
//! normalization, including frames the decoder rejects. It is independent of
//! the SBE capture: the SBE schema and its on-disk representation are
//! unchanged.
//!
//! # Durability states
//!
//! A record advances through observable stages:
//!
//! * `queued` — accepted into the bounded channel owned by the writer thread;
//! * `written` — handed to the operating system file buffer;
//! * `flushed` — removed from the process buffer with [`std::io::Write::flush`];
//! * `synced` — persisted with [`std::fs::File::sync_all`];
//! * `finalized` — covered by a segment footer and a manifest entry after the
//!   partial file was renamed.
//!
//! A record reported as `written` is not durable across a process or host
//! failure. The manifest names the configured policy, and the final run state
//! distinguishes a completed capture from an interrupted one.
//!
//! # File layout
//!
//! A run directory contains `manifest.json` and numbered segment files. A
//! segment is created as `<index>.pmj.partial`, receives a header, records,
//! and a footer, then is synced and renamed to `<index>.pmj`. The manifest is
//! rewritten through the atomic JSON helper after each finalized segment and at
//! shutdown. A crash can leave a partial segment and an open manifest, but
//! verification reports that state instead of treating it as complete.
//!
//! Segment header and footer contain a SHA-256 checksum. Every record carries a
//! checksum trailer over its framing, JSON header, and payload. Verification
//! recomputes every checksum, rejects truncated records, and compares the
//! footer and manifest counts, byte ranges, timestamps, and sequence ranges.
//!
//! # Backpressure
//!
//! The caller blocks on a full queue for `enqueue_timeout`. Exceeding that
//! deadline marks the run failed and returns an error. Write, flush, sync, or
//! rename failures follow the same rule: the writer stops, the status becomes
//! failed, and shutdown returns an error.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use crossbeam::channel::{Receiver, Sender as QueueSender, bounded, select, tick};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{model::SourceId, report::write_json_atomic, time::now_us, util::sha256_hex};

/// Current journal schema version.
pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

const RECORD_MAGIC: [u8; 4] = *b"PMR1";
const SEGMENT_MAGIC: [u8; 8] = *b"PMJSEG01";
const FOOTER_MAGIC: [u8; 8] = *b"PMJEND01";
const CHECKSUM_BYTES: usize = 64;
const RECORD_FRAMING_BYTES: usize = 4 + 4 + 4;
const PARTIAL_SUFFIX: &str = ".pmj.partial";
const FINAL_SUFFIX: &str = ".pmj";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_RECORD_BYTES: usize = 64 << 20;
const MAX_HEADER_BYTES: usize = 1 << 20;

/// WebSocket data-frame kind preserved in the record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    Text,
    Binary,
}

/// Outcome of decoding one frame, recorded without altering the payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DecodeOutcome {
    /// The decoder produced normalized events.
    Decoded { events: u32 },
    /// The frame was accepted but produced no events (control frame, empty
    /// payload, or an unknown additive event type).
    Ignored { reason: String },
    /// The decoder rejected the frame. `category` is a stable label.
    Rejected { category: String },
}

impl DecodeOutcome {
    #[must_use]
    pub fn decoded(events: usize) -> Self {
        Self::Decoded {
            events: u32::try_from(events).unwrap_or(u32::MAX),
        }
    }

    #[must_use]
    pub fn ignored(reason: &str) -> Self {
        Self::Ignored {
            reason: reason.to_owned(),
        }
    }

    #[must_use]
    pub fn rejected(category: &str) -> Self {
        Self::Rejected {
            category: category.to_owned(),
        }
    }
}

impl Default for DecodeOutcome {
    fn default() -> Self {
        Self::Ignored {
            reason: "no_data_frame".to_owned(),
        }
    }
}

/// One raw websocket payload with its transport kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl RawFrame {
    #[must_use]
    pub fn text(text: String) -> Self {
        Self {
            kind: FrameKind::Text,
            payload: text.into_bytes(),
        }
    }

    #[must_use]
    pub fn binary(payload: Vec<u8>) -> Self {
        Self {
            kind: FrameKind::Binary,
            payload,
        }
    }
}

/// Flush and sync policy for the writer thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Flush and `fsync` every record.
    Always,
    /// Flush and `fsync` on the configured intervals and at finalization.
    Interval,
}

/// Journal writer configuration.
#[derive(Debug, Clone)]
pub struct JournalConfig {
    /// Root directory. Each run writes a new subdirectory named by run id.
    pub directory: PathBuf,
    /// Bound on queued records between the session supervisor and writer.
    pub queue_capacity: usize,
    /// Maximum time a caller waits for queue space before the run fails.
    pub enqueue_timeout: Duration,
    /// Maximum time records may sit in the process buffer.
    pub flush_interval: Duration,
    /// Maximum time between storage syncs in interval mode.
    pub sync_interval: Duration,
    /// Rotate after this many record bytes. Zero disables size rotation.
    pub segment_max_bytes: u64,
    /// Rotate after this many seconds. Zero disables time rotation.
    pub segment_max_seconds: u64,
    pub sync_mode: SyncMode,
}

impl JournalConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.directory.as_os_str().is_empty(),
            "journal directory must not be empty"
        );
        anyhow::ensure!(
            self.queue_capacity >= 1,
            "journal queue_capacity must be >= 1"
        );
        anyhow::ensure!(
            !self.enqueue_timeout.is_zero(),
            "journal enqueue_timeout must be greater than zero"
        );
        anyhow::ensure!(
            !self.flush_interval.is_zero(),
            "journal flush_interval must be greater than zero"
        );
        anyhow::ensure!(
            !self.sync_interval.is_zero(),
            "journal sync_interval must be greater than zero"
        );
        anyhow::ensure!(
            self.segment_max_bytes == 0 || self.segment_max_bytes >= 4096,
            "journal segment_max_bytes must be zero or at least 4096"
        );
        Ok(())
    }
}

/// Values pinned into the run manifest.
#[derive(Debug, Clone)]
pub struct JournalMeta {
    pub run_id: String,
    pub venue: String,
    /// Source commit of the running binary, or `unknown` when not injected.
    pub code_commit: String,
    /// SHA-256 of the serialized configuration that affects capture.
    pub config_sha256: String,
}

impl JournalMeta {
    /// Build metadata for a new run. The run id combines wall-clock
    /// microseconds and the process id; the run directory is created
    /// exclusively.
    #[must_use]
    pub fn new(venue: &str, code_commit: &str, config_sha256: &str) -> Self {
        let now = now_us().unwrap_or(0);
        Self {
            run_id: format!("{now:x}-{:x}", std::process::id()),
            venue: venue.to_owned(),
            code_commit: code_commit.to_owned(),
            config_sha256: config_sha256.to_owned(),
        }
    }
}

/// State of one journal run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Starting,
    Open,
    Finalized,
    Incomplete,
    Failed,
}

impl RunState {
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Open)
    }
}

/// Counters shared between the writer thread and callers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JournalCounters {
    /// Records accepted into the writer queue.
    pub queued: u64,
    /// Records handed to the file buffer.
    pub written: u64,
    /// Records whose bytes were flushed from the process buffer.
    pub flushed: u64,
    /// Records covered by a completed `sync_all`.
    pub synced: u64,
    /// Records inside segments that were renamed and recorded in the manifest.
    pub finalized: u64,
    /// Finalized segments.
    pub segments: u64,
    /// Current queue depth.
    pub queue_depth: u64,
    /// Record-region bytes written in the active and finalized segments.
    pub bytes: u64,
}

/// Snapshot of journal health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalStatus {
    pub run_id: String,
    pub run_directory: String,
    pub state: RunState,
    pub counters: JournalCounters,
    pub durability: DurabilityPolicy,
    pub last_error: Option<String>,
}

impl JournalStatus {
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.state.is_active() && self.last_error.is_none()
    }
}

/// Configured flush and sync behavior, pinned in the manifest and reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityPolicy {
    pub sync_mode: SyncMode,
    pub flush_interval_ms: u64,
    pub sync_interval_ms: u64,
    pub queue_capacity: usize,
    pub enqueue_timeout_ms: u64,
}

/// Result of an orderly finalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalSummary {
    pub run_id: String,
    pub run_directory: String,
    pub state: RunState,
    pub record_count: u64,
    pub segment_count: u64,
    pub bytes: u64,
}

struct Inner {
    run_directory: PathBuf,
    run_id: String,
    durability: DurabilityPolicy,
    status: Mutex<JournalStatus>,
    queue: QueueSender<Command>,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown: Mutex<bool>,
    admission: Mutex<Admission>,
    admission_cv: Condvar,
    #[cfg(test)]
    record_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// Producer admission control. Closing admission before finalization prevents a
/// record from entering the queue behind the Finalize command.
struct Admission {
    accepting: bool,
    active: usize,
}

/// Keeps a producer counted as active until the whole record operation —
/// enqueue, failure latching, and counter updates — has completed.
///
/// Dropping the guard is the only place that decrements the active producer
/// count, so shutdown and abort cannot observe quiescence while a producer is
/// still about to mark the journal failed.
struct AdmissionGuard<'a> {
    inner: &'a Inner,
}

impl Drop for AdmissionGuard<'_> {
    fn drop(&mut self) {
        let mut admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        debug_assert!(admission.active > 0, "producer admission count underflowed");
        admission.active = admission.active.saturating_sub(1);
        if admission.active == 0 {
            self.inner.admission_cv.notify_all();
        }
    }
}

/// Cloneable handle to the journal writer.
#[derive(Clone)]
pub struct JournalHandle {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for JournalHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JournalHandle")
            .field("run_id", &self.inner.run_id)
            .field("run_directory", &self.inner.run_directory)
            .finish()
    }
}

impl JournalHandle {
    /// Create the run directory, manifest, and writer thread.
    pub fn start(config: JournalConfig, meta: JournalMeta) -> anyhow::Result<Self> {
        Self::start_inner(config, meta, None)
    }

    fn start_inner(
        config: JournalConfig,
        meta: JournalMeta,
        fault: Option<FaultHook>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        anyhow::ensure!(!meta.run_id.is_empty(), "journal run id must not be empty");
        let run_directory = config.directory.join(&meta.run_id);
        fs::create_dir_all(&run_directory)
            .with_context(|| format!("creating journal directory `{}`", run_directory.display()))?;
        anyhow::ensure!(
            !run_directory.join(MANIFEST_FILE).exists(),
            "journal run `{}` already has a manifest",
            meta.run_id
        );
        let durability = DurabilityPolicy {
            sync_mode: config.sync_mode,
            flush_interval_ms: duration_ms(config.flush_interval),
            sync_interval_ms: duration_ms(config.sync_interval),
            queue_capacity: config.queue_capacity,
            enqueue_timeout_ms: duration_ms(config.enqueue_timeout),
        };
        let manifest = Manifest::new(&meta, &durability);
        write_manifest(&run_directory, &manifest)?;

        let (queue, queue_rx) = bounded(config.queue_capacity);
        let inner = Arc::new(Inner {
            run_directory: run_directory.clone(),
            run_id: meta.run_id.clone(),
            durability: durability.clone(),
            status: Mutex::new(JournalStatus {
                run_id: meta.run_id.clone(),
                run_directory: run_directory.display().to_string(),
                state: RunState::Starting,
                counters: JournalCounters::default(),
                durability,
                last_error: None,
            }),
            queue,
            join: Mutex::new(None),
            shutdown: Mutex::new(false),
            admission: Mutex::new(Admission {
                accepting: true,
                active: 0,
            }),
            admission_cv: Condvar::new(),
            #[cfg(test)]
            record_hook: Mutex::new(None),
        });
        let worker_inner = Arc::clone(&inner);
        let join = std::thread::Builder::new()
            .name("raw-journal".to_owned())
            .spawn(move || {
                run_writer(worker_inner, queue_rx, config, meta, manifest, fault);
            })
            .context("spawning journal writer")?;
        *inner
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(join);
        set_state(&inner, RunState::Open, None);
        Ok(Self { inner })
    }

    /// Enqueue one received frame.
    ///
    /// The call blocks for at most `enqueue_timeout` while the queue is full.
    /// A timeout or a stopped writer marks the run failed and returns an error;
    /// the frame is not silently dropped while the capture remains complete.
    pub fn record(
        &self,
        source: SourceId,
        connection_epoch: u64,
        receive_seq: u64,
        received_at_us: i64,
        frame: RawFrame,
        outcome: DecodeOutcome,
    ) -> anyhow::Result<()> {
        // The guard spans the entire operation. Shutdown and abort wait for it
        // to drop, which happens only after a queue failure has been latched
        // and accepted records have updated their counters.
        let _admission = self.admit()?;
        let job = RecordJob {
            source,
            connection_epoch,
            receive_seq,
            received_at_us,
            kind: frame.kind,
            payload: frame.payload,
            outcome,
        };
        let timeout = Duration::from_millis(self.inner.durability.enqueue_timeout_ms);
        let result = self.inner.queue.send_timeout(Command::Record(job), timeout);
        #[cfg(test)]
        {
            // A deterministic test gate between the queue decision and the
            // failure latch. It makes the admission-ordering regression
            // reproducible instead of relying on scheduler timing.
            let hook = self
                .inner
                .record_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        match result {
            Ok(()) => {
                if let Some(reason) = self.failure_reason() {
                    bail!("journal failed while accepting a record: {reason}");
                }
                let mut status = self
                    .inner
                    .status
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                status.counters.queued = status.counters.queued.saturating_add(1);
                status.counters.queue_depth = self.inner.queue.len() as u64;
                Ok(())
            }
            Err(error) => {
                let reason = match error {
                    crossbeam::channel::SendTimeoutError::Timeout(_) => format!(
                        "journal queue stayed full for {} ms",
                        self.inner.durability.enqueue_timeout_ms
                    ),
                    crossbeam::channel::SendTimeoutError::Disconnected(_) => {
                        "journal writer stopped".to_owned()
                    }
                };
                self.mark_failed(&reason);
                bail!("{reason}")
            }
        }
    }

    fn admit(&self) -> anyhow::Result<AdmissionGuard<'_>> {
        let mut admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        anyhow::ensure!(
            admission.accepting,
            "journal is not accepting records: {}",
            self.failure_reason()
                .unwrap_or_else(|| "shutdown in progress".to_owned())
        );
        admission.active = admission.active.saturating_add(1);
        Ok(AdmissionGuard { inner: &self.inner })
    }

    #[cfg(test)]
    fn set_record_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .inner
            .record_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[must_use]
    pub fn status(&self) -> JournalStatus {
        self.inner
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn failure_reason(&self) -> Option<String> {
        run_failure(&self.inner)
    }

    fn mark_failed(&self, reason: &str) {
        let mut status = self
            .inner
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if status.state == RunState::Finalized {
            return;
        }
        status.state = RunState::Failed;
        status.last_error = Some(reason.to_owned());
    }

    /// Finalize every segment, write the run manifest, and join the writer.
    ///
    /// The timeout bounds producer quiescence, command delivery, response
    /// waiting, and writer observation. A deadline expiry returns an error and
    /// leaves the run failed or incomplete; it does not claim finalization.
    pub fn shutdown(&self, timeout: Duration) -> anyhow::Result<JournalSummary> {
        let deadline = Instant::now() + timeout;
        {
            let mut shutdown = self
                .inner
                .shutdown
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anyhow::ensure!(!*shutdown, "journal shutdown was already requested");
            *shutdown = true;
        }
        if self.status().state == RunState::Incomplete {
            bail!("journal run is incomplete and cannot be finalized");
        }

        self.close_admission_and_wait(deadline, timeout, "shutdown")?;

        if let Some(reason) = self.failure_reason() {
            let _ = self.send_control(Command::Abort, deadline);
            self.join_writer(deadline);
            bail!("journal run failed: {reason}");
        }

        let (reply_tx, reply_rx) = channel();
        if let Err(error) = self.send_control(Command::Finalize(reply_tx), deadline) {
            self.mark_failed(&format!("journal finalization command failed: {error:#}"));
            self.join_writer(deadline);
            return Err(error);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = if remaining.is_zero() {
            Err(anyhow::anyhow!("journal finalization timed out"))
        } else {
            match reply_rx.recv_timeout(remaining) {
                Ok(Ok(summary)) => Ok(summary),
                Ok(Err(error)) => Err(anyhow::anyhow!("{error}")),
                Err(_) => Err(anyhow::anyhow!(
                    "journal finalization timed out after {timeout:?}"
                )),
            }
        };
        if result.is_err() && self.status().state != RunState::Finalized {
            self.mark_failed("journal finalization timed out or failed");
        }
        self.join_writer(deadline);
        result
    }

    /// Stop the writer without finalizing.
    ///
    /// The open segment stays as a `.partial` file and the manifest keeps its
    /// open or failed state. Verification reports the run as incomplete; no
    /// footer or manifest entry is fabricated. The timeout bounds producer
    /// quiescence, command delivery, and writer observation; it cannot
    /// interrupt a write that the operating system is already performing.
    pub fn abort(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        // Abort shares shutdown's producer-quiescence contract: the writer
        // must not receive the stop command while a producer is still inside
        // `record`.
        if self
            .close_admission_and_wait(deadline, timeout, "abort")
            .is_err()
        {
            self.join_writer(deadline);
            return;
        }
        if self.send_control(Command::Abort, deadline).is_err() {
            self.mark_failed("journal abort command could not be delivered");
        }
        self.join_writer(deadline);
    }

    /// Close admission and wait until every admitted producer has completed,
    /// within the absolute deadline shared with the caller.
    fn close_admission_and_wait(
        &self,
        deadline: Instant,
        timeout: Duration,
        operation: &str,
    ) -> anyhow::Result<()> {
        let mut admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        admission.accepting = false;
        while admission.active > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(admission);
                let reason =
                    format!("journal {operation} timed out waiting for producers for {timeout:?}");
                self.mark_failed(&reason);
                return Err(anyhow::anyhow!("{reason}"));
            }
            admission = self
                .inner
                .admission_cv
                .wait_timeout(admission, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        Ok(())
    }

    fn send_control(&self, command: Command, deadline: Instant) -> anyhow::Result<()> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        anyhow::ensure!(
            !remaining.is_zero(),
            "journal control deadline expired before delivery"
        );
        self.inner
            .queue
            .send_timeout(command, remaining)
            .map_err(|error| match error {
                crossbeam::channel::SendTimeoutError::Timeout(_) => {
                    anyhow::anyhow!("journal control command timed out after {remaining:?}")
                }
                crossbeam::channel::SendTimeoutError::Disconnected(_) => {
                    anyhow::anyhow!("journal writer is not running")
                }
            })
    }

    fn join_writer(&self, deadline: Instant) {
        let join = self
            .inner
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(join) = join else {
            return;
        };
        while !join.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if join.is_finished() {
            let _ = join.join();
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.queue.try_send(Command::Abort);
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn set_state(inner: &Inner, state: RunState, error: Option<String>) {
    let mut status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.state = state;
    if error.is_some() {
        status.last_error = error;
    }
}

fn run_failure(inner: &Inner) -> Option<String> {
    let status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    (status.state == RunState::Failed).then(|| {
        status
            .last_error
            .clone()
            .unwrap_or_else(|| "failed".to_owned())
    })
}

struct RecordJob {
    source: SourceId,
    connection_epoch: u64,
    receive_seq: u64,
    received_at_us: i64,
    kind: FrameKind,
    payload: Vec<u8>,
    outcome: DecodeOutcome,
}

enum Command {
    Record(RecordJob),
    Finalize(Sender<Result<JournalSummary, String>>),
    Abort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordHeader {
    schema_version: u32,
    source: String,
    shard: u16,
    replica: u16,
    connection_epoch: u64,
    receive_seq: u64,
    received_at_us: i64,
    clock_source: String,
    frame_kind: FrameKind,
    payload_len: u64,
    decode: DecodeOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentHeader {
    schema_version: u32,
    run_id: String,
    venue: String,
    code_commit: String,
    config_sha256: String,
    segment_index: u64,
    created_at_us: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentFooter {
    schema_version: u32,
    finalized: bool,
    record_count: u64,
    byte_count: u64,
    checksum: String,
    first_received_at_us: Option<i64>,
    last_received_at_us: Option<i64>,
    sources: Vec<SourceRange>,
}

/// Per-source receive range inside a segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceRange {
    source: String,
    first_epoch: u64,
    last_epoch: u64,
    first_seq: u64,
    last_seq: u64,
    records: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    schema_version: u32,
    run_id: String,
    venue: String,
    code_commit: String,
    config_sha256: String,
    created_at_us: i64,
    state: RunState,
    durability: DurabilityPolicy,
    failure: Option<String>,
    finalized_at_us: Option<i64>,
    segments: Vec<ManifestSegment>,
}

impl Manifest {
    fn new(meta: &JournalMeta, durability: &DurabilityPolicy) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            run_id: meta.run_id.clone(),
            venue: meta.venue.clone(),
            code_commit: meta.code_commit.clone(),
            config_sha256: meta.config_sha256.clone(),
            created_at_us: now_us().unwrap_or(0),
            state: RunState::Open,
            durability: durability.clone(),
            failure: None,
            finalized_at_us: None,
            segments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestSegment {
    index: u64,
    file: String,
    state: String,
    record_count: u64,
    byte_count: u64,
    checksum: String,
    first_received_at_us: Option<i64>,
    last_received_at_us: Option<i64>,
    sources: Vec<SourceRange>,
}

struct SegmentWriter {
    index: u64,
    partial_path: PathBuf,
    final_path: PathBuf,
    file: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
    records: u64,
    first_received_at_us: Option<i64>,
    last_received_at_us: Option<i64>,
    sources: BTreeMap<String, SourceRange>,
    opened_at: Instant,
}

impl SegmentWriter {
    fn open(run_directory: &Path, index: u64, meta: &JournalMeta) -> anyhow::Result<Self> {
        let partial_path = run_directory.join(format!("{index:08}{PARTIAL_SUFFIX}"));
        let final_path = run_directory.join(format!("{index:08}{FINAL_SUFFIX}"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .with_context(|| format!("creating journal segment `{}`", partial_path.display()))?;
        let mut writer = Self {
            index,
            partial_path,
            final_path,
            file: BufWriter::new(file),
            hasher: Sha256::new(),
            bytes: 0,
            records: 0,
            first_received_at_us: None,
            last_received_at_us: None,
            sources: BTreeMap::new(),
            opened_at: Instant::now(),
        };
        writer.write_header(meta)?;
        Ok(writer)
    }

    fn write_header(&mut self, meta: &JournalMeta) -> anyhow::Result<()> {
        let header = SegmentHeader {
            schema_version: JOURNAL_SCHEMA_VERSION,
            run_id: meta.run_id.clone(),
            venue: meta.venue.clone(),
            code_commit: meta.code_commit.clone(),
            config_sha256: meta.config_sha256.clone(),
            segment_index: self.index,
            created_at_us: now_us().unwrap_or(0),
        };
        let json = serde_json::to_vec(&header).context("serializing segment header")?;
        let length = u32::try_from(json.len()).context("segment header exceeds u32")?;
        let mut framed = Vec::with_capacity(SEGMENT_MAGIC.len() + 4 + json.len());
        framed.extend_from_slice(&SEGMENT_MAGIC);
        framed.extend_from_slice(&length.to_le_bytes());
        framed.extend_from_slice(&json);
        self.file
            .write_all(&framed)
            .context("writing segment header")?;
        self.file
            .write_all(sha256_hex(&framed).as_bytes())
            .context("writing segment header checksum")?;
        self.file.flush().context("flushing segment header")?;
        self.file
            .get_ref()
            .sync_all()
            .context("syncing segment header")?;
        Ok(())
    }

    /// Append one record and return its framed length.
    fn write_record(&mut self, job: &RecordJob) -> anyhow::Result<u64> {
        let header = RecordHeader {
            schema_version: JOURNAL_SCHEMA_VERSION,
            source: job.source.to_string(),
            shard: job.source.shard,
            replica: job.source.replica,
            connection_epoch: job.connection_epoch,
            receive_seq: job.receive_seq,
            received_at_us: job.received_at_us,
            clock_source: "unix_epoch_microseconds".to_owned(),
            frame_kind: job.kind,
            payload_len: u64::try_from(job.payload.len()).unwrap_or(u64::MAX),
            decode: job.outcome.clone(),
        };
        let header_json = serde_json::to_vec(&header).context("serializing record header")?;
        let header_len = u32::try_from(header_json.len()).context("record header exceeds u32")?;
        let payload_len = u32::try_from(job.payload.len()).context("record payload exceeds u32")?;
        let record_len = u32::try_from(8 + header_json.len() + job.payload.len() + CHECKSUM_BYTES)
            .context("record exceeds u32")?;
        let mut record = Vec::with_capacity(RECORD_FRAMING_BYTES + record_len as usize);
        record.extend_from_slice(&RECORD_MAGIC);
        record.extend_from_slice(&record_len.to_le_bytes());
        record.extend_from_slice(&header_len.to_le_bytes());
        record.extend_from_slice(&payload_len.to_le_bytes());
        record.extend_from_slice(&header_json);
        record.extend_from_slice(&job.payload);
        record.extend_from_slice(sha256_hex(&record).as_bytes());
        let framed = u64::try_from(record.len()).unwrap_or(u64::MAX);
        self.file
            .write_all(&record)
            .context("writing journal record")?;
        self.hasher.update(&record);
        self.bytes = self.bytes.saturating_add(framed);
        self.records = self.records.saturating_add(1);
        self.first_received_at_us = self.first_received_at_us.or(Some(job.received_at_us));
        self.last_received_at_us = Some(job.received_at_us);
        let source = job.source.to_string();
        self.sources
            .entry(source.clone())
            .and_modify(|range| {
                range.last_epoch = job.connection_epoch;
                range.last_seq = job.receive_seq;
                range.records = range.records.saturating_add(1);
            })
            .or_insert(SourceRange {
                source,
                first_epoch: job.connection_epoch,
                last_epoch: job.connection_epoch,
                first_seq: job.receive_seq,
                last_seq: job.receive_seq,
                records: 1,
            });
        Ok(framed)
    }

    fn finalize(mut self) -> anyhow::Result<ManifestSegment> {
        self.file.flush().context("flushing journal segment")?;
        self.file
            .get_ref()
            .sync_all()
            .context("syncing journal segment")?;
        let checksum = hex_of(&self.hasher.clone().finalize());
        let footer = SegmentFooter {
            schema_version: JOURNAL_SCHEMA_VERSION,
            finalized: true,
            record_count: self.records,
            byte_count: self.bytes,
            checksum: checksum.clone(),
            first_received_at_us: self.first_received_at_us,
            last_received_at_us: self.last_received_at_us,
            sources: self.sources.values().cloned().collect(),
        };
        let json = serde_json::to_vec(&footer).context("serializing segment footer")?;
        let length = u32::try_from(json.len()).context("segment footer exceeds u32")?;
        let mut framed = Vec::with_capacity(FOOTER_MAGIC.len() + 4 + json.len());
        framed.extend_from_slice(&FOOTER_MAGIC);
        framed.extend_from_slice(&length.to_le_bytes());
        framed.extend_from_slice(&json);
        self.file
            .write_all(&framed)
            .context("writing segment footer")?;
        self.file
            .write_all(sha256_hex(&framed).as_bytes())
            .context("writing segment footer checksum")?;
        self.file
            .into_inner()
            .map_err(|error| error.into_error())
            .context("closing journal segment")?
            .sync_all()
            .context("syncing journal segment footer")?;
        fs::rename(&self.partial_path, &self.final_path).with_context(|| {
            format!(
                "renaming journal segment `{}` to `{}`",
                self.partial_path.display(),
                self.final_path.display()
            )
        })?;
        if let Some(parent) = self.final_path.parent() {
            sync_directory(parent)?;
        }
        Ok(ManifestSegment {
            index: self.index,
            file: self
                .final_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_owned(),
            state: "finalized".to_owned(),
            record_count: footer.record_count,
            byte_count: footer.byte_count,
            checksum,
            first_received_at_us: footer.first_received_at_us,
            last_received_at_us: footer.last_received_at_us,
            sources: footer.sources,
        })
    }
}

fn hex_of(bytes: &[u8]) -> String {
    crate::util::hex_lower(bytes)
}

type FaultHook = Arc<dyn Fn(FaultPoint) -> anyhow::Result<()> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultPoint {
    Write,
    Flush,
    Sync,
    Finalize,
}

fn run_fault(hook: Option<&FaultHook>, point: FaultPoint) -> anyhow::Result<()> {
    match hook {
        Some(hook) => hook(point),
        None => Ok(()),
    }
}

fn run_writer(
    inner: Arc<Inner>,
    queue: Receiver<Command>,
    config: JournalConfig,
    meta: JournalMeta,
    mut manifest: Manifest,
    fault: Option<FaultHook>,
) {
    let mut segment = match SegmentWriter::open(&inner.run_directory, 0, &meta) {
        Ok(segment) => segment,
        Err(error) => {
            fail(&inner, &mut manifest, &error);
            return;
        }
    };
    let mut next_index = 1u64;
    let tick_period = config.flush_interval.min(config.sync_interval);
    let ticks = tick(tick_period);
    let mut last_flush = Instant::now();
    let mut last_sync = Instant::now();

    loop {
        if let Some(reason) = run_failure(&inner) {
            fail_reason(&inner, &mut manifest, &reason);
            return;
        }
        select! {
            recv(queue) -> message => match message {
                Ok(Command::Record(job)) => {
                    let result = run_fault(fault.as_ref(), FaultPoint::Write)
                        .and_then(|()| segment.write_record(&job));
                    match result {
                        Ok(framed) => {
                            {
                                let mut status = inner
                                    .status
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                status.counters.written =
                                    status.counters.written.saturating_add(1);
                                status.counters.bytes =
                                    status.counters.bytes.saturating_add(framed);
                                status.counters.queue_depth = queue.len() as u64;
                            }
                            if config.sync_mode == SyncMode::Always {
                                if let Err(error) =
                                    flush_and_sync(&inner, &mut segment, fault.as_ref())
                                {
                                    fail(&inner, &mut manifest, &error);
                                    return;
                                }
                                last_flush = Instant::now();
                                last_sync = last_flush;
                            }
                            let rotate = (config.segment_max_bytes > 0
                                && segment.bytes >= config.segment_max_bytes)
                                || (config.segment_max_seconds > 0
                                    && segment.opened_at.elapsed()
                                        >= Duration::from_secs(config.segment_max_seconds));
                            if rotate
                                && let Err(error) = rotate_segment(
                                    &inner,
                                    &mut manifest,
                                    &mut segment,
                                    next_index,
                                    &meta,
                                    fault.as_ref(),
                                )
                            {
                                fail(&inner, &mut manifest, &error);
                                return;
                            }
                            if rotate {
                                next_index = next_index.saturating_add(1);
                            }
                        }
                        Err(error) => {
                            fail(&inner, &mut manifest, &error);
                            return;
                        }
                    }
                }
                Ok(Command::Finalize(reply)) => {
                    let result = finalize_run(&inner, &mut manifest, segment, fault.as_ref());
                    match result {
                        Ok(summary) => {
                            let _ = reply.send(Ok(summary));
                        }
                        Err(error) => {
                            fail(&inner, &mut manifest, &error);
                            let _ = reply.send(Err(format!("{error:#}")));
                        }
                    }
                    return;
                }
                Ok(Command::Abort) => {
                    if let Some(reason) = run_failure(&inner) {
                        fail_reason(&inner, &mut manifest, &reason);
                    } else {
                        set_state(
                            &inner,
                            RunState::Incomplete,
                            Some("journal stopped without finalization".to_owned()),
                        );
                    }
                    return;
                }
                Err(_) => {
                    set_state(
                        &inner,
                        RunState::Incomplete,
                        Some("journal writer lost all handles before finalization".to_owned()),
                    );
                    return;
                }
            },
            recv(ticks) -> _ => {
                let now = Instant::now();
                if now.duration_since(last_flush) >= config.flush_interval {
                    if let Err(error) = flush_only(&inner, &mut segment, fault.as_ref()) {
                        fail(&inner, &mut manifest, &error);
                        return;
                    }
                    last_flush = now;
                }
                if now.duration_since(last_sync) >= config.sync_interval {
                    if let Err(error) = sync_only(&inner, &mut segment, fault.as_ref()) {
                        fail(&inner, &mut manifest, &error);
                        return;
                    }
                    last_sync = now;
                }
            },
        }
    }
}

fn flush_only(
    inner: &Inner,
    segment: &mut SegmentWriter,
    fault: Option<&FaultHook>,
) -> anyhow::Result<()> {
    run_fault(fault, FaultPoint::Flush)?;
    segment.file.flush().context("flushing journal segment")?;
    let mut status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.counters.flushed = status.counters.written;
    Ok(())
}

fn sync_only(
    inner: &Inner,
    segment: &mut SegmentWriter,
    fault: Option<&FaultHook>,
) -> anyhow::Result<()> {
    run_fault(fault, FaultPoint::Sync)?;
    segment
        .file
        .get_ref()
        .sync_all()
        .context("syncing journal segment")?;
    let mut status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.counters.synced = status.counters.written;
    Ok(())
}

fn flush_and_sync(
    inner: &Inner,
    segment: &mut SegmentWriter,
    fault: Option<&FaultHook>,
) -> anyhow::Result<()> {
    flush_only(inner, segment, fault)?;
    sync_only(inner, segment, fault)
}

fn rotate_segment(
    inner: &Inner,
    manifest: &mut Manifest,
    segment: &mut SegmentWriter,
    next_index: u64,
    meta: &JournalMeta,
    fault: Option<&FaultHook>,
) -> anyhow::Result<()> {
    run_fault(fault, FaultPoint::Finalize)?;
    let replacement = SegmentWriter::open(&inner.run_directory, next_index, meta)?;
    let previous = std::mem::replace(segment, replacement);
    let entry = previous.finalize()?;
    let count = entry.record_count;
    manifest.segments.push(entry);
    write_manifest(&inner.run_directory, manifest)?;
    let mut status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.counters.finalized = status.counters.finalized.saturating_add(count);
    status.counters.segments = manifest.segments.len() as u64;
    Ok(())
}

fn finalize_run(
    inner: &Inner,
    manifest: &mut Manifest,
    segment: SegmentWriter,
    fault: Option<&FaultHook>,
) -> anyhow::Result<JournalSummary> {
    if let Some(reason) = run_failure(inner) {
        fail_reason(inner, manifest, &reason);
        bail!("journal run failed before finalization: {reason}");
    }
    run_fault(fault, FaultPoint::Finalize)?;
    let entry = segment.finalize()?;
    if let Some(reason) = run_failure(inner) {
        manifest.segments.push(entry);
        fail_reason(inner, manifest, &reason);
        bail!("journal run failed during finalization: {reason}");
    }
    manifest.segments.push(entry);
    manifest.state = RunState::Finalized;
    manifest.finalized_at_us = Some(now_us().unwrap_or(0));
    write_manifest(&inner.run_directory, manifest)?;
    let record_count = manifest
        .segments
        .iter()
        .map(|entry| entry.record_count)
        .sum();
    let bytes = manifest
        .segments
        .iter()
        .map(|entry| entry.byte_count)
        .sum::<u64>();
    let segment_count = manifest.segments.len() as u64;
    let mut status = inner
        .status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.state = RunState::Finalized;
    status.counters.finalized = status.counters.written;
    // Segment finalization flushed and synced every written record before the
    // rename, so the durability counters must not lag the footer.
    status.counters.flushed = status.counters.written;
    status.counters.synced = status.counters.written;
    status.counters.segments = segment_count;
    status.counters.bytes = bytes;
    status.counters.queue_depth = 0;
    Ok(JournalSummary {
        run_id: inner.run_id.clone(),
        run_directory: inner.run_directory.display().to_string(),
        state: RunState::Finalized,
        record_count,
        segment_count,
        bytes,
    })
}

fn fail(inner: &Inner, manifest: &mut Manifest, error: &anyhow::Error) {
    let message = format!("{error:#}");
    fail_reason(inner, manifest, &message);
}

fn fail_reason(inner: &Inner, manifest: &mut Manifest, message: &str) {
    manifest.state = RunState::Failed;
    manifest.failure = Some(message.to_owned());
    let _ = write_manifest(&inner.run_directory, manifest);
    set_state(inner, RunState::Failed, Some(message.to_owned()));
}

fn write_manifest(directory: &Path, manifest: &Manifest) -> anyhow::Result<()> {
    write_json_atomic(&directory.join(MANIFEST_FILE), manifest)
        .with_context(|| format!("writing journal manifest in `{}`", directory.display()))
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    let directory = File::open(path)
        .with_context(|| format!("opening directory `{}` for sync", path.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("syncing directory `{}`", path.display()))
}

/// Result of verifying one run directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalVerification {
    pub schema_version: u32,
    pub run_id: String,
    pub state: RunState,
    pub complete: bool,
    pub record_count: u64,
    pub segment_count: u64,
    pub errors: Vec<String>,
    pub segments: Vec<SegmentVerification>,
}

/// Per-segment verification detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentVerification {
    pub file: String,
    pub state: String,
    pub record_count: u64,
    pub byte_count: u64,
    pub checksum: String,
    pub first_received_at_us: Option<i64>,
    pub last_received_at_us: Option<i64>,
    pub sources: Vec<SourceRange>,
}

/// Verify one run directory against its manifest, segment footers, and record
/// checksums. A missing, partial, truncated, reordered, or checksum-mismatched
/// artifact makes `complete` false.
pub fn verify_run(run_directory: &Path) -> anyhow::Result<JournalVerification> {
    let manifest_path = run_directory.join(MANIFEST_FILE);
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("reading manifest `{}`", manifest_path.display()))?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing manifest `{}`", manifest_path.display()))?;
    anyhow::ensure!(
        manifest.schema_version == JOURNAL_SCHEMA_VERSION,
        "unsupported journal manifest schema {}",
        manifest.schema_version
    );
    let mut errors = Vec::new();
    if manifest.state != RunState::Finalized {
        errors.push(format!("run state is {:?}", manifest.state));
    }
    if let Some(failure) = &manifest.failure {
        errors.push(format!("run failure: {failure}"));
    }
    let mut segments = Vec::new();
    let mut record_count = 0u64;
    let mut previous_ranges: BTreeMap<String, SourceRange> = BTreeMap::new();
    for expected in &manifest.segments {
        match verify_segment(run_directory, &manifest, expected) {
            Ok((segment, count)) => {
                for range in &segment.sources {
                    if let Some(previous) = previous_ranges.get(&range.source) {
                        let ordered = range.first_epoch > previous.last_epoch
                            || (range.first_epoch == previous.last_epoch
                                && range.first_seq > previous.last_seq);
                        if !ordered {
                            errors.push(format!(
                                "segment {}: record order regressed for {}: ({}, {}) then ({}, {})",
                                expected.file,
                                range.source,
                                previous.last_epoch,
                                previous.last_seq,
                                range.first_epoch,
                                range.first_seq,
                            ));
                        }
                    }
                }
                for range in &segment.sources {
                    previous_ranges.insert(range.source.clone(), range.clone());
                }
                record_count = record_count.saturating_add(count);
                segments.push(segment);
            }
            Err(error) => errors.push(format!("segment {}: {error}", expected.file)),
        }
    }
    if let Ok(entries) = fs::read_dir(run_directory) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(PARTIAL_SUFFIX) {
                errors.push(format!("unfinalized segment `{name}`"));
            } else if name.ends_with(FINAL_SUFFIX)
                && !manifest.segments.iter().any(|segment| segment.file == name)
            {
                errors.push(format!("segment `{name}` is absent from the manifest"));
            }
        }
    }
    Ok(JournalVerification {
        schema_version: JOURNAL_SCHEMA_VERSION,
        run_id: manifest.run_id,
        state: manifest.state,
        complete: errors.is_empty(),
        record_count,
        segment_count: manifest.segments.len() as u64,
        errors,
        segments,
    })
}

fn verify_segment(
    run_directory: &Path,
    manifest: &Manifest,
    expected: &ManifestSegment,
) -> anyhow::Result<(SegmentVerification, u64)> {
    anyhow::ensure!(
        expected.state == "finalized",
        "manifest state is `{}`",
        expected.state
    );
    let path = run_directory.join(&expected.file);
    let mut file =
        File::open(&path).with_context(|| format!("opening segment `{}`", path.display()))?;
    let header = read_segment_header(&mut file)?;
    anyhow::ensure!(
        header.run_id == manifest.run_id,
        "segment run id does not match the manifest"
    );
    anyhow::ensure!(
        header.segment_index == expected.index,
        "segment index does not match the manifest"
    );
    anyhow::ensure!(
        header.code_commit == manifest.code_commit
            && header.config_sha256 == manifest.config_sha256,
        "segment provenance does not match the manifest"
    );
    let mut hasher = Sha256::new();
    let mut records = 0u64;
    let mut first_us: Option<i64> = None;
    let mut last_us: Option<i64> = None;
    let mut ranges: BTreeMap<String, SourceRange> = BTreeMap::new();
    let mut previous: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut body_bytes = 0u64;
    loop {
        let mut magic = [0u8; 4];
        let read = file.read(&mut magic).context("reading segment framing")?;
        if read == 0 {
            bail!("segment has no footer");
        }
        if read < 4 {
            bail!("segment ends inside a framing marker");
        }
        if magic == RECORD_MAGIC {
            let parsed = read_record(&mut file)?;
            body_bytes = body_bytes.saturating_add(parsed.raw.len() as u64);
            hasher.update(&parsed.raw);
            records = records.saturating_add(1);
            let header = parsed.header;
            first_us = Some(first_us.map_or(header.received_at_us, |value| {
                value.min(header.received_at_us)
            }));
            last_us = Some(last_us.map_or(header.received_at_us, |value| {
                value.max(header.received_at_us)
            }));
            let source = header.source.clone();
            if let Some((epoch, seq)) = previous.get(&source).copied() {
                let ordered = header.connection_epoch > epoch
                    || (header.connection_epoch == epoch && header.receive_seq > seq);
                anyhow::ensure!(
                    ordered,
                    "record order regressed for {source}: ({epoch}, {seq}) then ({}, {})",
                    header.connection_epoch,
                    header.receive_seq
                );
            }
            previous.insert(
                source.clone(),
                (header.connection_epoch, header.receive_seq),
            );
            ranges
                .entry(source.clone())
                .and_modify(|range| {
                    range.last_epoch = header.connection_epoch;
                    range.last_seq = header.receive_seq;
                    range.records = range.records.saturating_add(1);
                })
                .or_insert(SourceRange {
                    source,
                    first_epoch: header.connection_epoch,
                    last_epoch: header.connection_epoch,
                    first_seq: header.receive_seq,
                    last_seq: header.receive_seq,
                    records: 1,
                });
            continue;
        }
        if magic == FOOTER_MAGIC[..4] {
            let mut rest = [0u8; 4];
            file.read_exact(&mut rest).context("reading footer magic")?;
            anyhow::ensure!(
                rest == FOOTER_MAGIC[4..],
                "segment footer magic is not recognized"
            );
            let footer = read_footer(&mut file)?;
            let checksum = hex_of(&hasher.clone().finalize());
            anyhow::ensure!(
                footer.finalized,
                "segment footer does not declare finalization"
            );
            anyhow::ensure!(
                footer.checksum == checksum,
                "segment checksum mismatch: footer {} computed {}",
                footer.checksum,
                checksum
            );
            anyhow::ensure!(
                footer.record_count == records,
                "footer record count {} does not match {}",
                footer.record_count,
                records
            );
            anyhow::ensure!(
                footer.byte_count == body_bytes,
                "footer byte count {} does not match {}",
                footer.byte_count,
                body_bytes
            );
            anyhow::ensure!(
                footer.checksum == expected.checksum,
                "footer checksum does not match the manifest"
            );
            anyhow::ensure!(
                footer.record_count == expected.record_count
                    && footer.byte_count == expected.byte_count,
                "footer counts do not match the manifest"
            );
            anyhow::ensure!(
                footer.first_received_at_us == expected.first_received_at_us
                    && footer.last_received_at_us == expected.last_received_at_us,
                "footer timestamps do not match the manifest"
            );
            let mut trailing = [0u8; 1];
            anyhow::ensure!(
                file.read(&mut trailing).context("reading segment tail")? == 0,
                "segment contains bytes after the footer"
            );
            let source_ranges: Vec<SourceRange> = ranges.values().cloned().collect();
            anyhow::ensure!(
                source_ranges == expected.sources,
                "source ranges do not match the manifest"
            );
            let verification = SegmentVerification {
                file: expected.file.clone(),
                state: expected.state.clone(),
                record_count: records,
                byte_count: body_bytes,
                checksum,
                first_received_at_us: first_us,
                last_received_at_us: last_us,
                sources: source_ranges,
            };
            return Ok((verification, records));
        }
        bail!("unknown framing marker in segment");
    }
}

fn read_segment_header(file: &mut File) -> anyhow::Result<SegmentHeader> {
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)
        .context("reading segment header magic")?;
    anyhow::ensure!(
        magic == SEGMENT_MAGIC,
        "segment header magic is not recognized"
    );
    let json = read_json_block(file).context("reading segment header")?;
    let mut framed = Vec::with_capacity(SEGMENT_MAGIC.len() + 4 + json.len());
    framed.extend_from_slice(&SEGMENT_MAGIC);
    framed.extend_from_slice(
        &u32::try_from(json.len())
            .context("segment header exceeds u32")?
            .to_le_bytes(),
    );
    framed.extend_from_slice(&json);
    let mut expected = [0u8; CHECKSUM_BYTES];
    file.read_exact(&mut expected)
        .context("reading segment header checksum")?;
    anyhow::ensure!(
        expected == sha256_hex(&framed).as_bytes(),
        "segment header checksum mismatch"
    );
    serde_json::from_slice(&json).context("parsing segment header")
}

fn read_json_block(file: &mut File) -> anyhow::Result<Vec<u8>> {
    let mut length = [0u8; 4];
    file.read_exact(&mut length)
        .context("reading block length")?;
    let length = u32::from_le_bytes(length) as usize;
    anyhow::ensure!(
        length <= MAX_HEADER_BYTES,
        "block length {length} exceeds limit"
    );
    let mut json = vec![0u8; length];
    file.read_exact(&mut json).context("reading block body")?;
    Ok(json)
}

struct ParsedRecord {
    header: RecordHeader,
    payload: Vec<u8>,
    raw: Vec<u8>,
}

fn read_record(file: &mut File) -> anyhow::Result<ParsedRecord> {
    let mut record_len = [0u8; 4];
    file.read_exact(&mut record_len)
        .context("reading record length")?;
    let record_len = u32::from_le_bytes(record_len) as usize;
    anyhow::ensure!(
        record_len <= MAX_RECORD_BYTES,
        "record length {record_len} exceeds limit"
    );
    let mut rest = vec![0u8; record_len];
    file.read_exact(&mut rest).context("reading record body")?;
    anyhow::ensure!(
        rest.len() >= 8 + CHECKSUM_BYTES,
        "record is shorter than its framing"
    );
    let header_len = u32::from_le_bytes(
        rest[0..4]
            .try_into()
            .context("reading record header length")?,
    ) as usize;
    let payload_len = u32::from_le_bytes(
        rest[4..8]
            .try_into()
            .context("reading record payload length")?,
    ) as usize;
    anyhow::ensure!(
        header_len
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(8 + CHECKSUM_BYTES))
            == Some(rest.len()),
        "record length fields are inconsistent"
    );
    let header_json = &rest[8..8 + header_len];
    let payload = rest[8 + header_len..8 + header_len + payload_len].to_vec();
    let checksum = &rest[rest.len() - CHECKSUM_BYTES..];
    let mut raw = Vec::with_capacity(RECORD_FRAMING_BYTES + rest.len());
    raw.extend_from_slice(&RECORD_MAGIC);
    raw.extend_from_slice(
        &u32::try_from(rest.len())
            .context("record exceeds u32")?
            .to_le_bytes(),
    );
    raw.extend_from_slice(&rest);
    anyhow::ensure!(
        checksum == sha256_hex(&raw[..raw.len() - CHECKSUM_BYTES]).as_bytes(),
        "record checksum mismatch"
    );
    let header: RecordHeader =
        serde_json::from_slice(header_json).context("parsing record header")?;
    anyhow::ensure!(
        header.payload_len == payload_len as u64,
        "record payload length does not match its header"
    );
    Ok(ParsedRecord {
        header,
        payload,
        raw,
    })
}

fn read_footer(file: &mut File) -> anyhow::Result<SegmentFooter> {
    let json = read_json_block(file).context("reading segment footer")?;
    let mut framed = Vec::with_capacity(FOOTER_MAGIC.len() + 4 + json.len());
    framed.extend_from_slice(&FOOTER_MAGIC);
    framed.extend_from_slice(
        &u32::try_from(json.len())
            .context("segment footer exceeds u32")?
            .to_le_bytes(),
    );
    framed.extend_from_slice(&json);
    let mut expected = [0u8; CHECKSUM_BYTES];
    file.read_exact(&mut expected)
        .context("reading segment footer checksum")?;
    anyhow::ensure!(
        expected == sha256_hex(&framed).as_bytes(),
        "segment footer checksum mismatch"
    );
    serde_json::from_slice(&json).context("parsing segment footer")
}

/// One journal record, including the original payload bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecord {
    pub schema_version: u32,
    pub source: String,
    pub connection_epoch: u64,
    pub receive_seq: u64,
    pub received_at_us: i64,
    pub frame_kind: FrameKind,
    pub payload: Vec<u8>,
    pub outcome: DecodeOutcome,
}

/// Read every record from the finalized segments of one run in file order.
pub fn read_records(run_directory: &Path) -> anyhow::Result<Vec<JournalRecord>> {
    let verification = verify_run(run_directory)?;
    anyhow::ensure!(
        verification.complete,
        "journal run is not complete: {}",
        verification.errors.join("; ")
    );
    let manifest: Manifest = serde_json::from_slice(&fs::read(run_directory.join(MANIFEST_FILE))?)?;
    let mut records = Vec::new();
    for segment in &manifest.segments {
        let mut file = File::open(run_directory.join(&segment.file))?;
        read_segment_header(&mut file)?;
        loop {
            let mut magic = [0u8; 4];
            file.read_exact(&mut magic)?;
            if magic == RECORD_MAGIC {
                let parsed = read_record(&mut file)?;
                records.push(JournalRecord {
                    schema_version: parsed.header.schema_version,
                    source: parsed.header.source,
                    connection_epoch: parsed.header.connection_epoch,
                    receive_seq: parsed.header.receive_seq,
                    received_at_us: parsed.header.received_at_us,
                    frame_kind: parsed.header.frame_kind,
                    payload: parsed.payload,
                    outcome: parsed.header.decode,
                });
                continue;
            }
            if magic == FOOTER_MAGIC[..4] {
                break;
            }
            bail!("unknown framing marker while reading records");
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use tempfile::TempDir;

    use super::*;

    fn test_config(directory: &Path) -> JournalConfig {
        JournalConfig {
            directory: directory.to_path_buf(),
            queue_capacity: 16,
            enqueue_timeout: Duration::from_millis(250),
            flush_interval: Duration::from_millis(10),
            sync_interval: Duration::from_millis(20),
            segment_max_bytes: 0,
            segment_max_seconds: 0,
            sync_mode: SyncMode::Interval,
        }
    }

    fn meta(run_id: &str) -> JournalMeta {
        JournalMeta {
            run_id: run_id.to_owned(),
            venue: "polymarket".to_owned(),
            code_commit: "test".to_owned(),
            config_sha256: "test".to_owned(),
        }
    }

    fn start(directory: &Path, run_id: &str) -> JournalHandle {
        JournalHandle::start(test_config(directory), meta(run_id)).expect("journal start")
    }

    fn append(
        handle: &JournalHandle,
        source: SourceId,
        epoch: u64,
        seq: u64,
        payload: &[u8],
        outcome: DecodeOutcome,
    ) {
        handle
            .record(
                source,
                epoch,
                seq,
                1_700_000_000_000_000 + seq as i64,
                RawFrame::binary(payload.to_vec()),
                outcome,
            )
            .expect("record");
    }

    fn run_directory(root: &Path, run_id: &str) -> PathBuf {
        root.join(run_id)
    }

    fn wait_failed(handle: &JournalHandle) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if handle.status().state == RunState::Failed {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("journal did not report a failure");
    }

    fn wait_for_verification_state(directory: &Path, state: RunState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(verification) = verify_run(directory)
                && verification.state == state
            {
                return;
            }
            if Instant::now() >= deadline {
                panic!("run directory never reached {state:?}");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn payload_bytes_survive_write_and_read() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        let text = br#"{"event_type":"book","asset_id":"0xabc","bids":[],"asks":[]}"#;
        let binary = [0u8, 159, 146, 150, 255];
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            text,
            DecodeOutcome::decoded(1),
        );
        append(
            &handle,
            SourceId::new(0, 1),
            1,
            2,
            &binary,
            DecodeOutcome::ignored("unknown_event_type"),
        );
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, text);
        assert_eq!(records[1].payload, binary);
        assert_eq!(records[1].frame_kind, FrameKind::Binary);
    }

    #[test]
    fn rejected_and_unknown_frames_are_recorded() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"{not json",
            DecodeOutcome::rejected("invalid_json"),
        );
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            br#"{"event_type":"future_event"}"#,
            DecodeOutcome::ignored("unknown_event_type"),
        );
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
        assert!(matches!(records[0].outcome, DecodeOutcome::Rejected { .. }));
        assert!(matches!(records[1].outcome, DecodeOutcome::Ignored { .. }));
        assert_eq!(records[0].payload, b"{not json");
    }

    #[test]
    fn checked_in_fixtures_round_trip_byte_for_byte() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        let fixtures: [&[u8]; 3] = [
            include_bytes!("../fixtures/polymarket/book_snapshot.json"),
            include_bytes!("../fixtures/polymarket/price_change_multi_asset.json"),
            include_bytes!("../fixtures/polymarket/market_resolved.json"),
        ];
        for (index, fixture) in fixtures.iter().enumerate() {
            append(
                &handle,
                SourceId::new(0, 0),
                1,
                index as u64 + 1,
                fixture,
                DecodeOutcome::decoded(1),
            );
        }
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
        for (record, fixture) in records.iter().zip(fixtures.iter()) {
            assert_eq!(record.payload.as_slice(), *fixture);
        }
    }

    #[test]
    fn record_order_matches_source_epoch_and_sequence() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"a",
            DecodeOutcome::decoded(1),
        );
        append(
            &handle,
            SourceId::new(0, 1),
            4,
            2,
            b"b",
            DecodeOutcome::decoded(1),
        );
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            3,
            b"c",
            DecodeOutcome::decoded(1),
        );
        append(
            &handle,
            SourceId::new(0, 0),
            3,
            4,
            b"d",
            DecodeOutcome::decoded(1),
        );
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(verification.complete, "{:?}", verification.errors);
        let records = read_records(&directory).expect("read records");
        let identity: Vec<_> = records
            .iter()
            .map(|record| {
                (
                    record.source.as_str(),
                    record.connection_epoch,
                    record.receive_seq,
                )
            })
            .collect();
        assert_eq!(
            identity,
            vec![
                ("s0r0", 1, 1),
                ("s0r1", 4, 2),
                ("s0r0", 1, 3),
                ("s0r0", 3, 4)
            ]
        );
    }

    #[test]
    fn record_order_is_checked_across_segments() {
        let temp = TempDir::new().expect("tempdir");
        let mut config = test_config(temp.path());
        config.segment_max_bytes = 4096;
        let handle = JournalHandle::start(config, meta("run")).expect("start");
        for seq in 1..=12 {
            append(
                &handle,
                SourceId::new(0, 0),
                1,
                seq,
                &[b'x'; 1200],
                DecodeOutcome::decoded(1),
            );
        }
        let summary = handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        assert!(summary.segment_count >= 2);
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(verification.complete, "{:?}", verification.errors);
        let records = read_records(&directory).expect("read records");
        let sequences: Vec<_> = records.iter().map(|record| record.receive_seq).collect();
        assert_eq!(sequences, (1..=12).collect::<Vec<_>>());
    }

    #[test]
    fn truncated_final_record_is_detected() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        for seq in 1..=3 {
            append(
                &handle,
                SourceId::new(0, 0),
                1,
                seq,
                &[b'x'; 256],
                DecodeOutcome::decoded(1),
            );
        }
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let directory = run_directory(temp.path(), "run");
        let path = directory.join("00000000.pmj");
        let length = fs::metadata(&path).expect("metadata").len();
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open segment");
        file.set_len(length - 8).expect("truncate");
        drop(file);
        let verification = verify_run(&directory).expect("verify");
        assert!(!verification.complete);
        assert!(
            verification
                .errors
                .iter()
                .any(|error| error.contains("segment")),
            "{:?}",
            verification.errors
        );
    }

    #[test]
    fn checksum_mismatch_and_corrupt_manifest_fail_verification() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"payload-0001",
            DecodeOutcome::decoded(1),
        );
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            b"payload-0002",
            DecodeOutcome::decoded(1),
        );
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let directory = run_directory(temp.path(), "run");
        let segment = directory.join("00000000.pmj");
        let mut bytes = fs::read(&segment).expect("read segment");
        let offset = bytes
            .windows(b"payload-0002".len())
            .position(|window| window == b"payload-0002")
            .expect("payload offset");
        bytes[offset] = b'P';
        fs::write(&segment, &bytes).expect("write segment");
        let verification = verify_run(&directory).expect("verify");
        assert!(!verification.complete);
        assert!(
            verification
                .errors
                .iter()
                .any(|error| error.contains("checksum")),
            "{:?}",
            verification.errors
        );

        let manifest_path = directory.join(MANIFEST_FILE);
        fs::write(&manifest_path, b"{not json").expect("write manifest");
        assert!(verify_run(&directory).is_err());
    }

    #[test]
    fn unfinalized_segment_is_reported_incomplete() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"unfinished",
            DecodeOutcome::decoded(1),
        );
        handle.abort(Duration::from_secs(1));
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(!verification.complete);
        assert_eq!(verification.state, RunState::Open);
        assert!(
            verification
                .errors
                .iter()
                .any(|error| error.contains("unfinalized segment")),
            "{:?}",
            verification.errors
        );
    }

    #[test]
    fn storage_write_failure_marks_run_failed() {
        let temp = TempDir::new().expect("tempdir");
        let hook: FaultHook = Arc::new(|point| {
            if point == FaultPoint::Write {
                bail!("simulated write failure");
            }
            Ok(())
        });
        let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
            .expect("journal start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        wait_failed(&handle);
        assert!(!handle.status().healthy());
        assert!(
            handle
                .record(
                    SourceId::new(0, 0),
                    1,
                    2,
                    now_us().unwrap_or(0),
                    RawFrame::binary(b"second".to_vec()),
                    DecodeOutcome::decoded(1),
                )
                .is_err()
        );
        let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
        assert!(!verification.complete);
        assert_eq!(verification.state, RunState::Failed);
    }

    #[test]
    fn storage_sync_failure_marks_run_failed() {
        let temp = TempDir::new().expect("tempdir");
        let mut config = test_config(temp.path());
        config.sync_mode = SyncMode::Always;
        let hook: FaultHook = Arc::new(|point| {
            if point == FaultPoint::Sync {
                bail!("simulated sync failure");
            }
            Ok(())
        });
        let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"record",
            DecodeOutcome::decoded(1),
        );
        wait_failed(&handle);
        let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
        assert!(!verification.complete);
        assert_eq!(verification.state, RunState::Failed);
    }

    #[test]
    fn storage_flush_failure_marks_run_failed() {
        let temp = TempDir::new().expect("tempdir");
        let mut config = test_config(temp.path());
        config.sync_mode = SyncMode::Always;
        let hook: FaultHook = Arc::new(|point| {
            if point == FaultPoint::Flush {
                bail!("simulated flush failure");
            }
            Ok(())
        });
        let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"record",
            DecodeOutcome::decoded(1),
        );
        wait_failed(&handle);
        let status = handle.status();
        assert!(!status.healthy());
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("simulated flush failure")),
            "{:?}",
            status.last_error
        );
        let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
        assert!(!verification.complete);
        assert_eq!(verification.state, RunState::Failed);
    }

    #[test]
    fn queue_saturation_then_shutdown_stays_failed() {
        let temp = TempDir::new().expect("tempdir");
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let (entered_tx, entered_rx) = mpsc::channel();
        let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Write
                && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let _ = entered_tx.send(());
                if let Ok(receiver) = release_rx.lock() {
                    let _ = receiver.recv_timeout(Duration::from_secs(10));
                }
            }
            Ok(())
        });
        let mut config = test_config(temp.path());
        config.queue_capacity = 1;
        config.enqueue_timeout = Duration::from_millis(30);
        let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer entered hook");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            b"second",
            DecodeOutcome::decoded(1),
        );
        let error = handle
            .record(
                SourceId::new(0, 0),
                1,
                3,
                now_us().unwrap_or(0),
                RawFrame::binary(b"third".to_vec()),
                DecodeOutcome::decoded(1),
            )
            .expect_err("third record must fail");
        assert!(format!("{error:#}").contains("queue stayed full"));
        assert_eq!(handle.status().state, RunState::Failed);

        // The production coordinator finalizes through `shutdown`, not
        // `abort`. The latched failure must survive that path: the run may
        // never report a complete journal after a rejected frame.
        let _ = release_tx.send(());
        let shutdown = handle.shutdown(Duration::from_secs(2));
        assert!(shutdown.is_err(), "shutdown must fail after saturation");
        assert_eq!(handle.status().state, RunState::Failed);
        assert!(handle.status().last_error.is_some());
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(!verification.complete, "{:?}", verification.errors);
        assert_eq!(verification.state, RunState::Failed);
        assert!(
            verification
                .errors
                .iter()
                .any(|error| error.contains("Failed")),
            "verifier must surface the failed run state: {:?}",
            verification.errors
        );
        // The third payload was never accepted, so it must not appear anywhere.
        let records = read_records(&directory).unwrap_or_default();
        assert!(
            records.iter().all(|record| record.payload != b"third"),
            "a rejected payload must not be written"
        );
    }

    #[test]
    fn shutdown_waits_for_a_producer_failure_latch() {
        let temp = TempDir::new().expect("tempdir");
        let (release_writer_tx, release_writer_rx) = mpsc::channel();
        let release_writer_rx = Arc::new(std::sync::Mutex::new(release_writer_rx));
        let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
        let writer_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Write
                && !writer_blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let _ = writer_entered_tx.send(());
                if let Ok(receiver) = release_writer_rx.lock() {
                    let _ = receiver.recv_timeout(Duration::from_secs(10));
                }
            }
            Ok(())
        });
        let mut config = test_config(temp.path());
        config.queue_capacity = 1;
        config.enqueue_timeout = Duration::from_millis(150);
        let handle =
            Arc::new(JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start"));
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        writer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer entered hook");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            b"second",
            DecodeOutcome::decoded(1),
        );

        // Hold the producer after the queue decision but before the failure
        // latch. Its admission guard must keep shutdown from observing an
        // idle producer set.
        let (producer_entered_tx, producer_entered_rx) = mpsc::channel();
        let (release_producer_tx, release_producer_rx) = mpsc::channel();
        let release_producer_rx = Arc::new(std::sync::Mutex::new(release_producer_rx));
        let producer_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        handle.set_record_hook(Arc::new(move || {
            if producer_gate.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let _ = producer_entered_tx.send(());
            if let Ok(receiver) = release_producer_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }));

        let producer = {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                handle.record(
                    SourceId::new(0, 0),
                    1,
                    3,
                    now_us().unwrap_or(0),
                    RawFrame::binary(b"third".to_vec()),
                    DecodeOutcome::decoded(1),
                )
            })
        };
        producer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("producer reached the post-enqueue gate");

        let shutdown = {
            let handle = Arc::clone(&handle);
            thread::spawn(move || handle.shutdown(Duration::from_secs(2)))
        };
        thread::sleep(Duration::from_millis(100));
        assert!(
            !shutdown.is_finished(),
            "shutdown must wait for the active producer"
        );

        // Give shutdown queue space while the producer is still gated. If it
        // were allowed to proceed, it would submit Finalize before the queue
        // failure is latched.
        let _ = release_writer_tx.send(());
        thread::sleep(Duration::from_millis(150));
        assert!(
            !shutdown.is_finished(),
            "shutdown must not finalize before the producer latches its queue failure"
        );

        let _ = release_producer_tx.send(());
        assert!(
            producer.join().expect("producer joined").is_err(),
            "the timed-out enqueue must be reported"
        );
        assert!(
            shutdown.join().expect("shutdown joined").is_err(),
            "a latched queue failure must fail shutdown"
        );
        assert_eq!(handle.status().state, RunState::Failed);
        let directory = run_directory(temp.path(), "run");
        wait_for_verification_state(&directory, RunState::Failed);
        let verification = verify_run(&directory).expect("verify");
        assert!(!verification.complete, "{:?}", verification.errors);
        assert!(
            read_records(&directory)
                .unwrap_or_default()
                .iter()
                .all(|record| record.payload != b"third"),
            "the rejected payload must not be written"
        );
    }

    #[test]
    fn abort_waits_for_an_active_producer_before_stopping_the_writer() {
        let temp = TempDir::new().expect("tempdir");
        let (release_writer_tx, release_writer_rx) = mpsc::channel();
        let release_writer_rx = Arc::new(std::sync::Mutex::new(release_writer_rx));
        let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
        let writer_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Write
                && !writer_blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let _ = writer_entered_tx.send(());
                if let Ok(receiver) = release_writer_rx.lock() {
                    let _ = receiver.recv_timeout(Duration::from_secs(10));
                }
            }
            Ok(())
        });
        let mut config = test_config(temp.path());
        config.queue_capacity = 2;
        let handle =
            Arc::new(JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start"));
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        writer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer entered hook");

        // This producer has already been admitted and enqueued successfully,
        // but is held inside `record` before the admission guard is released.
        let (producer_entered_tx, producer_entered_rx) = mpsc::channel();
        let (release_producer_tx, release_producer_rx) = mpsc::channel();
        let release_producer_rx = Arc::new(std::sync::Mutex::new(release_producer_rx));
        let producer_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        handle.set_record_hook(Arc::new(move || {
            if producer_gate.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let _ = producer_entered_tx.send(());
            if let Ok(receiver) = release_producer_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }));
        let producer = {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                handle.record(
                    SourceId::new(0, 0),
                    1,
                    2,
                    now_us().unwrap_or(0),
                    RawFrame::binary(b"second".to_vec()),
                    DecodeOutcome::decoded(1),
                )
            })
        };
        producer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("producer reached the post-enqueue gate");

        let abort = {
            let handle = Arc::clone(&handle);
            thread::spawn(move || handle.abort(Duration::from_secs(2)))
        };
        thread::sleep(Duration::from_millis(100));
        assert!(
            !abort.is_finished(),
            "abort must wait for the active producer"
        );

        // Free the writer. An abort that only stopped the writer would now
        // complete while the producer is still inside `record`.
        let _ = release_writer_tx.send(());
        thread::sleep(Duration::from_millis(200));
        assert!(
            !abort.is_finished(),
            "abort must not stop the writer while a producer is still active"
        );

        let _ = release_producer_tx.send(());
        producer
            .join()
            .expect("producer joined")
            .expect("the record was accepted before abort closed admission");
        abort.join().expect("abort joined");
        assert_eq!(handle.status().state, RunState::Incomplete);
        let directory = run_directory(temp.path(), "run");
        wait_for_verification_state(&directory, RunState::Open);
        assert!(!verify_run(&directory).expect("verify").complete);
    }

    #[test]
    fn concurrent_producers_stop_after_failure() {
        let temp = TempDir::new().expect("tempdir");
        let hook: FaultHook = Arc::new(|point| {
            if point == FaultPoint::Write {
                bail!("simulated write failure");
            }
            Ok(())
        });
        let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
            .expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"trigger",
            DecodeOutcome::decoded(1),
        );
        wait_failed(&handle);

        let handle = Arc::new(handle);
        let workers: Vec<_> = (0..4)
            .map(|replica| {
                let handle = Arc::clone(&handle);
                thread::spawn(move || {
                    let source = SourceId::new(0, replica);
                    for seq in 1..=25u64 {
                        match handle.record(
                            source,
                            1,
                            seq,
                            now_us().unwrap_or(0),
                            RawFrame::binary(b"late".to_vec()),
                            DecodeOutcome::decoded(1),
                        ) {
                            Ok(()) => {}
                            Err(error) => return Err(format!("{error:#}")),
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for worker in workers {
            let outcome = worker.join().expect("worker joined");
            assert!(outcome.is_err(), "a latched journal must reject records");
        }
        assert_eq!(handle.status().state, RunState::Failed);
        let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
        assert!(!verification.complete);
        assert_eq!(verification.state, RunState::Failed);
    }

    #[test]
    fn blocked_writer_bounds_shutdown() {
        let temp = TempDir::new().expect("tempdir");
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let (entered_tx, entered_rx) = mpsc::channel();
        let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Write
                && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let _ = entered_tx.send(());
                if let Ok(receiver) = release_rx.lock() {
                    let _ = receiver.recv_timeout(Duration::from_secs(10));
                }
            }
            Ok(())
        });
        let mut config = test_config(temp.path());
        // One slot is held by the queued record, so the Finalize command can
        // only be delivered if the writer drains — which it cannot.
        config.queue_capacity = 1;
        let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer entered hook");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            b"second",
            DecodeOutcome::decoded(1),
        );

        // The writer is stuck in a write and the queue is full: shutdown must
        // still return close to its deadline instead of blocking on the
        // bounded channel forever.
        let started = Instant::now();
        let shutdown = handle.shutdown(Duration::from_millis(300));
        let elapsed = started.elapsed();
        assert!(shutdown.is_err(), "a blocked writer cannot be finalized");
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
            "shutdown elapsed {elapsed:?}, expected ~300ms"
        );
        assert_eq!(handle.status().state, RunState::Failed);

        let _ = release_tx.send(());
        let directory = run_directory(temp.path(), "run");
        wait_for_verification_state(&directory, RunState::Failed);
        assert!(!verify_run(&directory).expect("verify").complete);
    }

    #[test]
    fn blocked_writer_bounds_abort() {
        let temp = TempDir::new().expect("tempdir");
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let (entered_tx, entered_rx) = mpsc::channel();
        let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Write
                && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let _ = entered_tx.send(());
                if let Ok(receiver) = release_rx.lock() {
                    let _ = receiver.recv_timeout(Duration::from_secs(10));
                }
            }
            Ok(())
        });
        let mut config = test_config(temp.path());
        // One slot is held by the queued record, so the Abort command can only
        // be delivered if the writer drains — which it cannot.
        config.queue_capacity = 1;
        let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"first",
            DecodeOutcome::decoded(1),
        );
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer entered hook");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            2,
            b"second",
            DecodeOutcome::decoded(1),
        );

        // `abort` must not delay shutdown either: the undeliverable command
        // marks the run failed and the call returns within the deadline.
        let started = Instant::now();
        handle.abort(Duration::from_millis(300));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
            "abort elapsed {elapsed:?}, expected ~300ms"
        );
        assert_eq!(handle.status().state, RunState::Failed);
        assert!(
            handle
                .status()
                .last_error
                .is_some_and(|error| error.contains("abort")),
            "abort must record why the command was not delivered"
        );

        let _ = release_tx.send(());
        let directory = run_directory(temp.path(), "run");
        wait_for_verification_state(&directory, RunState::Failed);
        assert!(!verify_run(&directory).expect("verify").complete);
    }

    #[test]
    fn finalize_reply_timeout_stays_incomplete() {
        let temp = TempDir::new().expect("tempdir");
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let (entered_tx, _entered_rx) = mpsc::channel();
        let hook: FaultHook = Arc::new(move |point| {
            if point == FaultPoint::Finalize {
                let _ = entered_tx.send(());
                if let Ok(receiver) = release_rx.lock() {
                    // A dropped release channel means the stall never ends and
                    // finalization must not complete.
                    if receiver.recv_timeout(Duration::from_secs(10)).is_err() {
                        bail!("simulated finalize stall aborted");
                    }
                }
            }
            Ok(())
        });
        let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
            .expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"record",
            DecodeOutcome::decoded(1),
        );

        // The Finalize command was accepted but the writer never replies
        // within the deadline: shutdown returns an error and the run is not
        // reported complete.
        let started = Instant::now();
        let shutdown = handle.shutdown(Duration::from_millis(300));
        let elapsed = started.elapsed();
        assert!(shutdown.is_err(), "a stalled finalization cannot complete");
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
            "shutdown elapsed {elapsed:?}, expected ~300ms"
        );
        let directory = run_directory(temp.path(), "run");
        assert!(!verify_run(&directory).expect("verify").complete);

        // Ending the stall lets the writer resolve the run to a terminal
        // failed state instead of leaving a finalization claim behind.
        drop(release_tx);
        wait_for_verification_state(&directory, RunState::Failed);
    }

    #[test]
    fn repeated_concurrent_and_dead_writer_shutdowns_are_bounded() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"record",
            DecodeOutcome::decoded(1),
        );
        let first = handle.shutdown(Duration::from_secs(2)).expect("shutdown");
        assert_eq!(first.record_count, 1);

        // A second shutdown is refused immediately instead of re-entering the
        // writer lifecycle.
        let started = Instant::now();
        let repeated = handle.shutdown(Duration::from_secs(2));
        assert!(repeated.is_err());
        assert!(started.elapsed() <= Duration::from_secs(1));

        // Concurrent shutdowns: exactly one proceeds, the rest are refused
        // without blocking.
        let handle = Arc::new(handle);
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let handle = Arc::clone(&handle);
                thread::spawn(move || handle.shutdown(Duration::from_secs(2)))
            })
            .collect();
        let mut accepted = 0;
        for racer in racers {
            match racer.join().expect("racer joined") {
                Ok(_) => accepted += 1,
                Err(error) => assert!(format!("{error:#}").contains("already requested")),
            }
        }
        assert_eq!(accepted, 0, "a joined writer cannot finalize again");

        // A writer that died in a storage failure fails shutdown promptly.
        let failed = TempDir::new().expect("tempdir");
        let hook: FaultHook = Arc::new(|point| {
            if point == FaultPoint::Write {
                bail!("simulated write failure");
            }
            Ok(())
        });
        let handle =
            JournalHandle::start_inner(test_config(failed.path()), meta("run"), Some(hook))
                .expect("start");
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"record",
            DecodeOutcome::decoded(1),
        );
        wait_failed(&handle);
        let started = Instant::now();
        assert!(handle.shutdown(Duration::from_secs(2)).is_err());
        assert!(
            started.elapsed() <= Duration::from_secs(1),
            "shutdown against a dead writer must not wait for the full timeout"
        );
    }

    #[test]
    fn successful_shutdown_reconciles_counters() {
        let temp = TempDir::new().expect("tempdir");
        let handle = start(temp.path(), "run");
        for seq in 1..=5 {
            append(
                &handle,
                SourceId::new(0, 0),
                1,
                seq,
                &[b'r'; 64],
                DecodeOutcome::decoded(1),
            );
        }
        let summary = handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        let status = handle.status();
        assert_eq!(status.state, RunState::Finalized);
        assert!(status.last_error.is_none());
        assert_eq!(status.counters.queued, 5);
        assert_eq!(status.counters.written, 5);
        assert_eq!(status.counters.flushed, 5);
        assert_eq!(status.counters.synced, 5);
        assert_eq!(status.counters.finalized, 5);
        assert_eq!(status.counters.queue_depth, 0);
        assert_eq!(summary.record_count, 5);
        assert_eq!(summary.record_count, status.counters.finalized);
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(verification.complete, "{:?}", verification.errors);
        assert_eq!(verification.record_count, status.counters.finalized);
        assert_eq!(verification.segments.len() as u64, status.counters.segments);
    }

    #[test]
    fn graceful_shutdown_finalizes_rotated_segments() {
        let temp = TempDir::new().expect("tempdir");
        let mut config = test_config(temp.path());
        config.segment_max_bytes = 4096;
        let handle = JournalHandle::start(config, meta("run")).expect("start");
        for seq in 1..=12 {
            append(
                &handle,
                SourceId::new(seq as u16 % 2, 0),
                1,
                seq,
                &[b'z'; 1200],
                DecodeOutcome::decoded(1),
            );
        }
        let summary = handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
        assert_eq!(summary.record_count, 12);
        assert!(summary.segment_count >= 3);
        let directory = run_directory(temp.path(), "run");
        let verification = verify_run(&directory).expect("verify");
        assert!(verification.complete, "{:?}", verification.errors);
        assert_eq!(verification.state, RunState::Finalized);
        assert_eq!(verification.record_count, 12);
        assert_eq!(verification.segments.len() as u64, summary.segment_count);
    }

    #[test]
    fn identical_input_produces_identical_records() {
        let first = TempDir::new().expect("tempdir");
        let second = TempDir::new().expect("tempdir");
        for root in [&first, &second] {
            let handle = start(root.path(), "run");
            for (index, payload) in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma"]
                .iter()
                .enumerate()
            {
                append(
                    &handle,
                    SourceId::new(0, 0),
                    2,
                    index as u64 + 1,
                    payload,
                    DecodeOutcome::decoded(1),
                );
            }
            handle
                .shutdown(Duration::from_secs(2))
                .expect("journal shutdown");
        }
        let left = read_records(&run_directory(first.path(), "run")).expect("left");
        let right = read_records(&run_directory(second.path(), "run")).expect("right");
        let identity = |records: Vec<JournalRecord>| {
            records
                .into_iter()
                .map(|record| {
                    (
                        record.source,
                        record.connection_epoch,
                        record.receive_seq,
                        record.received_at_us,
                        record.payload,
                        record.outcome,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(identity(left), identity(right));
    }
}
