//! Single serialized SBE writer thread.
//!
//! All exchange tasks produce normalized messages into a bounded channel. One
//! thread owns the aligned writer, preserving total ordering of writes and
//! making flush/shutdown deterministic.
//!
//! Shutdown is a state transition with a documented durability policy:
//! admission is closed (the queue's sender is dropped), the worker drains
//! every message it already accepted, flushes, syncs the file when
//! configured, and is joined before the handle reports a result. A full
//! queue can delay that transition but never silently abandon the worker.

use std::{
    fs::File,
    io::{BufWriter, Stdout},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::{
    config::{Settings, rotation_manifest_path},
    metrics::{WriterCounters, WriterStats},
    model::OutputMessage,
    report::{serialize_json_pretty, write_bytes_atomic},
    time::now_us,
    util::AlignedSbeWriter,
};

/// Everything the writer thread needs to create and rotate its output.
#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub path: String,
    pub chunk_size: usize,
    pub queue: usize,
    pub sync_on_close: bool,
    pub overwrite: bool,
    pub rotate_bytes: u64,
    pub rotate_secs: u64,
    pub min_free_bytes: u64,
    pub fsync_interval_secs: u64,
}

impl WriterOptions {
    #[must_use]
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            path: settings.output.path.clone(),
            chunk_size: settings.output.chunk_size,
            queue: settings.collector.writer_queue,
            sync_on_close: settings.output.sync_on_close,
            overwrite: settings.output.overwrite,
            rotate_bytes: settings.output.rotate_bytes,
            rotate_secs: settings.output.rotate_secs,
            min_free_bytes: settings.output.min_free_bytes,
            fsync_interval_secs: settings.output.fsync_interval_secs,
        }
    }

    #[must_use]
    pub fn rotation_enabled(&self) -> bool {
        self.path != "-" && (self.rotate_bytes > 0 || self.rotate_secs > 0)
    }
}

enum WriterCommand {
    Message(OutputMessage),
    Flush(oneshot::Sender<Result<(), String>>),
    Shutdown(oneshot::Sender<Result<(), String>>),
}

/// The byte sink owned by the writer thread. Splitting the sink behind a
/// trait keeps the worker's durability contract testable: fault injection
/// only needs another implementation, not a real filesystem failure.
trait MessageSink: Send + 'static {
    /// Append one encoded message. A `Err` return is fatal: the worker stops
    /// and the error is surfaced through the handle.
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()>;
    /// Make accepted messages visible without padding the current chunk.
    fn flush(&mut self) -> anyhow::Result<()>;
    /// Finish the final chunk (padding it for file output) and flush.
    fn finish(&mut self) -> anyhow::Result<()>;
    /// Persist flushed bytes to the storage layer where the sink supports it.
    fn sync(&mut self) -> anyhow::Result<()>;
    fn bytes_written(&self) -> u64;
    /// Finalized rotation segments. Zero for unrotated output.
    fn segments(&self) -> u64;
}

/// One unrotated file with a free-space watermark and periodic fsync.
struct FileSink {
    writer: AlignedSbeWriter<BufWriter<File>>,
    path: PathBuf,
    min_free_bytes: u64,
    fsync_interval: Duration,
    last_sync: Instant,
    watermark_checked: bool,
    last_watermark: Instant,
}

impl FileSink {
    fn open(options: &WriterOptions) -> anyhow::Result<Self> {
        let path = Path::new(&options.path);
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory `{}`", parent.display()))?;
        }
        let writer = if options.overwrite {
            AlignedSbeWriter::create(path, options.chunk_size)
        } else {
            AlignedSbeWriter::create_new(path, options.chunk_size)
        }
        .with_context(|| {
            if options.overwrite {
                format!("creating output `{}`", options.path)
            } else {
                format!(
                    "creating output `{}` without replacing an existing file \
                     (set output.overwrite = true to replace it)",
                    options.path
                )
            }
        })?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            min_free_bytes: options.min_free_bytes,
            fsync_interval: Duration::from_secs(options.fsync_interval_secs),
            last_sync: Instant::now(),
            watermark_checked: false,
            last_watermark: Instant::now(),
        })
    }

    fn check_watermark(&mut self) -> anyhow::Result<()> {
        if self.min_free_bytes == 0 {
            return Ok(());
        }
        if self.watermark_checked && self.last_watermark.elapsed() < Duration::from_secs(5) {
            return Ok(());
        }
        self.watermark_checked = true;
        self.last_watermark = Instant::now();
        let directory = self.path.parent().unwrap_or_else(|| Path::new("."));
        let free = free_bytes(directory)?;
        anyhow::ensure!(
            free >= self.min_free_bytes,
            "output filesystem at `{}` has {free} free bytes, below \
             output.min_free_bytes = {}; refusing to continue toward a full disk",
            directory.display(),
            self.min_free_bytes
        );
        Ok(())
    }

    fn maybe_sync(&mut self) -> anyhow::Result<()> {
        if !self.fsync_interval.is_zero() && self.last_sync.elapsed() >= self.fsync_interval {
            self.writer.sync_data()?;
            self.last_sync = Instant::now();
        }
        Ok(())
    }
}

impl MessageSink for FileSink {
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        self.check_watermark()?;
        match message {
            OutputMessage::Snapshot(snapshot) => self.writer.write_msg(snapshot)?,
            OutputMessage::PriceLevelUpdate(update) => self.writer.write_msg(update)?,
        }
        self.maybe_sync()
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()?;
        self.maybe_sync()
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish()
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.writer.sync_all()
    }

    fn bytes_written(&self) -> u64 {
        self.writer.bytes_written()
    }

    fn segments(&self) -> u64 {
        0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentEntry {
    sequence: u64,
    file: String,
    bytes: u64,
    sha256: String,
    finalized_at_us: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentManifest {
    schema_version: u32,
    output: String,
    chunk_size: usize,
    created_at_us: i64,
    updated_at_us: i64,
    complete: bool,
    segments: Vec<SegmentEntry>,
}

/// Size- or time-bounded segments, each finalized atomically and listed in a
/// checksummed manifest.
struct RotatingSink {
    options: WriterOptions,
    base_path: PathBuf,
    manifest_path: PathBuf,
    created_at_us: i64,
    sequence: u64,
    writer: Option<AlignedSbeWriter<BufWriter<File>>>,
    segment_partial: Option<PathBuf>,
    segment_final: Option<PathBuf>,
    messages_in_segment: u64,
    segment_started: Instant,
    finalized_bytes: u64,
    segments: Vec<SegmentEntry>,
    last_sync: Instant,
    watermark_checked: bool,
    last_watermark: Instant,
}

impl RotatingSink {
    fn open(options: &WriterOptions) -> anyhow::Result<Self> {
        let base_path = PathBuf::from(&options.path);
        if let Some(parent) = base_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory `{}`", parent.display()))?;
        }
        let manifest_path = rotation_manifest_path(&options.path);
        if options.overwrite {
            remove_existing_segments(&base_path, &manifest_path)?;
        }
        let mut sink = Self {
            options: options.clone(),
            base_path,
            manifest_path,
            created_at_us: now_us().unwrap_or(0),
            sequence: 0,
            writer: None,
            segment_partial: None,
            segment_final: None,
            messages_in_segment: 0,
            segment_started: Instant::now(),
            finalized_bytes: 0,
            segments: Vec::new(),
            last_sync: Instant::now(),
            watermark_checked: false,
            last_watermark: Instant::now(),
        };
        sink.open_segment()?;
        sink.write_manifest(false)?;
        Ok(sink)
    }

    fn check_watermark(&mut self) -> anyhow::Result<()> {
        if self.options.min_free_bytes == 0 {
            return Ok(());
        }
        if self.watermark_checked && self.last_watermark.elapsed() < Duration::from_secs(5) {
            return Ok(());
        }
        self.watermark_checked = true;
        self.last_watermark = Instant::now();
        let directory = self.base_path.parent().unwrap_or_else(|| Path::new("."));
        let free = free_bytes(directory)?;
        anyhow::ensure!(
            free >= self.options.min_free_bytes,
            "output filesystem at `{}` has {free} free bytes, below \
             output.min_free_bytes = {}; refusing to continue toward a full disk",
            directory.display(),
            self.options.min_free_bytes
        );
        Ok(())
    }

    fn open_segment(&mut self) -> anyhow::Result<()> {
        let final_path = segment_path(&self.base_path, self.sequence);
        let partial = partial_path(&final_path);
        if !self.options.overwrite && final_path.exists() {
            anyhow::bail!(
                "rotation segment `{}` already exists; set output.overwrite = true \
                 to replace existing segments",
                final_path.display()
            );
        }
        let writer = if self.options.overwrite {
            AlignedSbeWriter::create(&partial, self.options.chunk_size)
        } else {
            AlignedSbeWriter::create_new(&partial, self.options.chunk_size)
        }
        .with_context(|| format!("creating rotation segment `{}`", partial.display()))?;
        let mut writer = writer;
        writer.enable_checksum()?;
        self.writer = Some(writer);
        self.segment_partial = Some(partial);
        self.segment_final = Some(final_path);
        self.messages_in_segment = 0;
        self.segment_started = Instant::now();
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        if self.messages_in_segment == 0 {
            return false;
        }
        let bytes_reached = self.options.rotate_bytes > 0
            && self
                .writer
                .as_ref()
                .is_some_and(|writer| writer.bytes_written() >= self.options.rotate_bytes);
        let time_reached = self.options.rotate_secs > 0
            && self.segment_started.elapsed()
                >= Duration::from_secs(self.options.rotate_secs.max(1));
        bytes_reached || time_reached
    }

    fn finalize_segment(&mut self) -> anyhow::Result<()> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        let Some(partial) = self.segment_partial.take() else {
            anyhow::bail!("rotation segment has no temporary path");
        };
        let Some(final_path) = self.segment_final.take() else {
            anyhow::bail!("rotation segment has no final path");
        };
        if self.messages_in_segment == 0 {
            drop(writer);
            let _ = std::fs::remove_file(&partial);
            return Ok(());
        }
        writer.finish()?;
        writer.sync_all()?;
        let sha256 = writer
            .checksum_hex()
            .context("rotation segment checksum was not enabled")?;
        let bytes = writer.bytes_written();
        drop(writer);
        if final_path.exists() {
            if !self.options.overwrite {
                anyhow::bail!("rotation segment `{}` already exists", final_path.display());
            }
            std::fs::remove_file(&final_path).with_context(|| {
                format!("replacing rotation segment `{}`", final_path.display())
            })?;
        }
        std::fs::rename(&partial, &final_path).with_context(|| {
            format!(
                "finalizing rotation segment `{}` as `{}`",
                partial.display(),
                final_path.display()
            )
        })?;
        self.finalized_bytes = self.finalized_bytes.saturating_add(bytes);
        let file = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .context("rotation segment has no file name")?
            .to_owned();
        self.segments.push(SegmentEntry {
            sequence: self.sequence,
            file,
            bytes,
            sha256,
            finalized_at_us: now_us().unwrap_or(0),
        });
        self.sequence = self.sequence.saturating_add(1);
        self.write_manifest(false)
    }

    fn write_manifest(&self, complete: bool) -> anyhow::Result<()> {
        let manifest = SegmentManifest {
            schema_version: 1,
            output: self.options.path.clone(),
            chunk_size: self.options.chunk_size,
            created_at_us: self.created_at_us,
            updated_at_us: now_us().unwrap_or(self.created_at_us),
            complete,
            segments: self.segments.clone(),
        };
        let bytes = serialize_json_pretty(&self.manifest_path, &manifest)?;
        write_bytes_atomic(&self.manifest_path, &bytes).with_context(|| {
            format!(
                "writing rotation manifest `{}`",
                self.manifest_path.display()
            )
        })
    }

    fn maybe_sync(&mut self) -> anyhow::Result<()> {
        let interval = Duration::from_secs(self.options.fsync_interval_secs);
        if interval.is_zero() || self.last_sync.elapsed() < interval {
            return Ok(());
        }
        if let Some(writer) = &mut self.writer {
            writer.sync_data()?;
        }
        self.last_sync = Instant::now();
        Ok(())
    }
}

impl MessageSink for RotatingSink {
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        self.check_watermark()?;
        if self.writer.is_none() {
            self.open_segment()?;
        }
        if self.should_rotate() {
            self.finalize_segment()?;
            self.open_segment()?;
        }
        let writer = self
            .writer
            .as_mut()
            .context("rotation segment is not open")?;
        match message {
            OutputMessage::Snapshot(snapshot) => writer.write_msg(snapshot)?,
            OutputMessage::PriceLevelUpdate(update) => writer.write_msg(update)?,
        }
        self.messages_in_segment = self.messages_in_segment.saturating_add(1);
        self.maybe_sync()
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }
        self.maybe_sync()
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.finalize_segment()?;
        self.write_manifest(true)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.sync_data()?;
        }
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.finalized_bytes.saturating_add(
            self.writer
                .as_ref()
                .map_or(0, AlignedSbeWriter::bytes_written),
        )
    }

    fn segments(&self) -> u64 {
        self.segments.len() as u64
    }
}

enum Sink {
    File(FileSink),
    Rotating(RotatingSink),
    Stdout(AlignedSbeWriter<BufWriter<Stdout>>),
}

impl Sink {
    fn open(options: &WriterOptions) -> anyhow::Result<Self> {
        if options.path == "-" {
            let writer =
                BufWriter::with_capacity(options.chunk_size.min(1 << 20), std::io::stdout());
            return Ok(Self::Stdout(AlignedSbeWriter::from_writer(
                writer,
                options.chunk_size,
            )?));
        }
        if options.rotation_enabled() {
            return Ok(Self::Rotating(RotatingSink::open(options)?));
        }
        Ok(Self::File(FileSink::open(options)?))
    }
}

impl MessageSink for Sink {
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        match self {
            Self::File(sink) => sink.write(message),
            Self::Rotating(sink) => sink.write(message),
            Self::Stdout(writer) => match message {
                OutputMessage::Snapshot(snapshot) => writer.write_msg(snapshot),
                OutputMessage::PriceLevelUpdate(update) => writer.write_msg(update),
            },
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::File(sink) => sink.flush(),
            Self::Rotating(sink) => sink.flush(),
            Self::Stdout(writer) => writer.flush(),
        }
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        match self {
            Self::File(sink) => sink.finish(),
            Self::Rotating(sink) => sink.finish(),
            Self::Stdout(writer) => writer.finish(),
        }
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        match self {
            Self::File(sink) => sink.sync(),
            Self::Rotating(sink) => sink.sync(),
            Self::Stdout(_) => Ok(()),
        }
    }

    fn bytes_written(&self) -> u64 {
        match self {
            Self::File(sink) => sink.bytes_written(),
            Self::Rotating(sink) => sink.bytes_written(),
            Self::Stdout(writer) => writer.bytes_written(),
        }
    }

    fn segments(&self) -> u64 {
        match self {
            Self::File(sink) => sink.segments(),
            Self::Rotating(sink) => sink.segments(),
            Self::Stdout(_) => 0,
        }
    }
}

fn segment_path(base: &Path, sequence: u64) -> PathBuf {
    let name = base
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("output");
    base.with_file_name(format!("{name}.{sequence:06}.sbe"))
}

fn partial_path(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.sbe");
    segment.with_file_name(format!("{name}.partial"))
}

/// Remove only the artifacts this run's rotation namespace owns.
fn remove_existing_segments(base: &Path, manifest: &Path) -> anyhow::Result<()> {
    let Some(parent) = base.parent() else {
        return Ok(());
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let prefix = format!(
        "{}.",
        base.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("output")
    );
    if parent.exists() {
        for entry in std::fs::read_dir(parent)?.flatten() {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if rotation_artifact_name(&prefix, file_name) {
                std::fs::remove_file(entry.path()).with_context(|| {
                    format!("removing previous segment `{}`", entry.path().display())
                })?;
            }
        }
    }
    if manifest.exists() {
        std::fs::remove_file(manifest)
            .with_context(|| format!("removing previous manifest `{}`", manifest.display()))?;
    }
    Ok(())
}

fn rotation_artifact_name(prefix: &str, name: &str) -> bool {
    let stem = name.strip_prefix(prefix).and_then(|value| {
        value
            .strip_suffix(".sbe")
            .or_else(|| value.strip_suffix(".sbe.partial"))
    });
    stem.is_some_and(|sequence| {
        sequence.len() == 6 && sequence.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[cfg(unix)]
fn free_bytes(path: &Path) -> anyhow::Result<u64> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    // `df -Pk` is the POSIX machine-readable form: one row per filesystem and
    // a 1024-byte block size. Parsing the fourth column avoids locale-specific
    // human formatting. The probe runs at most once every five seconds.
    let output = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .with_context(|| format!("running `df` for `{}`", path.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "`df` failed for `{}`: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let row = text
        .lines()
        .nth(1)
        .with_context(|| format!("`df` returned no data row for `{}`", path.display()))?;
    let available_kb = row
        .split_whitespace()
        .nth(3)
        .with_context(|| format!("`df` returned no available column for `{}`", path.display()))?
        .parse::<u64>()
        .with_context(|| format!("parsing `df` available space for `{}`", path.display()))?;
    Ok(available_kb.saturating_mul(1024))
}

#[cfg(not(unix))]
fn free_bytes(_path: &Path) -> anyhow::Result<u64> {
    // Without a portable free-space API the watermark is advisory. The
    // operating envelope documents Unix as the supported deployment target.
    Ok(u64::MAX)
}

/// Async handle used by the coordinator.
pub struct WriterHandle {
    sender: mpsc::Sender<WriterCommand>,
    counters: Arc<WriterCounters>,
    join: Option<std::thread::JoinHandle<()>>,
    output_path: String,
    fatal: Arc<Mutex<Option<String>>>,
}

impl WriterHandle {
    /// Start the writer thread. The output file is created immediately so an
    /// empty, valid file exists even if the process is interrupted during
    /// startup.
    pub fn start(options: WriterOptions) -> anyhow::Result<Self> {
        let sink = Sink::open(&options)?;
        Self::start_with_sink(
            Box::new(sink),
            options.queue,
            options.sync_on_close,
            &options.path,
        )
    }

    /// Start the writer thread over an injected sink. Production callers pass
    /// a file or stdout sink; tests pass fault-injecting sinks to exercise the
    /// durability contract.
    fn start_with_sink(
        sink: Box<dyn MessageSink>,
        queue: usize,
        sync_on_close: bool,
        path: &str,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(queue > 0, "writer queue must be positive");
        let counters = Arc::new(WriterCounters::default());
        let writer_counters = Arc::clone(&counters);
        let (sender, mut receiver) = mpsc::channel::<WriterCommand>(queue);
        let fatal = Arc::new(Mutex::new(None));
        let writer_fatal = Arc::clone(&fatal);
        let thread_name = "sbe-writer".to_owned();
        let join = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let mut sink = sink;
                let mut explicit_shutdown = false;
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        WriterCommand::Message(message) => {
                            writer_counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                            if let Err(error) = sink.write(&message) {
                                let text = format!("{error:#}");
                                error!(error = %text, "SBE writer failed");
                                set_fatal(&writer_fatal, &text);
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                // A failed write invalidates the sink; a final
                                // flush could mask the original error.
                                return;
                            }
                            writer_counters.messages.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                            writer_counters
                                .segments
                                .store(sink.segments(), Ordering::Relaxed);
                        }
                        WriterCommand::Flush(ack) => {
                            let result = sink.flush().map_err(|error| format!("{error:#}"));
                            if let Err(error) = &result {
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                set_fatal(&writer_fatal, error);
                            }
                            writer_counters.flushes.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                            writer_counters
                                .segments
                                .store(sink.segments(), Ordering::Relaxed);
                            let _ = ack.send(result);
                        }
                        WriterCommand::Shutdown(ack) => {
                            let mut result = sink.finish().map_err(|error| format!("{error:#}"));
                            if result.is_ok() && sync_on_close {
                                result = sink.sync().map_err(|error| format!("{error:#}"));
                            }
                            if let Err(error) = &result {
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                set_fatal(&writer_fatal, error);
                            }
                            writer_counters.flushes.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                            writer_counters
                                .segments
                                .store(sink.segments(), Ordering::Relaxed);
                            let _ = ack.send(result);
                            explicit_shutdown = true;
                            break;
                        }
                    }
                }
                // The receiver returns `None` only when every sender was
                // dropped - for example when shutdown hit a full queue.
                // Closing admission is still a clean close: the queue is
                // empty by now, so drain-then-exit must uphold the same
                // durability contract as an explicit shutdown command.
                if !explicit_shutdown {
                    if let Err(error) = sink.finish() {
                        let text = format!("{error:#}");
                        error!(error = %text, "SBE writer final flush failed");
                        set_fatal(&writer_fatal, &text);
                        writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                    } else if sync_on_close && let Err(error) = sink.sync() {
                        let text = format!("{error:#}");
                        error!(error = %text, "SBE writer final sync failed");
                        set_fatal(&writer_fatal, &text);
                        writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                    }
                    writer_counters
                        .bytes
                        .store(sink.bytes_written(), Ordering::Relaxed);
                    writer_counters
                        .segments
                        .store(sink.segments(), Ordering::Relaxed);
                }
            })
            .context("spawning SBE writer thread")?;

        info!(path, "SBE writer started");
        Ok(Self {
            sender,
            counters,
            join: Some(join),
            output_path: if path == "-" {
                "stdout".to_owned()
            } else {
                PathBuf::from(path).display().to_string()
            },
            fatal,
        })
    }

    /// Send one message, waiting for queue capacity. A timeout prevents a
    /// wedged writer from deadlocking the whole collector forever.
    ///
    /// Capacity is reserved before the message is handed over, so a timed-out
    /// send cannot have enqueued the message and cannot leave the queue-depth
    /// counter off by one.
    pub async fn send(&self, message: OutputMessage, timeout: Duration) -> anyhow::Result<()> {
        let permit = match tokio::time::timeout(timeout, self.sender.reserve()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                anyhow::bail!("SBE writer stopped: {}", self.fatal_message());
            }
            Err(_) => {
                anyhow::bail!("SBE writer queue full for {timeout:?}");
            }
        };
        self.counters.queue_depth.fetch_add(1, Ordering::Relaxed);
        permit.send(WriterCommand::Message(message));
        Ok(())
    }

    /// Flush currently encoded bytes without padding. The final chunk is
    /// padded by [`WriterHandle::shutdown`].
    pub async fn flush(&self, timeout: Duration) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        match tokio::time::timeout(timeout, self.sender.send(WriterCommand::Flush(tx))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                anyhow::bail!("SBE writer stopped: {}", self.fatal_message());
            }
            Err(_) => {
                anyhow::bail!("SBE writer flush enqueue timed out after {timeout:?}");
            }
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => anyhow::bail!("SBE writer flush failed: {error}"),
            Ok(Err(_)) => anyhow::bail!("SBE writer dropped flush acknowledgement"),
            Err(_) => anyhow::bail!("SBE writer flush timed out after {timeout:?}"),
        }
    }

    /// Flush, sync (for files), stop the writer thread, and return final
    /// counters. The counters are sampled after the worker terminates so a
    /// run report includes every accepted message.
    pub async fn shutdown_with_stats(self, timeout: Duration) -> anyhow::Result<WriterStats> {
        self.shutdown_inner(timeout).await
    }

    /// Flush, sync (for files), and stop the writer thread.
    pub async fn shutdown(self, timeout: Duration) -> anyhow::Result<()> {
        self.shutdown_with_stats(timeout).await.map(|_| ())
    }

    /// Durability-preserving shutdown.
    ///
    /// The transition has three bounded phases:
    ///
    /// 1. Deliver a `Shutdown` command (bounded by `timeout`). If the queue
    ///    is saturated the command is not delivered, but that must never
    ///    detach the worker.
    /// 2. Close admission by dropping the queue's sender. The worker then
    ///    drains every message it already accepted, flushes, syncs when
    ///    configured, and exits.
    /// 3. Join the worker (bounded by `timeout`). If it does not terminate
    ///    in time, return an explicit incomplete-durability error with exact
    ///    queued/written/error counts; the thread is detached because a
    ///    Rust thread cannot be killed, so the error is the cleanup policy.
    async fn shutdown_inner(self, timeout: Duration) -> anyhow::Result<WriterStats> {
        let Self {
            sender,
            counters,
            mut join,
            output_path: _,
            fatal,
        } = self;

        let (ack_tx, ack_rx) = oneshot::channel();
        let mut ack_result: Option<anyhow::Error> = None;
        let enqueue =
            tokio::time::timeout(timeout, sender.send(WriterCommand::Shutdown(ack_tx))).await;

        match enqueue {
            Ok(Ok(())) => match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(message))) => {
                    ack_result = Some(anyhow::anyhow!("SBE writer shutdown failed: {message}"));
                }
                Ok(Err(_)) => {
                    // The worker exited before answering (a fatal write error
                    // stopped it). The fatal slot records the cause.
                }
                Err(_) => {
                    // The worker is inside a long flush/sync. Closing
                    // admission lets it finish; the join bounds the wait.
                }
            },
            Ok(Err(_)) => {
                // The worker already stopped; the fatal slot records why.
            }
            Err(_) => {
                // Queue saturated: admission closes below and the worker
                // drains before exiting. This is exactly the backpressure
                // condition shutdown must survive.
            }
        }

        // Close admission. This is the only way to guarantee termination of
        // a worker whose queue is full: once every sender is dropped,
        // `blocking_recv` drains the queue and returns `None`.
        drop(sender);

        let joined = match join.take() {
            Some(handle) => {
                tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || handle.join()))
                    .await
            }
            None => Ok(Ok(Ok(()))),
        };

        let stats = counters.snapshot();
        let fatal_message = fatal
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "no writer error recorded".to_owned());

        let joined = match joined {
            Ok(Ok(joined)) => joined,
            Ok(Err(join_error)) => {
                return Err(anyhow::anyhow!(
                    "SBE writer join task failed: {join_error}; \
                     accepted={}, written={}, queued={}, errors={}",
                    stats.messages + stats.queue_depth,
                    stats.messages,
                    stats.queue_depth,
                    stats.errors
                ));
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "SBE writer did not terminate within {timeout:?}; \
                     durability is NOT guaranteed for the remaining messages; \
                     accepted={}, written={}, queued={}, errors={}, last_error={fatal_message}",
                    stats.messages + stats.queue_depth,
                    stats.messages,
                    stats.queue_depth,
                    stats.errors
                ));
            }
        };

        if let Err(panic) = joined {
            return Err(anyhow::anyhow!(
                "SBE writer thread panicked: {panic:?}; \
                 accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            ));
        }

        if let Some(error) = ack_result {
            return Err(error.context(format!(
                "accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            )));
        }
        if fatal_message != "no writer error recorded" {
            return Err(anyhow::anyhow!(
                "SBE writer stopped: {fatal_message}; \
                 accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            ));
        }
        Ok(stats)
    }

    #[must_use]
    pub fn stats(&self) -> WriterStats {
        self.counters.snapshot()
    }

    #[must_use]
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    #[must_use]
    pub fn fatal_message(&self) -> String {
        self.fatal
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "no writer error recorded".to_owned())
    }

    /// True while no fatal write, flush, finish, or sync error is recorded.
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.fatal
            .lock()
            .map(|slot| slot.is_none())
            .unwrap_or(false)
    }
}

fn set_fatal(slot: &Arc<Mutex<Option<String>>>, error: &str) {
    if let Ok(mut slot) = slot.lock()
        && slot.is_none()
    {
        *slot = Some(error.to_owned());
    }
}

/// Convenience used by replay to write a finite stream synchronously.
pub struct DirectWriter {
    sink: Sink,
    counters: WriterCounters,
    sync_on_close: bool,
}

impl DirectWriter {
    pub fn create(options: WriterOptions) -> anyhow::Result<Self> {
        Ok(Self {
            sink: Sink::open(&options)?,
            counters: WriterCounters::default(),
            sync_on_close: options.sync_on_close,
        })
    }

    pub fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        MessageSink::write(&mut self.sink, message)?;
        self.counters.messages.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        self.counters
            .segments
            .store(MessageSink::segments(&self.sink), Ordering::Relaxed);
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        MessageSink::flush(&mut self.sink)?;
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        self.counters
            .segments
            .store(MessageSink::segments(&self.sink), Ordering::Relaxed);
        Ok(())
    }

    pub fn shutdown(mut self) -> anyhow::Result<WriterStats> {
        MessageSink::finish(&mut self.sink)?;
        if self.sync_on_close {
            MessageSink::sync(&mut self.sink)?;
        }
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        self.counters
            .segments
            .store(MessageSink::segments(&self.sink), Ordering::Relaxed);
        Ok(self.counters.snapshot())
    }

    #[must_use]
    pub fn stats(&self) -> WriterStats {
        self.counters.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    const TINY_TIMEOUT: Duration = Duration::from_millis(100);
    const PATIENT_TIMEOUT: Duration = Duration::from_secs(10);

    fn options(
        path: &str,
        chunk_size: usize,
        queue: usize,
        sync: bool,
        overwrite: bool,
    ) -> WriterOptions {
        WriterOptions {
            path: path.to_owned(),
            chunk_size,
            queue,
            sync_on_close: sync,
            overwrite,
            rotate_bytes: 0,
            rotate_secs: 0,
            min_free_bytes: 0,
            fsync_interval_secs: 0,
        }
    }

    fn update(symbol: &str) -> OutputMessage {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: 500_000,
            quantity: 1_000_000,
            side: Side::Buy,
            symbol: symbol.to_owned(),
        })
    }

    /// Sink whose failures are injected at exact points.
    struct FaultySink {
        fail_write_at: Option<usize>,
        fail_flush: bool,
        fail_finish: bool,
        fail_sync: bool,
        /// Delay injected before every write, used to saturate the queue.
        stall_write: Option<Duration>,
        writes: usize,
    }

    impl FaultySink {
        fn healthy() -> Self {
            Self {
                fail_write_at: None,
                fail_flush: false,
                fail_finish: false,
                fail_sync: false,
                stall_write: None,
                writes: 0,
            }
        }
    }

    impl MessageSink for FaultySink {
        fn write(&mut self, _message: &OutputMessage) -> anyhow::Result<()> {
            if let Some(stall) = self.stall_write {
                std::thread::sleep(stall);
            }
            self.writes += 1;
            if Some(self.writes) == self.fail_write_at {
                anyhow::bail!("injected write failure");
            }
            Ok(())
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            if self.fail_flush {
                anyhow::bail!("injected flush failure");
            }
            Ok(())
        }

        fn finish(&mut self) -> anyhow::Result<()> {
            if self.fail_finish {
                anyhow::bail!("injected finish failure");
            }
            Ok(())
        }

        fn sync(&mut self) -> anyhow::Result<()> {
            if self.fail_sync {
                anyhow::bail!("injected sync failure");
            }
            Ok(())
        }

        fn bytes_written(&self) -> u64 {
            0
        }

        fn segments(&self) -> u64 {
            0
        }
    }

    async fn wait_for_fatal(writer: &WriterHandle) {
        for _ in 0..200 {
            if writer.fatal_message() != "no writer error recorded" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn normal_shutdown_drains_and_joins() {
        let writer =
            WriterHandle::start_with_sink(Box::new(FaultySink::healthy()), 16, true, "unused")
                .unwrap();
        for i in 0..4 {
            writer
                .send(update(&format!("sym-{i}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.messages, 4);
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.errors, 0);
    }

    #[tokio::test]
    async fn write_failure_surfaces_and_worker_joins() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_write_at: Some(1),
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        // The first send may still be accepted (queued); the worker fails on
        // it asynchronously.
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        wait_for_fatal(&writer).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            error.to_string().contains("injected write failure"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn finish_failure_on_shutdown_is_reported() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_finish: true,
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("injected finish failure"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn periodic_flush_failure_is_reported() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_flush: true,
                ..FaultySink::healthy()
            }),
            16,
            false,
            "unused",
        )
        .unwrap();
        let error = writer.flush(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("injected flush failure"),
            "unexpected error: {error:#}"
        );
        // A later successful shutdown must not erase the flush failure or
        // report the run as complete.
        let shutdown = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{shutdown:#}").contains("injected flush failure"),
            "unexpected shutdown error: {shutdown:#}"
        );
    }

    #[tokio::test]
    async fn sync_failure_on_shutdown_is_reported() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_sync: true,
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("injected sync failure"),
            "unexpected error: {error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_drains_saturated_queue_and_joins() {
        // A FIFO whose reader stalls blocks the worker's writes once the
        // pipe buffer fills, which saturates the bounded queue. Shutdown
        // must close admission, let the reader drain, join the worker, and
        // confirm every accepted message was written.
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("blocked.sbe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let reader_path = fifo.clone();
        let reader = tokio::task::spawn_blocking(move || {
            // Opening the read end blocks until the writer opens the FIFO.
            let mut file = std::fs::File::open(&reader_path).unwrap();
            std::thread::sleep(Duration::from_millis(400));
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes.len()
        });
        // Give the reader thread a moment to reach open().
        tokio::time::sleep(Duration::from_millis(50)).await;

        let writer = WriterHandle::start(options(
            fifo.to_str().unwrap(),
            1024,
            128,
            true, // sync on close; the drain path must sync too
            true, // the FIFO already exists; explicit overwrite is required
        ))
        .unwrap();

        // Fill the queue: keep sending until the bounded queue rejects.
        let mut accepted = 0usize;
        for i in 0..20_000 {
            match writer.send(update(&format!("sym-{i}")), TINY_TIMEOUT).await {
                Ok(()) => accepted += 1,
                Err(_) => break,
            }
        }
        assert!(
            accepted > 128,
            "expected the queue to saturate (accepted {accepted})"
        );

        // The reader wakes up around now and drains the pipe, so the worker
        // must finish draining, flush, sync, and exit within the budget.
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.queue_depth, 0, "every accepted message must drain");
        assert_eq!(stats.messages as usize, accepted);
        assert_eq!(stats.errors, 0);
        let read_bytes = reader.await.unwrap();
        assert!(
            read_bytes >= accepted,
            "reader saw {read_bytes} bytes for {accepted} messages"
        );
    }

    #[tokio::test]
    async fn saturated_send_times_out_without_queue_side_effects() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                stall_write: Some(Duration::from_millis(500)),
                ..FaultySink::healthy()
            }),
            2,
            false,
            "unused",
        )
        .unwrap();
        // The worker picks up the first message and stalls inside its write,
        // so the two remaining slots can be filled deterministically.
        writer.send(update("sym-a"), TINY_TIMEOUT).await.unwrap();
        for _ in 0..200 {
            if writer.stats().queue_depth == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            writer.stats().queue_depth,
            0,
            "worker did not pick up the first message"
        );
        writer.send(update("sym-b"), TINY_TIMEOUT).await.unwrap();
        writer.send(update("sym-c"), TINY_TIMEOUT).await.unwrap();
        let error = writer
            .send(update("sym-d"), Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("queue full"),
            "unexpected error: {error:#}"
        );
        // The timed-out send never entered the queue: exactly the three
        // accepted messages are written, and the depth returns to zero.
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.messages, 3);
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.errors, 0);
    }

    #[test]
    fn exclusive_creation_does_not_truncate_an_existing_capture() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("existing.sbe");
        std::fs::write(&path, b"previous capture").unwrap();
        assert!(crate::util::AlignedSbeWriter::create_new(&path, 4096).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"previous capture");
    }

    #[tokio::test]
    async fn flush_makes_output_visible_to_reader() {
        // Output-visibility contract: accepted messages reach the file after
        // the next flush, without padding a whole chunk. A reader can decode
        // the partial final chunk. Shutdown pads the final chunk so the
        // template's default reader can decode the file too.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("visible.sbe");
        let path = path.to_str().unwrap();
        let writer = WriterHandle::start(options(path, 4096, 16, false, false)).unwrap();
        for i in 0..3 {
            writer
                .send(update(&format!("sym-{i}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        // Give the worker a moment to consume the queue into its chunk
        // buffer, then confirm nothing has reached the file yet.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);

        writer.flush(PATIENT_TIMEOUT).await.unwrap();
        let visible_len = std::fs::metadata(path).unwrap().len();
        assert!(visible_len > 0, "flushed bytes must be visible on disk");
        assert!(
            visible_len < 4096,
            "a periodic flush must not pad a whole chunk (wrote {visible_len})"
        );

        let mut reader =
            AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(path, 4096).unwrap();
        let mut decoded = 0;
        while reader.read_msg().unwrap().is_some() {
            decoded += 1;
        }
        assert_eq!(decoded, 3, "the partial chunk must be decodable");
        drop(reader);

        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(std::fs::metadata(path).unwrap().len(), 4096);
    }

    #[tokio::test]
    async fn default_chunk_output_is_readable_by_default_reader() {
        // The template's `AlignedSbeReader::open()` hardcodes a 64 MiB chunk.
        // A default collector capture must therefore use the same chunk size
        // and be padded on close, otherwise the provided helper decodes zero
        // messages.
        const TEMPLATE_CHUNK: usize = 64 << 20;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("template-default.sbe");
        let path_str = path.to_str().unwrap();
        let writer =
            WriterHandle::start(options(path_str, TEMPLATE_CHUNK, 16, false, false)).unwrap();
        for i in 0..3 {
            writer
                .send(update(&format!("sym-{i}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.messages, 3);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            TEMPLATE_CHUNK as u64,
            "the final chunk must be padded to the template's chunk size"
        );

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        let mut decoded = 0;
        while reader.read_msg().unwrap().is_some() {
            decoded += 1;
        }
        assert_eq!(
            decoded, 3,
            "template-default reader must decode all messages"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_sink_flushes_and_skips_invalid_fsync() {
        // fsync on a FIFO returns EINVAL on Linux. A pipe output path must
        // still flush and shut down cleanly, including when sync_on_close is
        // enabled.
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe.sbe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let reader_path = fifo.clone();
        let reader = tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(&reader_path).unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes.len()
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let writer =
            WriterHandle::start(options(fifo.to_str().unwrap(), 1024, 16, true, true)).unwrap();
        writer.send(update("fifo"), TINY_TIMEOUT).await.unwrap();
        writer.flush(PATIENT_TIMEOUT).await.unwrap();
        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
        assert!(reader.await.unwrap() > 0);
    }

    #[tokio::test]
    async fn rotation_finalizes_independently_verifiable_segments() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("rotated.sbe");
        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, true, false);
        writer_options.rotate_bytes = 1024;
        let writer = WriterHandle::start(writer_options).unwrap();
        for index in 0..100 {
            writer
                .send(update(&format!("sym-{index}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert!(
            stats.segments >= 2,
            "expected at least two segments, got {}",
            stats.segments
        );

        let manifest_path = rotation_manifest_path(output.to_str().unwrap());
        let manifest: SegmentManifest =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        assert!(manifest.complete);
        assert_eq!(manifest.segments.len() as u64, stats.segments);
        assert_eq!(manifest.chunk_size, 1024);

        let mut decoded = 0usize;
        for entry in &manifest.segments {
            let path = directory.path().join(&entry.file);
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len() as u64, entry.bytes);
            assert_eq!(crate::util::sha256_hex(&bytes), entry.sha256);
            assert_eq!(bytes.len() % 1024, 0, "segments stay chunk-aligned");
            let mut reader =
                AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(&path, 1024).unwrap();
            while reader.read_msg().unwrap().is_some() {
                decoded += 1;
            }
        }
        assert_eq!(decoded, 100);
        // No temporary segment survives a clean finalization.
        for entry in std::fs::read_dir(directory.path()).unwrap().flatten() {
            assert!(
                !entry.file_name().to_string_lossy().ends_with(".partial"),
                "unfinalized segment left behind: {}",
                entry.path().display()
            );
        }
    }

    #[tokio::test]
    async fn rotation_does_not_reuse_a_partial_segment_from_an_interrupted_run() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("crash.sbe");
        let partial = directory.path().join("crash.sbe.000000.sbe.partial");
        std::fs::write(&partial, b"interrupted").unwrap();
        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, false, false);
        writer_options.rotate_bytes = 1024;
        let error = WriterHandle::start(writer_options)
            .err()
            .expect("an interrupted segment must not be adopted");
        assert!(
            format!("{error:#}").contains("rotation segment"),
            "unexpected error: {error:#}"
        );
        assert_eq!(std::fs::read(&partial).unwrap(), b"interrupted");

        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, false, true);
        writer_options.rotate_bytes = 1024;
        let writer = WriterHandle::start(writer_options).unwrap();
        writer.send(update("sym"), TINY_TIMEOUT).await.unwrap();
        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
        assert!(
            !partial.exists(),
            "explicit overwrite must discard the interrupted staging file"
        );
    }

    #[tokio::test]
    async fn rotation_refuses_existing_segments_without_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("rotated.sbe");
        std::fs::write(directory.path().join("rotated.sbe.000000.sbe"), b"old").unwrap();
        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, false, false);
        writer_options.rotate_bytes = 1024;
        let error = WriterHandle::start(writer_options)
            .err()
            .expect("existing segment must be rejected");
        assert!(
            format!("{error:#}").contains("already exists"),
            "unexpected error: {error:#}"
        );

        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, false, true);
        writer_options.rotate_bytes = 1024;
        let writer = WriterHandle::start(writer_options).unwrap();
        writer.send(update("sym"), TINY_TIMEOUT).await.unwrap();
        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
        assert!(
            !directory.path().join("rotated.sbe.000001.sbe").exists(),
            "explicit overwrite starts a fresh segment namespace"
        );
    }

    #[test]
    fn cleanup_matches_only_owned_rotation_artifacts() {
        assert!(rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.000001.sbe"
        ));
        assert!(rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.000001.sbe.partial"
        ));
        assert!(!rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.backup.sbe"
        ));
        assert!(!rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.000001.log"
        ));
    }

    #[tokio::test]
    async fn time_rotation_starts_a_new_segment_after_the_interval() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("timed.sbe");
        let mut writer_options = options(output.to_str().unwrap(), 1024, 16, true, false);
        writer_options.rotate_secs = 1;
        let writer = WriterHandle::start(writer_options).unwrap();
        writer.send(update("first"), TINY_TIMEOUT).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        writer.send(update("second"), TINY_TIMEOUT).await.unwrap();
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.segments, 2);
    }

    #[tokio::test]
    async fn watermark_failure_stops_the_writer_before_the_disk_fills() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("watermark.sbe");
        let mut writer_options = options(output.to_str().unwrap(), 4096, 16, false, false);
        writer_options.min_free_bytes = u64::MAX;
        let writer = WriterHandle::start(writer_options).unwrap();
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        wait_for_fatal(&writer).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("min_free_bytes"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn periodic_fsync_does_not_pad_the_active_chunk() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("fsync.sbe");
        let mut writer_options = options(output.to_str().unwrap(), 4096, 16, false, false);
        writer_options.fsync_interval_secs = 1;
        let writer = WriterHandle::start(writer_options).unwrap();
        writer.send(update("sym"), TINY_TIMEOUT).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        writer.flush(PATIENT_TIMEOUT).await.unwrap();
        let visible = std::fs::metadata(&output).unwrap().len();
        assert!(
            visible > 0 && visible < 4096,
            "periodic fsync must not pad a whole chunk, saw {visible} bytes"
        );
        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
    }
}
