//! Bounded, latest-wins persistence for metadata and catalog checkpoints.
//!
//! Serialization, fsync, and rename run on one dedicated thread. The
//! coordinator only publishes the newest immutable market snapshot; an older
//! pending version is replaced instead of queued, so a slow disk cannot grow an
//! unbounded backlog or stall event ingestion. Every completed write updates a
//! status snapshot used by `/readyz`, the run report, and shutdown.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::Catalog,
    model::MarketInfo,
    report::{MarketMetadata, serialize_json_pretty, write_bytes_atomic},
    time::now_us,
};

/// Result of the most recent persistence attempt.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistenceStatus {
    /// Backwards-compatible alias for `submitted_generation`.
    pub generation: u64,
    /// Newest generation accepted from the coordinator.
    pub submitted_generation: u64,
    /// Newest generation whose write attempt completed.
    pub completed_generation: u64,
    /// A newer snapshot is waiting for the writer.
    pub pending: bool,
    pub last_success_us: Option<i64>,
    pub last_error: Option<String>,
    pub metadata_bytes: u64,
    pub catalog_bytes: u64,
    pub serialize_ms: u64,
    pub write_ms: u64,
}

struct Job {
    generation: u64,
    markets: Arc<Vec<MarketInfo>>,
    generated_at_us: i64,
    /// Catalog generation time. `None` leaves the checkpoint untouched, so a
    /// warm checkpoint's age is not reset by an unrelated metadata refresh.
    catalog_generated_at_us: Option<i64>,
    ack: Option<Sender<Result<(), String>>>,
}

#[derive(Default)]
struct Queue {
    pending: Option<Job>,
    shutdown: bool,
}

struct Inner {
    queue: Mutex<Queue>,
    wake: Condvar,
    status: Mutex<PersistenceStatus>,
    metadata_path: Option<PathBuf>,
    catalog_path: Option<PathBuf>,
    exchange: String,
    hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Kept only by public handles, never by the worker. Its final drop is the
/// reliable owner-loss signal; counting `Inner` references cannot work because
/// the worker necessarily owns one while it is waiting.
struct Owner {
    inner: Arc<Inner>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.inner.queue.lock() {
            queue.shutdown = true;
            self.inner.wake.notify_all();
        }
    }
}

/// Handle used by the coordinator. Cloning shares the same worker and status.
#[derive(Clone)]
pub struct PersistenceHandle {
    inner: Arc<Inner>,
    _owner: Arc<Owner>,
    done: Arc<Mutex<Option<Receiver<()>>>>,
    join: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    shutdown: Arc<Mutex<bool>>,
}

impl PersistenceHandle {
    /// Start the worker. With no paths configured the handle is a no-op and
    /// still reports a successful, empty status.
    pub fn start(
        metadata_path: Option<PathBuf>,
        catalog_path: Option<PathBuf>,
        exchange: &str,
    ) -> anyhow::Result<Self> {
        Self::start_inner(metadata_path, catalog_path, exchange, None)
    }

    fn start_inner(
        metadata_path: Option<PathBuf>,
        catalog_path: Option<PathBuf>,
        exchange: &str,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> anyhow::Result<Self> {
        let inner = Arc::new(Inner {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            status: Mutex::new(PersistenceStatus::default()),
            metadata_path,
            catalog_path,
            exchange: exchange.to_owned(),
            hook,
        });
        let (done_tx, done_rx) = channel();
        let worker = Arc::clone(&inner);
        let join = std::thread::Builder::new()
            .name("persistence".to_owned())
            .spawn(move || {
                worker_loop(worker);
                let _ = done_tx.send(());
            })
            .context("spawning persistence worker")?;
        Ok(Self {
            _owner: Arc::new(Owner {
                inner: Arc::clone(&inner),
            }),
            inner,
            done: Arc::new(Mutex::new(Some(done_rx))),
            join: Arc::new(Mutex::new(Some(join))),
            shutdown: Arc::new(Mutex::new(false)),
        })
    }

    #[cfg(test)]
    fn start_with_hook(
        metadata_path: Option<PathBuf>,
        catalog_path: Option<PathBuf>,
        exchange: &str,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(metadata_path, catalog_path, exchange, Some(hook))
    }

    /// Publish a new immutable snapshot. Intermediate versions are replaced.
    ///
    /// `catalog_generated_at_us` is `None` when the snapshot came from a warm
    /// checkpoint rather than a fresh discovery round; rewriting it would
    /// falsely refresh the catalog's age.
    pub fn submit(
        &self,
        markets: Arc<Vec<MarketInfo>>,
        generated_at_us: i64,
        catalog_generated_at_us: Option<i64>,
    ) -> anyhow::Result<()> {
        self.submit_job(markets, generated_at_us, catalog_generated_at_us, None)
    }

    /// Publish the final snapshot and wait for its write to finish.
    pub fn submit_final(
        &self,
        markets: Arc<Vec<MarketInfo>>,
        generated_at_us: i64,
        catalog_generated_at_us: Option<i64>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = channel();
        self.submit_job(
            markets,
            generated_at_us,
            catalog_generated_at_us,
            Some(ack_tx),
        )?;
        match ack_rx.recv_timeout(timeout) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => anyhow::bail!("persistence write failed: {error}"),
            Err(_) => anyhow::bail!("persistence write timed out after {timeout:?}"),
        }
    }

    fn submit_job(
        &self,
        markets: Arc<Vec<MarketInfo>>,
        generated_at_us: i64,
        catalog_generated_at_us: Option<i64>,
        ack: Option<Sender<Result<(), String>>>,
    ) -> anyhow::Result<()> {
        if *self.shutdown.lock().unwrap_or_else(|e| e.into_inner()) {
            anyhow::bail!("persistence worker is shut down");
        }
        let mut queue = self.inner.queue.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(!queue.shutdown, "persistence worker is shut down");
        let generation = self
            .inner
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .submitted_generation
            .saturating_add(1);
        queue.pending = Some(Job {
            generation,
            markets,
            generated_at_us,
            catalog_generated_at_us,
            ack,
        });
        {
            let mut status = self.inner.status.lock().unwrap_or_else(|e| e.into_inner());
            status.generation = generation;
            status.submitted_generation = generation;
            status.pending = true;
        }
        self.inner.wake.notify_one();
        Ok(())
    }

    #[must_use]
    pub fn status(&self) -> PersistenceStatus {
        self.inner
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Stop the worker after draining the newest pending snapshot.
    pub fn shutdown(&self, timeout: Duration) -> anyhow::Result<()> {
        let mut already = self.shutdown.lock().unwrap_or_else(|e| e.into_inner());
        if *already {
            // A prior bounded attempt may have timed out. It signalled the
            // worker but did not join it, so a later call must retry rather
            // than falsely reporting a clean shutdown.
            if self
                .join
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
            {
                return Ok(());
            }
        }
        *already = true;
        drop(already);

        {
            let mut queue = self.inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            queue.shutdown = true;
            self.inner.wake.notify_all();
        }
        let mut done_guard = self.done.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(done) = done_guard.as_ref() {
            match done.recv_timeout(timeout) {
                Ok(()) => {}
                Err(_) => anyhow::bail!(
                    "persistence worker did not stop within {timeout:?}; \
                     the last metadata/catalog write may be incomplete"
                ),
            }
        }
        // Retire the completion receiver only after it actually completed.
        done_guard.take();
        drop(done_guard);
        if let Some(join) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = join.join();
        }
        Ok(())
    }
}

fn worker_loop(inner: Arc<Inner>) {
    loop {
        let job = {
            let mut queue = inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            while queue.pending.is_none() && !queue.shutdown {
                queue = inner.wake.wait(queue).unwrap_or_else(|e| e.into_inner());
            }
            if queue.pending.is_none() {
                break;
            }
            queue.pending.take()
        };
        let Some(job) = job else {
            break;
        };
        if let Some(hook) = &inner.hook {
            hook();
        }
        let result = write_job(&inner, &job);
        let mut status = inner.status.lock().unwrap_or_else(|e| e.into_inner());
        match &result {
            Ok((metadata_bytes, catalog_bytes, serialize_ms, write_ms)) => {
                status.last_success_us = Some(now_us().unwrap_or(0));
                status.last_error = None;
                status.metadata_bytes = *metadata_bytes;
                status.catalog_bytes = *catalog_bytes;
                status.serialize_ms = *serialize_ms;
                status.write_ms = *write_ms;
            }
            Err(error) => {
                status.last_error = Some(format!("{error:#}"));
            }
        }
        status.completed_generation = status.completed_generation.max(job.generation);
        // A later generation can have been accepted while this job was in
        // flight. It remains pending and must never be reported as completed.
        status.pending = status.completed_generation < status.submitted_generation;
        drop(status);
        if let Some(ack) = job.ack {
            let _ = ack.send(result.map(|_| ()).map_err(|error| format!("{error:#}")));
        }
    }
}

type WriteStats = (u64, u64, u64, u64);

fn write_job(inner: &Inner, job: &Job) -> anyhow::Result<WriteStats> {
    let mut metadata_bytes = 0u64;
    let mut catalog_bytes = 0u64;
    let mut serialize_ms = 0u64;
    let mut write_ms = 0u64;

    if let Some(path) = &inner.metadata_path {
        let started = Instant::now();
        let metadata = MarketMetadata::new_at(
            &inner.exchange,
            job.markets.as_ref().clone(),
            job.generated_at_us,
        );
        let bytes = serialize_json_pretty(path, &metadata)?;
        serialize_ms = serialize_ms.saturating_add(started.elapsed().as_millis() as u64);
        let started = Instant::now();
        write_bytes_atomic(path, &bytes)?;
        write_ms = write_ms.saturating_add(started.elapsed().as_millis() as u64);
        metadata_bytes = bytes.len() as u64;
    }
    if let Some(catalog_generated_at_us) = job.catalog_generated_at_us
        && let Some(path) = &inner.catalog_path
    {
        let catalog = Catalog::new(
            &inner.exchange,
            job.markets.as_ref().clone(),
            catalog_generated_at_us,
        )
        .context("building catalog checkpoint")?;
        let started = Instant::now();
        let bytes = serialize_json_pretty(path, &catalog)?;
        serialize_ms = serialize_ms.saturating_add(started.elapsed().as_millis() as u64);
        let started = Instant::now();
        write_bytes_atomic(path, &bytes)?;
        write_ms = write_ms.saturating_add(started.elapsed().as_millis() as u64);
        catalog_bytes = bytes.len() as u64;
    }
    Ok((metadata_bytes, catalog_bytes, serialize_ms, write_ms))
}

/// Convenience for startup paths that need a catalog checkpoint before the
/// worker exists.
pub fn save_catalog(
    path: &Path,
    exchange: &str,
    markets: &[MarketInfo],
    at_us: i64,
) -> anyhow::Result<()> {
    Catalog::new(exchange, markets.to_vec(), at_us)?.save(path)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    fn market(index: usize) -> MarketInfo {
        MarketInfo {
            exchange: "polymarket".to_owned(),
            native_symbol: format!("token-{index}"),
            symbol: format!("polymarket:0x{index}:0"),
            title: Some(format!("Market {index}")),
            outcome: Some("Yes".to_owned()),
            exchange_market_id: Some(format!("0x{index}")),
            close_time_us: None,
            tick_size_us: Some(1_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
            numeric_spec: crate::model::NumericSpec::default(),
        }
    }

    #[test]
    fn writes_metadata_and_catalog_and_reports_success() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata.json");
        let catalog_path = directory.path().join("catalog.json");
        let handle = PersistenceHandle::start(
            Some(metadata_path.clone()),
            Some(catalog_path.clone()),
            "polymarket",
        )
        .unwrap();
        handle
            .submit_final(
                Arc::new(vec![market(0), market(1)]),
                1_000,
                Some(1_000),
                Duration::from_secs(5),
            )
            .unwrap();
        let status = handle.status();
        assert_eq!(status.generation, 1);
        assert!(!status.pending);
        assert!(status.last_error.is_none());
        assert!(status.metadata_bytes > 0);
        assert!(status.catalog_bytes > 0);
        let metadata: MarketMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.count, 2);
        let catalog = Catalog::load(&catalog_path).unwrap();
        assert_eq!(catalog.markets.len(), 2);
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn catalog_generation_time_is_independent_of_metadata_time() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata.json");
        let catalog_path = directory.path().join("catalog.json");
        let handle = PersistenceHandle::start(
            Some(metadata_path.clone()),
            Some(catalog_path.clone()),
            "polymarket",
        )
        .unwrap();
        handle
            .submit_final(
                Arc::new(vec![market(0)]),
                5_000,
                Some(1_000),
                Duration::from_secs(5),
            )
            .unwrap();
        let metadata: MarketMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.generated_at_us, 5_000);
        let catalog = Catalog::load(&catalog_path).unwrap();
        assert_eq!(catalog.generated_at_us, 1_000);
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn absent_catalog_timestamp_leaves_the_checkpoint_untouched() {
        let directory = tempfile::tempdir().unwrap();
        let catalog_path = directory.path().join("catalog.json");
        let handle =
            PersistenceHandle::start(None, Some(catalog_path.clone()), "polymarket").unwrap();
        handle
            .submit_final(
                Arc::new(vec![market(0)]),
                1_000,
                Some(1_000),
                Duration::from_secs(5),
            )
            .unwrap();
        let original = Catalog::load(&catalog_path).unwrap();
        handle
            .submit_final(
                Arc::new(vec![market(1), market(2)]),
                2_000,
                None,
                Duration::from_secs(5),
            )
            .unwrap();
        let unchanged = Catalog::load(&catalog_path).unwrap();
        assert_eq!(unchanged.generated_at_us, original.generated_at_us);
        assert_eq!(unchanged.markets.len(), 1);
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn latest_submission_wins_over_intermediate_versions() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata.json");
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let entered_hook = Arc::clone(&entered);
        let release_hook = Arc::clone(&release);
        let hook = Arc::new(move || {
            if !entered_hook.swap(true, Ordering::SeqCst) {
                while !release_hook.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });
        let handle = PersistenceHandle::start_with_hook(
            Some(metadata_path.clone()),
            None,
            "polymarket",
            hook,
        )
        .unwrap();
        handle
            .submit(Arc::new(vec![market(0)]), 1_000, Some(1_000))
            .unwrap();
        while !entered.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        // While the first write is blocked, replace it twice.
        handle
            .submit(Arc::new(vec![market(1)]), 2_000, Some(2_000))
            .unwrap();
        handle
            .submit(Arc::new(vec![market(2), market(3)]), 3_000, Some(3_000))
            .unwrap();
        release.store(true, Ordering::SeqCst);
        handle
            .submit_final(
                Arc::new(vec![market(4), market(5), market(6)]),
                4_000,
                Some(4_000),
                Duration::from_secs(5),
            )
            .unwrap();
        let metadata: MarketMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.count, 3);
        assert_eq!(metadata.markets[0].native_symbol, "token-4");
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn submit_does_not_block_on_a_slow_write() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata.json");
        let release = Arc::new(AtomicBool::new(false));
        let release_hook = Arc::clone(&release);
        let hook = Arc::new(move || {
            while !release_hook.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let handle =
            PersistenceHandle::start_with_hook(Some(metadata_path), None, "polymarket", hook)
                .unwrap();
        let started = Instant::now();
        handle
            .submit(Arc::new(vec![market(0)]), 1_000, Some(1_000))
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "submit blocked for {:?}",
            started.elapsed()
        );
        release.store(true, Ordering::SeqCst);
        handle
            .submit_final(
                Arc::new(vec![market(0)]),
                1_000,
                Some(1_000),
                Duration::from_secs(5),
            )
            .unwrap();
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn write_failure_is_reported_and_the_worker_survives() {
        let directory = tempfile::tempdir().unwrap();
        let blocker = directory.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let metadata_path = blocker.join("metadata.json");
        let handle = PersistenceHandle::start(Some(metadata_path), None, "polymarket").unwrap();
        let error = handle
            .submit_final(
                Arc::new(vec![market(0)]),
                1_000,
                Some(1_000),
                Duration::from_secs(5),
            )
            .unwrap_err();
        assert!(error.to_string().contains("persistence write failed"));
        let status = handle.status();
        assert!(status.last_error.is_some());
        handle.shutdown(Duration::from_secs(5)).unwrap();
    }
}
