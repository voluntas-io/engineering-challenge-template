//! Run reports and market-metadata sidecars.
//!
//! These files are independent of the SBE stream: operators can
//! inspect what was collected, how the process ended, and which native exchange
//! identifiers correspond to each SBE symbol without decoding the binary file.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    capacity::CapacityPlan,
    config::{ExchangeKind, Settings},
    exchange::DiscoveryStats,
    metrics::{CollectorStats, CoverageStats, WriterStats},
    model::{MarketInfo, SourceStatus},
    persistence::PersistenceStatus,
    time::now_us,
};

/// Stable report schema version.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

/// Safe, non-secret configuration values included in a run report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    pub exchange: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kalshi_environment: Option<String>,
    pub shard_count: usize,
    pub replica_count: usize,
    pub snapshot_interval_secs: u64,
    pub discovery_interval_secs: u64,
    pub reconnect_after_secs: u64,
    pub connection_idle_timeout_secs: u64,
    pub stale_probe_after_secs: u64,
    pub probe_interval_secs: u64,
    pub max_probes: u32,
    pub max_markets: usize,
    pub writer_queue: usize,
    pub flush_interval_secs: u64,
    pub output_chunk_size: usize,
    /// Whether the capture file was fsynced on shutdown.
    #[serde(default = "default_true")]
    pub output_sync_on_close: bool,
}

/// Serde default for [`ReportConfig::output_sync_on_close`]; reports written
/// before the field existed ran with the collector default, which syncs.
fn default_true() -> bool {
    true
}

impl ReportConfig {
    #[must_use]
    pub fn from_settings(settings: &Settings, shard_count: usize) -> Self {
        Self {
            exchange: settings.exchange.to_string(),
            kalshi_environment: matches!(settings.exchange, ExchangeKind::Kalshi).then(|| {
                match settings.kalshi.environment {
                    crate::config::KalshiEnvironment::Demo => "demo".to_owned(),
                    crate::config::KalshiEnvironment::Production => "production".to_owned(),
                }
            }),
            shard_count,
            replica_count: settings.collector.replicas,
            snapshot_interval_secs: settings.collector.snapshot_interval_secs,
            discovery_interval_secs: settings.collector.discovery_interval_secs,
            reconnect_after_secs: settings.collector.reconnect_after_secs,
            connection_idle_timeout_secs: settings.collector.connection_idle_timeout_secs,
            stale_probe_after_secs: settings.collector.stale_probe_after_secs,
            probe_interval_secs: settings.collector.probe_interval_secs,
            max_probes: settings.collector.max_probes,
            max_markets: settings.collector.max_markets,
            writer_queue: settings.collector.writer_queue,
            flush_interval_secs: settings.collector.flush_interval_secs,
            output_chunk_size: settings.output.chunk_size,
            output_sync_on_close: settings.output.sync_on_close,
        }
    }
}

/// How the startup subscription set was obtained.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogReport {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `cold`, `warm`, or `warm-stale`.
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at_us: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_us: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    pub selected_markets: usize,
    pub outcome_symbols: usize,
}

/// Machine-readable summary written at the end of a live or replay run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub schema_version: u32,
    pub tool: String,
    pub version: String,
    pub mode: String,
    pub exchange: String,
    pub environment: String,
    pub started_at_us: i64,
    pub ended_at_us: i64,
    pub shutdown_reason: String,
    pub output_path: String,
    pub shard_count: usize,
    pub replica_count: usize,
    pub discovered_symbols: usize,
    pub final_symbols: usize,
    pub core: CollectorStats,
    pub writer: WriterStats,
    pub sources: Vec<SourceStatus>,
    pub config: ReportConfig,
    /// Startup capacity plan for a live run. Replay reports do not have one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<CapacityPlan>,
    /// Most recent complete discovery round.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<DiscoveryStats>,
    /// Startup catalog source and checkpoint identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogReport>,
    /// Final metadata/catalog persistence result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceStatus>,
    /// Final coverage gauges at shutdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage: Option<CoverageStats>,
    /// Rotation manifest when the run wrote multiple segments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_manifest: Option<String>,
    /// Final raw-frame journal state. Present when the journal was enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<crate::journal::JournalStatus>,
    /// Independent read-back of the journal manifest and finalized segments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_verification: Option<crate::journal::JournalVerification>,
    /// Error produced when report status and persisted journal state disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_consistency_error: Option<String>,
}

impl RunReport {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        mode: &str,
        exchange: &str,
        environment: &str,
        started_at_us: i64,
        ended_at_us: i64,
        shutdown_reason: &str,
        output_path: &str,
        shard_count: usize,
        replica_count: usize,
        discovered_symbols: usize,
        final_symbols: usize,
        core: CollectorStats,
        writer: WriterStats,
        sources: Vec<SourceStatus>,
        config: ReportConfig,
    ) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            tool: env!("CARGO_PKG_NAME").to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            mode: mode.to_owned(),
            exchange: exchange.to_owned(),
            environment: environment.to_owned(),
            started_at_us,
            ended_at_us,
            shutdown_reason: shutdown_reason.to_owned(),
            output_path: output_path.to_owned(),
            shard_count,
            replica_count,
            discovered_symbols,
            final_symbols,
            core,
            writer,
            sources,
            config,
            capacity: None,
            discovery: None,
            catalog: None,
            persistence: None,
            coverage: None,
            output_manifest: None,
            journal: None,
            journal_verification: None,
            journal_consistency_error: None,
        }
    }

    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        write_json_atomic(path, self)
    }
}

/// Metadata sidecar keyed by SBE symbol and native exchange symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetadata {
    pub schema_version: u32,
    pub generated_at_us: i64,
    pub exchange: String,
    pub count: usize,
    pub markets: Vec<MarketInfo>,
}

impl MarketMetadata {
    #[must_use]
    pub fn new(exchange: &str, markets: Vec<MarketInfo>) -> Self {
        Self::new_at(exchange, markets, now_us().unwrap_or(0))
    }

    #[must_use]
    pub fn new_at(exchange: &str, markets: Vec<MarketInfo>, generated_at_us: i64) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at_us,
            exchange: exchange.to_owned(),
            count: markets.len(),
            markets,
        }
    }

    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        write_json_atomic(path, self)
    }
}

/// Write JSON through a sibling temporary file and rename it into place. This
/// avoids exposing a truncated report if the process dies mid-write.
pub fn write_json_atomic<T: Serialize + ?Sized>(path: &Path, value: &T) -> anyhow::Result<()> {
    let bytes = serialize_json_pretty(path, value)?;
    write_bytes_atomic(path, &bytes)
}

/// Pretty JSON with a trailing newline, ready for [`write_bytes_atomic`].
pub fn serialize_json_pretty<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .with_context(|| format!("serializing sidecar `{}`", path.display()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Write bytes through an exclusively-created sibling temporary file, fsync,
/// rename, and (on Unix) fsync the parent directory.
///
/// The temporary file is removed on every failure path, so a failed write
/// leaves neither a truncated destination nor a stale temporary behind.
pub fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if path.as_os_str() == "-" {
        anyhow::bail!("JSON sidecar path must be a file, not stdout");
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating sidecar directory `{}`", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("sidecar path has no file name")?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = None;
    let mut file = None;
    for attempt in 0..64u32 {
        let candidate = parent.join(format!(
            ".{file_name}.tmp.{}.{}.{}",
            std::process::id(),
            now_us().unwrap_or(0),
            attempt
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(created) => {
                temporary = Some(candidate);
                file = Some(created);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("creating temporary sidecar in `{}`", parent.display())
                });
            }
        }
    }
    let temporary = temporary.context("exhausted unique temporary sidecar names")?;
    let file = file.context("temporary sidecar file was not created")?;
    let result = (|| -> anyhow::Result<()> {
        let mut writer = BufWriter::new(file);
        writer.write_all(bytes)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        std::fs::rename(&temporary, path)
            .with_context(|| format!("installing sidecar `{}`", path.display()))?;
        sync_parent_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> anyhow::Result<()> {
    File::open(parent)
        .with_context(|| format!("opening sidecar directory `{}`", parent.display()))?
        .sync_all()
        .with_context(|| format!("syncing sidecar directory `{}`", parent.display()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> anyhow::Result<()> {
    Ok(())
}

/// Best-effort sidecar path resolution. Empty paths are treated as absent.
#[must_use]
pub fn sidecar_path(path: &Option<PathBuf>) -> Option<&Path> {
    path.as_deref().filter(|path| !path.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MarketInfo;

    fn market() -> MarketInfo {
        MarketInfo {
            exchange: "kalshi".to_owned(),
            native_symbol: "KXTEST".to_owned(),
            symbol: "kalshi:KXTEST".to_owned(),
            title: Some("Test".to_owned()),
            outcome: None,
            exchange_market_id: None,
            close_time_us: None,
            tick_size_us: Some(10_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
            numeric_spec: crate::model::NumericSpec::default(),
        }
    }

    #[test]
    fn writes_and_replaces_sidecar() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/report.json");
        let metadata = MarketMetadata::new("kalshi", vec![market()]);
        metadata.write(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let decoded: MarketMetadata = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded.count, 1);
        assert_eq!(decoded.markets[0].native_symbol, "KXTEST");
        metadata.write(&path).unwrap();
        assert!(path.is_file());
    }

    #[test]
    fn rejects_stdout_sidecar() {
        assert!(write_json_atomic(Path::new("-"), &serde_json::json!({})).is_err());
    }

    #[test]
    fn report_config_records_the_durability_policy() {
        let mut settings = Settings::for_exchange(crate::config::ExchangeKind::Polymarket);
        settings.output.sync_on_close = false;
        let config = ReportConfig::from_settings(&settings, 4);
        assert!(!config.output_sync_on_close);
        let decoded: ReportConfig =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
        assert!(!decoded.output_sync_on_close);

        // Reports written before the field existed decode with the collector
        // default: captures were fsynced on close.
        let legacy: ReportConfig = serde_json::from_value(serde_json::json!({
            "exchange": "polymarket",
            "shard_count": 1,
            "replica_count": 2,
            "snapshot_interval_secs": 60,
            "discovery_interval_secs": 30,
            "reconnect_after_secs": 1800,
            "connection_idle_timeout_secs": 45,
            "stale_probe_after_secs": 120,
            "probe_interval_secs": 30,
            "max_probes": 3,
            "max_markets": 0,
            "writer_queue": 16384,
            "flush_interval_secs": 1,
            "output_chunk_size": 67108864
        }))
        .unwrap();
        assert!(legacy.output_sync_on_close);
    }
}
