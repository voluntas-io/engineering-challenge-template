//! Deterministic replay of normalized JSONL fixtures.
//!
//! The replay path feeds the same synchronous [`CollectorCore`] as
//! the live coordinator. A fixture is one JSON object per line; each record has
//! a local `received_at_us` timestamp so snapshot scheduling and source failover
//! are reproducible without sleeping. See `docs/replay-format.md` for the
//! complete schema.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{Context, bail};
use serde::Deserialize;
use tracing::{debug, info};

use crate::{
    Side,
    collector::{CollectorCore, CoreConfig},
    config::Settings,
    fixed::SCALE,
    model::{
        BookSnapshot, EventMeta, ExchangeEvent, Level, LifecycleKind, MarketInfo, QuantityChange,
        ResetReason, SourceId, SourceStatus,
    },
    report::{MarketMetadata, ReportConfig, RunReport, sidecar_path},
    time::{now_us, utc_from_us},
    writer::DirectWriter,
};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ReplayRecord {
    Discovery {
        received_at_us: i64,
        markets: Vec<MarketInfo>,
    },
    Snapshot {
        received_at_us: i64,
        #[serde(default)]
        source: SourceId,
        native_symbol: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        #[serde(default)]
        exchange_ts_us: Option<i64>,
        #[serde(default)]
        seq: Option<u64>,
        #[serde(default)]
        clock: Option<i64>,
    },
    Delta {
        received_at_us: i64,
        #[serde(default)]
        source: SourceId,
        native_symbol: String,
        side: String,
        price: i64,
        #[serde(default)]
        quantity: Option<i64>,
        #[serde(default)]
        delta: Option<i64>,
        #[serde(default)]
        exchange_ts_us: Option<i64>,
        #[serde(default)]
        seq: Option<u64>,
        #[serde(default)]
        clock: Option<i64>,
    },
    Lifecycle {
        received_at_us: i64,
        #[serde(default)]
        source: SourceId,
        native_symbol: String,
        kind: String,
        #[serde(default)]
        close_time_us: Option<i64>,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        tick_size_us: Option<i64>,
        #[serde(default)]
        price_ranges: Option<Vec<crate::model::PriceRange>>,
        #[serde(default)]
        price_level_structure: Option<String>,
        #[serde(default)]
        exchange_ts_us: Option<i64>,
    },
    Reset {
        received_at_us: i64,
        #[serde(default)]
        source: SourceId,
        #[serde(default)]
        native_symbols: Vec<String>,
        #[serde(default = "default_reset_reason")]
        reason: String,
    },
    Status {
        received_at_us: i64,
        source: SourceId,
        connected: bool,
        #[serde(default)]
        last_frame_us: Option<i64>,
        #[serde(default)]
        last_error: Option<String>,
    },
    Tick {
        received_at_us: i64,
    },
}

impl ReplayRecord {
    fn received_at_us(&self) -> i64 {
        match self {
            Self::Discovery { received_at_us, .. }
            | Self::Snapshot { received_at_us, .. }
            | Self::Delta { received_at_us, .. }
            | Self::Lifecycle { received_at_us, .. }
            | Self::Reset { received_at_us, .. }
            | Self::Status { received_at_us, .. }
            | Self::Tick { received_at_us } => *received_at_us,
        }
    }
}

fn default_reset_reason() -> String {
    "requested_refresh".to_owned()
}

/// Run one fixture through the collector core and write the SBE output.
pub fn run(settings: Settings, input: &Path) -> anyhow::Result<RunReport> {
    settings.validate()?;
    crate::config::preflight_output(&settings)?;
    let mut replay_settings = settings.clone();
    replay_settings.collector.shards = 1;
    replay_settings.collector.replicas = replay_settings.collector.replicas.max(2);
    let core_config = CoreConfig::from_settings(
        &replay_settings,
        1,
        crate::exchange::capabilities_for(replay_settings.exchange),
    )?;
    let mut core = CollectorCore::new(core_config)?;
    let mut writer = DirectWriter::create(crate::writer::WriterOptions::from_settings(&settings))?;

    let reader: Box<dyn BufRead> = if input.as_os_str() == "-" {
        Box::new(BufReader::new(std::io::stdin()))
    } else {
        Box::new(BufReader::new(File::open(input).with_context(|| {
            format!("opening replay fixture `{}`", input.display())
        })?))
    };

    let mut first_timestamp = None;
    let mut last_timestamp = None;
    let mut saw_discovery = false;
    let mut records = 0u64;
    let mut outputs = 0u64;
    for (line_number, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("reading replay fixture line {}", line_number + 1))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let record: ReplayRecord = serde_json::from_str(line)
            .with_context(|| format!("parsing replay fixture line {}", line_number + 1))?;
        let timestamp = record.received_at_us();
        anyhow::ensure!(
            timestamp >= 0,
            "negative replay timestamp on line {}",
            line_number + 1
        );
        // Reject timestamps that cannot be represented in SBE output before
        // they reach the core: serialization must never fall back to
        // wall-clock time for an out-of-range input.
        utc_from_us(timestamp).with_context(|| {
            format!(
                "replay timestamp {timestamp} is out of range on line {}",
                line_number + 1
            )
        })?;
        if let Some(previous) = last_timestamp {
            anyhow::ensure!(
                timestamp >= previous,
                "replay timestamps must be non-decreasing (line {}: {} < {})",
                line_number + 1,
                timestamp,
                previous
            );
        }
        first_timestamp.get_or_insert(timestamp);
        last_timestamp = Some(timestamp);
        records = records.saturating_add(1);
        match record {
            ReplayRecord::Discovery {
                received_at_us,
                markets,
            } => {
                saw_discovery = true;
                let messages = core
                    .apply_discovery(received_at_us, markets)
                    .context("applying replay discovery record")?;
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Snapshot {
                received_at_us,
                source,
                native_symbol,
                bids,
                asks,
                exchange_ts_us,
                seq,
                clock,
            } => {
                let event = ExchangeEvent::Snapshot {
                    native_symbol,
                    book: BookSnapshot { bids, asks },
                    meta: EventMeta {
                        received_at_us,
                        exchange_ts_us,
                        seq,
                        clock,
                        connection_epoch: 1,
                        receive_seq: 1,
                    },
                };
                let messages = core.handle_event(source, event);
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Delta {
                received_at_us,
                source,
                native_symbol,
                side,
                price,
                quantity,
                delta,
                exchange_ts_us,
                seq,
                clock,
            } => {
                let side = parse_side(&side)?;
                let change = match (quantity, delta) {
                    (Some(_), Some(_)) => bail!(
                        "replay delta line {} supplies both quantity and delta",
                        line_number + 1
                    ),
                    (Some(quantity), None) => QuantityChange::Absolute(quantity),
                    (None, Some(delta)) => QuantityChange::Delta(delta),
                    (None, None) => bail!(
                        "replay delta line {} supplies neither quantity nor delta",
                        line_number + 1
                    ),
                };
                anyhow::ensure!(
                    (0..=SCALE).contains(&price),
                    "replay delta price outside [0, 1] on line {}",
                    line_number + 1
                );
                let event = ExchangeEvent::LevelChange {
                    native_symbol,
                    side,
                    price,
                    change,
                    meta: EventMeta {
                        received_at_us,
                        exchange_ts_us,
                        seq,
                        clock,
                        connection_epoch: 1,
                        receive_seq: 1,
                    },
                };
                let messages = core.handle_event(source, event);
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Lifecycle {
                received_at_us,
                source,
                native_symbol,
                kind,
                close_time_us,
                title,
                tick_size_us,
                price_ranges,
                price_level_structure,
                exchange_ts_us,
            } => {
                let event = ExchangeEvent::Lifecycle {
                    native_symbol,
                    kind: parse_lifecycle(&kind),
                    close_time_us,
                    title,
                    tick_size_us,
                    price_ranges,
                    price_level_structure,
                    meta: EventMeta {
                        received_at_us,
                        exchange_ts_us,
                        seq: None,
                        clock: None,
                        connection_epoch: 0,
                        receive_seq: 0,
                    },
                };
                let messages = core.handle_event(source, event);
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Reset {
                received_at_us,
                source,
                native_symbols,
                reason,
            } => {
                let event = ExchangeEvent::StreamReset {
                    native_symbols,
                    reason: parse_reset_reason(&reason),
                    received_at_us,
                };
                let messages = core.handle_event(source, event);
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Status {
                received_at_us,
                source,
                connected,
                last_frame_us,
                last_error,
            } => {
                let messages = core.handle_status(
                    SourceStatus {
                        source,
                        connected,
                        last_frame_us: last_frame_us.or(Some(received_at_us)),
                        last_error,
                        ..SourceStatus::new(source)
                    },
                    received_at_us,
                );
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
            ReplayRecord::Tick { received_at_us } => {
                let messages = core.tick(received_at_us);
                outputs = outputs.saturating_add(messages.len() as u64);
                write_outputs(&mut writer, messages)?;
                drain_controls(&mut core);
            }
        }
    }
    if !saw_discovery {
        bail!("replay fixture has no `discovery` record");
    }
    let ended_at_us = last_timestamp.unwrap_or_else(|| now_us().unwrap_or(0));
    if settings.collector.final_snapshots {
        let messages = core.finalize(ended_at_us);
        outputs = outputs.saturating_add(messages.len() as u64);
        write_outputs(&mut writer, messages)?;
    }
    writer.flush()?;
    let writer_stats = writer.shutdown()?;
    let core_stats = core.stats();
    let mut report = RunReport::new(
        "replay",
        &settings.exchange.to_string(),
        "fixture",
        first_timestamp.unwrap_or(ended_at_us),
        ended_at_us,
        "replay complete",
        &settings.output.path,
        1,
        replay_settings.collector.replicas,
        core_stats.discovered_symbols as usize,
        core.symbol_count(),
        core_stats,
        writer_stats,
        core.statuses().values().cloned().collect(),
        ReportConfig::from_settings(&settings, 1),
    );
    if crate::writer::WriterOptions::from_settings(&settings).rotation_enabled() {
        report.output_manifest = Some(
            crate::config::rotation_manifest_path(&settings.output.path)
                .display()
                .to_string(),
        );
    }
    if let Some(path) = sidecar_path(&settings.output.metadata_path) {
        MarketMetadata::new(&settings.exchange.to_string(), core.market_infos())
            .write(path)
            .context("writing replay metadata sidecar")?;
    }
    if let Some(path) = sidecar_path(&settings.output.report_path) {
        report.write(path).context("writing replay run report")?;
    }
    info!(
        records,
        outputs,
        symbols = core.symbol_count(),
        "replay complete"
    );
    Ok(report)
}

fn write_outputs(
    writer: &mut DirectWriter,
    messages: Vec<crate::model::OutputMessage>,
) -> anyhow::Result<()> {
    for message in messages {
        writer.write(&message)?;
    }
    Ok(())
}

fn drain_controls(core: &mut CollectorCore) {
    use crate::model::Control;
    let controls = core.drain_controls();
    if !controls.is_empty() {
        debug!(
            count = controls.len(),
            "replay has no workers; discarding subscription controls"
        );
    }
    // Replay acks recovery controls immediately, as a live worker would.
    // Snapshots/deltas use epoch 1 so they are strictly after the (0,0) fence.
    for (source, control) in controls {
        if let Control::Recover { generation, .. } = control {
            core.handle_recovery_ready(source, generation, 0, 0);
        }
    }
}

fn parse_side(value: &str) -> anyhow::Result<Side> {
    match value.to_ascii_lowercase().as_str() {
        "buy" | "bid" | "yes" => Ok(Side::Buy),
        "sell" | "ask" | "no" => Ok(Side::Sell),
        other => bail!("unknown replay side `{other}`; expected Buy or Sell"),
    }
}

fn parse_lifecycle(value: &str) -> LifecycleKind {
    match value.to_ascii_lowercase().as_str() {
        "created" => LifecycleKind::Created,
        "activated" => LifecycleKind::Activated,
        "deactivated" => LifecycleKind::Deactivated,
        "close_date_updated" => LifecycleKind::CloseDateUpdated,
        "determined" => LifecycleKind::Determined,
        "settled" => LifecycleKind::Settled,
        "price_structure_updated" | "price_level_structure_updated" => {
            LifecycleKind::PriceStructureUpdated
        }
        "metadata_updated" => LifecycleKind::MetadataUpdated,
        "new_market" => LifecycleKind::NewMarket,
        "resolved" => LifecycleKind::Resolved,
        other => LifecycleKind::Other(other.to_owned()),
    }
}

fn parse_reset_reason(value: &str) -> ResetReason {
    match value.to_ascii_lowercase().as_str() {
        "connected" => ResetReason::Connected,
        "sequence_gap" | "sequence-gap" => ResetReason::SequenceGap,
        "protocol_error" | "protocol-error" => ResetReason::ProtocolError,
        "subscription_refresh" | "subscription-refresh" => ResetReason::SubscriptionRefresh,
        _ => ResetReason::RequestedRefresh,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sides_and_lifecycle_names() {
        assert_eq!(parse_side("buy").unwrap(), Side::Buy);
        assert_eq!(parse_side("SELL").unwrap(), Side::Sell);
        assert!(parse_side("maybe").is_err());
        assert_eq!(parse_lifecycle("deactivated"), LifecycleKind::Deactivated);
        assert_eq!(parse_lifecycle("new_market"), LifecycleKind::NewMarket);
        assert_eq!(parse_reset_reason("sequence-gap"), ResetReason::SequenceGap);
    }
}
