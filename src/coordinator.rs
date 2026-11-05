//! Live collector coordinator.
//!
//! One serialized writer owns the SBE file. Every websocket source is a worker
//! task supervised by [`crate::session::run_worker`]; this module feeds decoded
//! events into the synchronous [`CollectorCore`], dispatches subscription and
//! recovery controls, runs discovery, schedules periodic snapshots, and handles
//! graceful shutdown.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout, timeout_at},
};
use tracing::{error, info, warn};

use crate::{
    capacity,
    catalog::Catalog,
    collector::{CollectorCore, CoreConfig},
    config::{CatalogMode, ExchangeKind, JournalSyncMode, Settings},
    exchange::{DiscoveryResult, DiscoveryStats, Exchange, KalshiExchange, PolymarketExchange},
    journal::{JournalConfig, JournalHandle, JournalMeta, SyncMode},
    metrics::{CatalogHealth, MetricsRegistry, ReadinessPolicy, RuntimeStatus, run_server_on},
    model::{Control, MarketInfo, OutputMessage, SourceId},
    persistence::PersistenceHandle,
    report::{CatalogReport, ReportConfig, RunReport, sidecar_path},
    session::{WorkerConfig, WorkerEvent, run_worker},
    time::now_us,
    writer::WriterHandle,
};

/// Build the selected exchange adapter. `require_auth` is true for live
/// collection and false for public discovery-only commands.
pub fn build_exchange(
    settings: &Settings,
    require_auth: bool,
) -> anyhow::Result<Arc<dyn Exchange>> {
    match settings.exchange {
        ExchangeKind::Kalshi => Ok(Arc::new(KalshiExchange::new(
            settings.kalshi.clone(),
            settings.network.clone(),
            require_auth,
        )?)),
        ExchangeKind::Polymarket => Ok(Arc::new(PolymarketExchange::new(
            settings.polymarket.clone(),
            settings.network.clone(),
        )?)),
    }
}

/// Start the raw-frame journal when a directory is configured.
///
/// The manifest records the source commit injected at build time and a digest
/// of the capture-affecting configuration. Credentials never enter either
/// value.
fn start_journal(settings: &Settings, venue: &str) -> anyhow::Result<Option<JournalHandle>> {
    let Some(directory) = settings.journal.directory.clone() else {
        return Ok(None);
    };
    let config = JournalConfig {
        directory,
        queue_capacity: settings.journal.queue_capacity,
        enqueue_timeout: Duration::from_millis(settings.journal.enqueue_timeout_ms),
        flush_interval: Duration::from_millis(settings.journal.flush_interval_ms),
        sync_interval: Duration::from_millis(settings.journal.sync_interval_ms),
        segment_max_bytes: settings.journal.segment_max_bytes,
        segment_max_seconds: settings.journal.segment_max_secs,
        sync_mode: match settings.journal.sync_mode {
            JournalSyncMode::Interval => SyncMode::Interval,
            JournalSyncMode::Always => SyncMode::Always,
        },
    };
    let code_commit = option_env!("GIT_COMMIT")
        .or(option_env!("GITHUB_SHA"))
        .unwrap_or("unknown");
    let config_sha256 = settings.capture_config_sha256()?;
    let meta = JournalMeta::new(venue, code_commit, &config_sha256);
    JournalHandle::start(config, meta)
        .map(Some)
        .context("starting raw-frame journal")
}

/// Readiness inputs owned by the coordinator loop.
struct RuntimeState {
    catalog: CatalogHealth,
    discovery: Option<DiscoveryStats>,
    discovery_failures: u64,
    fatal_error: Option<String>,
}

/// Startup subscription set and its provenance.
#[derive(Debug)]
struct Bootstrap {
    markets: Vec<MarketInfo>,
    discovery: Option<DiscoveryStats>,
    catalog: CatalogReport,
    /// Catalog generation time to persist with the startup snapshot. `None`
    /// means the checkpoint was an input and must not be refreshed.
    catalog_generated_at_us: Option<i64>,
}

fn catalog_mode_name(mode: CatalogMode) -> &'static str {
    match mode {
        CatalogMode::Off => "off",
        CatalogMode::Warm => "warm",
        CatalogMode::Require => "require",
        CatalogMode::Stale => "stale",
    }
}

/// Load the startup set according to the catalog policy.
///
/// A checkpoint is trusted only after its schema, counts, market fields, and
/// content checksum verify. `warm` uses a fresh checkpoint and falls back to
/// discovery; `require` demands a fresh checkpoint; `stale` accepts
/// an old checkpoint so a total discovery outage does not block a restart.
async fn bootstrap_markets(
    settings: &Settings,
    exchange: &Arc<dyn Exchange>,
    now_us: i64,
) -> anyhow::Result<Bootstrap> {
    let mode = settings.collector.catalog_mode;
    let mut report = CatalogReport {
        mode: catalog_mode_name(mode).to_owned(),
        path: settings
            .collector
            .catalog_path
            .as_ref()
            .map(|path| path.display().to_string()),
        source: "cold".to_owned(),
        generated_at_us: None,
        age_us: None,
        checksum: None,
        selected_markets: 0,
        outcome_symbols: 0,
    };
    if mode != CatalogMode::Off {
        let path = settings
            .collector
            .catalog_path
            .as_deref()
            .context("catalog mode requires collector.catalog_path")?;
        match Catalog::load(path) {
            Ok(catalog) => {
                let age_us = catalog.age_us(now_us);
                let max_age_us = i64::try_from(
                    settings
                        .collector
                        .catalog_max_age_secs
                        .saturating_mul(1_000_000),
                )
                .unwrap_or(i64::MAX);
                let fresh = age_us <= max_age_us;
                let nonempty =
                    !catalog.markets.is_empty() || settings.collector.allow_empty_catalog;
                let exchange_matches = catalog.exchange == exchange.name();
                let usable = nonempty && exchange_matches && (fresh || mode == CatalogMode::Stale);
                if usable {
                    report.source = if fresh { "warm" } else { "warm-stale" }.to_owned();
                    report.generated_at_us = Some(catalog.generated_at_us);
                    report.age_us = Some(age_us);
                    report.checksum = Some(catalog.checksum.clone());
                    report.selected_markets = catalog.selected_markets;
                    report.outcome_symbols = catalog.outcome_symbols;
                    info!(
                        path = %path.display(),
                        source = %report.source,
                        age_secs = age_us / 1_000_000,
                        selected_markets = catalog.selected_markets,
                        outcome_symbols = catalog.outcome_symbols,
                        "using catalog checkpoint"
                    );
                    return Ok(Bootstrap {
                        markets: catalog.markets,
                        discovery: None,
                        catalog: report,
                        catalog_generated_at_us: None,
                    });
                }
                let reason = if !exchange_matches {
                    "belongs to another exchange"
                } else if !nonempty {
                    "contains no markets"
                } else {
                    "is older than catalog_max_age_secs"
                };
                if matches!(mode, CatalogMode::Require | CatalogMode::Stale) {
                    bail!(
                        "catalog checkpoint `{}` {reason}; refusing to start",
                        path.display()
                    );
                }
                warn!(
                    path = %path.display(),
                    reason,
                    "catalog checkpoint unusable; falling back to discovery"
                );
            }
            Err(error) => {
                if matches!(mode, CatalogMode::Require | CatalogMode::Stale) {
                    return Err(error).with_context(|| {
                        format!("loading required catalog checkpoint `{}`", path.display())
                    });
                }
                warn!(
                    path = %path.display(),
                    error = %format!("{error:#}"),
                    "catalog checkpoint unreadable; falling back to discovery"
                );
            }
        }
    }

    let discovery =
        discover_with_retry_stats(exchange, settings.collector.max_markets, exchange.name())
            .await?;
    if discovery.markets.is_empty() && !settings.collector.allow_empty_catalog {
        bail!(
            "{} returned no live, non-MVE markets; refusing to start an empty collector",
            exchange.name()
        );
    }
    let stats = discovery.stats;
    let finished_at_us = stats.finished_at_us;
    report.source = "cold".to_owned();
    report.generated_at_us = Some(finished_at_us);
    report.age_us = Some(0);
    report.checksum = Some(crate::catalog::checksum_for(
        exchange.name(),
        stats.finished_at_us,
        &discovery.markets,
    )?);
    report.selected_markets = usize::try_from(stats.selected_markets).unwrap_or(usize::MAX);
    report.outcome_symbols = usize::try_from(stats.outcome_symbols).unwrap_or(usize::MAX);
    Ok(Bootstrap {
        markets: discovery.markets,
        discovery: Some(stats),
        catalog: report,
        catalog_generated_at_us: Some(finished_at_us),
    })
}

/// Run a live collection until SIGINT/SIGTERM, the configured duration, or a
/// fatal source/writer error.
pub async fn run(settings: Settings) -> anyhow::Result<RunReport> {
    let duration_secs = settings.collector.duration_secs;
    run_with_shutdown(settings, async move {
        let signal = wait_for_shutdown_signal();
        tokio::pin!(signal);
        if duration_secs == 0 {
            signal.await
        } else {
            tokio::select! {
                reason = &mut signal => reason,
                _ = tokio::time::sleep(Duration::from_secs(duration_secs)) => {
                    format!("duration elapsed ({duration_secs}s)")
                }
            }
        }
    })
    .await
}

/// Run a live collection until the supplied future resolves.
///
/// `shutdown` returns the human-readable shutdown reason recorded in the run
/// report. This is the same loop used by [`run`]; separating the trigger keeps
/// the live orchestration testable without relying on process signals or a
/// fixed wall-clock duration. `collector.duration_secs` is interpreted by
/// [`run`], not by this function.
pub async fn run_with_shutdown<F>(settings: Settings, shutdown: F) -> anyhow::Result<RunReport>
where
    F: std::future::Future<Output = String>,
{
    settings.validate()?;
    // Refuse to replace any previous capture or sidecar before discovery or
    // writer creation can touch the filesystem.
    crate::config::preflight_output(&settings)?;
    let started_at_us = now_us().context("reading wall clock at startup")?;
    let exchange = build_exchange(&settings, true)?;
    let bootstrap = bootstrap_markets(&settings, &exchange, started_at_us).await?;
    let initial_markets = bootstrap.markets;
    if initial_markets.is_empty() {
        bail!(
            "{} returned no live, non-MVE markets; refusing to start an empty collector",
            exchange.name()
        );
    }
    let capacity =
        capacity::plan(&settings, &initial_markets).context("planning collector capacity")?;
    let shard_count = capacity.shards;
    let replica_count = settings.collector.replicas;
    let core_config = CoreConfig::from_settings(&settings, shard_count)?;
    let mut core = CollectorCore::new(core_config)?;
    // The initial discovery cannot produce removal snapshots, but applying it
    // here establishes shard assignments before workers are spawned.
    core.apply_discovery(started_at_us, initial_markets.clone())
        .context("applying initial market discovery")?;

    let persistence = PersistenceHandle::start(
        sidecar_path(&settings.output.metadata_path).map(std::path::Path::to_path_buf),
        settings.collector.catalog_path.clone(),
        exchange.name(),
    )
    .context("starting metadata/catalog persistence worker")?;
    let persistence_timeout =
        Duration::from_secs(settings.collector.writer_send_timeout_secs.max(1));
    persistence
        .submit(
            Arc::new(core.market_infos()),
            started_at_us,
            bootstrap.catalog_generated_at_us,
        )
        .context("queueing initial metadata/catalog snapshot")?;
    let writer = WriterHandle::start(crate::writer::WriterOptions::from_settings(&settings))?;
    let send_timeout = persistence_timeout;
    let registry = MetricsRegistry::new(
        exchange.name(),
        ReadinessPolicy {
            min_anchored_fraction: settings.collector.readiness_min_anchored_fraction,
            max_catalog_age_secs: settings.collector.readiness_max_catalog_age_secs,
            max_stale_secs: settings.collector.readiness_max_stale_secs,
        },
    );
    let stale_after_us = i64::try_from(
        settings
            .collector
            .readiness_max_stale_secs
            .saturating_mul(1_000_000),
    )
    .unwrap_or(i64::MAX);
    let mut runtime = RuntimeState {
        catalog: CatalogHealth {
            mode: bootstrap.catalog.mode.clone(),
            source: bootstrap.catalog.source.clone(),
            generated_at_us: bootstrap.catalog.generated_at_us,
            last_discovery_finished_at_us: bootstrap
                .discovery
                .as_ref()
                .map(|round| round.finished_at_us)
                .or(bootstrap.catalog.generated_at_us),
            last_discovery_error: None,
        },
        discovery: bootstrap.discovery.clone(),
        discovery_failures: 0,
        fatal_error: None,
    };
    let (metrics_shutdown, metrics_handle) = start_metrics(&settings, &registry).await?;
    let journal = start_journal(&settings, exchange.name())?;

    let factory = Arc::clone(&exchange).session_factory();
    let mut worker_config = WorkerConfig::from_settings(&settings.collector);
    worker_config.journal = journal.clone();
    let (worker_event_tx, mut worker_event_rx) = mpsc::channel::<WorkerEvent>(8192);
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut commands: BTreeMap<SourceId, mpsc::Sender<Control>> = BTreeMap::new();
    let mut handles: BTreeMap<SourceId, JoinHandle<()>> = BTreeMap::new();
    let mut active_sources = BTreeSet::new();
    for shard in 0..shard_count {
        for replica in 0..replica_count {
            let source = SourceId::new(shard as u16, replica as u16);
            let (command_tx, command_rx) = mpsc::channel::<Control>(256);
            commands.insert(source, command_tx);
            active_sources.insert(source);
            let handle = tokio::spawn(run_worker(
                Arc::clone(&factory),
                source,
                command_rx,
                worker_event_tx.clone(),
                worker_config.clone(),
                shutdown_tx.subscribe(),
            ));
            handles.insert(source, handle);
        }
    }
    drop(worker_event_tx);

    dispatch_controls(&mut core, &commands, send_timeout).await?;
    update_metrics(
        &registry,
        &core,
        &writer,
        &persistence,
        &runtime,
        stale_after_us,
    );
    info!(
        exchange = exchange.name(),
        environment = exchange.environment(),
        symbols = core.symbol_count(),
        shards = shard_count,
        replicas = replica_count,
        output = %settings.output.path,
        metadata = ?settings.output.metadata_path,
        report = ?settings.output.report_path,
        journal = ?settings.journal.directory,
        capacity = %capacity.summary(),
        "collector started"
    );

    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut discovery_ticker = interval(settings.discovery_interval());
    discovery_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The initial discovery already ran; consume the immediate interval tick.
    discovery_ticker.tick().await;
    let mut flush_ticker = interval(Duration::from_secs(
        settings.collector.flush_interval_secs.max(1),
    ));
    flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    flush_ticker.tick().await;
    let (discovery_tx, mut discovery_rx) = mpsc::channel::<anyhow::Result<DiscoveryResult>>(1);
    let mut discovery_in_flight = false;

    let mut shutdown = std::pin::pin!(shutdown);

    let mut shutdown_reason = String::new();
    let mut fatal: Option<String> = None;
    loop {
        tokio::select! {
            reason = &mut shutdown => {
                shutdown_reason = reason;
                break;
            }
            event = worker_event_rx.recv() => {
                match event {
                    Some(event) => {
                        if let Err(error) = process_worker_event(
                            event,
                            &mut core,
                            &writer,
                            &commands,
                            &mut active_sources,
                            send_timeout,
                        ).await {
                            fatal = Some(format!("{error:#}"));
                            break;
                        }
                        if active_sources.is_empty() {
                            fatal = Some("all websocket sources stopped".to_owned());
                            break;
                        }
                    }
                    None => {
                        fatal = Some("worker event channel closed unexpectedly".to_owned());
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                // Every serialized timestamp comes from the wall clock, so
                // generated snapshots share one timeline with event receive
                // times. The tick cadence itself is the Tokio timer.
                let now = now_us().unwrap_or(started_at_us);
                let outputs = core.tick(now);
                if let Err(error) = emit_outputs(&writer, outputs, send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                if let Err(error) = dispatch_controls(&mut core, &commands, send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                update_metrics(
                    &registry,
                    &core,
                    &writer,
                    &persistence,
                    &runtime,
                    stale_after_us,
                );
            }
            _ = discovery_ticker.tick() => {
                // Run REST discovery off the event loop. Paginating thousands
                // of markets while holding this loop would stop draining
                // websocket events and can cause avoidable gaps.
                if !discovery_in_flight {
                    let exchange = Arc::clone(&exchange);
                    let max_markets = settings.collector.max_markets;
                    let result_tx = discovery_tx.clone();
                    discovery_in_flight = true;
                    tokio::spawn(async move {
                        // The release profile uses panic=abort,
                        // so catch_unwind cannot provide recovery there. A
                        // panic is process-fatal and systemd restarts from a
                        // clean state; ordinary discovery failures use Result.
                        let result = exchange.discover_markets_with_stats(max_markets).await;
                        let _ = result_tx.send(result).await;
                    });
                }
            }
            result = discovery_rx.recv() => {
                discovery_in_flight = false;
                match result {
                    Some(Ok(round))
                        if round.markets.is_empty()
                            && core.symbol_count() > 0
                            && !settings.collector.allow_empty_catalog =>
                    {
                        runtime.discovery_failures =
                            runtime.discovery_failures.saturating_add(1);
                        runtime.catalog.last_discovery_error = Some(
                            "complete discovery round returned no markets; \
                             the current subscription set was retained"
                                .to_owned(),
                        );
                        warn!(
                            current = core.symbol_count(),
                            "discovery returned no markets; keeping the current subscription set"
                        );
                        update_metrics(
                            &registry,
                            &core,
                            &writer,
                            &persistence,
                            &runtime,
                            stale_after_us,
                        );
                    }
                    Some(Ok(round)) => {
                        let DiscoveryResult { markets, stats } = round;
                        let finished_at_us = stats.finished_at_us;
                        let now = now_us().unwrap_or(started_at_us);
                        let admission = core.projected_shard_loads(&markets)
                            .and_then(|loads| capacity::plan_fixed_topology(&settings, &markets, &loads));
                        match admission.and_then(|_| core.apply_discovery(now, markets)) {
                            Ok(outputs) => {
                                if let Err(error) =
                                    emit_outputs(&writer, outputs, send_timeout).await
                                {
                                    fatal = Some(format!("{error:#}"));
                                    break;
                                }
                                if let Err(error) =
                                    dispatch_controls(&mut core, &commands, send_timeout).await
                                {
                                    fatal = Some(format!("{error:#}"));
                                    break;
                                }
                                runtime.catalog.last_discovery_finished_at_us =
                                    Some(finished_at_us);
                                runtime.catalog.last_discovery_error = None;
                                runtime.discovery = Some(stats);
                                if let Err(error) = persistence.submit(
                                    Arc::new(core.market_infos()),
                                    now,
                                    Some(finished_at_us),
                                ) {
                                    runtime.catalog.last_discovery_error =
                                        Some(format!("{error:#}"));
                                    warn!(
                                        error = %format!("{error:#}"),
                                        "failed to queue metadata/catalog snapshot"
                                    );
                                }
                                update_metrics(
                                    &registry,
                                    &core,
                                    &writer,
                                    &persistence,
                                    &runtime,
                                    stale_after_us,
                                );
                            }
                            Err(error) => {
                                runtime.discovery_failures =
                                    runtime.discovery_failures.saturating_add(1);
                                runtime.catalog.last_discovery_error =
                                    Some(format!("{error:#}"));
                                warn!(
                                    error = %format!("{error:#}"),
                                    "invalid discovery result; retaining current subscriptions"
                                );
                                update_metrics(
                                    &registry,
                                    &core,
                                    &writer,
                                    &persistence,
                                    &runtime,
                                    stale_after_us,
                                );
                            }
                        }
                    }
                    Some(Err(error)) => {
                        runtime.discovery_failures =
                            runtime.discovery_failures.saturating_add(1);
                        runtime.catalog.last_discovery_error = Some(format!("{error:#}"));
                        warn!(error = %format!("{error:#}"), "market discovery failed; retaining current subscriptions");
                        update_metrics(
                            &registry,
                            &core,
                            &writer,
                            &persistence,
                            &runtime,
                            stale_after_us,
                        );
                    }
                    None => {
                        warn!("market discovery result channel closed; continuing");
                    }
                }
            }
            _ = flush_ticker.tick() => {
                if let Err(error) = writer.flush(send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                update_metrics(
                    &registry,
                    &core,
                    &writer,
                    &persistence,
                    &runtime,
                    stale_after_us,
                );
            }
        }
    }

    if let Some(error) = fatal {
        shutdown_reason = format!("fatal: {error}");
        error!(error, "collector stopping after fatal error");
    }
    info!(reason = %shutdown_reason, "shutting down collector");
    let mut shutdown_error: Option<anyhow::Error> = None;

    // Stop accepting new work first. The broadcast wakes workers blocked in a
    // handshake or backoff; the per-source command is best-effort because a
    // full queue must never delay process shutdown.
    let _ = shutdown_tx.send(());
    for command in commands.values() {
        let _ = command.try_send(Control::Shutdown);
    }

    // Apply every event already accepted by the channel before the final
    // snapshot. Workers stop producing after the broadcast, so this reaches a
    // fixed point. Status transitions are deferred: a worker disconnect would
    // otherwise invalidate the books that the final snapshot must capture.
    let mut deferred_statuses = Vec::new();
    let drain_deadline = tokio::time::Instant::now() + send_timeout;
    'drain: while !active_sources.is_empty() {
        match timeout_at(drain_deadline, worker_event_rx.recv()).await {
            Ok(Some(WorkerEvent::Stopped { source })) => {
                active_sources.remove(&source);
            }
            Ok(Some(WorkerEvent::Status(status))) => deferred_statuses.push(status),
            Ok(Some(WorkerEvent::Events { source, events })) => {
                for event in events {
                    let outputs = core.handle_event(source, event);
                    if let Err(error) = emit_outputs(&writer, outputs, send_timeout).await {
                        shutdown_error.get_or_insert(error);
                        break 'drain;
                    }
                }
            }
            Ok(Some(WorkerEvent::Fatal { source, error })) => {
                warn!(%source, error, "source failed during shutdown");
            }
            Ok(Some(WorkerEvent::RecoveryReady {
                source,
                generation,
                connection_epoch,
                receive_seq,
            })) => {
                core.handle_recovery_ready(source, generation, connection_epoch, receive_seq);
            }
            Ok(None) | Err(_) => break,
        }
    }

    let ended_at_us = now_us().unwrap_or(started_at_us);
    core.begin_shutdown();
    if settings.collector.final_snapshots {
        let outputs = core.finalize(ended_at_us);
        if let Err(error) = emit_outputs(&writer, outputs, send_timeout).await {
            shutdown_error.get_or_insert(error);
        }
    }
    // Capture coverage before the deferred disconnect transitions invalidate
    // the final books. The transitions still update the per-source report.
    let final_coverage = core.coverage(ended_at_us, stale_after_us);
    for status in deferred_statuses {
        let _ = core.handle_status(status, ended_at_us);
    }
    for handle in handles.into_values() {
        handle.abort();
    }
    if let Err(error) = writer.flush(send_timeout).await {
        shutdown_error.get_or_insert(error);
    }
    let stats_before_shutdown = writer.stats();
    let writer_stats = match writer.shutdown_with_stats(send_timeout).await {
        Ok(stats) => stats,
        Err(error) => {
            shutdown_error.get_or_insert(error);
            stats_before_shutdown
        }
    };
    // Workers have been aborted and no handle records after shutdown is
    // requested. Finalizing here makes every accepted frame durable before the
    // report claims a complete run.
    let journal_status = match journal.as_ref() {
        Some(handle) => {
            if let Err(error) = handle.shutdown(send_timeout) {
                shutdown_error.get_or_insert(error);
            }
            Some(handle.status())
        }
        None => None,
    };
    let mut journal_consistency_error = None;
    let journal_verification = journal_status.as_ref().and_then(|status| {
        match crate::journal::verify_run(std::path::Path::new(&status.run_directory)) {
            Ok(mut verification) => {
                if let Err(error) = crate::journal::validate_report_status(status, &verification) {
                    let message = format!("{error:#}");
                    verification.complete = false;
                    verification
                        .errors
                        .push(format!("journal report/manifest mismatch: {message}"));
                    journal_consistency_error = Some(message);
                    shutdown_error.get_or_insert(error);
                }
                Some(verification)
            }
            Err(error) => {
                journal_consistency_error = Some(format!("{error:#}"));
                shutdown_error.get_or_insert(error);
                None
            }
        }
    });
    stop_metrics(metrics_shutdown, metrics_handle).await;
    let core_stats = core.stats();
    let sources: Vec<_> = core.statuses().values().cloned().collect();
    let mut report = RunReport::new(
        "live",
        exchange.name(),
        exchange.environment(),
        started_at_us,
        ended_at_us,
        &shutdown_reason,
        &settings.output.path,
        shard_count,
        replica_count,
        core_stats.discovered_symbols as usize,
        core.symbol_count(),
        core_stats,
        writer_stats,
        sources,
        ReportConfig::from_settings(&settings, shard_count),
    );
    report.capacity = Some(capacity);
    report.discovery = runtime.discovery.clone();
    report.catalog = Some(bootstrap.catalog);
    report.coverage = Some(final_coverage);
    report.journal = journal_status;
    report.journal_verification = journal_verification;
    report.journal_consistency_error = journal_consistency_error;

    // The final snapshot is persisted through the same latest-wins worker as
    // every intermediate one, then the worker is drained and joined. A warm
    // checkpoint that no discovery round refreshed keeps its original
    // generation time, so readiness cannot mistake a restart for a refresh.
    let catalog_generated_at_us = runtime.discovery.as_ref().map(|round| round.finished_at_us);
    if let Err(error) = persistence.submit_final(
        Arc::new(core.market_infos()),
        ended_at_us,
        catalog_generated_at_us,
        send_timeout,
    ) {
        warn!(
            error = %format!("{error:#}"),
            "failed to persist final metadata/catalog snapshot"
        );
        shutdown_error.get_or_insert(error);
    }
    let persistence_status = persistence.status();
    report.persistence = Some(persistence_status);
    if let Err(error) = persistence.shutdown(send_timeout) {
        warn!(
            error = %format!("{error:#}"),
            "metadata/catalog persistence worker did not stop cleanly"
        );
        shutdown_error.get_or_insert(error);
    }
    if crate::writer::WriterOptions::from_settings(&settings).rotation_enabled() {
        report.output_manifest = Some(
            crate::config::rotation_manifest_path(&settings.output.path)
                .display()
                .to_string(),
        );
    }
    if let Some(path) = sidecar_path(&settings.output.report_path) {
        report.write(path).context("writing run report")?;
    }
    if let Some(error) = shutdown_error {
        return Err(error);
    }
    Ok(report)
}

async fn discover_with_retry_stats(
    exchange: &Arc<dyn Exchange>,
    max_markets: usize,
    name: &str,
) -> anyhow::Result<DiscoveryResult> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 0..5u32 {
        match exchange.discover_markets_with_stats(max_markets).await {
            Ok(mut result) => {
                result.stats.retries = result.stats.retries.saturating_add(u64::from(attempt));
                return Ok(result);
            }
            Err(error) => {
                warn!(
                    exchange = name,
                    attempt = attempt + 1,
                    error = %format!("{error:#}"),
                    "initial market discovery failed"
                );
                last_error = Some(error);
                if attempt < 4 {
                    let delay = 500u64.saturating_mul(1u64 << attempt);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("initial market discovery failed")))
}

async fn start_metrics(
    settings: &Settings,
    registry: &MetricsRegistry,
) -> anyhow::Result<(
    Option<broadcast::Sender<()>>,
    Option<JoinHandle<anyhow::Result<()>>>,
)> {
    let Some(address) = settings.metrics.listen else {
        return Ok((None, None));
    };
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("binding metrics listener {address}"))?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let registry = registry.clone();
    let handle = tokio::spawn(async move { run_server_on(listener, registry, shutdown_rx).await });
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn stop_metrics(
    shutdown: Option<broadcast::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
) {
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(());
    }
    if let Some(handle) = handle {
        match timeout(Duration::from_secs(2), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!(error = %format!("{error:#}"), "metrics server failed"),
            Ok(Err(error)) => warn!(error = %error, "metrics server task failed"),
            Err(_) => warn!("metrics server did not stop within two seconds"),
        }
    }
}

async fn process_worker_event(
    event: WorkerEvent,
    core: &mut CollectorCore,
    writer: &WriterHandle,
    commands: &BTreeMap<SourceId, mpsc::Sender<Control>>,
    active_sources: &mut BTreeSet<SourceId>,
    send_timeout: Duration,
) -> anyhow::Result<()> {
    match event {
        WorkerEvent::Events { source, events } => {
            for event in events {
                let outputs = core.handle_event(source, event);
                emit_outputs(writer, outputs, send_timeout).await?;
            }
            dispatch_controls(core, commands, send_timeout).await?;
        }
        WorkerEvent::Status(status) => {
            let outputs = core.handle_status(status, now_us().unwrap_or(0));
            emit_outputs(writer, outputs, send_timeout).await?;
            dispatch_controls(core, commands, send_timeout).await?;
        }
        WorkerEvent::Fatal { source, error } => {
            bail!("source {source} reported a fatal error: {error}");
        }
        WorkerEvent::RecoveryReady {
            source,
            generation,
            connection_epoch,
            receive_seq,
        } => {
            core.handle_recovery_ready(source, generation, connection_epoch, receive_seq);
        }
        WorkerEvent::Stopped { source } => {
            active_sources.remove(&source);
        }
    }
    Ok(())
}

async fn emit_outputs(
    writer: &WriterHandle,
    outputs: Vec<OutputMessage>,
    timeout: Duration,
) -> anyhow::Result<()> {
    for output in outputs {
        writer.send(output, timeout).await?;
    }
    Ok(())
}

async fn dispatch_controls(
    core: &mut CollectorCore,
    commands: &BTreeMap<SourceId, mpsc::Sender<Control>>,
    timeout_duration: Duration,
) -> anyhow::Result<()> {
    for (source, control) in core.drain_controls() {
        let Some(sender) = commands.get(&source) else {
            bail!("no command channel for source {source}");
        };
        match timeout(timeout_duration, sender.send(control)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                bail!("source {source} command channel closed: {error}");
            }
            Err(_) => {
                bail!(
                    "source {source} command channel was backpressured for {} seconds",
                    timeout_duration.as_secs()
                );
            }
        }
    }
    Ok(())
}

fn update_metrics(
    registry: &MetricsRegistry,
    core: &CollectorCore,
    writer: &WriterHandle,
    persistence: &PersistenceHandle,
    runtime: &RuntimeState,
    stale_after_us: i64,
) {
    let now = now_us().unwrap_or(0);
    let status = RuntimeStatus {
        coverage: core.coverage(now, stale_after_us),
        catalog: runtime.catalog.clone(),
        discovery: runtime.discovery.clone(),
        discovery_failures: runtime.discovery_failures,
        persistence: persistence.status(),
        writer_healthy: writer.healthy(),
        fatal_error: runtime.fatal_error.clone(),
    };
    registry.update(core.stats(), writer.stats(), core.statuses(), status);
}

async fn wait_for_shutdown_signal() -> String {
    #[cfg(unix)]
    {
        let mut terminate = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(signal) => signal,
            Err(error) => {
                warn!(error = %error, "could not install SIGTERM handler; only SIGINT will be handled");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT".to_owned();
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT".to_owned(),
            _ = terminate.recv() => "SIGTERM".to_owned(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        config::CatalogMode,
        exchange::DiscoveryStats,
        model::MarketInfo,
        session::{MarketDataSession, SessionFactory},
    };

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
        }
    }

    struct FakeFactory;

    #[async_trait]
    impl SessionFactory for FakeFactory {
        async fn create_session(
            &self,
            _source: SourceId,
        ) -> anyhow::Result<Box<dyn MarketDataSession>> {
            anyhow::bail!("bootstrap tests never create a session")
        }
    }

    struct FakeExchange {
        markets: Vec<MarketInfo>,
        discovery_calls: AtomicUsize,
    }

    impl FakeExchange {
        fn new(markets: Vec<MarketInfo>) -> Self {
            Self {
                markets,
                discovery_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Exchange for FakeExchange {
        fn name(&self) -> &'static str {
            "polymarket"
        }

        fn environment(&self) -> &'static str {
            "public"
        }

        async fn discover_markets(&self, _max_markets: usize) -> anyhow::Result<Vec<MarketInfo>> {
            Ok(self.markets.clone())
        }

        async fn discover_markets_with_stats(
            &self,
            _max_markets: usize,
        ) -> anyhow::Result<DiscoveryResult> {
            self.discovery_calls.fetch_add(1, Ordering::SeqCst);
            let now = now_us().unwrap_or(0);
            let stats = DiscoveryStats {
                pages: 1,
                selected_markets: self.markets.len() as u64,
                outcome_symbols: self.markets.len() as u64,
                duration_ms: 1,
                started_at_us: now,
                finished_at_us: now,
                ..DiscoveryStats::default()
            };
            Ok(DiscoveryResult {
                markets: self.markets.clone(),
                stats,
            })
        }

        fn session_factory(self: Arc<Self>) -> Arc<dyn SessionFactory> {
            Arc::new(FakeFactory)
        }
    }

    fn settings_with_catalog(mode: CatalogMode, path: &std::path::Path) -> Settings {
        let mut settings = Settings::for_exchange(ExchangeKind::Polymarket);
        settings.collector.catalog_mode = mode;
        settings.collector.catalog_path = Some(path.to_path_buf());
        settings.collector.catalog_max_age_secs = 3_600;
        settings
    }

    #[tokio::test]
    async fn warm_mode_uses_a_fresh_checkpoint_without_discovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let now = now_us().unwrap();
        Catalog::new("polymarket", vec![market(0), market(1)], now - 60_000_000)
            .unwrap()
            .save(&path)
            .unwrap();
        let settings = settings_with_catalog(CatalogMode::Warm, &path);
        let exchange: Arc<dyn Exchange> = Arc::new(FakeExchange::new(vec![market(9)]));
        let bootstrap = bootstrap_markets(&settings, &exchange, now).await.unwrap();
        assert_eq!(bootstrap.catalog.source, "warm");
        assert_eq!(bootstrap.markets.len(), 2);
        assert!(bootstrap.discovery.is_none());
        assert_eq!(
            bootstrap.catalog_generated_at_us, None,
            "a warm checkpoint must not be rewritten by startup"
        );
    }

    #[tokio::test]
    async fn warm_mode_falls_back_to_discovery_for_a_stale_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let now = now_us().unwrap();
        Catalog::new("polymarket", vec![market(0)], now - 7_200_000_000)
            .unwrap()
            .save(&path)
            .unwrap();
        let settings = settings_with_catalog(CatalogMode::Warm, &path);
        let exchange: Arc<dyn Exchange> = Arc::new(FakeExchange::new(vec![market(9)]));
        let bootstrap = bootstrap_markets(&settings, &exchange, now).await.unwrap();
        assert_eq!(bootstrap.catalog.source, "cold");
        assert_eq!(bootstrap.markets[0].symbol, "polymarket:0x9:0");
        assert!(bootstrap.discovery.is_some());
        assert!(bootstrap.catalog_generated_at_us.is_some());
    }

    #[tokio::test]
    async fn require_rejects_stale_while_stale_mode_accepts_it_explicitly() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let now = now_us().unwrap();
        Catalog::new("polymarket", vec![market(0)], now - 7_200_000_000)
            .unwrap()
            .save(&path)
            .unwrap();
        let exchange: Arc<dyn Exchange> = Arc::new(FakeExchange::new(vec![market(9)]));

        let require = settings_with_catalog(CatalogMode::Require, &path);
        let error = bootstrap_markets(&require, &exchange, now)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("older than catalog_max_age_secs"),
            "unexpected error: {error:#}"
        );

        let stale = settings_with_catalog(CatalogMode::Stale, &path);
        let bootstrap = bootstrap_markets(&stale, &exchange, now).await.unwrap();
        assert_eq!(bootstrap.catalog.source, "warm-stale");
        assert_eq!(bootstrap.markets.len(), 1);
    }

    #[tokio::test]
    async fn required_checkpoint_must_match_the_exchange_and_checksum() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let now = now_us().unwrap();
        Catalog::new("kalshi", vec![market(0)], now)
            .unwrap()
            .save(&path)
            .unwrap();
        let exchange: Arc<dyn Exchange> = Arc::new(FakeExchange::new(vec![market(9)]));
        let settings = settings_with_catalog(CatalogMode::Require, &path);
        let error = bootstrap_markets(&settings, &exchange, now)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("another exchange"),
            "unexpected error: {error:#}"
        );

        Catalog::new("polymarket", vec![market(0)], now)
            .unwrap()
            .save(&path)
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        value["markets"][0]["native_symbol"] = serde_json::Value::String("tampered".to_owned());
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        let error = bootstrap_markets(&settings, &exchange, now)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("checksum"),
            "unexpected error: {error:#}"
        );
    }
}
