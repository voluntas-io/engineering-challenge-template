//! Synchronous collector state machine.
//!
//! Exchange sessions and transport concerns live outside this module. The core
//! consumes normalized events, maintains one shadow book per source, chooses
//! the active source for each symbol, and produces serialized SBE messages.
//! This makes the correctness-critical logic deterministic and testable without
//! a network.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use tracing::{debug, error, warn};

use crate::{
    Side,
    config::Settings,
    exchange::ExchangeCapabilities,
    fixed::SCALE,
    metrics::CollectorStats,
    model::{
        BookSnapshot, Control, DiscontinuityReason, EventMeta, ExchangeEvent, LifecycleKind,
        MarketInfo, OutputMessage, QuantityChange, ResetReason, SourceId, SourceStatus,
    },
    orderbook::OrderBook,
    time::utc_from_us,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};

/// Tuning for the synchronous core. Durations are microseconds so the core can
/// be driven by replay timestamps as well as wall-clock ticks.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub exchange: String,
    pub shard_count: usize,
    pub replica_count: usize,
    pub snapshot_interval_us: i64,
    pub stale_probe_after_us: i64,
    pub probe_interval_us: i64,
    pub max_probes: u32,
    pub crossed_grace_us: i64,
    /// Venue capabilities supplied by the exchange adapter.
    pub capabilities: ExchangeCapabilities,
}

impl CoreConfig {
    pub fn from_settings(
        settings: &Settings,
        shard_count: usize,
        capabilities: ExchangeCapabilities,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(shard_count >= 1, "shard count must be >= 1");
        anyhow::ensure!(
            settings.collector.replicas >= 1,
            "replica count must be >= 1"
        );
        let seconds_to_us = |seconds: u64| -> anyhow::Result<i64> {
            i64::try_from(seconds)
                .context("duration exceeds i64 seconds")?
                .checked_mul(1_000_000)
                .context("duration overflows microseconds")
        };
        Ok(Self {
            exchange: settings.exchange.to_string(),
            shard_count,
            replica_count: settings.collector.replicas,
            snapshot_interval_us: seconds_to_us(settings.collector.snapshot_interval_secs)?,
            stale_probe_after_us: seconds_to_us(settings.collector.stale_probe_after_secs)?,
            probe_interval_us: seconds_to_us(settings.collector.probe_interval_secs)?,
            max_probes: settings.collector.max_probes,
            crossed_grace_us: 2_000_000,
            capabilities,
        })
    }
}

/// Locally assigned receive ordering for one source frame. The epoch increases
/// on every successful reconnect; the sequence is monotonic within an epoch.
/// Both are assigned by the session supervisor before decoding is visible to
/// the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
struct FrameOrder {
    connection_epoch: u64,
    receive_seq: u64,
}

impl FrameOrder {
    const fn new(connection_epoch: u64, receive_seq: u64) -> Self {
        Self {
            connection_epoch,
            receive_seq,
        }
    }
}

#[derive(Debug, Clone)]
struct RecoveryRequest {
    generation: u64,
    /// Set when the worker confirms the recovery control was applied. A
    /// snapshot is eligible only when its receive fence is strictly newer.
    fence: Option<FrameOrder>,
}

#[derive(Debug, Clone)]
struct PendingRecovery {
    generation: u64,
    symbols: BTreeSet<String>,
    reconnect: bool,
}

#[derive(Debug, Clone, Default)]
struct SourceBook {
    book: OrderBook,
    valid: bool,
    /// Clock attached to the currently valid book.
    last_clock: Option<i64>,
    /// Most recent clock observed from this connection, retained even after
    /// the book is invalidated. This distinguishes a shadow that was already
    /// behind from a source that was current and then reported a lower clock
    /// after exchange maintenance.
    last_observed_clock: Option<i64>,
    last_exchange_ts_us: Option<i64>,
    last_received_at_us: Option<i64>,
}

impl SourceBook {
    fn invalidate(&mut self) {
        self.book = OrderBook::new();
        self.valid = false;
        self.last_clock = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscontinuityIncident {
    /// Symbol name
    symbol: String,
    /// Start of the gap (when continuity was lost)
    started_at_us: i64,
    /// End of the gap (when re-anchored), if resolved
    resolved_at_us: Option<i64>,
    /// Reason continuity was lost
    reason: crate::model::DiscontinuityReason,
    /// Whether the gap interval is quantifiable (Kalshi has seq/clock; Polymarket does not)
    quantifiable: bool,
}

#[derive(Debug)]
struct MarketState {
    info: MarketInfo,
    shard: usize,
    active: Option<SourceId>,
    books: BTreeMap<SourceId, SourceBook>,
    /// Outstanding explicit recovery requests. A lower clock on one of these
    /// snapshots is evidence of a Kalshi maintenance reset, not a stale shadow
    /// frame. For clockless venues the request also carries the receive fence
    /// that a replacement snapshot must beat.
    recovery_requests: BTreeMap<SourceId, RecoveryRequest>,
    emitted_clock: Option<i64>,
    emitted_exchange_ts_us: Option<i64>,
    emitted_any: bool,
    last_emit_us: Option<i64>,
    next_snapshot_us: i64,
    probes: u32,
    last_probe_us: i64,
    crossed_since_us: Option<i64>,
    /// Per-symbol continuity state
    continuity: crate::model::ContinuityState,
    /// Open discontinuity incident (None if no active gap)
    incident: Option<DiscontinuityIncident>,
    /// All resolved incidents for this symbol (retained for reporting)
    resolved_incidents: Vec<DiscontinuityIncident>,
}

impl MarketState {
    fn active_book(&self) -> Option<&SourceBook> {
        self.active
            .and_then(|source| self.books.get(&source))
            .filter(|book| book.valid)
    }

    fn set_continuity(&mut self, new_state: crate::model::ContinuityState) {
        self.continuity = new_state;
    }

    /// Open a discontinuity incident. Returns true only when a new incident
    /// was created, which is the sole condition for incrementing start
    /// counters.
    fn start_incident(
        &mut self,
        reason: DiscontinuityReason,
        now_us: i64,
        quantifiable: bool,
    ) -> bool {
        if self.incident.is_some() || self.continuity == crate::model::ContinuityState::Terminal {
            return false;
        }
        self.incident = Some(DiscontinuityIncident {
            symbol: self.info.symbol.clone(),
            started_at_us: now_us,
            resolved_at_us: None,
            reason,
            quantifiable,
        });
        self.continuity = crate::model::ContinuityState::Resyncing;
        true
    }

    fn resolve_incident(&mut self, now_us: i64) {
        if let Some(mut incident) = self.incident.take() {
            incident.resolved_at_us = Some(now_us);
            self.resolved_incidents.push(incident);
        }
        // If no other blocker, go Live; otherwise stay in current state
        if self.continuity == crate::model::ContinuityState::Resyncing
            || self.continuity == crate::model::ContinuityState::Stale
        {
            self.continuity = crate::model::ContinuityState::Live;
        }
    }
}

#[derive(Debug, Default)]
struct LifecycleMetadata {
    close_time_us: Option<i64>,
    title: Option<String>,
    tick_size_us: Option<i64>,
    price_ranges: Option<Vec<crate::model::PriceRange>>,
    price_level_structure: Option<String>,
}

#[derive(Debug, Default)]
struct ShardState {
    symbols: BTreeSet<String>,
    sources: Vec<SourceId>,
}

/// The collector core. All methods are synchronous; callers batch outputs and
/// apply control messages to the appropriate source workers.
pub struct CollectorCore {
    config: CoreConfig,
    markets: BTreeMap<String, MarketState>,
    shards: Vec<ShardState>,
    statuses: BTreeMap<SourceId, SourceStatus>,
    dirty_shards: BTreeSet<usize>,
    /// Recovery controls that have not been handed to a worker yet.
    pending_recovery: BTreeMap<SourceId, PendingRecovery>,
    next_recovery_generation: u64,
    shutting_down: bool,
    stats: CollectorStats,
}

impl CollectorCore {
    pub fn new(config: CoreConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(config.shard_count >= 1, "at least one shard is required");
        anyhow::ensure!(
            config.replica_count >= 1,
            "at least one replica is required"
        );
        anyhow::ensure!(
            config.shard_count <= u16::MAX as usize,
            "too many shards for SourceId"
        );
        anyhow::ensure!(
            config.replica_count <= u16::MAX as usize,
            "too many replicas for SourceId"
        );
        anyhow::ensure!(
            config.snapshot_interval_us > 0,
            "snapshot interval must be positive"
        );
        let shards = (0..config.shard_count)
            .map(|shard| ShardState {
                symbols: BTreeSet::new(),
                sources: (0..config.replica_count)
                    .map(|replica| SourceId::new(shard as u16, replica as u16))
                    .collect(),
            })
            .collect();
        let stats = CollectorStats {
            shard_count: config.shard_count as u64,
            ..CollectorStats::default()
        };
        Ok(Self {
            config,
            markets: BTreeMap::new(),
            shards,
            statuses: BTreeMap::new(),
            dirty_shards: BTreeSet::new(),
            pending_recovery: BTreeMap::new(),
            next_recovery_generation: 1,
            shutting_down: false,
            stats,
        })
    }

    #[must_use]
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    #[must_use]
    pub fn stats(&self) -> CollectorStats {
        self.stats.clone()
    }

    /// Preview the exact shard loads that [`Self::apply_discovery`] would
    /// produce. This has no side effects so the coordinator can
    /// reject an over-capacity discovery round before it retires books,
    /// dispatches controls, emits snapshots, or persists a checkpoint.
    pub fn projected_shard_loads(&self, markets: &[MarketInfo]) -> anyhow::Result<Vec<usize>> {
        let mut desired = BTreeMap::new();
        let mut output_symbols = BTreeSet::new();
        for info in markets {
            info.validate().with_context(|| {
                format!(
                    "invalid discovery metadata for `{}` ({})",
                    info.native_symbol, info.symbol
                )
            })?;
            anyhow::ensure!(
                output_symbols.insert(info.symbol.clone()),
                "duplicate output symbol `{}` in discovery result",
                info.symbol
            );
            anyhow::ensure!(
                desired.insert(info.native_symbol.clone(), info).is_none(),
                "duplicate native symbol `{}` in discovery result",
                info.native_symbol
            );
        }
        let mut loads: Vec<usize> = self
            .shards
            .iter()
            .map(|shard| shard.symbols.len())
            .collect();
        // Existing identities retain their shard. Remove anything that will
        // disappear or be reidentified, exactly as apply_discovery does.
        for (native, state) in &self.markets {
            if desired
                .get(native)
                .is_none_or(|info| info.symbol != state.info.symbol)
            {
                loads[state.shard] = loads[state.shard].saturating_sub(1);
            }
        }
        let mut additions: Vec<_> = desired
            .iter()
            .filter(|(native, info)| {
                self.markets
                    .get(*native)
                    .is_none_or(|state| state.info.symbol != info.symbol)
            })
            .collect();
        additions.sort_by(|left, right| left.0.cmp(right.0));
        for _ in additions {
            let shard = loads
                .iter()
                .enumerate()
                .min_by_key(|(index, load)| (**load, *index))
                .map_or(0, |(index, _)| index);
            loads[shard] = loads[shard].saturating_add(1);
        }
        Ok(loads)
    }

    #[must_use]
    pub fn statuses(&self) -> &BTreeMap<SourceId, SourceStatus> {
        &self.statuses
    }

    #[must_use]
    pub fn market_infos(&self) -> Vec<MarketInfo> {
        let mut infos: Vec<MarketInfo> = self
            .markets
            .values()
            .map(|state| state.info.clone())
            .collect();
        infos.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        infos
    }

    #[must_use]
    pub fn symbol_count(&self) -> usize {
        self.markets.len()
    }

    /// Coverage gauges for readiness.
    ///
    /// A symbol is anchored only when an active source has a valid book that
    /// was established by an exchange snapshot. A subscription command that
    /// was sent but not answered therefore never counts as anchored. Stale
    /// symbols are anchored books whose last valid update is older than
    /// `stale_after_us`.
    #[must_use]
    pub fn coverage(&self, now_us: i64, stale_after_us: i64) -> crate::metrics::CoverageStats {
        let mut anchored = 0u64;
        let mut stale = 0u64;
        let mut oldest_anchor_us: Option<i64> = None;
        let mut unseeded = 0u64;
        let mut live = 0u64;
        let mut stale_detail = 0u64;
        let mut resyncing = 0u64;
        let mut terminal = 0u64;
        for state in self.markets.values() {
            match state.continuity {
                crate::model::ContinuityState::Unseeded => unseeded = unseeded.saturating_add(1),
                crate::model::ContinuityState::Live => live = live.saturating_add(1),
                crate::model::ContinuityState::Stale => {
                    stale_detail = stale_detail.saturating_add(1)
                }
                crate::model::ContinuityState::Resyncing => resyncing = resyncing.saturating_add(1),
                crate::model::ContinuityState::Terminal => terminal = terminal.saturating_add(1),
            }
            let Some(book) = state.active_book() else {
                continue;
            };
            anchored = anchored.saturating_add(1);
            let last = book.last_received_at_us.unwrap_or(0);
            if now_us.saturating_sub(last) > stale_after_us {
                stale = stale.saturating_add(1);
            }
            oldest_anchor_us = Some(oldest_anchor_us.map_or(last, |oldest| oldest.min(last)));
        }
        let expected_subscriptions = self
            .shards
            .iter()
            .map(|shard| shard.symbols.len().saturating_mul(shard.sources.len()))
            .sum::<usize>() as u64;
        let subscribed_symbols = self
            .statuses
            .values()
            .filter(|status| status.connected)
            .map(|status| status.subscriptions as u64)
            .sum();
        let shards_with_connected_source = self
            .shards
            .iter()
            .filter(|shard| {
                shard.sources.iter().any(|source| {
                    self.statuses
                        .get(source)
                        .is_some_and(|status| status.connected)
                })
            })
            .count() as u64;
        crate::metrics::CoverageStats {
            expected_symbols: self.markets.len() as u64,
            anchored_symbols: anchored,
            stale_symbols: stale,
            unseeded_symbols: unseeded,
            live_symbols: live,
            stale_symbols_detail: stale_detail,
            resyncing_symbols: resyncing,
            terminal_symbols: terminal,
            expected_subscriptions,
            subscribed_symbols,
            shards_with_connected_source,
            shards: self.shards.len() as u64,
            sources_connected: self
                .statuses
                .values()
                .filter(|status| status.connected)
                .count() as u64,
            sources_total: self
                .shards
                .iter()
                .map(|shard| shard.sources.len())
                .sum::<usize>() as u64,
            oldest_anchor_us,
        }
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
    }

    /// Reconcile the desired symbol set.
    ///
    /// A discovery result is accepted or rejected as a whole. Silently skipping
    /// one malformed market would make every other current symbol look absent
    /// and could tear down a healthy subscription set, so callers retain the
    /// previous set when this returns an error.
    ///
    /// Output-symbol identity is treated as immutable per native symbol. A
    /// rediscovery that keeps the native symbol but changes the output symbol
    /// (for example after a venue identifier migration) is applied as an
    /// explicit retirement of the old identity — emitting a final empty
    /// snapshot under the old symbol — followed by a fresh market state under
    /// the new symbol. Downstream consumers therefore never see deltas for a
    /// symbol whose full snapshot they have not observed.
    pub fn apply_discovery(
        &mut self,
        now_us: i64,
        markets: Vec<MarketInfo>,
    ) -> anyhow::Result<Vec<OutputMessage>> {
        self.stats.discovery_rounds = self.stats.discovery_rounds.saturating_add(1);
        let mut desired: BTreeMap<String, MarketInfo> = BTreeMap::new();
        let mut symbols: BTreeSet<String> = BTreeSet::new();
        for info in markets {
            info.validate().with_context(|| {
                format!(
                    "invalid discovery metadata for `{}` ({})",
                    info.native_symbol, info.symbol
                )
            })?;
            if !symbols.insert(info.symbol.clone()) {
                bail!(
                    "duplicate output symbol `{}` in discovery result",
                    info.symbol
                );
            }
            let native = info.native_symbol.clone();
            if desired.contains_key(&native) {
                bail!("duplicate native symbol `{native}` in discovery result");
            }
            desired.insert(native, info);
        }
        self.stats.discovered_symbols = desired.len() as u64;

        let mut outputs = Vec::new();
        let removals: Vec<String> = self
            .markets
            .keys()
            .filter(|native| !desired.contains_key(*native))
            .cloned()
            .collect();
        for native in removals {
            outputs.extend(self.remove_market(&native, now_us, true));
        }

        let mut additions = Vec::new();
        let mut reidentities: Vec<(String, MarketInfo)> = Vec::new();
        for (native, info) in desired {
            if let Some(state) = self.markets.get_mut(&native) {
                if state.info.symbol == info.symbol {
                    // Same identity: refresh mutable metadata only.
                    state.info = info;
                } else {
                    // Changed output identity: retire the old symbol first.
                    reidentities.push((native, info));
                }
            } else {
                additions.push((native, info));
            }
        }
        for (native, info) in reidentities {
            outputs.extend(self.remove_market(&native, now_us, true));
            self.add_market(native, info, now_us);
        }
        additions.sort_by(|left, right| left.0.cmp(&right.0));
        for (native, info) in additions {
            self.add_market(native, info, now_us);
        }

        self.stats.current_symbols = self.markets.len() as u64;
        Ok(outputs)
    }

    fn add_market(&mut self, native: String, info: MarketInfo, now_us: i64) {
        let shard = self
            .shards
            .iter()
            .enumerate()
            .min_by_key(|(index, shard)| (shard.symbols.len(), *index))
            .map_or(0, |(index, _)| index);
        let sources = self.shards[shard].sources.clone();
        let mut books = BTreeMap::new();
        for source in sources {
            books.insert(source, SourceBook::default());
        }
        let state = MarketState {
            info,
            shard,
            active: None,
            books,
            recovery_requests: BTreeMap::new(),
            emitted_clock: None,
            emitted_exchange_ts_us: None,
            emitted_any: false,
            last_emit_us: None,
            next_snapshot_us: now_us.saturating_add(self.config.snapshot_interval_us),
            probes: 0,
            last_probe_us: now_us,
            crossed_since_us: None,
            continuity: crate::model::ContinuityState::Unseeded,
            incident: None,
            resolved_incidents: Vec::new(),
        };
        self.shards[shard].symbols.insert(native.clone());
        self.markets.insert(native, state);
        self.dirty_shards.insert(shard);
        self.stats.added_symbols = self.stats.added_symbols.saturating_add(1);
        self.stats.current_symbols = self.markets.len() as u64;
    }

    fn remove_market(&mut self, native: &str, now_us: i64, emit_empty: bool) -> Vec<OutputMessage> {
        let Some(state) = self.markets.remove(native) else {
            return Vec::new();
        };
        self.shards[state.shard].symbols.remove(native);
        self.dirty_shards.insert(state.shard);
        self.stats.removed_symbols = self.stats.removed_symbols.saturating_add(1);
        self.stats.current_symbols = self.markets.len() as u64;

        let mut outputs = Vec::new();
        if emit_empty && state.emitted_any {
            let exchange_ts = state.emitted_exchange_ts_us.unwrap_or(now_us);
            outputs.push(OutputMessage::Snapshot(build_empty_snapshot(
                &state.info,
                now_us,
                exchange_ts,
            )));
            self.stats.empty_snapshots = self.stats.empty_snapshots.saturating_add(1);
        }
        outputs
    }

    /// Consume one normalized event from a source.
    pub fn handle_event(&mut self, source: SourceId, event: ExchangeEvent) -> Vec<OutputMessage> {
        match event {
            ExchangeEvent::Snapshot {
                native_symbol,
                book,
                meta,
            } => self.handle_snapshot(source, &native_symbol, book, meta),
            ExchangeEvent::LevelChange {
                native_symbol,
                side,
                price,
                change,
                meta,
            } => self.handle_level_change(source, &native_symbol, side, price, change, meta),
            ExchangeEvent::Lifecycle {
                native_symbol,
                kind,
                close_time_us,
                title,
                tick_size_us,
                price_ranges,
                price_level_structure,
                meta,
            } => self.handle_lifecycle(
                &native_symbol,
                kind,
                LifecycleMetadata {
                    close_time_us,
                    title,
                    tick_size_us,
                    price_ranges,
                    price_level_structure,
                },
                meta,
            ),
            ExchangeEvent::StreamReset {
                native_symbols,
                reason,
                received_at_us,
            } => self.handle_stream_reset(source, native_symbols, reason, received_at_us),
        }
    }

    fn handle_snapshot(
        &mut self,
        source: SourceId,
        native: &str,
        snapshot: BookSnapshot,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        if !state.books.contains_key(&source) {
            warn!(%source, native, "snapshot from a source not assigned to this shard");
            return Vec::new();
        }
        if state.continuity == crate::model::ContinuityState::Terminal {
            return Vec::new();
        }

        let mut book = OrderBook::new();
        if let Err(error) = book.replace(&snapshot) {
            warn!(%source, native, error = %error, "rejecting malformed snapshot");
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
            }
            return self.reseat(native, meta.received_at_us);
        }
        if book.is_crossed() {
            self.stats.crossed_snapshots_skipped =
                self.stats.crossed_snapshots_skipped.saturating_add(1);
            warn!(%source, native, "rejecting crossed snapshot");
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
            }
            return self.reseat(native, meta.received_at_us);
        }

        let exchange_ts = meta.exchange_ts_or_received();
        let was_emitted = state.emitted_any;
        let was_active = state.active;
        let source_book_was_valid = state.books.get(&source).is_some_and(|book| book.valid);
        let order = FrameOrder::new(meta.connection_epoch, meta.receive_seq);
        let fenced = meta.has_receive_fence();
        let recovery_request = state.recovery_requests.get(&source);
        let recovery = recovery_request.is_some();
        let recovery_generation = recovery_request.map(|request| request.generation);
        let recovery_fence_ok = recovery_request
            .is_some_and(|request| request.fence.is_some_and(|fence| fenced && order > fence));
        let clock_regression = clock_regressed(state.emitted_clock, meta.clock);

        // A clockless venue has no exchange ordering that proves a shadow book
        // is newer than the state already published. Promoting a source or
        // repairing an active gap therefore requires an acknowledged recovery
        // generation and a frame read strictly after that acknowledgement.
        // Validate before mutating the source book, timestamps, active source,
        // incident, or request state.
        let promotes = was_active.is_none() && was_emitted;
        let repairs_active = was_active == Some(source)
            && !matches!(state.continuity, crate::model::ContinuityState::Live);
        if !self.config.capabilities.has_sequence_clock
            && (promotes || repairs_active)
            && !recovery_fence_ok
        {
            self.stats.rejected_recovery_snapshots =
                self.stats.rejected_recovery_snapshots.saturating_add(1);
            debug!(
                %source,
                native,
                generation = recovery_generation.unwrap_or(0),
                epoch = meta.connection_epoch,
                seq = meta.receive_seq,
                "rejecting ineligible clockless recovery snapshot before mutation"
            );
            return Vec::new();
        }
        if was_active.is_none()
            && was_emitted
            && state.emitted_clock.is_some()
            && meta.clock.is_none()
        {
            return Vec::new();
        }

        // Kalshi's clock is not comparable across exchange maintenance. A lower
        // clock is trusted only from the active connection, or from a requested
        // snapshot on a source whose old book was already invalidated (for
        // example after reconnecting). A lower clock on a still-valid shadow
        // book is stale and must not be promoted.
        let recovery_trusted = recovery
            && !source_book_was_valid
            && match (
                state.books[&source].last_observed_clock,
                state.emitted_clock,
            ) {
                (None, _) => true,
                (Some(observed), Some(emitted)) => observed >= emitted,
                (Some(_), None) => true,
            };
        if clock_regression && was_active != Some(source) && !recovery_trusted {
            if let Some(source_book) = state.books.get_mut(&source) {
                if let Some(clock) = meta.clock {
                    source_book.last_observed_clock = Some(
                        source_book
                            .last_observed_clock
                            .map_or(clock, |observed| observed.max(clock)),
                    );
                }
                // A lower-clock snapshot on a shadow cannot establish the
                // current epoch. Invalidate it so a later failover requires a
                // fresh connection instead of trusting this stale book.
                source_book.invalidate();
            }
            return Vec::new();
        }

        if clock_regression {
            self.stats.clock_resets = self.stats.clock_resets.saturating_add(1);
            for (book_source, source_book) in &mut state.books {
                if *book_source != source {
                    source_book.invalidate();
                }
            }
            state
                .recovery_requests
                .retain(|book_source, _| *book_source == source);
            // Clock regression on the active source creates a discontinuity.
            if was_active == Some(source) {
                let quantifiable = meta.clock.is_some();
                let started = state.start_incident(
                    DiscontinuityReason::SequenceGap,
                    meta.received_at_us,
                    quantifiable,
                );
                record_incident(&mut self.stats, started, quantifiable);
            }
        }

        {
            let Some(source_book) = state.books.get_mut(&source) else {
                error!(%source, native, "source book vanished while applying a snapshot");
                return Vec::new();
            };
            source_book.book = book;
            source_book.valid = true;
            // A snapshot establishes the complete clock state. Retaining an
            // older clock when the snapshot omits one would let a stale shadow
            // book win a later failover comparison.
            source_book.last_clock = meta.clock;
            if meta.clock.is_some() {
                source_book.last_observed_clock = meta.clock;
            }
            source_book.last_exchange_ts_us = Some(exchange_ts);
            source_book.last_received_at_us = Some(meta.received_at_us);
        }

        let should_emit = was_active.is_none() || was_active == Some(source);
        if !should_emit {
            // A shadow snapshot is evidence for its own book only. It must not
            // resolve the active symbol's incident or consume a recovery token.
            return Vec::new();
        }
        state.recovery_requests.remove(&source);

        // Probe and crossed-book state belong to the active source. A healthy
        // shadow replica must not reset them, or a quiet active source would
        // never reach the stale-probe threshold.
        state.probes = 0;
        state.last_probe_us = meta.received_at_us;
        state.crossed_since_us = None;

        if was_active.is_none() {
            if was_emitted {
                // Failover/recovery: only an exchange snapshot accepted above
                // resolves the incident.
                state.resolve_incident(meta.received_at_us);
            } else {
                state.set_continuity(crate::model::ContinuityState::Live);
                state.recovery_requests.clear();
            }
        } else if was_active == Some(source)
            && !clock_regression
            && matches!(
                state.continuity,
                crate::model::ContinuityState::Stale | crate::model::ContinuityState::Resyncing
            )
        {
            // Fresh active repair. For clockless venues the recovery gate above
            // already proved the frame came after an acknowledged request.
            state.resolve_incident(meta.received_at_us);
        }

        let message = build_snapshot(
            &state.info,
            &state.books[&source].book,
            meta.received_at_us,
            exchange_ts,
        );
        if was_active.is_none() {
            state.active = Some(source);
            if was_emitted {
                self.stats.failover_snapshots = self.stats.failover_snapshots.saturating_add(1);
                self.stats.source_switches = self.stats.source_switches.saturating_add(1);
            } else {
                self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
            }
        } else {
            self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
        }
        state.emitted_any = true;
        state.emitted_clock = if clock_regression {
            meta.clock
        } else {
            meta.clock.or(state.emitted_clock)
        };
        state.emitted_exchange_ts_us = Some(exchange_ts);
        state.last_emit_us = Some(meta.received_at_us);
        state.next_snapshot_us = if was_emitted {
            meta.received_at_us
                .saturating_add(self.config.snapshot_interval_us)
        } else {
            first_snapshot_deadline(
                meta.received_at_us,
                self.config.snapshot_interval_us,
                &state.info.symbol,
            )
        };
        vec![OutputMessage::Snapshot(message)]
    }

    fn handle_level_change(
        &mut self,
        source: SourceId,
        native: &str,
        side: Side,
        price: i64,
        change: QuantityChange,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        // Drop deltas for symbols in Resyncing/Stale/Unseeded/Terminal continuity state.
        // Only an accepted exchange snapshot (via handle_snapshot) may transition
        // continuity to Live. Deltas cannot re-anchor a book after a possible gap.
        if state.continuity != crate::model::ContinuityState::Live {
            return Vec::new();
        }
        let clock_regression = clock_regressed(state.emitted_clock, meta.clock);
        let (quantity, crossed) = {
            let Some(source_book) = state.books.get_mut(&source) else {
                return Vec::new();
            };
            if !source_book.valid {
                return Vec::new();
            }
            // A lower clock on a shadow connection is either stale or belongs
            // to a new maintenance epoch that this source has not established
            // with a snapshot. Never apply it to the shadow book.
            if clock_regression && state.active != Some(source) {
                return Vec::new();
            }
            if let Err(error) = source_book.book.apply(side, price, change) {
                self.stats.deltas_rejected = self.stats.deltas_rejected.saturating_add(1);
                warn!(%source, native, error = %error, "rejecting order-book delta");
                source_book.invalidate();
                if state.active == Some(source) {
                    state.active = None;
                }
                return self.reseat(native, meta.received_at_us);
            }

            let exchange_ts = meta.exchange_ts_or_received();
            source_book.last_clock = meta.clock.or(source_book.last_clock);
            if meta.clock.is_some() {
                source_book.last_observed_clock = meta.clock;
            }
            source_book.last_exchange_ts_us = Some(exchange_ts);
            source_book.last_received_at_us = Some(meta.received_at_us);
            (
                source_book.book.quantity(side, price).unwrap_or(0),
                source_book.book.is_crossed(),
            )
        };

        if clock_regression {
            self.stats.clock_resets = self.stats.clock_resets.saturating_add(1);
            for (book_source, source_book) in &mut state.books {
                if *book_source != source {
                    source_book.invalidate();
                }
            }
            state
                .recovery_requests
                .retain(|book_source, _| *book_source == source);
        }

        let exchange_ts = meta.exchange_ts_or_received();
        if state.active == Some(source) {
            state.probes = 0;
            state.last_probe_us = meta.received_at_us;
            if matches!(change, QuantityChange::Delta(0)) {
                if clock_regression {
                    state.emitted_clock = meta.clock;
                }
                return Vec::new();
            }
            state.emitted_clock = if clock_regression {
                meta.clock
            } else {
                meta.clock.or(state.emitted_clock)
            };
            state.emitted_exchange_ts_us = Some(exchange_ts);
            state.last_emit_us = Some(meta.received_at_us);
            if crossed {
                state.crossed_since_us.get_or_insert(meta.received_at_us);
            } else {
                state.crossed_since_us = None;
            }
            let symbol = state.info.symbol.clone();
            let update = build_delta(
                symbol,
                side,
                price,
                quantity,
                meta.received_at_us,
                exchange_ts,
            );
            self.stats.deltas_emitted = self.stats.deltas_emitted.saturating_add(1);
            vec![OutputMessage::PriceLevelUpdate(update)]
        } else {
            Vec::new()
        }
    }

    fn handle_lifecycle(
        &mut self,
        native: &str,
        kind: LifecycleKind,
        update: LifecycleMetadata,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        self.stats.lifecycle_events = self.stats.lifecycle_events.saturating_add(1);
        if kind.is_terminal() || matches!(kind, LifecycleKind::Deactivated) {
            if let Some(state) = self.markets.get_mut(native) {
                // Terminal lifecycle is not a continuity gap; it's an expected
                // end of life for the market. Mark as Terminal without starting
                // a discontinuity incident, then remove.
                state.set_continuity(crate::model::ContinuityState::Terminal);
            }
            return self.remove_market(native, meta.received_at_us, true);
        }
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        let LifecycleMetadata {
            close_time_us,
            title,
            tick_size_us,
            price_ranges,
            price_level_structure,
        } = update;
        match kind {
            LifecycleKind::CloseDateUpdated => {
                state.info.close_time_us = close_time_us;
            }
            LifecycleKind::MetadataUpdated => {
                if title.is_some() {
                    state.info.title = title;
                }
            }
            LifecycleKind::PriceStructureUpdated => {
                if let Some(ranges) = price_ranges {
                    state.info.tick_size_us = tick_size_us.or_else(|| min_tick(&ranges));
                    state.info.price_ranges = ranges;
                } else if tick_size_us.is_some() {
                    state.info.tick_size_us = tick_size_us;
                }
                if price_level_structure.is_some() {
                    state.info.price_level_structure = price_level_structure;
                }
            }
            LifecycleKind::Created
            | LifecycleKind::Activated
            | LifecycleKind::NewMarket
            | LifecycleKind::Other(_) => {
                if let Some(close_time_us) = close_time_us {
                    state.info.close_time_us = Some(close_time_us);
                }
                if title.is_some() {
                    state.info.title = title;
                }
                if tick_size_us.is_some() {
                    state.info.tick_size_us = tick_size_us;
                }
                if let Some(ranges) = price_ranges {
                    state.info.tick_size_us = tick_size_us.or_else(|| min_tick(&ranges));
                    state.info.price_ranges = ranges;
                }
                if price_level_structure.is_some() {
                    state.info.price_level_structure = price_level_structure;
                }
            }
            LifecycleKind::Determined
            | LifecycleKind::Settled
            | LifecycleKind::Resolved
            | LifecycleKind::Deactivated => {}
        }
        Vec::new()
    }

    fn handle_stream_reset(
        &mut self,
        source: SourceId,
        native_symbols: Vec<String>,
        reason: ResetReason,
        received_at_us: i64,
    ) -> Vec<OutputMessage> {
        if matches!(
            reason,
            ResetReason::SequenceGap | ResetReason::ProtocolError
        ) {
            self.stats.sequence_resets = self.stats.sequence_resets.saturating_add(1);
        }
        let affected: Vec<String> = if native_symbols.is_empty() {
            self.markets
                .keys()
                .filter(|native| {
                    self.markets
                        .get(*native)
                        .is_some_and(|state| state.shard == source.shard as usize)
                })
                .cloned()
                .collect()
        } else {
            native_symbols
        };

        let quantifiable = self.config.capabilities.has_sequence_clock;
        let mut outputs = Vec::new();
        let mut retry_shadow = Vec::new();
        for native in affected {
            let mut lost_active = false;
            {
                let Some(state) = self.markets.get_mut(&native) else {
                    continue;
                };
                if state.continuity == crate::model::ContinuityState::Terminal {
                    continue;
                }
                if let Some(book) = state.books.get_mut(&source) {
                    book.invalidate();
                    if matches!(reason, ResetReason::Connected) {
                        // A new connection has no cross-epoch clock history.
                        book.last_observed_clock = None;
                    }
                }
                if state.active == Some(source) {
                    state.active = None;
                    let disc_reason = match reason {
                        ResetReason::SequenceGap => DiscontinuityReason::SequenceGap,
                        ResetReason::ProtocolError => DiscontinuityReason::ProtocolError,
                        ResetReason::SubscriptionRefresh | ResetReason::RequestedRefresh => {
                            DiscontinuityReason::StreamReset
                        }
                        ResetReason::Connected => DiscontinuityReason::ActiveSourceLost,
                    };
                    let started = state.start_incident(disc_reason, received_at_us, quantifiable);
                    record_incident(&mut self.stats, started, quantifiable);
                    lost_active = true;
                } else if matches!(reason, ResetReason::ProtocolError) {
                    retry_shadow.push(native.clone());
                }
            }
            if lost_active && !self.shutting_down {
                outputs.extend(self.reseat(&native, received_at_us));
            }
        }
        for native in retry_shadow {
            self.request_recovery(source, &native, false);
        }
        outputs
    }

    /// Apply a status transition. Losing a source immediately invalidates all of
    /// its shadow books and fails over any symbols where it was active.
    ///
    /// `now_us` is the generation time of this transition. Generated failover
    /// snapshots carry it, so their local timestamp never precedes the receive
    /// time of any update included in the replacement book. The lost source's
    /// own frame time stays in its [`SourceStatus`] and is not reused as a
    /// snapshot timestamp.
    pub fn handle_status(&mut self, status: SourceStatus, now_us: i64) -> Vec<OutputMessage> {
        let source = status.source;
        if source.shard as usize >= self.shards.len() {
            return Vec::new();
        }
        let was_connected = self.statuses.get(&source).is_some_and(|old| old.connected);
        self.statuses.insert(source, status.clone());
        if status.connected || !was_connected {
            return Vec::new();
        }
        if self.shutting_down {
            // Shutdown disconnects are an artefact of stopping the workers, not
            // a continuity gap in the captured stream. Record the source state
            // for the run report without opening an incident or changing the
            // books that final snapshots were built from.
            return Vec::new();
        }

        let quantifiable = self.config.capabilities.has_sequence_clock;
        let mut affected = Vec::new();
        for (native, state) in &mut self.markets {
            if state.shard != source.shard as usize {
                continue;
            }
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
                let started = state.start_incident(
                    DiscontinuityReason::ActiveSourceLost,
                    now_us,
                    quantifiable,
                );
                record_incident(&mut self.stats, started, quantifiable);
                affected.push(native.clone());
            }
        }
        let mut outputs = Vec::new();
        for native in affected {
            outputs.extend(self.reseat(&native, now_us));
        }
        outputs
    }

    /// Periodic maintenance: local snapshots, stale probes, and failover.
    pub fn tick(&mut self, now_us: i64) -> Vec<OutputMessage> {
        let statuses = &self.statuses;
        let interval = self.config.snapshot_interval_us;
        let stale_after = self.config.stale_probe_after_us;
        let probe_spacing = self.config.probe_interval_us;
        let max_probes = self.config.max_probes;
        let crossed_grace = self.config.crossed_grace_us;
        let quantifiable = self.config.capabilities.has_sequence_clock;
        let mut outputs = Vec::new();
        let mut probes: Vec<(SourceId, String)> = Vec::new();
        let mut reconnects: Vec<(SourceId, String)> = Vec::new();
        let mut reseats: Vec<String> = Vec::new();
        let mut periodic_count = 0u64;
        let mut crossed_count = 0u64;
        let mut probe_count = 0u64;

        for (native, state) in &mut self.markets {
            if state.continuity == crate::model::ContinuityState::Terminal {
                continue;
            }
            let Some(active) = state.active else {
                if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                    let candidates: Vec<SourceId> = state
                        .books
                        .keys()
                        .copied()
                        .filter(|source| source_healthy(statuses, *source))
                        .collect();
                    if !candidates.is_empty() {
                        for source in candidates {
                            probes.push((source, native.clone()));
                        }
                        state.last_probe_us = now_us;
                    }
                }
                continue;
            };
            let Some(book) = state.books.get(&active).filter(|book| book.valid) else {
                state.active = None;
                reseats.push(native.clone());
                continue;
            };

            if state.continuity == crate::model::ContinuityState::Live
                && now_us >= state.next_snapshot_us
            {
                if book.book.is_crossed() {
                    crossed_count = crossed_count.saturating_add(1);
                    state.next_snapshot_us = now_us.saturating_add(1_000_000);
                    if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                        probes.push((active, native.clone()));
                        state.last_probe_us = now_us;
                    }
                } else {
                    let exchange_ts = book.last_exchange_ts_us.unwrap_or(now_us);
                    outputs.push(OutputMessage::Snapshot(build_snapshot(
                        &state.info,
                        &book.book,
                        now_us,
                        exchange_ts,
                    )));
                    state.emitted_any = true;
                    state.last_emit_us = Some(now_us);
                    state.emitted_exchange_ts_us = Some(exchange_ts);
                    state.next_snapshot_us = now_us.saturating_add(interval);
                    state.crossed_since_us = None;
                    periodic_count = periodic_count.saturating_add(1);
                }
            }

            if let Some(crossed_since) = state.crossed_since_us
                && now_us.saturating_sub(crossed_since) >= crossed_grace
                && now_us.saturating_sub(state.last_probe_us) >= probe_spacing
            {
                probes.push((active, native.clone()));
                state.last_probe_us = now_us;
            }

            let last_activity = book.last_received_at_us.unwrap_or(0);
            if last_activity > 0 && now_us.saturating_sub(last_activity) >= stale_after {
                // Transition Live -> Stale when freshness window exceeded
                if state.continuity == crate::model::ContinuityState::Live {
                    state.set_continuity(crate::model::ContinuityState::Stale);
                }
                if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                    if state.probes < max_probes {
                        probes.push((active, native.clone()));
                        state.probes = state.probes.saturating_add(1);
                        state.last_probe_us = now_us;
                        probe_count = probe_count.saturating_add(1);
                    } else {
                        // Max probes exceeded: Stale -> Resyncing
                        let started = state.start_incident(
                            DiscontinuityReason::StaleProbeEscalation,
                            now_us,
                            quantifiable,
                        );
                        record_incident(&mut self.stats, started, quantifiable);
                        state.probes = 0;
                        state.last_probe_us = now_us;
                        reconnects.push((active, native.clone()));
                        reseats.push(native.clone());
                        probe_count = probe_count.saturating_add(1);
                    }
                }
            } else if last_activity > state.last_probe_us {
                state.probes = 0;
            }
        }

        for (source, native) in probes {
            self.request_recovery(source, &native, false);
        }
        for (source, native) in reconnects {
            self.request_recovery(source, &native, true);
        }
        self.stats.stale_probes = self.stats.stale_probes.saturating_add(probe_count);
        self.stats.periodic_snapshots =
            self.stats.periodic_snapshots.saturating_add(periodic_count);
        self.stats.crossed_snapshots_skipped = self
            .stats
            .crossed_snapshots_skipped
            .saturating_add(crossed_count);
        self.stats.stale_failovers = self
            .stats
            .stale_failovers
            .saturating_add(reseats.len() as u64);
        for native in reseats {
            outputs.extend(self.reseat(&native, now_us));
        }
        self.stats.current_symbols = self.markets.len() as u64;
        // Update continuity state breakdown
        let mut resyncing = 0u64;
        let mut stale = 0u64;
        for state in self.markets.values() {
            match state.continuity {
                crate::model::ContinuityState::Resyncing => resyncing = resyncing.saturating_add(1),
                crate::model::ContinuityState::Stale => stale = stale.saturating_add(1),
                _ => {}
            }
        }
        self.stats.resyncing_symbols = resyncing;
        self.stats.stale_symbols = stale;
        outputs
    }

    /// Emit one final snapshot for every live active book and suppress further
    /// failover activity.
    pub fn finalize(&mut self, now_us: i64) -> Vec<OutputMessage> {
        self.shutting_down = true;
        let mut outputs = Vec::new();
        let mut count = 0u64;
        for state in self.markets.values_mut() {
            let Some(book) = state.active_book() else {
                continue;
            };
            let exchange_ts = book.last_exchange_ts_us.unwrap_or(now_us);
            outputs.push(OutputMessage::Snapshot(build_snapshot(
                &state.info,
                &book.book,
                now_us,
                exchange_ts,
            )));
            state.last_emit_us = Some(now_us);
            count = count.saturating_add(1);
        }
        self.stats.final_snapshots = self.stats.final_snapshots.saturating_add(count);
        outputs
    }

    /// Drain queued subscription and recovery controls.
    pub fn drain_controls(&mut self) -> Vec<(SourceId, Control)> {
        let mut controls = Vec::new();
        let dirty: Vec<usize> = self.dirty_shards.iter().copied().collect();
        for shard_index in dirty {
            let Some(shard) = self.shards.get(shard_index) else {
                continue;
            };
            let symbols = shard.symbols.clone();
            for source in &shard.sources {
                controls.push((*source, Control::SetSubscriptions(symbols.clone())));
            }
        }
        self.dirty_shards.clear();

        for (source, pending) in std::mem::take(&mut self.pending_recovery) {
            if pending.symbols.is_empty() && !pending.reconnect {
                continue;
            }
            controls.push((
                source,
                Control::Recover {
                    generation: pending.generation,
                    symbols: pending.symbols.into_iter().collect(),
                    reconnect: pending.reconnect,
                },
            ));
        }
        controls
    }

    /// Record the worker's acknowledgement of a recovery control.
    ///
    /// The acknowledgement carries the receive fence of the last frame the
    /// worker had read when it applied the control. Only snapshots read after
    /// that point are eligible to repair a clockless gap.
    pub fn handle_recovery_ready(
        &mut self,
        source: SourceId,
        generation: u64,
        connection_epoch: u64,
        receive_seq: u64,
    ) {
        let fence = FrameOrder::new(connection_epoch, receive_seq);
        for state in self.markets.values_mut() {
            if let Some(request) = state.recovery_requests.get_mut(&source)
                && request.generation == generation
            {
                request.fence = Some(fence);
            }
        }
        if self
            .pending_recovery
            .get(&source)
            .is_some_and(|pending| pending.generation == generation)
        {
            self.pending_recovery.remove(&source);
        }
    }

    /// Create an explicit recovery request before a snapshot or reconnect
    /// control is dispatched for `source`.
    fn request_recovery(&mut self, source: SourceId, native: &str, reconnect: bool) {
        let Some(state) = self.markets.get(native) else {
            return;
        };
        if !state.books.contains_key(&source) {
            return;
        }
        let generation = match self.pending_recovery.get(&source) {
            Some(pending) => pending.generation,
            None => {
                let generation = self.next_recovery_generation;
                self.next_recovery_generation = generation.saturating_add(1);
                generation
            }
        };
        self.pending_recovery
            .entry(source)
            .and_modify(|pending| {
                pending.reconnect |= reconnect;
                pending.symbols.insert(native.to_owned());
            })
            .or_insert_with(|| PendingRecovery {
                generation,
                symbols: BTreeSet::from([native.to_owned()]),
                reconnect,
            });
        if let Some(state) = self.markets.get_mut(native) {
            state.recovery_requests.insert(
                source,
                RecoveryRequest {
                    generation,
                    fence: None,
                },
            );
        }
    }

    /// Choose a replacement source with a book at least as new as the last
    /// emitted state. If no replacement is ready, request recovery
    /// from every healthy replica and wait for a fresh exchange snapshot.
    fn reseat(&mut self, native: &str, now_us: i64) -> Vec<OutputMessage> {
        let statuses = &self.statuses;
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        let previous_active = state.active;
        let was_emitted = state.emitted_any;
        let backup = best_backup(state, statuses, self.config.capabilities.has_sequence_clock);
        let Some(backup) = backup else {
            state.active = None;
            let candidates: Vec<(SourceId, bool)> = state
                .books
                .iter()
                .map(|(source, book)| {
                    let observed_behind = match (book.last_observed_clock, state.emitted_clock) {
                        (Some(observed), Some(emitted)) => observed < emitted,
                        _ => false,
                    };
                    // Request recovery even from a source that is currently
                    // disconnected: its worker keeps the request and will
                    // reconnect with a new epoch, making the first snapshot on
                    // that connection strictly newer than the acknowledgement.
                    // A still-valid book that is not an eligible backup is
                    // either behind or crossed. Reconnecting is the only way
                    // to prove a fresh connection epoch. An invalid book can
                    // be repaired by a fenced snapshot request on the current
                    // connection.
                    (*source, book.valid || observed_behind)
                })
                .collect();
            for (source, reconnect) in candidates {
                self.request_recovery(source, native, reconnect);
            }
            return Vec::new();
        };

        let exchange_ts = state.books[&backup].last_exchange_ts_us.unwrap_or(now_us);
        let message = build_snapshot(&state.info, &state.books[&backup].book, now_us, exchange_ts);
        state.active = Some(backup);
        state.emitted_any = true;
        state.emitted_clock = state.books[&backup].last_clock.or(state.emitted_clock);
        state.recovery_requests.clear();
        state.emitted_exchange_ts_us = Some(exchange_ts);
        state.last_emit_us = Some(now_us);
        state.next_snapshot_us = now_us.saturating_add(self.config.snapshot_interval_us);
        state.probes = 0;
        state.last_probe_us = now_us;
        state.crossed_since_us = None;
        // A direct clock-based promotion only exists for venues with an
        // exchange clock. Clockless venues always wait for a fresh snapshot.
        if self.config.capabilities.has_sequence_clock && (was_emitted || previous_active.is_some())
        {
            state.resolve_incident(now_us);
        }
        let cancel_pending = self
            .pending_recovery
            .get_mut(&backup)
            .is_some_and(|pending| {
                pending.symbols.remove(native);
                pending.symbols.is_empty()
            });
        if cancel_pending {
            self.pending_recovery.remove(&backup);
        }
        if was_emitted || previous_active.is_some() {
            self.stats.source_switches = self.stats.source_switches.saturating_add(1);
            self.stats.failover_snapshots = self.stats.failover_snapshots.saturating_add(1);
        } else {
            self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
        }
        vec![OutputMessage::Snapshot(message)]
    }
}

fn clock_regressed(emitted: Option<i64>, incoming: Option<i64>) -> bool {
    matches!((emitted, incoming), (Some(emitted), Some(incoming)) if incoming < emitted)
}

/// Count a discontinuity incident exactly once, when it is first opened.
fn record_incident(stats: &mut CollectorStats, started: bool, quantifiable: bool) {
    if !started {
        return;
    }
    stats.discontinuity_incidents = stats.discontinuity_incidents.saturating_add(1);
    if !quantifiable {
        stats.unquantifiable_gaps = stats.unquantifiable_gaps.saturating_add(1);
    }
}

fn min_tick(ranges: &[crate::model::PriceRange]) -> Option<i64> {
    ranges.iter().map(|range| range.step_us).min()
}

fn source_healthy(statuses: &BTreeMap<SourceId, SourceStatus>, source: SourceId) -> bool {
    statuses.get(&source).is_none_or(|status| status.connected)
}

fn best_backup(
    state: &MarketState,
    statuses: &BTreeMap<SourceId, SourceStatus>,
    has_sequence_clock: bool,
) -> Option<SourceId> {
    // A clockless venue cannot prove that a shadow book is newer than the
    // published state. Promotion always requires a fresh exchange snapshot
    // accepted against an explicit recovery generation.
    if !has_sequence_clock {
        return None;
    }
    let emitted_clock = state.emitted_clock;
    state
        .books
        .iter()
        .filter(|(source, book)| {
            book.valid
                && !book.book.is_crossed()
                && Some(**source) != state.active
                && source_healthy(statuses, **source)
                && match emitted_clock {
                    Some(clock) => book
                        .last_clock
                        .is_some_and(|book_clock| book_clock >= clock),
                    None => true,
                }
        })
        .max_by_key(|(source, book)| {
            let clock = book.last_clock.unwrap_or(i64::MIN);
            let latency = statuses
                .get(*source)
                .and_then(|status| status.latency_ewma_us)
                .unwrap_or(i64::MAX / 2);
            (
                clock,
                std::cmp::Reverse(latency),
                std::cmp::Reverse(**source),
            )
        })
        .map(|(source, _)| *source)
}

fn first_snapshot_deadline(now_us: i64, interval_us: i64, symbol: &str) -> i64 {
    // Spread the first periodic snapshots over the second half of the interval
    // while guaranteeing that no symbol waits longer than one full interval.
    let hash = stable_hash(symbol);
    let fraction = (hash % 5_000) as i64; // 0..4999 => 0..49.99%
    let stagger = interval_us.saturating_mul(fraction) / 10_000;
    now_us
        .saturating_add(interval_us.saturating_sub(stagger))
        .max(now_us.saturating_add(1_000_000))
}

fn stable_hash(text: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn build_snapshot(
    info: &MarketInfo,
    book: &OrderBook,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> SnapshotDomain {
    let received_at = timestamp(received_at_us);
    let exchange_timestamp = timestamp_or(exchange_ts_us, received_at);
    let levels = book.to_snapshot();
    SnapshotDomain {
        received_at,
        exchange_timestamp,
        bids: levels
            .bids
            .into_iter()
            .map(|level| SnapshotBidsEntryDomain {
                price_level: level.price,
                quantity: level.quantity,
            })
            .collect(),
        asks: levels
            .asks
            .into_iter()
            .map(|level| SnapshotAsksEntryDomain {
                price_level: level.price,
                quantity: level.quantity,
            })
            .collect(),
        symbol: info.symbol.clone(),
    }
}

fn build_empty_snapshot(
    info: &MarketInfo,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> SnapshotDomain {
    let received_at = timestamp(received_at_us);
    SnapshotDomain {
        received_at,
        exchange_timestamp: timestamp_or(exchange_ts_us, received_at),
        bids: Vec::new(),
        asks: Vec::new(),
        symbol: info.symbol.clone(),
    }
}

fn build_delta(
    symbol: String,
    side: Side,
    price: i64,
    quantity: i64,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> PriceLevelUpdateDomain {
    debug_assert!((0..=SCALE).contains(&price));
    let backtest_timestamp = timestamp(received_at_us);
    PriceLevelUpdateDomain {
        backtest_timestamp,
        exchange_timestamp: timestamp_or(exchange_ts_us, backtest_timestamp),
        price_level: price,
        quantity,
        side,
        symbol,
    }
}

/// Convert a validated timestamp for SBE serialization.
///
/// Timestamps must be range-validated at adapter or replay boundaries before
/// they reach the core. The epoch fallback exists only so a conversion failure
/// can never inject wall-clock time into deterministic (replayed) output; it
/// is unreachable for valid inputs.
fn timestamp(timestamp_us: i64) -> DateTime<Utc> {
    utc_from_us(timestamp_us).unwrap_or(DateTime::UNIX_EPOCH)
}

fn timestamp_or(timestamp_us: i64, fallback: DateTime<Utc>) -> DateTime<Utc> {
    utc_from_us(timestamp_us).unwrap_or(fallback)
}

#[cfg(test)]
mod tests;
