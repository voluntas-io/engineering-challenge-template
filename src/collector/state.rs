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
mod tests {
    use super::*;
    use crate::config::ExchangeKind;
    use crate::metrics::{
        CatalogHealth, MetricsRegistry, ReadinessPolicy, RuntimeStatus, WriterStats,
    };
    use crate::model::{
        ContinuityState, Control, DiscontinuityReason, EventMeta, ExchangeEvent, Level, MarketInfo,
        ResetReason, SourceStatus,
    };
    use crate::time::now_us;

    const NOW: i64 = 1_700_000_000_000_000;
    const SOURCE_A: SourceId = SourceId::new(0, 0);
    const SOURCE_B: SourceId = SourceId::new(0, 1);

    fn settings_for(exchange: ExchangeKind) -> Settings {
        let mut settings = Settings::for_exchange(exchange);
        settings.collector.shards = 1;
        settings.collector.replicas = 2;
        settings.collector.snapshot_interval_secs = 60;
        settings.collector.stale_probe_after_secs = 120;
        settings.collector.probe_interval_secs = 30;
        settings.collector.max_probes = 3;
        settings
    }

    fn core() -> CollectorCore {
        CollectorCore::new(
            CoreConfig::from_settings(
                &settings_for(ExchangeKind::Polymarket),
                1,
                ExchangeCapabilities::new(false),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn clocked_core() -> CollectorCore {
        CollectorCore::new(
            CoreConfig::from_settings(
                &settings_for(ExchangeKind::Kalshi),
                1,
                ExchangeCapabilities::new(true),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn capabilities_are_not_derived_from_the_venue_name() {
        let unclocked_kalshi = CoreConfig::from_settings(
            &settings_for(ExchangeKind::Kalshi),
            1,
            ExchangeCapabilities::new(false),
        )
        .unwrap();
        assert!(!unclocked_kalshi.capabilities.has_sequence_clock);

        let clocked_polymarket = CoreConfig::from_settings(
            &settings_for(ExchangeKind::Polymarket),
            1,
            ExchangeCapabilities::new(true),
        )
        .unwrap();
        assert!(clocked_polymarket.capabilities.has_sequence_clock);
    }

    fn market(native: &str) -> MarketInfo {
        MarketInfo {
            exchange: "test".to_owned(),
            native_symbol: native.to_owned(),
            symbol: native.to_owned(),
            title: Some(native.to_owned()),
            outcome: None,
            exchange_market_id: None,
            close_time_us: None,
            tick_size_us: Some(10_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
        }
    }

    fn book(bid: i64, ask: i64) -> BookSnapshot {
        BookSnapshot {
            bids: vec![Level {
                price: bid,
                quantity: 10_000_000,
            }],
            asks: vec![Level {
                price: ask,
                quantity: 5_000_000,
            }],
        }
    }

    fn snapshot_event_fenced(
        native: &str,
        bid: i64,
        ask: i64,
        clock: Option<i64>,
        received_at_us: i64,
        connection_epoch: u64,
        receive_seq: u64,
    ) -> ExchangeEvent {
        ExchangeEvent::Snapshot {
            native_symbol: native.to_owned(),
            book: book(bid, ask),
            meta: EventMeta {
                received_at_us,
                exchange_ts_us: Some(received_at_us - 1_000),
                seq: Some(1),
                clock,
                connection_epoch,
                receive_seq,
            },
        }
    }

    fn snapshot_event_at(
        native: &str,
        bid: i64,
        ask: i64,
        clock: i64,
        received_at_us: i64,
    ) -> ExchangeEvent {
        snapshot_event_fenced(
            native,
            bid,
            ask,
            (clock != 0).then_some(clock),
            received_at_us,
            0,
            0,
        )
    }

    fn snapshot_event(native: &str, bid: i64, ask: i64, clock: i64) -> ExchangeEvent {
        snapshot_event_at(native, bid, ask, clock, NOW)
    }

    #[allow(clippy::too_many_arguments)]
    fn delta_event_fenced(
        native: &str,
        side: Side,
        price: i64,
        quantity: i64,
        clock: Option<i64>,
        received_at_us: i64,
        connection_epoch: u64,
        receive_seq: u64,
    ) -> ExchangeEvent {
        ExchangeEvent::LevelChange {
            native_symbol: native.to_owned(),
            side,
            price,
            change: QuantityChange::Absolute(quantity),
            meta: EventMeta {
                received_at_us,
                exchange_ts_us: Some(received_at_us - 1_000),
                seq: Some(2),
                clock,
                connection_epoch,
                receive_seq,
            },
        }
    }

    fn delta_event_at(
        native: &str,
        side: Side,
        price: i64,
        quantity: i64,
        clock: i64,
        received_at_us: i64,
    ) -> ExchangeEvent {
        delta_event_fenced(
            native,
            side,
            price,
            quantity,
            (clock != 0).then_some(clock),
            received_at_us,
            0,
            0,
        )
    }

    fn delta_event(
        native: &str,
        side: Side,
        price: i64,
        quantity: i64,
        received_at_us: i64,
    ) -> ExchangeEvent {
        delta_event_at(native, side, price, quantity, 0, received_at_us)
    }

    fn received_at(message: &OutputMessage) -> i64 {
        match message {
            OutputMessage::Snapshot(snapshot) => snapshot.received_at.timestamp_micros(),
            OutputMessage::PriceLevelUpdate(update) => update.backtest_timestamp.timestamp_micros(),
        }
    }

    fn connected_status(source: SourceId, connected: bool) -> SourceStatus {
        SourceStatus {
            source,
            connected,
            ..SourceStatus::new(source)
        }
    }

    fn connect_sources(core: &mut CollectorCore) {
        for source in [SOURCE_A, SOURCE_B] {
            core.handle_status(connected_status(source, true), NOW);
        }
    }

    fn discover_one(core: &mut CollectorCore) {
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
    }

    fn recovery_control(
        core: &mut CollectorCore,
        expected_source: SourceId,
    ) -> (u64, Vec<String>, bool) {
        core.drain_controls()
            .into_iter()
            .find_map(|(source, control)| match control {
                Control::Recover {
                    generation,
                    symbols,
                    reconnect,
                } if source == expected_source => Some((generation, symbols, reconnect)),
                _ => None,
            })
            .expect("expected a recovery control")
    }

    fn readiness(core: &CollectorCore) -> crate::metrics::ReadinessReport {
        let current = now_us().unwrap();
        let runtime = RuntimeStatus {
            coverage: core.coverage(NOW + 10, 120_000_000),
            catalog: CatalogHealth {
                last_discovery_finished_at_us: Some(current),
                ..CatalogHealth::default()
            },
            writer_healthy: true,
            ..RuntimeStatus::default()
        };
        let registry = MetricsRegistry::new("test", ReadinessPolicy::default());
        registry.update(
            core.stats(),
            WriterStats::default(),
            core.statuses(),
            runtime,
        );
        registry.readiness()
    }

    #[test]
    fn discovery_adds_and_removes_with_controls() {
        let mut core = core();
        let outputs = core
            .apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        assert!(outputs.is_empty());
        assert_eq!(core.symbol_count(), 2);
        assert_eq!(core.stats().added_symbols, 2);
        let controls = core.drain_controls();
        assert_eq!(controls.len(), 2);
        assert!(matches!(
            &controls[0].1,
            Control::SetSubscriptions(symbols) if symbols.len() == 2
        ));

        let outputs = core.apply_discovery(NOW + 1, vec![market("A")]).unwrap();
        assert_eq!(core.symbol_count(), 1);
        assert!(outputs.is_empty());
        let controls = core.drain_controls();
        assert!(controls.iter().any(|(_, control)| matches!(
            control,
            Control::SetSubscriptions(symbols) if symbols.len() == 1
        )));
    }

    #[test]
    fn coverage_separates_sent_subscriptions_from_anchored_books() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        core.drain_controls();
        for source in [SOURCE_A, SOURCE_B] {
            let mut status = SourceStatus::new(source);
            status.connected = true;
            status.subscriptions = 2;
            core.handle_status(status, NOW);
        }

        let coverage = core.coverage(NOW, 1_000_000);
        assert_eq!(coverage.expected_symbols, 2);
        assert_eq!(coverage.anchored_symbols, 0);
        assert_eq!(coverage.expected_subscriptions, 4);
        assert_eq!(coverage.subscribed_symbols, 4);
        assert_eq!(coverage.shards_with_connected_source, 1);
        assert_eq!(coverage.sources_connected, 2);
        assert_eq!(coverage.sources_total, 2);
        assert_eq!(coverage.oldest_anchor_us, None);

        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        assert_eq!(outputs.len(), 1);
        let coverage = core.coverage(NOW, 1_000_000);
        assert_eq!(coverage.anchored_symbols, 1);
        assert_eq!(coverage.stale_symbols, 0);
        assert_eq!(coverage.oldest_anchor_us, Some(NOW));

        // An anchored book that has not received a valid update within the
        // freshness window is counted as stale, not silently healthy.
        let coverage = core.coverage(NOW + 2_000_000, 1_000_000);
        assert_eq!(coverage.anchored_symbols, 1);
        assert_eq!(coverage.stale_symbols, 1);
    }

    #[test]
    fn snapshot_anchors_delta_application_in_per_symbol_order() {
        let mut core = core();
        discover_one(&mut core);
        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW);
        assert_eq!(snapshot.bids[0].price_level, 400_000);
        assert_eq!(snapshot.asks[0].price_level, 600_000);

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Delta(2_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 1,
                    exchange_ts_us: Some(NOW),
                    seq: Some(2),
                    clock: None,
                    connection_epoch: 0,
                    receive_seq: 0,
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected delta");
        };
        assert_eq!(update.quantity, 12_000_000);
        assert_eq!(update.price_level, 400_000);
        // The writer must not clamp or rewrite the receive timestamp.
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 1);

        // A later delta applies to the state produced by the snapshot plus the
        // first delta, in processing order, without re-emitting a snapshot.
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Absolute(9_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 2,
                    exchange_ts_us: Some(NOW + 1),
                    seq: Some(3),
                    clock: None,
                    connection_epoch: 0,
                    receive_seq: 0,
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected delta");
        };
        assert_eq!(update.quantity, 9_000_000);
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 2);
        assert_eq!(core.stats().deltas_emitted, 2);
        assert_eq!(core.stats().exchange_snapshots, 1);
    }

    #[test]
    fn clocked_failover_anchors_resumed_deltas_and_preserves_receive_times() {
        let mut core = clocked_core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        assert_eq!(received_at(&outputs[0]), NOW);

        // The shadow applies its own updates without emitting. Its clock is
        // current when the active source is lost.
        assert!(
            core.handle_event(
                SOURCE_B,
                snapshot_event_at("A", 405_000, 595_000, 11, NOW + 100)
            )
            .is_empty()
        );
        assert!(
            core.handle_event(
                SOURCE_B,
                delta_event_at("A", Side::Buy, 405_000, 11_000_000, 12, NOW + 200)
            )
            .is_empty()
        );

        core.handle_status(connected_status(SOURCE_A, true), NOW + 250);
        let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 300);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 300);
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_micros(),
            NOW + 200 - 1_000
        );
        assert_eq!(snapshot.bids[0].price_level, 405_000);
        assert_eq!(snapshot.bids[0].quantity, 11_000_000);

        let outputs = core.handle_event(
            SOURCE_B,
            delta_event_at("A", Side::Sell, 595_000, 4_000_000, 13, NOW + 400),
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected resumed delta");
        };
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 400);
        assert_eq!(update.quantity, 4_000_000);
    }

    #[test]
    fn generated_snapshots_follow_the_supplied_wall_clock_across_a_step() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

        let outputs = core.tick(NOW + 61_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 61_000_000);

        // A backward wall-clock step must not emit early: the stored deadline
        // is still ahead of the stepped time.
        assert!(core.tick(NOW + 30_000_000).is_empty());

        let outputs = core.tick(NOW + 122_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 122_000_000);
        assert_eq!(core.stats().periodic_snapshots, 2);
    }

    #[test]
    fn cross_symbol_output_order_is_processing_order_and_not_globally_sorted() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        core.drain_controls();

        let first = core.handle_event(
            SOURCE_A,
            snapshot_event_at("A", 400_000, 600_000, 0, NOW + 20),
        );
        let second = core.handle_event(
            SOURCE_A,
            snapshot_event_at("B", 410_000, 590_000, 0, NOW + 10),
        );
        let first_at = received_at(&first[0]);
        let second_at = received_at(&second[0]);
        assert_eq!((first_at, second_at), (NOW + 20, NOW + 10));
        assert!(
            first_at > second_at,
            "the test must demonstrate a cross-symbol timestamp regression"
        );
    }

    #[test]
    fn clocked_shadow_replica_takes_over_after_source_loss() {
        let mut core = clocked_core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 12));

        let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        assert_eq!(snapshot.bids[0].price_level, 410_000);
        assert_eq!(core.stats().source_switches, 1);
    }

    #[test]
    fn shadow_snapshot_does_not_reset_active_probe_state() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

        let mut shadow = snapshot_event("A", 405_000, 595_000, 0);
        if let ExchangeEvent::Snapshot { meta, .. } = &mut shadow {
            meta.received_at_us = NOW + 100;
        }
        core.handle_event(SOURCE_B, shadow);

        let state = core.markets.get("A").unwrap();
        assert_eq!(state.probes, 0);
        assert_eq!(state.last_probe_us, NOW);
    }

    #[test]
    fn removal_emits_empty_snapshot() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        let outputs = core.apply_discovery(NOW + 10, Vec::new()).unwrap();
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected removal snapshot");
        };
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(core.stats().empty_snapshots, 1);
    }

    #[test]
    fn periodic_snapshot_is_spread_and_emitted() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        let outputs = core.tick(NOW + 61_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert_eq!(core.stats().periodic_snapshots, 1);
    }

    #[test]
    fn unacknowledged_shadow_snapshot_does_not_emit_after_source_loss() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert!(outputs.is_empty());

        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));
        assert!(outputs.is_empty());
        assert!(core.markets["A"].active.is_none());
    }

    #[test]
    fn maintenance_clock_regression_starts_a_new_epoch() {
        let mut core = clocked_core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 110));

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Delta(1_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 1,
                    exchange_ts_us: Some(NOW),
                    seq: Some(2),
                    clock: Some(50),
                    connection_epoch: 0,
                    receive_seq: 0,
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        assert_eq!(core.stats().clock_resets, 1);
        assert_eq!(core.markets["A"].emitted_clock, Some(50));
        assert!(!core.markets["A"].books[&SOURCE_B].valid);

        // A lower-clock shadow snapshot is stale in the new epoch and must not
        // replace the active book.
        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 40));
        assert!(outputs.is_empty());
        assert!(!core.markets["A"].books[&SOURCE_B].valid);

        // Once the shadow catches up to the new epoch it can be a backup again.
        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 60));
        assert!(outputs.is_empty());
        assert!(core.markets["A"].books[&SOURCE_B].valid);
    }

    #[test]
    fn requested_recovery_snapshot_after_reconnect_accepts_new_lower_epoch() {
        let mut core = clocked_core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
        // B is stale and its book is rejected, leaving an observed clock behind.
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 90));
        // A fresh connection clears that cross-epoch history.
        core.handle_event(
            SOURCE_B,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::Connected,
                received_at_us: NOW + 1,
            },
        );
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert!(outputs.is_empty());

        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 50));
        assert_eq!(outputs.len(), 1);
        assert_eq!(core.stats().clock_resets, 1);
        assert_eq!(core.markets["A"].active, Some(SOURCE_B));
        assert_eq!(core.markets["A"].emitted_clock, Some(50));
    }

    #[test]
    fn lifecycle_price_structure_updates_market_metadata() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::Lifecycle {
                native_symbol: "A".to_owned(),
                kind: LifecycleKind::PriceStructureUpdated,
                close_time_us: None,
                title: None,
                tick_size_us: Some(1_000),
                price_ranges: Some(vec![
                    crate::model::PriceRange {
                        start_us: 0,
                        end_us: 100_000,
                        step_us: 1_000,
                    },
                    crate::model::PriceRange {
                        start_us: 100_000,
                        end_us: 900_000,
                        step_us: 10_000,
                    },
                ]),
                price_level_structure: Some("tapered_deci_cent".to_owned()),
                meta: EventMeta {
                    received_at_us: NOW,
                    exchange_ts_us: None,
                    seq: Some(2),
                    clock: None,
                    connection_epoch: 0,
                    receive_seq: 0,
                },
            },
        );
        let info = &core.market_infos()[0];
        assert_eq!(info.tick_size_us, Some(1_000));
        assert_eq!(info.price_ranges.len(), 2);
        assert_eq!(
            info.price_level_structure.as_deref(),
            Some("tapered_deci_cent")
        );
    }

    #[test]
    fn invalid_discovery_is_rejected_without_removing_current_symbols() {
        let mut core = core();
        discover_one(&mut core);
        let duplicate = core
            .apply_discovery(NOW + 1, vec![market("A"), market("A")])
            .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate"));
        assert_eq!(core.symbol_count(), 1);

        let mut invalid = market("B");
        invalid.symbol.clear();
        assert!(
            core.apply_discovery(NOW + 2, vec![market("A"), invalid])
                .is_err()
        );
        assert_eq!(core.symbol_count(), 1);
        assert!(core.markets.contains_key("A"));
    }

    #[test]
    fn duplicate_output_symbols_are_rejected() {
        let mut core = core();
        discover_one(&mut core);
        let mut impostor = market("B");
        impostor.symbol = "A".to_owned();
        let error = core
            .apply_discovery(NOW + 1, vec![market("A"), impostor])
            .unwrap_err();
        assert!(error.to_string().contains("duplicate output symbol"));
        assert_eq!(core.symbol_count(), 1);
    }

    #[test]
    fn rediscovery_with_changed_metadata_keeps_symbol_stable() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

        let mut renamed = market("A");
        renamed.title = Some("renamed".to_owned());
        let outputs = core.apply_discovery(NOW + 1, vec![renamed]).unwrap();
        assert!(outputs.is_empty());
        assert_eq!(core.markets["A"].info.title.as_deref(), Some("renamed"));
        assert_eq!(core.markets["A"].info.symbol, "A");
        assert!(core.markets["A"].emitted_any);
    }

    #[test]
    fn identity_change_retires_old_symbol_and_reseeds_state() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

        let mut migrated = market("A");
        migrated.symbol = "A-v2".to_owned();
        let outputs = core.apply_discovery(NOW + 1, vec![migrated]).unwrap();
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected retirement snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(core.stats().empty_snapshots, 1);

        assert_eq!(core.markets["A"].info.symbol, "A-v2");
        assert!(!core.markets["A"].emitted_any);
        assert!(core.markets["A"].active.is_none());

        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 410_000, 590_000, 0));
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected fresh snapshot under the new symbol");
        };
        assert_eq!(snapshot.symbol, "A-v2");
    }

    #[test]
    fn clocked_sequence_reset_fails_over_to_valid_shadow() {
        let mut core = clocked_core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_status(connected_status(SOURCE_B, true), NOW);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 11));
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        assert_eq!(snapshot.bids[0].price_level, 405_000);
        assert_eq!(core.stats().sequence_resets, 1);
    }

    #[test]
    fn source_disconnect_opens_one_unquantifiable_incident_and_blocks_deltas() {
        let mut core = core();
        assert!(!core.config.capabilities.has_sequence_clock);
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));

        let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        assert!(outputs.is_empty());
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        let incident = core.markets["A"].incident.as_ref().unwrap();
        assert_eq!(incident.reason, DiscontinuityReason::ActiveSourceLost);
        assert!(!incident.quantifiable);
        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, 1);

        let delta = core.handle_event(
            SOURCE_B,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 2),
        );
        assert!(delta.is_empty(), "deltas must not apply while Resyncing");
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    }

    #[test]
    fn clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);

        let initial = core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        assert_eq!(initial.len(), 1);
        assert!(
            core.handle_event(
                SOURCE_B,
                snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2)
            )
            .is_empty()
        );
        assert_eq!(core.markets["A"].active, Some(SOURCE_A));

        // Active source loss: no shadow may be promoted before a new recovery
        // control and its acknowledgement.
        let lost = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        assert!(lost.is_empty());
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, 1);

        let (generation, symbols, reconnect) = recovery_control(&mut core, SOURCE_B);
        assert_eq!(symbols, vec!["A".to_owned()]);
        assert!(reconnect, "a valid shadow must reconnect before promotion");

        // Before the worker acknowledges the control, even a newer connection
        // epoch cannot be promoted.
        let before_ack = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 2, 2, 1),
        );
        assert!(before_ack.is_empty());
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        assert!(core.markets["A"].active.is_none());
        assert_eq!(core.stats().rejected_recovery_snapshots, 1);

        core.handle_recovery_ready(SOURCE_B, generation, 1, 7);

        // Equal to the acknowledgement fence is not newer.
        let equal_fence = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 3, 1, 7),
        );
        assert!(equal_fence.is_empty());
        assert_eq!(core.stats().rejected_recovery_snapshots, 2);

        // The production reconnect path resets the connection and resubscribes.
        core.handle_event(
            SOURCE_B,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::Connected,
                received_at_us: NOW + 4,
            },
        );
        let recovered = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 425_000, 575_000, None, NOW + 10, 2, 1),
        );
        assert_eq!(recovered.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &recovered[0] else {
            panic!("expected replacement snapshot");
        };
        assert_eq!(snapshot.bids[0].price_level, 425_000);
        assert_eq!(core.markets["A"].active, Some(SOURCE_B));
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
        assert!(core.markets["A"].incident.is_none());
        assert_eq!(core.markets["A"].resolved_incidents.len(), 1);
        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, 1);
        assert_eq!(core.stats().rejected_recovery_snapshots, 2);
        assert_eq!(core.stats().failover_snapshots, 1);
        assert_eq!(core.stats().source_switches, 1);
        assert!(core.drain_controls().is_empty());

        let delta = core.handle_event(
            SOURCE_B,
            delta_event_fenced("A", Side::Buy, 425_000, 12_000_000, None, NOW + 11, 2, 2),
        );
        assert_eq!(delta.len(), 1);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    }

    #[test]
    fn disconnected_replica_reconnect_is_fenced_by_a_requested_recovery() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
        );

        assert!(
            core.handle_status(connected_status(SOURCE_A, false), NOW + 2)
                .is_empty()
        );
        let controls = core.drain_controls();
        let (generation, reconnect) = controls
            .iter()
            .find_map(|(source, control)| match control {
                Control::Recover {
                    generation,
                    reconnect,
                    ..
                } if *source == SOURCE_A => Some((*generation, *reconnect)),
                _ => None,
            })
            .expect("the lost source must receive a recovery request before it reconnects");
        assert!(
            !reconnect,
            "an invalidated book is repaired by a fenced snapshot request"
        );
        assert!(controls.iter().any(|(source, control)| {
            *source == SOURCE_B
                && matches!(
                    control,
                    Control::Recover {
                        reconnect: true,
                        ..
                    }
                )
        }));

        // The worker acknowledges while backing off, then opens a new
        // connection. That connection's first snapshot must be eligible even
        // though the source was disconnected when the incident started.
        core.handle_recovery_ready(SOURCE_A, generation, 1, 5);
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::Connected,
                received_at_us: NOW + 3,
            },
        );
        let recovered = core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 425_000, 575_000, None, NOW + 10, 2, 1),
        );
        assert_eq!(recovered.len(), 1);
        assert_eq!(core.markets["A"].active, Some(SOURCE_A));
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
        assert!(core.markets["A"].incident.is_none());
        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, 1);
    }

    #[test]
    fn recovery_snapshot_before_ack_is_rejected_without_mutation() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2),
        );
        core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        let (_generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);

        let before_book = core.markets["A"].books[&SOURCE_B].book.clone();
        let before_valid = core.markets["A"].books[&SOURCE_B].valid;
        let before_received = core.markets["A"].books[&SOURCE_B].last_received_at_us;
        let before_exchange = core.markets["A"].books[&SOURCE_B].last_exchange_ts_us;
        let before_incident = core.markets["A"].incident.clone();
        let before_count = core.stats().discontinuity_incidents;
        let before_unquantifiable = core.stats().unquantifiable_gaps;

        let rejected = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 2, 2, 1),
        );
        assert!(rejected.is_empty());
        assert_eq!(core.markets["A"].books[&SOURCE_B].book, before_book);
        assert_eq!(core.markets["A"].books[&SOURCE_B].valid, before_valid);
        assert_eq!(
            core.markets["A"].books[&SOURCE_B].last_received_at_us,
            before_received
        );
        assert_eq!(
            core.markets["A"].books[&SOURCE_B].last_exchange_ts_us,
            before_exchange
        );
        assert_eq!(core.markets["A"].incident, before_incident);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        assert!(core.markets["A"].active.is_none());
        assert_eq!(core.stats().discontinuity_incidents, before_count);
        assert_eq!(core.stats().unquantifiable_gaps, before_unquantifiable);
        assert_eq!(core.stats().rejected_recovery_snapshots, 1);
    }

    #[test]
    fn delayed_pre_incident_frame_is_rejected_by_receive_fence() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2),
        );
        core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        let (generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);
        core.handle_recovery_ready(SOURCE_B, generation, 1, 5);

        let before_book = core.markets["A"].books[&SOURCE_B].book.clone();
        let before_received = core.markets["A"].books[&SOURCE_B].last_received_at_us;

        // The frame was read before the recovery acknowledgement but is
        // processed afterwards. Its wall-clock receive time is before the
        // incident, and its receive sequence is behind the fence.
        let delayed = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 430_000, 570_000, None, NOW, 1, 3),
        );
        assert!(delayed.is_empty());
        assert_eq!(core.markets["A"].books[&SOURCE_B].book, before_book);
        assert_eq!(
            core.markets["A"].books[&SOURCE_B].last_received_at_us,
            before_received
        );
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        assert_eq!(core.stats().rejected_recovery_snapshots, 1);

        // A frame read after the acknowledgement repairs the gap.
        let accepted = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 430_000, 570_000, None, NOW + 10, 1, 6),
        );
        assert_eq!(accepted.len(), 1);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    }

    #[test]
    fn shadow_snapshot_does_not_consume_recovery_token() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
        );

        core.handle_event(
            SOURCE_B,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::ProtocolError,
                received_at_us: NOW + 2,
            },
        );
        let (generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);
        core.handle_recovery_ready(SOURCE_B, generation, 1, 3);

        // A fresh shadow snapshot is applied but must not resolve the active
        // symbol's incident or consume the token needed for a later failover.
        let outputs = core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 3, 1, 4),
        );
        assert!(outputs.is_empty());
        assert_eq!(core.markets["A"].active, Some(SOURCE_A));
        assert!(core.markets["A"].recovery_requests.contains_key(&SOURCE_B));
        assert!(core.markets["A"].incident.is_none());
    }

    #[test]
    fn deltas_cannot_restore_live_in_any_uncertain_state() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);

        // Unseeded: no snapshot has anchored a book.
        assert!(
            core.handle_event(
                SOURCE_A,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW)
            )
            .is_empty()
        );

        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
        );

        // Stale: both active and shadow deltas are ignored.
        core.markets
            .get_mut("A")
            .unwrap()
            .set_continuity(ContinuityState::Stale);
        assert!(
            core.handle_event(
                SOURCE_A,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 2)
            )
            .is_empty()
        );
        assert!(
            core.handle_event(
                SOURCE_B,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 3)
            )
            .is_empty()
        );

        // Resyncing: the active source reconnects/resubscribes; deltas from
        // either source cannot re-anchor the book.
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 4,
            },
        );
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
        assert!(
            core.handle_event(
                SOURCE_A,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 5)
            )
            .is_empty()
        );
        assert!(
            core.handle_event(
                SOURCE_B,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 6)
            )
            .is_empty()
        );

        // Terminal: the retired identity is never reopened.
        core.markets
            .get_mut("A")
            .unwrap()
            .set_continuity(ContinuityState::Terminal);
        assert!(
            core.handle_event(
                SOURCE_A,
                delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 7)
            )
            .is_empty()
        );
        assert!(
            core.handle_event(
                SOURCE_A,
                snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 8, 1, 7)
            )
            .is_empty()
        );
        assert_eq!(core.markets["A"].continuity, ContinuityState::Terminal);
    }

    #[test]
    fn fresh_active_repair_requires_acknowledged_recovery_fence() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_status(connected_status(SOURCE_A, true), NOW);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
        );
        core.config.stale_probe_after_us = 1_000_000;
        core.config.probe_interval_us = 1_000_000;
        core.markets.get_mut("A").unwrap().next_snapshot_us = NOW + 1_000_000_000;

        assert!(core.tick(NOW + 2_000_000).is_empty());
        assert_eq!(core.markets["A"].continuity, ContinuityState::Stale);
        let (generation, symbols, reconnect) = recovery_control(&mut core, SOURCE_A);
        assert_eq!(symbols, vec!["A".to_owned()]);
        assert!(!reconnect);

        let before_ack = core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 2_000_001, 1, 2),
        );
        assert!(before_ack.is_empty());
        assert_eq!(core.markets["A"].continuity, ContinuityState::Stale);

        core.handle_recovery_ready(SOURCE_A, generation, 1, 1);
        let repaired = core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 2_000_002, 1, 2),
        );
        assert_eq!(repaired.len(), 1);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
        assert_eq!(core.markets["A"].active, Some(SOURCE_A));
        assert!(core.markets["A"].incident.is_none());
        assert_eq!(core.stats().discontinuity_incidents, 0);
    }

    #[test]
    fn repeated_failure_events_do_not_double_count_incident() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

        core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        core.handle_status(connected_status(SOURCE_A, false), NOW + 2);
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::ProtocolError,
                received_at_us: NOW + 3,
            },
        );
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 4,
            },
        );

        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, 1);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    }

    #[test]
    fn incident_quantifiability_follows_venue_semantics() {
        for (mut core, quantifiable) in [(core(), false), (clocked_core(), true)] {
            discover_one(&mut core);
            connect_sources(&mut core);
            let clock = quantifiable.then_some(10);
            core.handle_event(
                SOURCE_A,
                snapshot_event_fenced("A", 400_000, 600_000, clock, NOW, 1, 1),
            );
            core.handle_status(connected_status(SOURCE_A, false), NOW + 1);

            let incident = core.markets["A"].incident.as_ref().unwrap();
            assert_eq!(incident.quantifiable, quantifiable);
            assert_eq!(core.stats().discontinuity_incidents, 1);
            assert_eq!(core.stats().unquantifiable_gaps, u64::from(!quantifiable));
        }
    }

    #[test]
    fn readiness_fails_for_each_blocking_continuity_state() {
        let mut unseeded = core();
        unseeded.apply_discovery(NOW, vec![market("A")]).unwrap();
        unseeded.drain_controls();
        connect_sources(&mut unseeded);
        let report = readiness(&unseeded);
        assert!(!report.ready);
        assert!(!report.checks["unseeded"].ok);
        assert!(report.checks["unseeded"].detail.contains('1'));

        let mut stale = core();
        discover_one(&mut stale);
        connect_sources(&mut stale);
        stale.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        stale
            .markets
            .get_mut("A")
            .unwrap()
            .set_continuity(ContinuityState::Stale);
        let report = readiness(&stale);
        assert!(!report.ready);
        assert!(!report.checks["stale_detail"].ok);

        let mut resyncing = core();
        discover_one(&mut resyncing);
        connect_sources(&mut resyncing);
        resyncing.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        resyncing
            .markets
            .get_mut("A")
            .unwrap()
            .set_continuity(ContinuityState::Resyncing);
        let report = readiness(&resyncing);
        assert!(!report.ready);
        assert!(!report.checks["resyncing"].ok);

        let mut terminal = core();
        discover_one(&mut terminal);
        connect_sources(&mut terminal);
        terminal.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        terminal
            .markets
            .get_mut("A")
            .unwrap()
            .set_continuity(ContinuityState::Terminal);
        let report = readiness(&terminal);
        assert!(!report.ready);
        assert!(!report.checks["terminal"].ok);
    }

    #[test]
    fn shutdown_disconnect_is_not_a_discontinuity_incident() {
        let mut core = core();
        discover_one(&mut core);
        connect_sources(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        assert_eq!(core.stats().discontinuity_incidents, 0);

        core.begin_shutdown();
        let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
        assert!(outputs.is_empty());
        assert_eq!(core.stats().discontinuity_incidents, 0);
        assert_eq!(core.stats().unquantifiable_gaps, 0);
        assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
        assert_eq!(core.markets["A"].active, Some(SOURCE_A));
        assert!(!core.statuses()[&SOURCE_A].connected);
        assert!(core.drain_controls().is_empty());
    }

    #[test]
    fn terminal_lifecycle_removes_market_without_discontinuity_incident() {
        let mut core = core();
        discover_one(&mut core);
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
        let incident_count_before = core.stats().discontinuity_incidents;

        core.handle_event(
            SOURCE_A,
            ExchangeEvent::Lifecycle {
                native_symbol: "A".to_owned(),
                kind: LifecycleKind::Resolved,
                close_time_us: Some(NOW + 1),
                title: None,
                tick_size_us: None,
                price_ranges: None,
                price_level_structure: None,
                meta: EventMeta::new(NOW + 1),
            },
        );

        assert!(!core.markets.contains_key("A"));
        assert_eq!(core.stats().discontinuity_incidents, incident_count_before);
    }
}
