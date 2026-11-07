//! Startup capacity planning for one discovered universe.
//!
//! The plan is computed before any source worker starts. It converts the
//! selected venue records and outcome symbols into the resources the run will
//! use and rejects a configuration that exceeds a declared ceiling. The
//! estimates are conservative fixed costs, recorded in
//! `docs/operating-envelope.md`; they are planning bounds, not a substitute for
//! the measured scale runs.

use std::collections::BTreeSet;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::{config::Settings, model::MarketInfo};

/// Planning estimate for one symbol's resident book state: a bounded two-sided
/// book, per-symbol metadata, and the collector's shadow copy.
pub const ESTIMATED_BYTES_PER_BOOK: u64 = 4 << 10;

/// Planning estimate for one encoded full snapshot at the default book depth.
pub const ESTIMATED_BYTES_PER_SNAPSHOT: u64 = 8 << 10;

/// JSON envelope around one initial subscription frame.
pub const SUBSCRIPTION_FRAME_OVERHEAD: u64 = 128;

/// The resources implied by a discovered universe and the configured limits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapacityPlan {
    /// Distinct venue market records selected by discovery.
    pub selected_markets: usize,
    /// Outcome tokens, or one symbol per market for venues that publish one.
    pub outcome_symbols: usize,
    pub shards: usize,
    pub replicas: usize,
    pub total_connections: usize,
    pub max_total_connections: usize,
    pub max_symbols_per_socket: usize,
    pub initial_subscription_bytes: u64,
    pub max_initial_subscription_bytes: usize,
    pub estimated_book_memory_bytes: u64,
    pub max_estimated_book_memory_bytes: u64,
    pub estimated_snapshot_bytes_per_sec: u64,
    pub max_snapshot_bytes_per_sec: u64,
    pub snapshot_interval_secs: u64,
}

impl CapacityPlan {
    /// Human-readable one-line summary used in startup logs.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} venue markets -> {} outcome symbols, {} shards x {} replicas = {} sockets, \
             ~{} B initial subscription, ~{} MiB books, ~{} B/s snapshots",
            self.selected_markets,
            self.outcome_symbols,
            self.shards,
            self.replicas,
            self.total_connections,
            self.initial_subscription_bytes,
            self.estimated_book_memory_bytes / (1 << 20),
            self.estimated_snapshot_bytes_per_sec,
        )
    }
}

/// Build and validate the capacity plan for a discovered market set.
///
/// Shard derivation enforces the per-socket bound. The remaining checks reject
/// a run whose total connection count, initial subscription frame, estimated
/// book memory, or estimated snapshot rate exceeds its configured ceiling.
pub fn plan(settings: &Settings, markets: &[MarketInfo]) -> anyhow::Result<CapacityPlan> {
    let outcome_symbols = markets.len();
    let selected_markets = count_selected_markets(markets);
    let shards = derive_shard_count(settings, outcome_symbols)?;
    let replicas = settings.collector.replicas;
    let total_connections = shards.saturating_mul(replicas);
    anyhow::ensure!(
        total_connections <= settings.collector.max_total_connections,
        "{shards} shards x {replicas} replicas needs {total_connections} websocket \
         connections, above max_total_connections = {}",
        settings.collector.max_total_connections
    );

    let per_socket = outcome_symbols.div_ceil(shards.max(1));
    let longest_symbol = markets
        .iter()
        .map(|market| market.native_symbol.len() as u64)
        .max()
        .unwrap_or(0);
    // Each token is quoted and comma-separated; the envelope is counted once.
    let initial_subscription_bytes = SUBSCRIPTION_FRAME_OVERHEAD
        .saturating_add((per_socket as u64).saturating_mul(longest_symbol.saturating_add(3)));
    anyhow::ensure!(
        initial_subscription_bytes <= settings.collector.max_initial_subscription_bytes as u64,
        "the largest initial subscription frame is ~{initial_subscription_bytes} bytes \
         ({per_socket} symbols), above max_initial_subscription_bytes = {}",
        settings.collector.max_initial_subscription_bytes
    );

    let estimated_book_memory_bytes =
        (outcome_symbols as u64).saturating_mul(ESTIMATED_BYTES_PER_BOOK);
    let max_estimated_book_memory_bytes = settings
        .collector
        .max_estimated_book_memory_mb
        .saturating_mul(1 << 20);
    anyhow::ensure!(
        estimated_book_memory_bytes <= max_estimated_book_memory_bytes,
        "estimated book memory is ~{} MiB, above max_estimated_book_memory_mb = {}",
        estimated_book_memory_bytes / (1 << 20),
        settings.collector.max_estimated_book_memory_mb
    );

    let estimated_snapshot_bytes_per_sec = (outcome_symbols as u64)
        .saturating_mul(ESTIMATED_BYTES_PER_SNAPSHOT)
        / settings.collector.snapshot_interval_secs.max(1);
    anyhow::ensure!(
        estimated_snapshot_bytes_per_sec <= settings.collector.max_snapshot_bytes_per_sec,
        "estimated snapshot output is ~{estimated_snapshot_bytes_per_sec} B/s, above \
         max_snapshot_bytes_per_sec = {}",
        settings.collector.max_snapshot_bytes_per_sec
    );

    Ok(CapacityPlan {
        selected_markets,
        outcome_symbols,
        shards,
        replicas,
        total_connections,
        max_total_connections: settings.collector.max_total_connections,
        max_symbols_per_socket: settings.collector.max_symbols_per_socket,
        initial_subscription_bytes,
        max_initial_subscription_bytes: settings.collector.max_initial_subscription_bytes,
        estimated_book_memory_bytes,
        max_estimated_book_memory_bytes,
        estimated_snapshot_bytes_per_sec,
        max_snapshot_bytes_per_sec: settings.collector.max_snapshot_bytes_per_sec,
        snapshot_interval_secs: settings.collector.snapshot_interval_secs,
    })
}

/// Validate a refresh against the already-running topology. Unlike [`plan`],
/// this never derives a new shard count: workers and sockets already exist.
pub fn plan_fixed_topology(
    settings: &Settings,
    markets: &[MarketInfo],
    shard_loads: &[usize],
) -> anyhow::Result<CapacityPlan> {
    anyhow::ensure!(!shard_loads.is_empty(), "collector has no running shards");
    let shards = shard_loads.len();
    let max_load = shard_loads.iter().copied().max().unwrap_or(0);
    anyhow::ensure!(
        max_load <= settings.collector.max_symbols_per_socket,
        "projected shard load is {max_load} symbols, above max_symbols_per_socket = {}; refresh rejected without changing subscriptions",
        settings.collector.max_symbols_per_socket
    );
    let mut fixed = settings.clone();
    fixed.collector.shards = shards;
    let plan = plan(&fixed, markets)?;
    anyhow::ensure!(
        plan.shards == shards,
        "refresh would require a topology change"
    );
    Ok(plan)
}

/// Count distinct venue market records represented by the selected symbols.
///
/// Polymarket repeats one `exchange_market_id` for every outcome token; Kalshi
/// supplies one symbol per market and usually no separate market id.
#[must_use]
pub fn count_selected_markets(markets: &[MarketInfo]) -> usize {
    let mut records = BTreeSet::new();
    for market in markets {
        records.insert(
            market
                .exchange_market_id
                .as_deref()
                .unwrap_or(market.native_symbol.as_str()),
        );
    }
    records.len()
}

/// Choose the shard count for the discovered symbol set.
///
/// The per-socket bound is enforced, not advisory: a run that cannot keep
/// every socket at or below `max_symbols_per_socket` fails at startup. With an
/// explicit `shards`, the configured count must already satisfy the bound;
/// with `shards = 0`, the derived count must fit inside `max_shards`.
pub fn derive_shard_count(settings: &Settings, symbol_count: usize) -> anyhow::Result<usize> {
    let per_socket = settings.collector.max_symbols_per_socket.max(1);
    let shards = if settings.collector.shards > 0 {
        settings.collector.shards.clamp(1, 256)
    } else {
        let needed = symbol_count.div_ceil(per_socket).max(1);
        if needed > settings.collector.max_shards.max(1) {
            bail!(
                "discovered {symbol_count} symbols: keeping every socket at or below \
                 {per_socket} symbols needs {needed} shards, above max_shards = {}; \
                 raise max_shards or the per-socket bound",
                settings.collector.max_shards.max(1)
            );
        }
        needed.clamp(1, 256)
    };
    let per_shard = symbol_count.div_ceil(shards);
    anyhow::ensure!(
        per_shard <= per_socket,
        "{symbol_count} symbols across {shards} shards would put up to {per_shard} \
         symbols on one socket, above max_symbols_per_socket = {per_socket}; \
         increase the shard count or the per-socket bound"
    );
    Ok(shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(index: usize) -> MarketInfo {
        MarketInfo {
            exchange: "polymarket".to_owned(),
            native_symbol: format!("token-{index:08}"),
            symbol: format!("polymarket:0x{index:08}:0"),
            title: None,
            outcome: None,
            exchange_market_id: Some(format!("0x{index:08}")),
            close_time_us: None,
            tick_size_us: Some(1_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
            numeric_spec: crate::model::NumericSpec::default(),
        }
    }

    fn markets(count: usize) -> Vec<MarketInfo> {
        (0..count).map(market).collect()
    }

    fn settings(shards: usize, per_socket: usize, max_shards: usize) -> Settings {
        let mut settings = Settings::for_exchange(crate::config::ExchangeKind::Polymarket);
        settings.collector.shards = shards;
        settings.collector.max_symbols_per_socket = per_socket;
        settings.collector.max_shards = max_shards;
        settings
    }

    #[test]
    fn explicit_shard_count_must_satisfy_the_per_socket_bound() {
        assert_eq!(
            derive_shard_count(&settings(2, 5_000, 64), 9_998).unwrap(),
            2
        );
        assert_eq!(
            derive_shard_count(&settings(2, 5_000, 64), 10_000).unwrap(),
            2
        );
        let error = derive_shard_count(&settings(2, 5_000, 64), 10_001).unwrap_err();
        assert!(
            error.to_string().contains("max_symbols_per_socket"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn fixed_topology_uses_actual_maximum_shard_load() {
        let mut configured = settings(2, 3, 64);
        configured.collector.max_total_connections = 4;
        assert!(plan_fixed_topology(&configured, &markets(5), &[3, 2]).is_ok());
        let error = plan_fixed_topology(&configured, &markets(5), &[4, 1]).unwrap_err();
        assert!(error.to_string().contains("projected shard load"));
    }

    #[test]
    fn automatic_sharding_derives_the_required_count() {
        assert_eq!(
            derive_shard_count(&settings(0, 5_000, 64), 20_000).unwrap(),
            4
        );
        assert_eq!(
            derive_shard_count(&settings(0, 5_000, 64), 20_001).unwrap(),
            5
        );
        assert_eq!(
            derive_shard_count(&settings(0, 5_000, 64), 320_000).unwrap(),
            64
        );
        let error = derive_shard_count(&settings(0, 5_000, 64), 320_001).unwrap_err();
        assert!(
            error.to_string().contains("max_shards"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plan_reports_markets_and_outcomes_separately() {
        let mut settings = settings(2, 5_000, 64);
        settings.collector.snapshot_interval_secs = 60;
        let mut selected = markets(3);
        // Two outcome tokens share one Gamma record.
        selected[1].exchange_market_id = selected[0].exchange_market_id.clone();
        let plan = plan(&settings, &selected).unwrap();
        assert_eq!(plan.selected_markets, 2);
        assert_eq!(plan.outcome_symbols, 3);
        assert_eq!(plan.shards, 2);
        assert_eq!(plan.total_connections, 4);
    }

    #[test]
    fn plan_rejects_each_declared_ceiling() {
        let mut settings = settings(2, 5_000, 64);
        let selected = markets(100);
        settings.collector.max_total_connections = 3;
        assert!(
            plan(&settings, &selected)
                .unwrap_err()
                .to_string()
                .contains("max_total_connections")
        );

        settings.collector.max_total_connections = 64;
        settings.collector.max_initial_subscription_bytes = 512;
        assert!(
            plan(&settings, &selected)
                .unwrap_err()
                .to_string()
                .contains("max_initial_subscription_bytes")
        );

        settings.collector.max_initial_subscription_bytes = 1 << 20;
        settings.collector.max_estimated_book_memory_mb = 0;
        assert!(
            plan(&settings, &selected)
                .unwrap_err()
                .to_string()
                .contains("max_estimated_book_memory_mb")
        );

        settings.collector.max_estimated_book_memory_mb = 1024;
        settings.collector.max_snapshot_bytes_per_sec = 1024;
        assert!(
            plan(&settings, &selected)
                .unwrap_err()
                .to_string()
                .contains("max_snapshot_bytes_per_sec")
        );
    }

    #[test]
    fn plan_is_inclusive_at_each_boundary() {
        let mut settings = settings(2, 5_000, 64);
        settings.collector.max_total_connections = 4;
        let selected = markets(100);
        let plan = plan(&settings, &selected).unwrap();
        assert_eq!(plan.total_connections, 4);
        assert_eq!(
            plan.estimated_book_memory_bytes,
            100 * ESTIMATED_BYTES_PER_BOOK
        );
    }
}
