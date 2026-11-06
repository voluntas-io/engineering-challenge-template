//! Exchange adapters and their common discovery/session interface.

pub mod kalshi;
pub mod polymarket;

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    capacity::count_selected_markets, config::ExchangeKind, model::MarketInfo,
    session::SessionFactory, time::now_us,
};

/// Venue features that the exchange adapter reports to the collector core.
///
/// The core consumes these values instead of identifying a venue by name, so
/// adding an exchange requires a new adapter and capability declaration, not a
/// change to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExchangeCapabilities {
    /// The venue supplies a sequence number or clock that can verify backup
    /// book freshness and quantify a recovery gap.
    pub has_sequence_clock: bool,
}

impl ExchangeCapabilities {
    #[must_use]
    pub const fn new(has_sequence_clock: bool) -> Self {
        Self { has_sequence_clock }
    }
}

/// Capability declaration for a configured venue.
///
/// The coordinator obtains capabilities from the constructed adapter. Replay
/// has no adapter, so this mapping is the composition-root equivalent for the
/// selected configuration.
#[must_use]
pub fn capabilities_for(kind: ExchangeKind) -> ExchangeCapabilities {
    match kind {
        // Kalshi `seq` is per-subscription and cannot compare replicas. The
        // wire `clock` is optional and is not assumed cross-replica
        // comparable, so the core uses the clockless recovery path.
        ExchangeKind::Kalshi => ExchangeCapabilities::new(false),
        ExchangeKind::Polymarket => ExchangeCapabilities::new(false),
    }
}

/// Telemetry for one complete discovery round.
///
/// `selected_markets` counts venue records; `outcome_symbols` counts the
/// subscribable tokens those records produced. The two differ on Polymarket,
/// where one Gamma record expands into several outcome tokens.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveryStats {
    pub pages: u64,
    pub selected_markets: u64,
    pub outcome_symbols: u64,
    pub malformed_records: u64,
    pub retries: u64,
    pub duration_ms: u64,
    pub started_at_us: i64,
    pub finished_at_us: i64,
}

/// A complete discovery result with its telemetry.
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub markets: Vec<MarketInfo>,
    pub stats: DiscoveryStats,
}

/// One exchange's discovery and websocket implementation.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Stable lowercase exchange name used in SBE symbols and reports.
    fn name(&self) -> &'static str;

    /// Human-readable environment (`production`, `demo`, or `public`).
    fn environment(&self) -> &'static str;

    /// Features the collector core may rely on for this venue.
    fn capabilities(&self) -> ExchangeCapabilities;

    /// Fetch the current live, non-MVE market set. Implementations cap the
    /// result when `max_markets` is non-zero.
    async fn discover_markets(&self, max_markets: usize) -> anyhow::Result<Vec<MarketInfo>>;

    /// Fetch a discovery round with page and record telemetry.
    ///
    /// The default implementation preserves adapters that do not publish
    /// page-level counters; both built-in adapters override it.
    async fn discover_markets_with_stats(
        &self,
        max_markets: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let started = Instant::now();
        let started_at_us = now_us().unwrap_or(0);
        let markets = self.discover_markets(max_markets).await?;
        let stats = DiscoveryStats {
            pages: 1,
            selected_markets: count_selected_markets(&markets) as u64,
            outcome_symbols: markets.len() as u64,
            duration_ms: started.elapsed().as_millis() as u64,
            started_at_us,
            finished_at_us: now_us().unwrap_or(started_at_us),
            ..DiscoveryStats::default()
        };
        Ok(DiscoveryResult { markets, stats })
    }

    /// Build the factory used by websocket source workers.
    fn session_factory(self: Arc<Self>) -> Arc<dyn SessionFactory>;
}

pub use kalshi::{KalshiExchange, KalshiSessionFactory};
pub use polymarket::{PolymarketExchange, PolymarketSessionFactory};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_venue_capabilities_are_explicit() {
        // Per-subscription `seq` does not compare replicas, and the wire
        // `clock` is optional. Kalshi must not claim a sequence clock until
        // a cross-replica comparable clock is verified.
        assert!(!capabilities_for(ExchangeKind::Kalshi).has_sequence_clock);
        assert!(!capabilities_for(ExchangeKind::Polymarket).has_sequence_clock);
    }
}
