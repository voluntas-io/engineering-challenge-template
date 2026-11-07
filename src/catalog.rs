//! Persisted last-known-good discovery catalog.
//!
//! A checkpoint is written only after a complete discovery round has been
//! accepted. It carries a schema version, the generation time, the selected
//! record and outcome counts, and a SHA-256 digest over the exchange name and
//! market list. Loading verifies the digest before any market is used, so a
//! truncated or hand-edited checkpoint cannot silently become the subscription
//! set.

use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    capacity::count_selected_markets, model::MarketInfo, report::write_json_atomic,
    util::sha256_hex,
};

/// Catalog schema version. A future incompatible layout increments this and
/// old checkpoints are rejected instead of guessed at.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub schema_version: u32,
    pub exchange: String,
    /// Wall-clock time at which the accepted discovery round finished.
    pub generated_at_us: i64,
    pub selected_markets: usize,
    pub outcome_symbols: usize,
    /// SHA-256 over the exchange name and the market list.
    pub checksum: String,
    pub markets: Vec<MarketInfo>,
}

impl Catalog {
    pub fn new(
        exchange: &str,
        markets: Vec<MarketInfo>,
        generated_at_us: i64,
    ) -> anyhow::Result<Self> {
        let checksum = checksum_for(exchange, generated_at_us, &markets)?;
        let selected_markets = count_selected_markets(&markets);
        let catalog = Self {
            schema_version: CATALOG_SCHEMA_VERSION,
            exchange: exchange.to_owned(),
            generated_at_us,
            selected_markets,
            outcome_symbols: markets.len(),
            checksum,
            markets,
        };
        catalog.verify()?;
        Ok(catalog)
    }

    /// Verify schema, counts, per-market fields, and the content digest.
    pub fn verify(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema_version == CATALOG_SCHEMA_VERSION,
            "unsupported catalog schema version {}",
            self.schema_version
        );
        anyhow::ensure!(!self.exchange.is_empty(), "catalog has no exchange name");
        anyhow::ensure!(
            self.generated_at_us >= 0,
            "catalog generation time is negative"
        );
        anyhow::ensure!(
            self.outcome_symbols == self.markets.len(),
            "catalog outcome count {} does not match {} markets",
            self.outcome_symbols,
            self.markets.len()
        );
        anyhow::ensure!(
            self.selected_markets == count_selected_markets(&self.markets),
            "catalog selected-market count does not match its market list"
        );
        for market in &self.markets {
            market
                .validate()
                .with_context(|| format!("invalid catalog market `{}`", market.symbol))?;
        }
        let expected = checksum_for(&self.exchange, self.generated_at_us, &self.markets)?;
        anyhow::ensure!(
            self.checksum == expected,
            "catalog checksum mismatch: stored {}, computed {expected}",
            self.checksum
        );
        Ok(())
    }

    /// Age in microseconds at `now_us`, saturating at zero for a clock step.
    #[must_use]
    pub fn age_us(&self, now_us: i64) -> i64 {
        now_us.saturating_sub(self.generated_at_us).max(0)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        self.verify()?;
        write_json_atomic(path, self)
            .with_context(|| format!("writing catalog checkpoint `{}`", path.display()))
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading catalog checkpoint `{}`", path.display()))?;
        let catalog: Self = serde_json::from_str(&text)
            .with_context(|| format!("parsing catalog checkpoint `{}`", path.display()))?;
        catalog.verify()?;
        Ok(catalog)
    }
}

/// SHA-256 over the exchange name and a canonical compact JSON market list.
pub fn checksum_for(
    exchange: &str,
    generated_at_us: i64,
    markets: &[MarketInfo],
) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(&(exchange, generated_at_us, markets))
        .context("serializing catalog checksum")?;
    Ok(sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
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
    fn round_trips_and_reports_age() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let catalog = Catalog::new("polymarket", vec![market(0), market(1)], 1_000).unwrap();
        catalog.save(&path).unwrap();
        let loaded = Catalog::load(&path).unwrap();
        assert_eq!(loaded.markets.len(), 2);
        assert_eq!(loaded.selected_markets, 2);
        assert_eq!(loaded.outcome_symbols, 2);
        assert_eq!(loaded.age_us(5_000), 4_000);
        // A backward wall-clock step does not produce a negative age.
        assert_eq!(loaded.age_us(500), 0);
    }

    #[test]
    fn rejects_a_modified_market_list() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let catalog = Catalog::new("polymarket", vec![market(0)], 1_000).unwrap();
        catalog.save(&path).unwrap();

        let mut value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        value["markets"][0]["native_symbol"] = serde_json::Value::String("tampered".to_owned());
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();

        let error = Catalog::load(&path).unwrap_err();
        assert!(
            error.to_string().contains("checksum"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_a_modified_generation_timestamp() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        Catalog::new("polymarket", vec![market(0)], 1_000)
            .unwrap()
            .save(&path)
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        value["generated_at_us"] = serde_json::json!(9_999_999);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            Catalog::load(&path)
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
    }

    #[test]
    fn rejects_count_mismatch_and_unknown_schema() {
        let mut catalog = Catalog::new("polymarket", vec![market(0)], 1_000).unwrap();
        catalog.outcome_symbols = 2;
        assert!(catalog.verify().is_err());

        let mut catalog = Catalog::new("polymarket", vec![market(0)], 1_000).unwrap();
        catalog.schema_version = CATALOG_SCHEMA_VERSION + 1;
        assert!(catalog.verify().is_err());
    }

    #[test]
    fn checksum_is_exchange_specific() {
        let markets = vec![market(0)];
        assert_ne!(
            checksum_for("polymarket", 1_000, &markets).unwrap(),
            checksum_for("kalshi", 1_000, &markets).unwrap()
        );
    }
}
