//! SBE output writer: wraps [`AlignedSbeWriter`] and converts canonical types.
//!
//! This is the only module that imports from the template's `util` layer.
//! Everything above this in the stack works with Rust-native types; only here
//! do we pay the SBE encoding cost and flush bytes to stdout.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::util::AlignedSbeWriter;
use crate::Side;

use crate::orderbook::PriceLevel;

// 64 MiB chunk size, matching the template example.
const CHUNK_SIZE_64_MIB: usize = 64 << 20;

/// Output sink that writes SBE messages to stdout.
pub struct Output {
    writer: AlignedSbeWriter,
}

impl Output {
    /// Open the output sink for SBE-encoded binary data.
    ///
    /// If `path` is provided, writes to that file.
    /// If `OUTPUT_PATH` env var is set, writes to that path.
    /// Otherwise defaults to `/dev/stdout`.
    pub fn new(path: Option<&str>) -> Result<Self> {
        let target = path
            .map(|s| s.to_string())
            .or_else(|| std::env::var("OUTPUT_PATH").ok())
            .unwrap_or_else(|| "/dev/stdout".to_string());

        let writer = AlignedSbeWriter::create(&target, CHUNK_SIZE_64_MIB)
            .with_context(|| format!("opening AlignedSbeWriter on {target}"))?;
        Ok(Self { writer })
    }

    /// Write a full orderbook snapshot for one symbol.
    pub fn write_snapshot(
        &mut self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
    ) -> Result<()> {
        let snapshot = SnapshotDomain {
            symbol: symbol.to_owned(),
            received_at,
            exchange_timestamp: exchange_ts,
            bids: bids
                .iter()
                .map(|l| SnapshotBidsEntryDomain {
                    price_level: l.price,
                    quantity: l.qty,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|l| SnapshotAsksEntryDomain {
                    price_level: l.price,
                    quantity: l.qty,
                })
                .collect(),
        };
        self.writer
            .write_msg(&snapshot)
            .context("writing snapshot to SBE output")
    }

    /// Write a single price-level delta for one symbol.
    pub fn write_delta(
        &mut self,
        symbol: &str,
        backtest_ts: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
        price: i64,
        qty: i64,
        side: Side,
    ) -> Result<()> {
        let update = PriceLevelUpdateDomain {
            symbol: symbol.to_owned(),
            backtest_timestamp: backtest_ts,
            exchange_timestamp: exchange_ts,
            price_level: price,
            quantity: qty,
            side,
        };
        self.writer
            .write_msg(&update)
            .context("writing delta to SBE output")
    }

    /// Flush the current chunk to the underlying file descriptor.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flushing SBE output")
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        // Best-effort flush on shutdown; errors are logged elsewhere.
        let _ = self.writer.flush();
    }
}

