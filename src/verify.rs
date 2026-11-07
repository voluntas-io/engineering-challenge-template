//! Offline verification of an aligned SBE capture.
//!
//! `inspect` proves that every message decodes. `verify` additionally
//! reconstructs each symbol's book from the captured deltas and compares that
//! state with the next full snapshot. A mismatch means the capture does not
//! describe a single, complete book history: a delta was lost, duplicated, or
//! applied out of order. The check is strict because a silently
//! inconsistent backtest is worse than a failed capture.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::Context;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    Side,
    model::{BookSnapshot, Level, Price, Quantity, QuantityChange},
    orderbook::OrderBook,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};

#[derive(Debug, Serialize)]
pub struct VerifySummary {
    pub file: String,
    pub chunk_size: usize,
    pub messages: u64,
    pub snapshots: u64,
    pub price_level_updates: u64,
    pub unknown_messages: u64,
    pub symbols: usize,
    pub reconstructed_symbols: usize,
    pub empty_snapshots: u64,
    pub deltas_before_snapshot: u64,
    pub snapshot_mismatches: u64,
    pub first_received_at_us: Option<i64>,
    pub last_received_at_us: Option<i64>,
    pub first_exchange_timestamp_us: Option<i64>,
    pub last_exchange_timestamp_us: Option<i64>,
    pub mismatch_samples: Vec<Value>,
    pub valid: bool,
    pub notes: Vec<String>,
}

/// Verify one capture. `chunk_size` is either an integer or `auto`.
pub fn run(input: &Path, chunk_size: &str) -> anyhow::Result<VerifySummary> {
    let mut reader = if chunk_size.eq_ignore_ascii_case("auto") {
        AlignedSbeReader::<OrderbookSchema>::open_auto(input)
            .with_context(|| format!("opening `{}` with auto chunk detection", input.display()))?
    } else {
        let chunk_size: usize = chunk_size
            .parse()
            .with_context(|| format!("invalid chunk size `{chunk_size}`"))?;
        AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(input, chunk_size)
            .with_context(|| format!("opening `{}`", input.display()))?
    };

    let mut summary = VerifySummary {
        file: input.display().to_string(),
        chunk_size: reader.chunk_size(),
        messages: 0,
        snapshots: 0,
        price_level_updates: 0,
        unknown_messages: 0,
        symbols: 0,
        reconstructed_symbols: 0,
        empty_snapshots: 0,
        deltas_before_snapshot: 0,
        snapshot_mismatches: 0,
        first_received_at_us: None,
        last_received_at_us: None,
        first_exchange_timestamp_us: None,
        last_exchange_timestamp_us: None,
        mismatch_samples: Vec::new(),
        valid: false,
        notes: vec![
            "snapshot_mismatches compares the book rebuilt from deltas with the next full snapshot"
                .to_owned(),
            "deltas_before_snapshot counts updates that cannot be anchored to a snapshot"
                .to_owned(),
            "unknown_messages makes the capture unverifiable rather than silently ignored"
                .to_owned(),
        ],
    };
    let mut books: BTreeMap<String, OrderBook> = BTreeMap::new();
    let mut symbols = BTreeSet::new();
    let mut snapshots_seen: BTreeSet<String> = BTreeSet::new();

    while let Some(message) = reader.read_msg()? {
        summary.messages = summary.messages.saturating_add(1);
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot =
                    SnapshotDomain::try_from_decoder(decoder).context("decoding SBE snapshot")?;
                summary.snapshots = summary.snapshots.saturating_add(1);
                symbols.insert(snapshot.symbol.clone());
                update_bounds(
                    &mut summary,
                    snapshot.received_at.timestamp_micros(),
                    snapshot.exchange_timestamp.timestamp_micros(),
                );
                let incoming = book_from_snapshot(&snapshot)?;
                let empty = incoming.is_empty();
                if empty {
                    summary.empty_snapshots = summary.empty_snapshots.saturating_add(1);
                }
                // An empty snapshot is the collector's retirement/empty-book
                // boundary. It carries no level evidence, so comparing it with
                // the book rebuilt from earlier deltas would flag every normal
                // discovery removal and identity change. Count it and use it as
                // the new anchor instead. An empty snapshot cannot prove that a
                // suppressed update emptied a book; only a later non-empty
                // snapshot can expose that gap.
                if !empty
                    && let Some(current) = books.get(&snapshot.symbol)
                    && snapshots_seen.contains(&snapshot.symbol)
                    && current != &incoming
                {
                    summary.snapshot_mismatches = summary.snapshot_mismatches.saturating_add(1);
                    if summary.mismatch_samples.len() < 10 {
                        summary.mismatch_samples.push(json!({
                            "symbol": snapshot.symbol,
                            "received_at_us": snapshot.received_at.timestamp_micros(),
                            "expected_bids": levels_json(current.to_snapshot().bids),
                            "expected_asks": levels_json(current.to_snapshot().asks),
                            "snapshot_bids": levels_json(incoming.to_snapshot().bids),
                            "snapshot_asks": levels_json(incoming.to_snapshot().asks),
                        }));
                    }
                }
                snapshots_seen.insert(snapshot.symbol.clone());
                books.insert(snapshot.symbol, incoming);
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)
                    .context("decoding SBE price-level update")?;
                summary.price_level_updates = summary.price_level_updates.saturating_add(1);
                symbols.insert(update.symbol.clone());
                update_bounds(
                    &mut summary,
                    update.backtest_timestamp.timestamp_micros(),
                    update.exchange_timestamp.timestamp_micros(),
                );
                if !snapshots_seen.contains(&update.symbol) {
                    summary.deltas_before_snapshot =
                        summary.deltas_before_snapshot.saturating_add(1);
                }
                let book = books.entry(update.symbol.clone()).or_default();
                book.apply(
                    update.side,
                    Price::from_raw(update.price_level),
                    QuantityChange::Absolute(Quantity::new(update.quantity).with_context(
                        || {
                            format!(
                                "negative {} quantity {} for `{}`",
                                side_name(update.side),
                                update.quantity,
                                update.symbol
                            )
                        },
                    )?),
                )
                .with_context(|| {
                    format!(
                        "applying {} update for `{}` at price {} and quantity {}",
                        side_name(update.side),
                        update.symbol,
                        update.price_level,
                        update.quantity
                    )
                })?;
            }
            AnyMessage::Unknown { .. } => {
                summary.unknown_messages = summary.unknown_messages.saturating_add(1);
            }
        }
    }

    summary.symbols = symbols.len();
    summary.reconstructed_symbols = books.len();
    summary.valid = summary.unknown_messages == 0
        && summary.deltas_before_snapshot == 0
        && summary.snapshot_mismatches == 0;
    Ok(summary)
}

fn book_from_snapshot(snapshot: &SnapshotDomain) -> anyhow::Result<OrderBook> {
    let book_snapshot = BookSnapshot {
        bids: snapshot
            .bids
            .iter()
            .map(|level| Level::new(level.price_level, level.quantity))
            .collect::<anyhow::Result<Vec<_>>>()?,
        asks: snapshot
            .asks
            .iter()
            .map(|level| Level::new(level.price_level, level.quantity))
            .collect::<anyhow::Result<Vec<_>>>()?,
    };
    let mut book = OrderBook::new();
    book.replace(&book_snapshot)
        .with_context(|| format!("validating snapshot for `{}`", snapshot.symbol))?;
    Ok(book)
}

fn levels_json(levels: Vec<Level>) -> Vec<Value> {
    levels
        .into_iter()
        .map(|level| {
            json!({
                "price": level.price.raw(),
                "quantity": level.quantity.raw(),
            })
        })
        .collect()
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    }
}

fn update_bounds(summary: &mut VerifySummary, received: i64, exchange: i64) {
    summary.first_received_at_us = Some(
        summary
            .first_received_at_us
            .map_or(received, |value| value.min(received)),
    );
    summary.last_received_at_us = Some(
        summary
            .last_received_at_us
            .map_or(received, |value| value.max(received)),
    );
    summary.first_exchange_timestamp_us = Some(
        summary
            .first_exchange_timestamp_us
            .map_or(exchange, |value| value.min(exchange)),
    );
    summary.last_exchange_timestamp_us = Some(
        summary
            .last_exchange_timestamp_us
            .map_or(exchange, |value| value.max(exchange)),
    );
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::{
        model::OutputMessage,
        util::dtos::challenge_orderbook::{
            SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
        },
        writer::DirectWriter,
    };

    const SYMBOL: &str = "test:book";

    fn timestamp(us: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(us).unwrap()
    }

    fn snapshot(us: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> OutputMessage {
        OutputMessage::Snapshot(SnapshotDomain {
            received_at: timestamp(us),
            exchange_timestamp: timestamp(us),
            bids: bids
                .iter()
                .map(|(price, quantity)| SnapshotBidsEntryDomain {
                    price_level: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(price, quantity)| SnapshotAsksEntryDomain {
                    price_level: *price,
                    quantity: *quantity,
                })
                .collect(),
            symbol: SYMBOL.to_owned(),
        })
    }

    fn update(us: i64, side: Side, price: i64, quantity: i64) -> OutputMessage {
        OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
            backtest_timestamp: timestamp(us),
            exchange_timestamp: timestamp(us),
            price_level: price,
            quantity,
            side,
            symbol: SYMBOL.to_owned(),
        })
    }

    fn write_capture(messages: Vec<OutputMessage>) -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("capture.sbe");
        let mut writer = DirectWriter::create(crate::writer::WriterOptions {
            path: path.to_str().unwrap().to_owned(),
            chunk_size: 4096,
            queue: 16,
            sync_on_close: false,
            overwrite: false,
            rotate_bytes: 0,
            rotate_secs: 0,
            min_free_bytes: 0,
            fsync_interval_secs: 0,
        })
        .unwrap();
        for message in messages {
            writer.write(&message).unwrap();
        }
        writer.shutdown().unwrap();
        (directory, path)
    }

    #[test]
    fn verifies_a_consistent_snapshot_delta_history() {
        let (_directory, path) = write_capture(vec![
            snapshot(1, &[(400_000, 10)], &[(600_000, 20)]),
            update(2, Side::Buy, 400_000, 15),
            update(3, Side::Sell, 600_000, 0),
            snapshot(4, &[(400_000, 15)], &[]),
        ]);
        let summary = run(&path, "auto").unwrap();
        assert!(summary.valid);
        assert_eq!(summary.snapshot_mismatches, 0);
        assert_eq!(summary.deltas_before_snapshot, 0);
        assert_eq!(summary.snapshots, 2);
        assert_eq!(summary.price_level_updates, 2);
    }

    #[test]
    fn reports_a_snapshot_that_disagrees_with_reconstructed_deltas() {
        let (_directory, path) = write_capture(vec![
            snapshot(1, &[(400_000, 10)], &[(600_000, 20)]),
            update(2, Side::Buy, 400_000, 15),
            snapshot(3, &[(400_000, 20)], &[(600_000, 20)]),
        ]);
        let summary = run(&path, "auto").unwrap();
        assert!(!summary.valid);
        assert_eq!(summary.snapshot_mismatches, 1);
        assert_eq!(summary.mismatch_samples.len(), 1);
    }

    #[test]
    fn empty_snapshot_is_a_retirement_boundary_not_a_mismatch() {
        let (_directory, path) = write_capture(vec![
            snapshot(1, &[(400_000, 10)], &[(600_000, 20)]),
            update(2, Side::Buy, 400_000, 15),
            snapshot(3, &[], &[]),
        ]);
        let summary = run(&path, "auto").unwrap();
        assert!(summary.valid);
        assert_eq!(summary.snapshot_mismatches, 0);
        assert_eq!(summary.empty_snapshots, 1);
    }

    #[test]
    fn an_empty_capture_validates_with_zero_messages() {
        // A capture that contains only the format's end sentinel is
        // self-consistent. Validity alone is not completeness: the sentinel
        // defines the stream's end, so the message count must be compared
        // with the run report's `writer.messages` to expose truncation.
        let (_directory, path) = write_capture(Vec::new());
        let summary = run(&path, "4096").unwrap();
        assert!(summary.valid);
        assert_eq!(summary.messages, 0);
        assert_eq!(summary.symbols, 0);
    }

    #[test]
    fn reports_a_delta_before_any_snapshot() {
        let (_directory, path) = write_capture(vec![
            update(1, Side::Buy, 400_000, 15),
            snapshot(2, &[(400_000, 15)], &[]),
        ]);
        let summary = run(&path, "auto").unwrap();
        assert!(!summary.valid);
        assert_eq!(summary.deltas_before_snapshot, 1);
    }

    #[test]
    fn rejects_an_invalid_delta() {
        let (_directory, path) = write_capture(vec![
            snapshot(1, &[(400_000, 10)], &[]),
            update(2, Side::Buy, 400_000, -1),
        ]);
        assert!(run(&path, "auto").is_err());
    }
}
