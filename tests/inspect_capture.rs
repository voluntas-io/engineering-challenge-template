#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::path::PathBuf;

use chrono::{DateTime, Utc};

use engineering_challenge::{
    Side, inspect,
    model::OutputMessage,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
    writer::DirectWriter,
};

fn timestamp(us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(us).unwrap()
}

fn snapshot(symbol: &str, received_us: i64, exchange_us: i64) -> OutputMessage {
    OutputMessage::Snapshot(SnapshotDomain {
        received_at: timestamp(received_us),
        exchange_timestamp: timestamp(exchange_us),
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 400_000,
            quantity: 10,
        }],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 600_000,
            quantity: 20,
        }],
        symbol: symbol.to_owned(),
    })
}

fn update(symbol: &str, received_us: i64, exchange_us: i64) -> OutputMessage {
    OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
        backtest_timestamp: timestamp(received_us),
        exchange_timestamp: timestamp(exchange_us),
        price_level: 400_000,
        quantity: 15,
        side: Side::Buy,
        symbol: symbol.to_owned(),
    })
}

fn write_capture(messages: Vec<OutputMessage>) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("capture.sbe");
    let mut writer = DirectWriter::create(engineering_challenge::writer::WriterOptions {
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

fn summary_value(summary: &inspect::InspectSummary) -> serde_json::Value {
    serde_json::to_value(summary).unwrap()
}

#[test]
fn mixed_snapshots_and_updates_account_for_every_message() {
    // Out-of-order timestamps prove bounds use min/max, not arrival order.
    let (_dir, path) = write_capture(vec![
        snapshot("A", 40, 400),
        update("B", 20, 200),
        snapshot("A", 30, 300),
    ]);
    let summary = inspect::run(&path, "4096", 10).unwrap();
    let value = summary_value(&summary);
    assert_eq!(value["messages"], 3);
    assert_eq!(value["snapshots"], 2);
    assert_eq!(value["price_level_updates"], 1);
    assert_eq!(
        value["messages"].as_u64().unwrap(),
        value["snapshots"].as_u64().unwrap()
            + value["price_level_updates"].as_u64().unwrap()
            + value["unknown_messages"].as_u64().unwrap()
    );
    assert_eq!(value["symbols"], 2);
    assert_eq!(value["first_received_at_us"], 20);
    assert_eq!(value["last_received_at_us"], 40);
    assert_eq!(value["first_exchange_timestamp_us"], 200);
    assert_eq!(value["last_exchange_timestamp_us"], 400);
}

#[test]
fn repeated_symbols_are_counted_once() {
    let (_dir, path) = write_capture(vec![
        snapshot("A", 10, 100),
        snapshot("A", 20, 200),
        update("A", 30, 300),
    ]);
    let summary = inspect::run(&path, "4096", 10).unwrap();
    let value = summary_value(&summary);
    assert_eq!(value["symbols"], 1);
    assert_eq!(value["messages"], 3);
}

#[test]
fn sample_limit_bounds_samples() {
    let (_dir, path) = write_capture(vec![
        snapshot("A", 10, 100),
        update("B", 20, 200),
        snapshot("A", 30, 300),
    ]);
    let limited = inspect::run(&path, "4096", 2).unwrap();
    assert_eq!(
        summary_value(&limited)["samples"].as_array().unwrap().len(),
        2
    );
    let zero = inspect::run(&path, "4096", 0).unwrap();
    assert_eq!(summary_value(&zero)["samples"].as_array().unwrap().len(), 0);
}

#[test]
fn empty_capture_returns_zero_counts_and_absent_bounds() {
    let (_dir, path) = write_capture(Vec::new());
    let summary = inspect::run(&path, "4096", 10).unwrap();
    let value = summary_value(&summary);
    assert_eq!(value["messages"], 0);
    assert_eq!(value["snapshots"], 0);
    assert_eq!(value["price_level_updates"], 0);
    assert_eq!(value["symbols"], 0);
    assert!(value["first_received_at_us"].is_null());
    assert!(value["last_received_at_us"].is_null());
    assert!(value["first_exchange_timestamp_us"].is_null());
    assert!(value["last_exchange_timestamp_us"].is_null());
}

#[test]
fn explicit_chunk_size_is_reported() {
    let (_dir, path) = write_capture(vec![snapshot("A", 10, 100)]);
    let summary = inspect::run(&path, "4096", 10).unwrap();
    assert_eq!(summary_value(&summary)["chunk_size"], 4096);
}

#[test]
fn automatic_chunk_size_inspects_the_same_messages() {
    let (_dir, path) = write_capture(vec![snapshot("A", 10, 100), update("B", 20, 200)]);
    let explicit = inspect::run(&path, "4096", 10).unwrap();
    let automatic = inspect::run(&path, "auto", 10).unwrap();
    let e = summary_value(&explicit);
    let a = summary_value(&automatic);
    assert_eq!(a["messages"], e["messages"]);
    assert_eq!(a["symbols"], e["symbols"]);
    assert_eq!(a["snapshots"], e["snapshots"]);
    assert_eq!(a["price_level_updates"], e["price_level_updates"]);
}

#[test]
fn invalid_chunk_argument_returns_error_without_panic() {
    let (_dir, path) = write_capture(vec![snapshot("A", 10, 100)]);
    assert!(inspect::run(&path, "not-a-number", 10).is_err());
    assert!(inspect::run(&path, "-5", 10).is_err());
}

#[test]
fn invalid_header_returns_error_without_panic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.sbe");
    std::fs::write(&path, [0xFFu8; 256]).unwrap();
    assert!(inspect::run(&path, "4096", 10).is_err());
}

#[test]
fn invalid_sentinel_returns_error_without_panic() {
    let (_dir, path) = write_capture(vec![snapshot("A", 10, 100)]);
    let mut bytes = std::fs::read(&path).unwrap();
    // Corrupt the final chunk sentinel padding.
    if let Some(last) = bytes.last_mut() {
        *last ^= 0xFF;
    }
    std::fs::write(&path, &bytes).unwrap();
    assert!(inspect::run(&path, "4096", 10).is_err());
}
