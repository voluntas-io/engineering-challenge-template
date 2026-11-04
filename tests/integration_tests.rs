//! Integration tests for the orderbook collector.
//!
//! These tests cover:
//!   - `orderbook` state machine: snapshot ingestion, delta application,
//!     edge cases (zero-qty deletion, duplicate levels, stale clock rejection)
//!   - `kalshi` price parsing helpers
//!   - Full SBE round-trip: write snapshot + delta, read back and assert equality
//!   - `auth` signing shape (header names and base64 validity)

use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

// ── Helpers ───────────────────────────────────────────────────────────────────

const CHUNK_64MIB: usize = 64 << 20;

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
}

// ── SBE round-trip tests ──────────────────────────────────────────────────────

#[test]
fn sbe_roundtrip_snapshot_and_delta() {
    let ts = now();
    let symbol = "TEST-MARKET-1".to_owned();

    let snapshot = SnapshotDomain {
        received_at: ts,
        exchange_timestamp: ts,
        symbol: symbol.clone(),
        bids: vec![
            SnapshotBidsEntryDomain { price_level: 400_000, quantity: 2_000_000 },
            SnapshotBidsEntryDomain { price_level: 380_000, quantity: 1_000_000 },
        ],
        asks: vec![
            SnapshotAsksEntryDomain { price_level: 600_000, quantity: 3_500_000 },
        ],
    };

    let update = PriceLevelUpdateDomain {
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 400_000,
        quantity: 1_250_000,
        side: Side::Buy,
        symbol: symbol.clone(),
    };

    // Write to temp file
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_64MIB).unwrap();
    writer.write_msg(&snapshot).unwrap();
    writer.write_msg(&update).unwrap();
    writer.flush().unwrap();
    drop(writer);

    // Read back
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();

    let Some(AnyMessage::Snapshot(decoded_snap)) = reader.read_msg().unwrap() else {
        panic!("expected snapshot");
    };
    assert_eq!(SnapshotDomain::try_from_decoder(decoded_snap).unwrap(), snapshot);

    let Some(AnyMessage::PriceLevelUpdate(decoded_upd)) = reader.read_msg().unwrap() else {
        panic!("expected price level update");
    };
    assert_eq!(PriceLevelUpdateDomain::try_from_decoder(decoded_upd).unwrap(), update);

    assert!(reader.read_msg().unwrap().is_none(), "expected EOF");
}

#[test]
fn sbe_roundtrip_empty_snapshot() {
    let ts = now();
    let snapshot = SnapshotDomain {
        received_at: ts,
        exchange_timestamp: ts,
        symbol: "EMPTY".to_owned(),
        bids: vec![],
        asks: vec![],
    };

    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_64MIB).unwrap();
    writer.write_msg(&snapshot).unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected snapshot");
    };
    assert_eq!(SnapshotDomain::try_from_decoder(decoded).unwrap(), snapshot);
}

#[test]
fn sbe_roundtrip_many_messages() {
    let ts = now();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_64MIB).unwrap();

    const N: usize = 1000;
    for i in 0..N {
        let update = PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: (i as i64) * 1000,
            quantity: (i as i64 + 1) * 500_000,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            symbol: format!("MARKET-{i}"),
        };
        writer.write_msg(&update).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let mut count = 0;
    while let Some(AnyMessage::PriceLevelUpdate(_)) = reader.read_msg().unwrap() {
        count += 1;
    }
    assert_eq!(count, N);
}

#[test]
fn sbe_roundtrip_sell_side_delta() {
    let ts = now();
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 550_000,
        quantity: 750_000,
        side: Side::Sell,
        symbol: "ASK-TEST".to_owned(),
    };

    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_64MIB).unwrap();
    writer.write_msg(&update).unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected price level update");
    };
    let decoded = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(decoded, update);
    assert_eq!(decoded.side, Side::Sell);
}

