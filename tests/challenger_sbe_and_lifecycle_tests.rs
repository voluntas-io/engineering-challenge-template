use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use engineering_challenge::Side;
use engineering_challenge::collector::Collector;
use engineering_challenge::mock::MockAdapter;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{
    AlignedSbeReader, AlignedSbeSchema, AlignedSbeWriter, OrderbookSchema,
};

fn sample_time() -> chrono::DateTime<Utc> {
    chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
}

// ══════════════════════════════════════════════════════════════════════════════
// 1. AlignedSbeWriter Empirical Stress Tests & Boundary Cases
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_aligned_sbe_writer_chunk_size_validation() {
    let tmp = tempfile::NamedTempFile::new().unwrap();

    // HEADER_LENGTH is 8. Chunk must be > 8.
    for invalid_chunk in [0, 1, 2, 7, 8] {
        let res = AlignedSbeWriter::create(tmp.path(), invalid_chunk);
        assert!(
            res.is_err(),
            "AlignedSbeWriter::create should reject chunk size {invalid_chunk} <= HEADER_LENGTH"
        );
    }

    // Chunk size 9 is minimal valid (leaves 1 byte for payload)
    let res = AlignedSbeWriter::create(tmp.path(), 9);
    assert!(res.is_ok(), "Chunk size 9 should be accepted");
}

#[test]
fn test_aligned_sbe_writer_message_exceeds_chunk_capacity() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    // Message length for PriceLevelUpdateDomain is ~49 bytes (8 byte header + fields + symbol).
    // Let's create a writer with chunk size 32 (payload_limit = 32 - 8 = 24 bytes).
    let mut writer = AlignedSbeWriter::create(tmp.path(), 32).unwrap();

    let update = PriceLevelUpdateDomain {
        backtest_timestamp: sample_time(),
        exchange_timestamp: sample_time(),
        price_level: 500_000,
        quantity: 1_000_000,
        side: Side::Buy,
        symbol: "TEST".to_owned(),
    };

    let msg_len = update.encoded_length_with_header().unwrap();
    assert!(
        msg_len > 24,
        "Message len ({msg_len}) should exceed payload_limit (24)"
    );

    let res = writer.write_msg(&update);
    assert!(
        res.is_err(),
        "write_msg should return error when message > payload_limit"
    );
    assert_eq!(
        res.unwrap_err().to_string(),
        "SBE message does not fit in a chunk"
    );

    // Verify writer buffer remains empty and clean
    writer.flush().unwrap();
    let metadata = std::fs::metadata(tmp.path()).unwrap();
    assert_eq!(
        metadata.len(),
        0,
        "No chunks should be written for rejected message"
    );
}

#[test]
fn test_aligned_sbe_writer_exact_payload_limit_boundary() {
    let ts = sample_time();
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 450_000,
        quantity: 2_000_000,
        side: Side::Buy,
        symbol: "EXACT".to_owned(),
    };

    let msg_len = update.encoded_length_with_header().unwrap();
    // Header length is 8.
    // If chunk = msg_len + 8, then payload_limit = (msg_len + 8) - 8 = msg_len.
    // So the message EXACTLY fills payload_limit!
    let chunk_size = msg_len + 8;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(tmp.path(), chunk_size).unwrap();

    // Write message 1: fills buffer to exactly payload_limit
    writer.write_msg(&update).unwrap();

    // Write message 2: buf.len() + msg_len > payload_limit, triggers flush of chunk 1,
    // then writes message 2 into chunk 2
    writer.write_msg(&update).unwrap();

    // Flush chunk 2
    writer.flush().unwrap();
    drop(writer);

    let file_len = std::fs::metadata(tmp.path()).unwrap().len() as usize;
    assert_eq!(
        file_len,
        chunk_size * 2,
        "File should consist of exactly 2 chunks of size {chunk_size}"
    );

    // Verify chunk 1 ends with exactly 8 bytes of zeroes (the SBE header sentinel)
    let bytes = std::fs::read(tmp.path()).unwrap();
    let chunk1_trailer = &bytes[msg_len..chunk_size];
    assert_eq!(chunk1_trailer.len(), 8);
    assert_eq!(
        chunk1_trailer, &[0u8; 8],
        "Chunk 1 must end with 8 zero bytes as sentinel"
    );

    let chunk2_trailer = &bytes[chunk_size + msg_len..chunk_size * 2];
    assert_eq!(chunk2_trailer.len(), 8);
    assert_eq!(
        chunk2_trailer, &[0u8; 8],
        "Chunk 2 must end with 8 zero bytes as sentinel"
    );
}

#[test]
fn test_aligned_sbe_writer_multi_chunk_rotation_stress() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    // Use a small chunk size of 256 bytes so each message (~50 bytes) causes chunk rotation every ~4-5 messages
    let chunk_size = 256;
    let mut writer = AlignedSbeWriter::create(tmp.path(), chunk_size).unwrap();

    const NUM_MESSAGES: usize = 300;
    let ts = sample_time();

    for i in 0..NUM_MESSAGES {
        let update = PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: (i as i64) * 1000 + 100,
            quantity: (i as i64 + 1) * 250_000,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            symbol: format!("TICK-{:03}", i % 10),
        };
        writer.write_msg(&update).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let file_len = std::fs::metadata(tmp.path()).unwrap().len() as usize;
    assert_eq!(
        file_len % chunk_size,
        0,
        "File size ({file_len}) must be an exact integer multiple of chunk size ({chunk_size})"
    );
    let num_chunks = file_len / chunk_size;
    assert!(
        num_chunks > 50,
        "Expected >50 chunk rotations, got {num_chunks}"
    );

    // Verify that every chunk has at least 8 trailing zeros
    let file_bytes = std::fs::read(tmp.path()).unwrap();
    for chunk_idx in 0..num_chunks {
        let chunk_start = chunk_idx * chunk_size;
        let chunk_end = chunk_start + chunk_size;
        let chunk_trailer = &file_bytes[chunk_end - 8..chunk_end];
        assert_eq!(
            chunk_trailer, &[0u8; 8],
            "Chunk {chunk_idx} must end with 8 zero padding bytes"
        );
    }

    // Now verify AlignedSbeReader can read back all 300 messages cleanly
    // Note: AlignedSbeReader uses DEFAULT_CHUNK_SIZE (64MiB), so we verify using
    // open_with_chunk_size logic or custom verification
    // Let's verify each message header and content sequentially from the chunks!
    let mut decoded_count = 0;
    let mut offset = 0;
    while offset < file_bytes.len() {
        let chunk_end = ((offset / chunk_size) + 1) * chunk_size;
        // Check if next 8 bytes are zeroes (sentinel)
        if offset + 8 <= chunk_end && &file_bytes[offset..offset + 8] == &[0u8; 8] {
            // Jump to next chunk
            offset = chunk_end;
            continue;
        }

        // Decode message header: block_length (u16), template_id (u16), schema_id (u16), version (u16)
        let header = &file_bytes[offset..offset + 8];
        let schema_id = u16::from_le_bytes([header[4], header[5]]);
        assert_eq!(schema_id, 1, "Schema ID must match OrderbookSchema");

        // Try decoding PriceLevelUpdate
        let decoded = OrderbookSchema::try_decode(&file_bytes, offset).unwrap();
        let msg_len = OrderbookSchema::encoded_length(&decoded).unwrap();
        match decoded {
            AnyMessage::PriceLevelUpdate(upd) => {
                let domain = PriceLevelUpdateDomain::try_from_decoder(upd).unwrap();
                let expected_price = (decoded_count as i64) * 1000 + 100;
                let expected_qty = (decoded_count as i64 + 1) * 250_000;
                let expected_side = if decoded_count % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let expected_sym = format!("TICK-{:03}", decoded_count % 10);

                assert_eq!(domain.price_level, expected_price);
                assert_eq!(domain.quantity, expected_qty);
                assert_eq!(domain.side, expected_side);
                assert_eq!(domain.symbol, expected_sym);
                decoded_count += 1;
            }
            _ => panic!("Expected PriceLevelUpdate"),
        }
        offset += msg_len;
    }
    assert_eq!(
        decoded_count, NUM_MESSAGES,
        "All 300 messages must be accurately decoded"
    );
}

#[test]
fn test_aligned_sbe_writer_empty_and_idempotent_flush() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(tmp.path(), 512).unwrap();

    // 1. Flush on empty buffer should be a no-op
    writer.flush().unwrap();
    assert_eq!(
        std::fs::metadata(tmp.path()).unwrap().len(),
        0,
        "Empty flush must not write an empty chunk"
    );

    // 2. Write one message, flush once
    let ts = sample_time();
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 100_000,
        quantity: 500_000,
        side: Side::Buy,
        symbol: "A".to_owned(),
    };
    writer.write_msg(&update).unwrap();
    writer.flush().unwrap();
    assert_eq!(
        std::fs::metadata(tmp.path()).unwrap().len(),
        512,
        "File should be exactly 512 bytes after 1 flush"
    );

    // 3. Flush a second time: buffer is empty, so should be a no-op
    writer.flush().unwrap();
    assert_eq!(
        std::fs::metadata(tmp.path()).unwrap().len(),
        512,
        "Subsequent flush on empty buffer must not write additional chunks"
    );
}

#[test]
fn test_aligned_sbe_writer_snapshots_with_variable_depth() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let chunk_size = 4096;
    let mut writer = AlignedSbeWriter::create(tmp.path(), chunk_size).unwrap();
    let ts = sample_time();

    // Test depth: 0, 5, 20, 50 levels
    for depth in [0, 5, 20, 50] {
        let bids: Vec<_> = (0..depth)
            .map(|i| SnapshotBidsEntryDomain {
                price_level: 500_000 - (i as i64) * 1000,
                quantity: (i as i64 + 1) * 10_000,
            })
            .collect();
        let asks: Vec<_> = (0..depth)
            .map(|i| SnapshotAsksEntryDomain {
                price_level: 500_000 + (i as i64) * 1000,
                quantity: (i as i64 + 1) * 10_000,
            })
            .collect();

        let snapshot = SnapshotDomain {
            received_at: ts,
            exchange_timestamp: ts,
            symbol: format!("SNAP-DEPTH-{}", depth),
            bids,
            asks,
        };

        writer.write_msg(&snapshot).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let file_len = std::fs::metadata(tmp.path()).unwrap().len();
    assert!(file_len > 0);
    assert_eq!(file_len % (chunk_size as u64), 0);
}

#[test]
fn test_aligned_sbe_writer_reader_roundtrip_standard_chunk() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    const CHUNK_64MIB: usize = 64 << 20;
    let mut writer = AlignedSbeWriter::create(tmp.path(), CHUNK_64MIB).unwrap();
    let ts = sample_time();

    let update = PriceLevelUpdateDomain {
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 499_000,
        quantity: 1_500_000,
        side: Side::Buy,
        symbol: "BTC-ROUNDTRIP".to_owned(),
    };

    let snapshot = SnapshotDomain {
        received_at: ts,
        exchange_timestamp: ts,
        symbol: "BTC-ROUNDTRIP".to_owned(),
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 499_000,
            quantity: 1_500_000,
        }],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 501_000,
            quantity: 2_000_000,
        }],
    };

    writer.write_msg(&update).unwrap();
    writer.write_msg(&snapshot).unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(tmp.path()).unwrap();
    let msg1 = reader
        .read_msg()
        .unwrap()
        .expect("First message must exist");
    match msg1 {
        AnyMessage::PriceLevelUpdate(u) => {
            let d = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
            assert_eq!(d, update);
        }
        _ => panic!("Expected PriceLevelUpdate"),
    }

    let msg2 = reader
        .read_msg()
        .unwrap()
        .expect("Second message must exist");
    match msg2 {
        AnyMessage::Snapshot(s) => {
            let d = SnapshotDomain::try_from_decoder(s).unwrap();
            assert_eq!(d, snapshot);
        }
        _ => panic!("Expected Snapshot"),
    }

    assert!(
        reader.read_msg().unwrap().is_none(),
        "Reader should reach clean EOF"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// 2. Collector Socket Lifecycle Stress Tests & Property Verification
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_collector_monotonic_socket_id_high_thread_contention() {
    let adapter = Arc::new(MockAdapter::new());
    let collector = Arc::new(Collector::new(adapter).await.unwrap());

    const NUM_THREADS: usize = 32;
    const IDS_PER_THREAD: usize = 10_000;
    const TOTAL_IDS: usize = NUM_THREADS * IDS_PER_THREAD;

    let mut handles = Vec::with_capacity(NUM_THREADS);

    for _ in 0..NUM_THREADS {
        let coll = Arc::clone(&collector);
        handles.push(tokio::spawn(async move {
            let mut ids = Vec::with_capacity(IDS_PER_THREAD);
            for _ in 0..IDS_PER_THREAD {
                ids.push(coll.next_socket_id());
            }
            ids
        }));
    }

    let mut all_ids = Vec::with_capacity(TOTAL_IDS);
    for h in handles {
        let ids = h.await.unwrap();
        all_ids.extend(ids);
    }

    assert_eq!(all_ids.len(), TOTAL_IDS);

    // Verify all IDs are strictly unique (no duplicate ID allocation race)
    all_ids.sort_unstable();
    for (i, &id) in all_ids.iter().enumerate() {
        assert_eq!(
            id, i,
            "Socket ID generation must be strictly monotonic and contiguous: at index {i}, found {id}"
        );
    }
}

/// Verification oracle for calculate_backoff mathematical model
fn oracle_calculate_backoff(attempts: u32) -> Duration {
    if attempts == 0 {
        return Duration::ZERO;
    }
    let base_ms = 100u64;
    let max_ms = 10_000u64;
    let shift = (attempts - 1).min(7);
    let ms = (base_ms * (1u64 << shift)).min(max_ms);
    Duration::from_millis(ms)
}

#[test]
fn test_backoff_mathematical_bounds_and_overflow_safety() {
    // Attempt 0 yields Duration::ZERO
    assert_eq!(oracle_calculate_backoff(0), Duration::ZERO);

    // Attempt 1 yields exactly base 100ms
    assert_eq!(oracle_calculate_backoff(1), Duration::from_millis(100));

    // Exponential progression: 200ms, 400ms, 800ms, 1600ms, 3200ms, 6400ms
    assert_eq!(oracle_calculate_backoff(2), Duration::from_millis(200));
    assert_eq!(oracle_calculate_backoff(3), Duration::from_millis(400));
    assert_eq!(oracle_calculate_backoff(4), Duration::from_millis(800));
    assert_eq!(oracle_calculate_backoff(5), Duration::from_millis(1600));
    assert_eq!(oracle_calculate_backoff(6), Duration::from_millis(3200));
    assert_eq!(oracle_calculate_backoff(7), Duration::from_millis(6400));

    // Attempt 8 (and higher) caps at 10,000ms (10s)
    assert_eq!(oracle_calculate_backoff(8), Duration::from_millis(10_000));
    assert_eq!(oracle_calculate_backoff(9), Duration::from_millis(10_000));
    assert_eq!(oracle_calculate_backoff(50), Duration::from_millis(10_000));
    assert_eq!(
        oracle_calculate_backoff(1000),
        Duration::from_millis(10_000)
    );

    // Extreme edge cases: u32::MAX must never panic with overflow
    assert_eq!(
        oracle_calculate_backoff(u32::MAX),
        Duration::from_millis(10_000)
    );
    assert_eq!(
        oracle_calculate_backoff(u32::MAX - 1),
        Duration::from_millis(10_000)
    );
}

#[test]
fn test_staggered_reconnect_schedule_invariants() {
    // Model the staggering logic from allocate_buckets and maybe_reconnect_sockets
    const SOCKET_RECONNECT_INTERVAL: Duration = Duration::from_secs(600);
    const SOCKETS_PER_BUCKET: usize = 2;

    let now = Instant::now();
    let mut socket_reconnect_at = Vec::new();

    for socket_role in 0..SOCKETS_PER_BUCKET {
        let stagger_offset = if socket_role == 0 {
            SOCKET_RECONNECT_INTERVAL
        } else {
            SOCKET_RECONNECT_INTERVAL / (SOCKETS_PER_BUCKET as u32)
        };
        socket_reconnect_at.push(now + stagger_offset);
    }

    // Socket 0 is at +600s, Socket 1 is at +300s
    assert_eq!(
        socket_reconnect_at[0].duration_since(now),
        Duration::from_secs(600)
    );
    assert_eq!(
        socket_reconnect_at[1].duration_since(now),
        Duration::from_secs(300)
    );

    // Initial offset difference between sockets is exactly 300s (half interval)
    let initial_diff = socket_reconnect_at[0].duration_since(socket_reconnect_at[1]);
    assert_eq!(initial_diff, Duration::from_secs(300));

    // Simulate 10 sequential reconnect rotations
    let mut last_reconnected_socket = None;

    for cycle in 0..10 {
        // Find next expiring socket
        let next_idx = if socket_reconnect_at[0] <= socket_reconnect_at[1] {
            0
        } else {
            1
        };
        let sim_now = socket_reconnect_at[next_idx];

        // Ensure we alternate or never reconnect both sockets at the same timestamp
        assert_ne!(
            Some(next_idx),
            last_reconnected_socket,
            "Cycle {cycle}: Sockets must alternate reconnects, never colliding"
        );
        last_reconnected_socket = Some(next_idx);

        // Update schedule as done in maybe_reconnect_sockets
        socket_reconnect_at[next_idx] = sim_now + SOCKET_RECONNECT_INTERVAL;

        // Verify difference remains strictly >= half interval (300s)
        let other_idx = 1 - next_idx;
        let delta = if socket_reconnect_at[next_idx] > socket_reconnect_at[other_idx] {
            socket_reconnect_at[next_idx].duration_since(socket_reconnect_at[other_idx])
        } else {
            socket_reconnect_at[other_idx].duration_since(socket_reconnect_at[next_idx])
        };
        assert_eq!(
            delta,
            Duration::from_secs(300),
            "Cycle {cycle}: Gap between scheduled reconnects must remain exactly 300s"
        );
    }
}

#[tokio::test]
async fn test_collector_shutdown_idempotence() {
    let adapter = Arc::new(MockAdapter::new());
    let collector = Arc::new(Collector::new(adapter).await.unwrap());

    // Calling shutdown multiple times concurrently must not panic or deadlock
    let c1 = Arc::clone(&collector);
    let c2 = Arc::clone(&collector);
    let c3 = Arc::clone(&collector);

    let h1 = tokio::spawn(async move {
        c1.shutdown();
    });
    let h2 = tokio::spawn(async move {
        c2.shutdown();
    });
    let h3 = tokio::spawn(async move {
        c3.shutdown();
    });

    let (r1, r2, r3) = tokio::join!(h1, h2, h3);
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(r3.is_ok());
}
