//! Challenger tests for Milestone 2: Microbenchmarks empirical validation.
//! Stress-tests benchmark targets for dead code elimination, depth scaling monotonicity,
//! linear time complexity, and data consistency under the benchmark workloads.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use engineering_challenge::adapter::ExchangeAdapter;
use engineering_challenge::auth::Credential;
use engineering_challenge::kalshi::{parse_dollars_fp, KalshiAdapter};
use engineering_challenge::orderbook::Orderbook;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::dtos::SbeEncode;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::Side;

const BENCH_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\
-----END PRIVATE KEY-----";

fn create_test_adapter() -> KalshiAdapter {
    let cred = Arc::new(
        Credential::from_pem("test-key-id", BENCH_PEM)
            .expect("valid benchmark Ed25519 PEM credential"),
    );
    KalshiAdapter::new(cred)
}

fn test_time() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
}

// ── 1. Depth Scaling Monotonicity & Linearity ─────────────────────────────────

#[test]
fn test_orderbook_snapshot_extraction_depth_scaling() {
    let depths = [10, 50, 100, 500];
    let iters = 20_000;

    let mut times = Vec::new();

    for &depth in &depths {
        let mut book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=depth {
            book.apply_delta(Side::Buy, 500_000 - i * 100, 1_000_000, clock);
            clock += 1;
            book.apply_delta(Side::Sell, 500_000 + i * 100, 1_000_000, clock);
            clock += 1;
        }

        // Warm up for 50ms to reach stable CPU frequency and cache state
        let warm_start = Instant::now();
        while warm_start.elapsed() < std::time::Duration::from_millis(50) {
            for _ in 0..100 {
                let b = book.bids_snapshot();
                let a = book.asks_snapshot();
                black_box((b, a));
            }
        }

        // Measure minimum of 5 sample runs to filter out OS scheduler noise
        let mut sample_elapsed = std::time::Duration::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            for _ in 0..iters {
                let bids = book.bids_snapshot();
                let asks = book.asks_snapshot();
                black_box((bids, asks));
            }
            let elapsed = start.elapsed();
            if elapsed < sample_elapsed {
                sample_elapsed = elapsed;
            }
        }
        times.push((depth, sample_elapsed));
    }

    println!("\nSnapshot Extraction Depth Scaling ({} iters):", iters);
    for (depth, elapsed) in &times {
        let per_iter_ns = elapsed.as_nanos() as f64 / iters as f64;
        let per_elem_ns = per_iter_ns / (depth * 2) as f64;
        println!(
            "  Depth {:3}: total {:?}, per iter {:6.2} ns, per level {:5.2} ns",
            depth, elapsed, per_iter_ns, per_elem_ns
        );
    }

    // 1. Verify strict monotonicity: T(10) < T(50) < T(100) < T(500)
    for i in 0..times.len() - 1 {
        assert!(
            times[i].1 < times[i + 1].1,
            "Extraction time must be strictly monotonic: depth {} ({:?}) should be < depth {} ({:?})",
            times[i].0,
            times[i].1,
            times[i + 1].0,
            times[i + 1].1
        );
    }

    // 2. Verify depth 500 takes significantly more time than depth 10 (at least 5x)
    let ratio_500_to_10 = times[3].1.as_secs_f64() / times[0].1.as_secs_f64();
    assert!(
        ratio_500_to_10 >= 5.0,
        "Depth 500 should take significantly more time than depth 10, got ratio {:.2}",
        ratio_500_to_10
    );

    // 3. Verify complexity is linear O(N), not quadratic O(N^2)
    // Per element time at depth 500 should be within 3x of per element time at depth 10
    let per_elem_10 = times[0].1.as_secs_f64() / (depths[0] * 2) as f64;
    let per_elem_500 = times[3].1.as_secs_f64() / (depths[3] * 2) as f64;
    let ratio_per_elem = per_elem_500 / per_elem_10;
    assert!(
        ratio_per_elem < 3.0,
        "Complexity should be O(N): per-element ratio between depth 500 and 10 is {:.2} (expected < 3.0)",
        ratio_per_elem
    );
}

#[test]
fn test_snapshot_dto_generation_depth_scaling() {
    let depths = [10, 50, 100, 500];
    let iters = 20_000;
    let ts = test_time();

    let mut times = Vec::new();

    for &depth in &depths {
        let mut book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=depth {
            book.apply_delta(Side::Buy, 500_000 - i * 100, 1_000_000, clock);
            clock += 1;
            book.apply_delta(Side::Sell, 500_000 + i * 100, 1_000_000, clock);
            clock += 1;
        }

        // Warm up for 50ms to reach stable CPU frequency and cache state
        let warm_start = Instant::now();
        while warm_start.elapsed() < std::time::Duration::from_millis(50) {
            for _ in 0..100 {
                let snap = SnapshotDomain {
                    symbol: "KXFED-24DEC-T5.00".to_owned(),
                    received_at: ts,
                    exchange_timestamp: ts,
                    bids: book
                        .bids
                        .iter()
                        .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                            price_level,
                            quantity,
                        })
                        .collect(),
                    asks: book
                        .asks
                        .iter()
                        .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                            price_level,
                            quantity,
                        })
                        .collect(),
                };
                black_box(snap);
            }
        }

        // Measure minimum of 5 sample runs to filter out OS scheduler noise
        let mut sample_elapsed = std::time::Duration::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            for _ in 0..iters {
                let snap = SnapshotDomain {
                    symbol: "KXFED-24DEC-T5.00".to_owned(),
                    received_at: ts,
                    exchange_timestamp: ts,
                    bids: book
                        .bids
                        .iter()
                        .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                            price_level,
                            quantity,
                        })
                        .collect(),
                    asks: book
                        .asks
                        .iter()
                        .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                            price_level,
                            quantity,
                        })
                        .collect(),
                };
                black_box(snap);
            }
            let elapsed = start.elapsed();
            if elapsed < sample_elapsed {
                sample_elapsed = elapsed;
            }
        }
        times.push((depth, sample_elapsed));
    }

    println!("\nSnapshot DTO Generation Depth Scaling ({} iters):", iters);
    for (depth, elapsed) in &times {
        let per_iter_ns = elapsed.as_nanos() as f64 / iters as f64;
        let per_elem_ns = per_iter_ns / (depth * 2) as f64;
        println!(
            "  Depth {:3}: total {:?}, per iter {:6.2} ns, per level {:5.2} ns",
            depth, elapsed, per_iter_ns, per_elem_ns
        );
    }

    // Strictly monotonic scaling
    for i in 0..times.len() - 1 {
        assert!(
            times[i].1 < times[i + 1].1,
            "DTO generation must be strictly monotonic: depth {} should be < depth {}",
            times[i].0,
            times[i + 1].0
        );
    }
}

#[test]
fn test_sbe_encode_snapshot_scaling_monotonicity() {
    let ts = test_time();
    let symbol = "KXFED-24DEC-T5.00".to_owned();

    let snap_small = SnapshotDomain {
        symbol: symbol.clone(),
        received_at: ts,
        exchange_timestamp: ts,
        bids: (0..5)
            .map(|i| SnapshotBidsEntryDomain {
                price_level: 450_000 - i * 1_000,
                quantity: 2_000_000,
            })
            .collect(),
        asks: (0..5)
            .map(|i| SnapshotAsksEntryDomain {
                price_level: 550_000 + i * 1_000,
                quantity: 3_000_000,
            })
            .collect(),
    };

    let snap_large = SnapshotDomain {
        symbol: symbol.clone(),
        received_at: ts,
        exchange_timestamp: ts,
        bids: (0..100)
            .map(|i| SnapshotBidsEntryDomain {
                price_level: 490_000 - i * 100,
                quantity: 1_000_000 + i * 10_000,
            })
            .collect(),
        asks: (0..100)
            .map(|i| SnapshotAsksEntryDomain {
                price_level: 510_000 + i * 100,
                quantity: 1_000_000 + i * 10_000,
            })
            .collect(),
    };

    let iters = 50_000;
    let mut small_buf = vec![0u8; snap_small.encoded_length_with_header().unwrap() + 64];
    let mut large_buf = vec![0u8; snap_large.encoded_length_with_header().unwrap() + 64];

    let start_small = Instant::now();
    for _ in 0..iters {
        black_box(snap_small.encode(&mut small_buf).unwrap());
    }
    let elapsed_small = start_small.elapsed();

    let start_large = Instant::now();
    for _ in 0..iters {
        black_box(snap_large.encode(&mut large_buf).unwrap());
    }
    let elapsed_large = start_large.elapsed();

    println!(
        "\nSBE Snapshot Encoding: small (10 levels) = {:?}, large (200 levels) = {:?}",
        elapsed_small, elapsed_large
    );

    assert!(
        elapsed_large > elapsed_small,
        "Encoding 200 levels must take longer than 10 levels"
    );
    let ratio = elapsed_large.as_secs_f64() / elapsed_small.as_secs_f64();
    assert!(
        ratio >= 2.0,
        "Large snapshot encoding should take at least 2x small snapshot, got {:.2}x",
        ratio
    );
}

// ── 2. Workload Correctness and Data Invariant Verification ───────────────────

#[test]
fn test_benchmark_delta_burst_workload_invariants() {
    // Replicate the exact 10,000 operation workload generator from benches/collector_benches.rs
    let mut base_book = Orderbook::default();
    let mut clock = 1u64;
    for i in 1..=100 {
        base_book.apply_delta(Side::Buy, 500_000 - i * 1_000, 10_000_000, clock);
        clock += 1;
        base_book.apply_delta(Side::Sell, 500_000 + i * 1_000, 10_000_000, clock);
        clock += 1;
    }

    let mut rng_state = 123456789u64;
    let mut next_rng = || -> u64 {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        rng_state
    };

    let mut operations: Vec<(Side, i64, i64, u64)> = Vec::with_capacity(10_000);
    let mut current_clock = clock;
    for _ in 0..10_000 {
        current_clock += 1;
        let roll = next_rng() % 100;
        let is_buy = (next_rng() % 2) == 0;
        let side = if is_buy { Side::Buy } else { Side::Sell };

        if roll < 50 {
            let idx = (next_rng() % 100 + 1) as i64;
            let price = if is_buy {
                500_000 - idx * 1_000
            } else {
                500_000 + idx * 1_000
            };
            let delta_qty = if (next_rng() % 2) == 0 { 500_000 } else { -500_000 };
            operations.push((side, price, delta_qty, current_clock));
        } else if roll < 75 {
            let offset = (101 + (next_rng() % 200)) as i64;
            let price = if is_buy {
                500_000 - offset * 1_000
            } else {
                500_000 + offset * 1_000
            };
            operations.push((side, price, 5_000_000, current_clock));
        } else {
            let idx = (next_rng() % 100 + 1) as i64;
            let price = if is_buy {
                500_000 - idx * 1_000
            } else {
                500_000 + idx * 1_000
            };
            operations.push((side, price, -500_000_000, current_clock));
        }
    }

    let mut book = Orderbook {
        bids: base_book.bids.clone(),
        asks: base_book.asks.clone(),
        clock: base_book.clock,
    };

    for &(side, price, delta_qty, clock) in &operations {
        book.apply_delta(side, price, delta_qty, clock);
    }

    // Invariant 1: Clock advanced to the last operation clock
    assert_eq!(book.clock, current_clock);

    // Invariant 2: All remaining bid quantities must be strictly positive
    for (&price, &qty) in &book.bids {
        assert!(
            qty > 0,
            "Bid price {} had invalid quantity {}",
            price,
            qty
        );
    }

    // Invariant 3: All remaining ask quantities must be strictly positive
    for (&price, &qty) in &book.asks {
        assert!(
            qty > 0,
            "Ask price {} had invalid quantity {}",
            price,
            qty
        );
    }

    // Invariant 4: Best bid < Best ask (no inverted market cross from synthetic workload)
    if let (Some((best_bid_p, _)), Some((best_ask_p, _))) = (book.best_bid(), book.best_ask()) {
        assert!(
            best_bid_p < best_ask_p,
            "Synthetic workload should maintain non-crossed book: best_bid={}, best_ask={}",
            best_bid_p,
            best_ask_p
        );
    }
}

// ── 3. Empirical Dead Code Elimination Resistance ─────────────────────────────

#[test]
fn test_no_dead_code_elimination_in_benchmarked_paths() {
    // 1. parse_dollars_fp must compute real values
    let test_strings = [
        ("0.42", Some(420_000)),
        ("0.4200", Some(420_000)),
        ("-0.50", Some(-500_000)),
        ("-1.5000", Some(-1_500_000)),
        ("13.00", Some(13_000_000)),
        ("-0.000001", Some(-1)),
        ("0.000000", Some(0)),
        ("+99.9999", Some(99_999_900)),
    ];

    for &(s, expected) in &test_strings {
        let parsed = parse_dollars_fp(black_box(s));
        assert_eq!(parsed, expected, "Mismatch for string {}", s);
    }

    // 2. decode_message must produce genuine InboundMessage
    let adapter = create_test_adapter();
    let json_delta = r#"{"type":"orderbook_delta","sid":101,"seq":54321,"msg":{"market_ticker":"KXFED-24DEC-T5.00","clock":4096,"ts_ms":1700000000123,"price":"0.4200","delta":"-5.00","side":"yes"}}"#;
    let decoded = adapter
        .decode_message(black_box(json_delta))
        .expect("decode succeeds");
    assert!(decoded.is_some());

    // 3. AlignedSbeWriter must actually write bytes
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(tmp.path(), 64 << 20).unwrap();
    let update = PriceLevelUpdateDomain {
        symbol: "KXFED-24DEC-T5.00".to_owned(),
        backtest_timestamp: test_time(),
        exchange_timestamp: test_time(),
        price_level: 420_000,
        quantity: 12_500_000,
        side: Side::Buy,
    };
    writer.write_msg(&update).unwrap();
    writer.flush().unwrap();

    let file_len = std::fs::metadata(tmp.path()).unwrap().len();
    assert_eq!(file_len, 64 << 20, "Writer must flush 64 MiB chunk");
}

#[test]
fn test_sbe_decode_integrity() {
    let update = PriceLevelUpdateDomain {
        symbol: "KXFED-24DEC-T5.00".to_owned(),
        backtest_timestamp: test_time(),
        exchange_timestamp: test_time(),
        price_level: 420_000,
        quantity: 12_500_000,
        side: Side::Buy,
    };
    let bytes = update.to_bytes().unwrap();
    let any = AnyMessage::try_decode(&bytes, 0).expect("valid decode");
    match any {
        AnyMessage::PriceLevelUpdate(dec) => {
            assert_eq!(dec.price_level(), 420_000);
            assert_eq!(dec.quantity(), 12_500_000);
        }
        _ => panic!("Expected PriceLevelUpdate message"),
    }
}
