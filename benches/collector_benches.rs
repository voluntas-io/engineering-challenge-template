use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use engineering_challenge::Side;
use engineering_challenge::adapter::ExchangeAdapter;
use engineering_challenge::auth::Credential;
use engineering_challenge::kalshi::{KalshiAdapter, parse_dollars_fp};
use engineering_challenge::orderbook::Orderbook;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::SbeEncode;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};

const CHUNK_SIZE_64_MIB: usize = 64 << 20;

// Valid Ed25519 PKCS#8 PEM with 32-byte zero seed for mock/bench adapter instantiation
const BENCH_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\
-----END PRIVATE KEY-----";

fn create_bench_adapter() -> KalshiAdapter {
    let cred = Arc::new(
        Credential::from_pem("bench-key-id", BENCH_PEM)
            .expect("valid benchmark Ed25519 PEM credential"),
    );
    KalshiAdapter::new(cred)
}

fn bench_time() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
}

// ── 1. Orderbook Mutation Benchmarks ──────────────────────────────────────────

fn bench_orderbook(c: &mut Criterion) {
    let mut group = c.benchmark_group("orderbook");

    // 1.1 Insert 1,000 distinct price levels
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("delta_insert_new", |b| {
        // Pre-compute 1,000 alternating bid/ask level updates
        let deltas: Vec<(Side, i64, i64, u64)> = (0..1_000)
            .map(|i| {
                let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                let price = if side == Side::Buy {
                    500_000 - (i / 2 + 1) * 100
                } else {
                    500_000 + (i / 2 + 1) * 100
                };
                let qty = 10_000_000;
                let clock = (i + 1) as u64;
                (side, price, qty, clock)
            })
            .collect();

        b.iter_batched(
            Orderbook::default,
            |mut book| {
                for &(side, price, qty, clock) in &deltas {
                    black_box(book.apply_delta(side, price, qty, clock));
                }
                black_box(book)
            },
            BatchSize::SmallInput,
        );
    });

    // 1.2 Update existing 50 bid and 50 ask levels (100 total updates)
    group.throughput(Throughput::Elements(100));
    group.bench_function("delta_update_existing", |b| {
        // Build base book with 50 bids and 50 asks
        let mut base_book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=50 {
            base_book.apply_delta(Side::Buy, 500_000 - i * 1_000, 10_000_000, clock);
            clock += 1;
            base_book.apply_delta(Side::Sell, 500_000 + i * 1_000, 10_000_000, clock);
            clock += 1;
        }

        let updates: Vec<(Side, i64, i64, u64)> = (1..=50)
            .flat_map(|i| {
                vec![
                    (Side::Buy, 500_000 - i * 1_000, 500_000, 100 + i as u64),
                    (Side::Sell, 500_000 + i * 1_000, 500_000, 150 + i as u64),
                ]
            })
            .collect();

        b.iter_batched(
            || {
                (
                    base_book.bids.clone(),
                    base_book.asks.clone(),
                    base_book.clock,
                )
            },
            |(bids, asks, clock)| {
                let mut book = Orderbook { bids, asks, clock };
                for &(side, price, delta_qty, clock) in &updates {
                    black_box(book.apply_delta(side, price, delta_qty, clock));
                }
                black_box(book)
            },
            BatchSize::SmallInput,
        );
    });

    // 1.3 Delete price levels via negative quantity deltas (100 deletes)
    group.throughput(Throughput::Elements(100));
    group.bench_function("delta_delete_level", |b| {
        let mut base_book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=50 {
            base_book.apply_delta(Side::Buy, 500_000 - i * 1_000, 10_000_000, clock);
            clock += 1;
            base_book.apply_delta(Side::Sell, 500_000 + i * 1_000, 10_000_000, clock);
            clock += 1;
        }

        let deletes: Vec<(Side, i64, i64, u64)> = (1..=50)
            .flat_map(|i| {
                vec![
                    (Side::Buy, 500_000 - i * 1_000, -10_000_000, 200 + i as u64),
                    (Side::Sell, 500_000 + i * 1_000, -10_000_000, 250 + i as u64),
                ]
            })
            .collect();

        b.iter_batched(
            || {
                (
                    base_book.bids.clone(),
                    base_book.asks.clone(),
                    base_book.clock,
                )
            },
            |(bids, asks, clock)| {
                let mut book = Orderbook { bids, asks, clock };
                for &(side, price, delta_qty, clock) in &deletes {
                    black_box(book.apply_delta(side, price, delta_qty, clock));
                }
                black_box(book)
            },
            BatchSize::SmallInput,
        );
    });

    // 1.4 Mixed burst workload: 10,000 operations (50% update, 25% insert, 25% delete)
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("delta_burst_mixed", |b| {
        // Deterministic synthetic stream generator
        let mut base_book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=100 {
            base_book.apply_delta(Side::Buy, 500_000 - i * 1_000, 10_000_000, clock);
            clock += 1;
            base_book.apply_delta(Side::Sell, 500_000 + i * 1_000, 10_000_000, clock);
            clock += 1;
        }

        // Generate 10,000 deterministic operations
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
                // 50% Modify existing level
                let idx = (next_rng() % 100 + 1) as i64;
                let price = if is_buy {
                    500_000 - idx * 1_000
                } else {
                    500_000 + idx * 1_000
                };
                let delta_qty = if (next_rng() % 2) == 0 {
                    500_000
                } else {
                    -500_000
                };
                operations.push((side, price, delta_qty, current_clock));
            } else if roll < 75 {
                // 25% Insert new level
                let offset = (101 + (next_rng() % 200)) as i64;
                let price = if is_buy {
                    500_000 - offset * 1_000
                } else {
                    500_000 + offset * 1_000
                };
                operations.push((side, price, 5_000_000, current_clock));
            } else {
                // 25% Delete level (negative large quantity)
                let idx = (next_rng() % 100 + 1) as i64;
                let price = if is_buy {
                    500_000 - idx * 1_000
                } else {
                    500_000 + idx * 1_000
                };
                operations.push((side, price, -500_000_000, current_clock));
            }
        }

        b.iter_batched(
            || {
                (
                    base_book.bids.clone(),
                    base_book.asks.clone(),
                    base_book.clock,
                )
            },
            |(bids, asks, clock)| {
                let mut book = Orderbook { bids, asks, clock };
                for &(side, price, delta_qty, clock) in &operations {
                    black_box(book.apply_delta(side, price, delta_qty, clock));
                }
                black_box(book)
            },
            BatchSize::LargeInput,
        );
    });

    // 1.5 Snapshot ingestion: 100 bids + 100 asks replacing existing book
    group.throughput(Throughput::Elements(200));
    group.bench_function("snapshot_ingestion", |b| {
        let mut base_book = Orderbook::default();
        for i in 1..=50 {
            base_book.apply_delta(Side::Buy, 500_000 - i * 1_000, 10_000_000, i as u64);
            base_book.apply_delta(Side::Sell, 500_000 + i * 1_000, 10_000_000, (50 + i) as u64);
        }

        let new_bids: Vec<(i64, i64)> = (1..=100)
            .map(|i| (500_000 - i * 500, 1_000_000 * i))
            .collect();
        let new_asks: Vec<(i64, i64)> = (1..=100)
            .map(|i| (500_000 + i * 500, 1_000_000 * i))
            .collect();

        b.iter_batched(
            || (base_book.bids.clone(), base_book.asks.clone(), 100u64),
            |(bids, asks, clock)| {
                let mut book = Orderbook { bids, asks, clock };
                book.apply_snapshot_side(Side::Buy, &new_bids, clock + 1);
                book.apply_snapshot_side(Side::Sell, &new_asks, clock + 2);
                black_box(book)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ── 2. Snapshot Extraction Benchmarks ─────────────────────────────────────────

fn bench_snapshots(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshots");

    for &depth in &[10, 50, 100, 500] {
        let mut book = Orderbook::default();
        let mut clock = 1u64;
        for i in 1..=depth {
            book.apply_delta(Side::Buy, 500_000 - i * 100, 1_000_000, clock);
            clock += 1;
            book.apply_delta(Side::Sell, 500_000 + i * 100, 1_000_000, clock);
            clock += 1;
        }

        group.throughput(Throughput::Elements((depth * 2) as u64));
        group.bench_with_input(BenchmarkId::new("extraction", depth), &depth, |b, _| {
            b.iter(|| {
                let bids = book.bids_snapshot();
                let asks = book.asks_snapshot();
                black_box((bids, asks))
            });
        });

        // Also benchmark DTO creation (converting PriceLevel to SnapshotDomain DTOs)
        group.bench_with_input(BenchmarkId::new("dto_generation", depth), &depth, |b, _| {
            b.iter(|| {
                let bids: Vec<SnapshotBidsEntryDomain> = book
                    .bids
                    .iter()
                    .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect();
                let asks: Vec<SnapshotAsksEntryDomain> = book
                    .asks
                    .iter()
                    .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect();
                let snap = SnapshotDomain {
                    symbol: "KXFED-24DEC-T5.00".to_owned(),
                    received_at: bench_time(),
                    exchange_timestamp: bench_time(),
                    bids,
                    asks,
                };
                black_box(snap)
            });
        });
    }

    group.finish();
}

// ── 3. SBE Encoding, Decoding & Writer Throughput Benchmarks ──────────────────

fn bench_sbe(c: &mut Criterion) {
    let mut group = c.benchmark_group("sbe");

    let ts = bench_time();
    let symbol = "KXFED-24DEC-T5.00".to_owned();

    let update = PriceLevelUpdateDomain {
        symbol: symbol.clone(),
        backtest_timestamp: ts,
        exchange_timestamp: ts,
        price_level: 420_000,
        quantity: 12_500_000,
        side: Side::Buy,
    };

    let snapshot_small = SnapshotDomain {
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

    let snapshot_large = SnapshotDomain {
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

    // 3.1 SBE encode PriceLevelUpdateDomain
    let update_len = update.encoded_length_with_header().unwrap();
    let mut update_buf = vec![0u8; update_len + 64];
    group.throughput(Throughput::Bytes(update_len as u64));
    group.bench_function("encode_delta", |b| {
        b.iter(|| {
            black_box(update.encode(&mut update_buf).unwrap());
        });
    });

    // 3.2 SBE encode small snapshot (5 bids, 5 asks)
    let snap_small_len = snapshot_small.encoded_length_with_header().unwrap();
    let mut snap_small_buf = vec![0u8; snap_small_len + 64];
    group.throughput(Throughput::Bytes(snap_small_len as u64));
    group.bench_function("encode_snapshot_small", |b| {
        b.iter(|| {
            black_box(snapshot_small.encode(&mut snap_small_buf).unwrap());
        });
    });

    // 3.3 SBE encode large snapshot (100 bids, 100 asks)
    let snap_large_len = snapshot_large.encoded_length_with_header().unwrap();
    let mut snap_large_buf = vec![0u8; snap_large_len + 64];
    group.throughput(Throughput::Bytes(snap_large_len as u64));
    group.bench_function("encode_snapshot_large", |b| {
        b.iter(|| {
            black_box(snapshot_large.encode(&mut snap_large_buf).unwrap());
        });
    });

    // Pre-encode buffers for decoding benchmarks
    let encoded_delta = update.to_bytes().unwrap();
    let encoded_snap_large = snapshot_large.to_bytes().unwrap();

    // 3.4 SBE decode delta payload
    group.throughput(Throughput::Bytes(encoded_delta.len() as u64));
    group.bench_function("decode_delta", |b| {
        b.iter(|| {
            black_box(AnyMessage::try_decode(&encoded_delta, 0).unwrap());
        });
    });

    // 3.5 SBE decode snapshot payload (100 levels)
    group.throughput(Throughput::Bytes(encoded_snap_large.len() as u64));
    group.bench_function("decode_snapshot", |b| {
        b.iter(|| {
            black_box(AnyMessage::try_decode(&encoded_snap_large, 0).unwrap());
        });
    });

    // 3.6 SBE Writer throughput: continuous streaming of 10,000 updates
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("writer_throughput", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::NamedTempFile::new().unwrap();
                let writer = AlignedSbeWriter::create(tmp.path(), CHUNK_SIZE_64_MIB).unwrap();
                (tmp, writer)
            },
            |(_tmp, mut writer)| {
                for _ in 0..10_000 {
                    writer.write_msg(&update).unwrap();
                }
                black_box(&mut writer);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ── 4. Wire Parsing & Fixed-Point Conversion Benchmarks ───────────────────────

fn bench_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parsing");

    // 4.1 parse_dollars_fp across diverse decimal string shapes
    let test_strings = [
        "0.42",
        "0.4200",
        "-0.50",
        "-1.5000",
        "13.00",
        "-0.000001",
        "0.000000",
        "+99.9999",
    ];

    for &s in &test_strings {
        group.bench_with_input(BenchmarkId::new("dollars_fixed_point", s), s, |b, s| {
            b.iter(|| black_box(parse_dollars_fp(s)));
        });
    }

    // 4.2 Raw JSON WebSocket delta wire parsing
    let adapter = create_bench_adapter();
    let json_delta = r#"{"type":"orderbook_delta","sid":101,"seq":54321,"msg":{"market_ticker":"KXFED-24DEC-T5.00","clock":4096,"ts_ms":1700000000123,"price_dollars":"0.4200","delta_fp":"-5.00","side":"yes"}}"#;

    group.throughput(Throughput::Bytes(json_delta.len() as u64));
    group.bench_function("kalshi_json_delta", |b| {
        b.iter(|| {
            let msg = adapter
                .decode_message(json_delta)
                .expect("JSON decode succeeds");
            black_box(msg);
        });
    });

    // 4.3 Raw JSON WebSocket snapshot wire parsing (10 bids, 10 asks)
    let json_snapshot = r#"{"type":"orderbook_snapshot","sid":101,"seq":1,"msg":{"market_ticker":"KXFED-24DEC-T5.00","clock":1000,"yes_dollars_fp":[["0.4500","10.00"],["0.4400","20.00"],["0.4300","30.00"],["0.4200","40.00"],["0.4100","50.00"]],"no_dollars_fp":[["0.5500","15.00"],["0.5600","25.00"],["0.5700","35.00"],["0.5800","45.00"],["0.5900","55.00"]]}}"#;

    group.throughput(Throughput::Bytes(json_snapshot.len() as u64));
    group.bench_function("kalshi_json_snapshot", |b| {
        b.iter(|| {
            let msg = adapter
                .decode_message(json_snapshot)
                .expect("JSON snapshot decode succeeds");
            black_box(msg);
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(250));
    targets = bench_orderbook, bench_snapshots, bench_sbe, bench_parsing
}

criterion_main!(benches);
