//! Proptest property testing suite for Milestone 3:
//! - Wire parsing (`parse_dollars_fp`, `parse_qty_fp`, decimal strings, fuzzing, roundtrip precision)
//! - Side enum mapping & Kalshi complement mathematics
//! - Orderbook state invariants (non-negativity, strict price ordering, zero-quantity removal)
//! - Sequence handling and clock monotonicity protection
//! - SBE serialization roundtrips (PriceLevelUpdateDomain, SnapshotDomain)
//! - Aligned SBE chunk boundary framing & zero-padded chunk streaming

use chrono::{DateTime, Utc};
use proptest::prelude::*;
use std::collections::BTreeMap;

use engineering_challenge::adapter::{ExchangeAdapter, InboundMessage};
use engineering_challenge::auth::Credential;
use engineering_challenge::kalshi::KalshiAdapter;
use engineering_challenge::kalshi::{parse_dollars_fp, parse_qty_fp};
use engineering_challenge::orderbook::Orderbook;
use engineering_challenge::util::dtos::SbeEncode;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};
use engineering_challenge::{InvalidSide, Side};
use std::sync::Arc;

const BENCH_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\
-----END PRIVATE KEY-----";

fn test_adapter() -> KalshiAdapter {
    let cred = Arc::new(Credential::from_pem("test-key-id", BENCH_PEM).expect("valid Ed25519 PEM"));
    KalshiAdapter::new(cred)
}

// ── Strategy Helpers ──────────────────────────────────────────────────────────

fn valid_dollar_string_strategy() -> impl Strategy<Value = (String, i64)> {
    (
        any::<bool>(),   // negative
        0..2u8,          // sign prefix: 0 = none, 1 = explicit sign (+ or -)
        0..10_000i64,    // integer part
        0..=8usize,      // number of fractional digits
        0..1_000_000i64, // fractional value
    )
        .prop_map(|(negative, sign_style, int_val, frac_len, frac_val)| {
            let sign_str = if negative {
                "-"
            } else if sign_style == 1 {
                "+"
            } else {
                ""
            };

            if frac_len == 0 {
                let s = format!("{}{}", sign_str, int_val);
                let val = if negative {
                    -int_val * 1_000_000
                } else {
                    int_val * 1_000_000
                };
                (s, val)
            } else {
                let frac_str_full = format!("{:06}", frac_val);
                let frac_slice: String = if frac_len <= 6 {
                    frac_str_full[..frac_len].to_string()
                } else {
                    let extra_zeros = "0".repeat(frac_len - 6);
                    format!("{}{}", frac_str_full, extra_zeros)
                };

                let s = format!("{}{}.{}", sign_str, int_val, frac_slice);

                // Calculate expected fixed-point value
                let mut multiplier: i64 = 100_000;
                let mut expected_frac: i64 = 0;
                for b in frac_slice.bytes().take(6) {
                    expected_frac += (b - b'0') as i64 * multiplier;
                    multiplier /= 10;
                }

                let total_abs = int_val * 1_000_000 + expected_frac;
                let expected = if negative { -total_abs } else { total_abs };
                (s, expected)
            }
        })
}

fn valid_timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate valid timestamp in microseconds after Unix epoch
    (1_000_000_000_000_000i64..=2_000_000_000_000_000i64).prop_map(|micros| {
        DateTime::from_timestamp_micros(micros).expect("valid microsecond timestamp")
    })
}

fn symbol_strategy() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Z0-9_-]{1,24}")
}

// ── 1. Message & Wire Parsing Properties ──────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(150))]

    #[test]
    fn test_prop_parse_dollars_fp_valid_decimals((s, expected) in valid_dollar_string_strategy()) {
        let parsed = parse_dollars_fp(&s);
        prop_assert_eq!(
            parsed,
            Some(expected),
            "Failed parsing dollar string {:?}: expected Some({}), got {:?}",
            s,
            expected,
            parsed
        );
    }

    #[test]
    fn test_prop_parse_dollars_fp_sign_preservation(
        int_val in 0..10_000i64,
        frac_val in 1..999_999i64,
    ) {
        let s_pos = format!("{}.{:06}", int_val, frac_val);
        let s_neg = format!("-{}.{:06}", int_val, frac_val);

        let pos_res = parse_dollars_fp(&s_pos).expect("valid positive");
        let neg_res = parse_dollars_fp(&s_neg).expect("valid negative");

        prop_assert!(pos_res > 0, "Expected positive > 0, got {}", pos_res);
        prop_assert!(neg_res < 0, "Expected negative < 0, got {}", neg_res);
        prop_assert_eq!(
            pos_res,
            -neg_res,
            "Symmetry failure: pos {} != -neg {}",
            pos_res,
            neg_res
        );
    }

    #[test]
    fn test_prop_parse_dollars_fp_roundtrip_precision(
        int_units in -50_000_000..=50_000_000i64,
        frac_micros in 0..1_000_000i64,
    ) {
        let total_fp = if int_units < 0 {
            int_units * 1_000_000 - frac_micros
        } else {
            int_units * 1_000_000 + frac_micros
        };

        let formatted = if total_fp < 0 {
            let abs_total = total_fp.unsigned_abs();
            format!("-{}.{:06}", abs_total / 1_000_000, abs_total % 1_000_000)
        } else {
            format!("{}.{:06}", total_fp / 1_000_000, total_fp % 1_000_000)
        };

        let parsed = parse_dollars_fp(&formatted);
        prop_assert_eq!(
            parsed,
            Some(total_fp),
            "Roundtrip precision mismatch for formatted {:?}: expected {}, got {:?}",
            formatted,
            total_fp,
            parsed
        );
    }

    #[test]
    fn test_prop_parse_dollars_fp_arbitrary_fuzz_never_panics(s in any::<String>()) {
        // Must never panic on arbitrary input, whether valid, invalid, or corrupted
        let _ = parse_dollars_fp(&s);
    }

    #[test]
    fn test_prop_parse_qty_fp_matches_parse_dollars_fp(s in any::<String>()) {
        prop_assert_eq!(parse_qty_fp(&s), parse_dollars_fp(&s));
    }

    #[test]
    fn test_prop_kalshi_json_delta_decoding(
        ticker in symbol_strategy(),
        clock in any::<u64>(),
        price_units in 1i64..99i64,
        delta_units in -50i64..50i64,
        is_yes in any::<bool>(),
        ts_ms in 1_700_000_000_000i64..1_800_000_000_000i64,
    ) {
        let adapter = test_adapter();
        let side_str = if is_yes { "yes" } else { "no" };
        let price_str = format!("0.{:02}", price_units);
        let delta_str = format!("{}.00", delta_units);

        let json = format!(
            r#"{{"type":"orderbook_delta","sid":10,"seq":1,"msg":{{"market_ticker":"{}","clock":{},"price_dollars":"{}","delta_fp":"{}","side":"{}","ts_ms":{}}}}}"#,
            ticker, clock, price_str, delta_str, side_str, ts_ms
        );

        let decoded = adapter.decode_message(&json).expect("decode should not error");
        let Some(InboundMessage::Delta {
            ticker: d_ticker,
            clock: d_clock,
            price: d_price,
            qty_delta: d_delta,
            side: d_side,
            exchange_ts_ms: d_ts,
        }) = decoded else {
            prop_assert!(false, "Expected Delta message");
            return Ok(());
        };

        prop_assert_eq!(&d_ticker, &ticker);
        prop_assert_eq!(d_clock, clock);
        let expected_raw_price = price_units * 10_000;
        let expected_price = if is_yes {
            expected_raw_price
        } else {
            1_000_000 - expected_raw_price
        };
        prop_assert_eq!(d_price, expected_price);
        prop_assert_eq!(d_delta, delta_units * 1_000_000);
        let expected_side = if is_yes { Side::Buy } else { Side::Sell };
        prop_assert_eq!(d_side, expected_side);
        prop_assert_eq!(d_ts, Some(ts_ms));
    }

    #[test]
    fn test_prop_kalshi_json_arbitrary_fuzz_never_panics(s in any::<String>()) {
        let adapter = test_adapter();
        let _ = adapter.decode_message(&s);
    }
}

// ── 2. Side Enum & Kalshi Complement Math Properties ──────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(150))]

    #[test]
    fn test_prop_side_try_from_byte(b in any::<u8>()) {
        let side_res = Side::try_from(b);
        match b {
            0 => {
                prop_assert_eq!(side_res, Ok(Side::Buy));
                prop_assert_eq!(Side::Buy as u8, 0);
                prop_assert_eq!(format!("{}", Side::Buy), "Buy");
            }
            1 => {
                prop_assert_eq!(side_res, Ok(Side::Sell));
                prop_assert_eq!(Side::Sell as u8, 1);
                prop_assert_eq!(format!("{}", Side::Sell), "Sell");
            }
            invalid => {
                prop_assert_eq!(side_res, Err(InvalidSide(invalid)));
                prop_assert_eq!(
                    format!("{}", InvalidSide(invalid)),
                    format!("invalid side {}; expected 0 (Buy) or 1 (Sell)", invalid)
                );
            }
        }
    }

    #[test]
    fn test_prop_kalshi_complement_math(price in 1i64..1_000_000i64) {
        // Kalshi NO-bid complement is: 1_000_000 - price
        let complement = 1_000_000 - price;

        // Invariant 1: Inversion involution (complement of complement is identity)
        prop_assert_eq!(
            1_000_000 - complement,
            price,
            "Involution failed: complement of complement did not restore price"
        );

        // Invariant 2: Complemented price strictly within bounds (0, 1_000_000)
        prop_assert!(
            complement > 0 && complement < 1_000_000,
            "Complement {} out of bounds for price {}",
            complement,
            price
        );

        // Invariant 3: Sum of price and complement is exactly $1.00 (1_000_000)
        prop_assert_eq!(price + complement, 1_000_000);
    }
}

// ── 3. Orderbook State Invariants & Delta Applications ─────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(120))]

    #[test]
    fn test_prop_orderbook_deltas_maintain_non_negativity_and_sorting(
        deltas in prop::collection::vec(
            (
                prop::sample::select(vec![Side::Buy, Side::Sell]),
                1i64..100i64,            // small price range to trigger updates and collisions
                -2_000_000i64..2_000_000i64, // signed delta quantity
            ),
            20..100,
        )
    ) {
        let mut book = Orderbook::default();
        let mut oracle_bids: BTreeMap<i64, i64> = BTreeMap::new();
        let mut oracle_asks: BTreeMap<i64, i64> = BTreeMap::new();
        let mut clock = 100u64;

        for (side, price, delta_qty) in deltas {
            clock += 1;
            let prev_qty = book.apply_delta(side, price, delta_qty, clock);

            // Oracle update
            let oracle_map = match side {
                Side::Buy => &mut oracle_bids,
                Side::Sell => &mut oracle_asks,
            };
            let expected_prev = oracle_map.get(&price).copied().unwrap_or(0);
            prop_assert_eq!(
                prev_qty,
                expected_prev,
                "apply_delta returned incorrect previous quantity"
            );

            let new_oracle_qty = expected_prev + delta_qty;
            if new_oracle_qty <= 0 {
                oracle_map.remove(&price);
            } else {
                oracle_map.insert(price, new_oracle_qty);
            }

            // Invariant 1: State matches oracle
            prop_assert_eq!(&book.bids, &oracle_bids, "Book bids diverged from oracle");
            prop_assert_eq!(&book.asks, &oracle_asks, "Book asks diverged from oracle");

            // Invariant 2: Non-negativity — no price level ever has quantity <= 0
            for (&p, &q) in book.bids.iter() {
                prop_assert!(
                    q > 0,
                    "Bid level at price {} has non-positive quantity {}",
                    p,
                    q
                );
            }
            for (&p, &q) in book.asks.iter() {
                prop_assert!(
                    q > 0,
                    "Ask level at price {} has non-positive quantity {}",
                    p,
                    q
                );
            }

            // Invariant 3: Strict price ordering
            let bid_prices: Vec<i64> = book.bids.keys().copied().collect();
            for window in bid_prices.windows(2) {
                prop_assert!(
                    window[0] < window[1],
                    "Bids not strictly ascending: {} >= {}",
                    window[0],
                    window[1]
                );
            }
            let ask_prices: Vec<i64> = book.asks.keys().copied().collect();
            for window in ask_prices.windows(2) {
                prop_assert!(
                    window[0] < window[1],
                    "Asks not strictly ascending: {} >= {}",
                    window[0],
                    window[1]
                );
            }

            // Invariant 4: best_bid is maximum price in bids, best_ask is minimum in asks
            if let Some((best_p, best_q)) = book.best_bid() {
                prop_assert_eq!(
                    Some((best_p, best_q)),
                    book.bids.iter().next_back().map(|(&p, &q)| (p, q))
                );
            } else {
                prop_assert!(book.bids.is_empty());
            }

            if let Some((best_p, best_q)) = book.best_ask() {
                prop_assert_eq!(
                    Some((best_p, best_q)),
                    book.asks.iter().next().map(|(&p, &q)| (p, q))
                );
            } else {
                prop_assert!(book.asks.is_empty());
            }
        }
    }

    #[test]
    fn test_prop_orderbook_snapshot_side_replacement(
        side in prop::sample::select(vec![Side::Buy, Side::Sell]),
        initial_levels in prop::collection::vec((1i64..100i64, 1i64..1_000_000i64), 5..20),
        snapshot_levels in prop::collection::vec(
            (
                1i64..100i64,
                -500_000i64..1_000_000i64, // includes zero and negative quantities
            ),
            0..30,
        ),
    ) {
        let mut book = Orderbook::default();

        // Populate initial book
        book.apply_snapshot_side(side, &initial_levels, 10);
        let other_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        book.apply_snapshot_side(other_side, &[(500, 1000)], 10);

        // Apply new snapshot side
        book.apply_snapshot_side(side, &snapshot_levels, 20);

        // Invariant 1: The other side is completely untouched
        let other_map = match other_side {
            Side::Buy => &book.bids,
            Side::Sell => &book.asks,
        };
        prop_assert_eq!(other_map.len(), 1);
        prop_assert_eq!(other_map.get(&500), Some(&1000));

        // Invariant 2: The updated side contains only positive quantities from snapshot
        let target_map = match side {
            Side::Buy => &book.bids,
            Side::Sell => &book.asks,
        };
        for (&p, &q) in target_map.iter() {
            prop_assert!(
                q > 0,
                "Snapshot side has non-positive quantity {} at price {}",
                q,
                p
            );
        }

        // Invariant 3: Zero or negative levels in snapshot are ignored, positive levels populate the side
        let mut expected: BTreeMap<i64, i64> = BTreeMap::new();
        for &(p, q) in &snapshot_levels {
            if q > 0 {
                expected.insert(p, q);
            }
        }
        prop_assert_eq!(target_map, &expected);
    }
}

// ── 4. Sequence Handling & Clock Monotonicity Properties ──────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(150))]

    #[test]
    fn test_prop_orderbook_clock_monotonicity_and_stale_rejection(
        clock_deltas in prop::collection::vec(
            (
                1i64..50i64,             // price
                100_000i64..500_000i64,  // qty
                0u64..1000u64,           // arbitrary clock (may be stale, equal, or advance)
            ),
            20..80,
        )
    ) {
        let mut book = Orderbook { clock: 500, ..Default::default() };

        for (price, qty, clock) in clock_deltas {
            let prev_book_clock = book.clock;
            let bids_before = book.bids.clone();

            book.apply_delta(Side::Buy, price, qty, clock);

            // Invariant 1: Clock is monotonically non-decreasing
            prop_assert!(
                book.clock >= prev_book_clock,
                "Book clock decreased from {} to {}",
                prev_book_clock,
                book.clock
            );

            if clock < prev_book_clock {
                // Invariant 2: Stale clock does NOT mutate the book or the clock
                prop_assert_eq!(
                    book.clock,
                    prev_book_clock,
                    "Stale clock {} mutated book clock {}",
                    clock,
                    book.clock
                );
                prop_assert_eq!(
                    &book.bids,
                    &bids_before,
                    "Stale delta mutated book state at clock {}",
                    clock
                );
            } else {
                // Invariant 3: Equal or advancing clock updates book clock to new clock
                prop_assert_eq!(book.clock, clock);
            }
        }
    }

    #[test]
    fn test_prop_stale_snapshot_side_rejection(
        stale_clock in 0u64..500u64,
        advancing_clock in 500u64..1000u64,
    ) {
        let mut book = Orderbook::default();
        book.apply_snapshot_side(Side::Buy, &[(100, 50)], 500);
        prop_assert_eq!(book.clock, 500);

        // Attempt stale snapshot side
        book.apply_snapshot_side(Side::Buy, &[(200, 80)], stale_clock);
        prop_assert_eq!(book.clock, 500, "Stale snapshot modified clock");
        prop_assert_eq!(book.bids.get(&100), Some(&50), "Stale snapshot modified state");
        prop_assert_eq!(book.bids.get(&200), None, "Stale snapshot inserted level");

        // Advance snapshot side
        book.apply_snapshot_side(Side::Buy, &[(300, 90)], advancing_clock);
        prop_assert_eq!(book.clock, advancing_clock);
        prop_assert_eq!(book.bids.get(&300), Some(&90));
    }
}

// ── 5. SBE Serialization Roundtrip Properties ─────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_sbe_price_level_update_roundtrip(
        symbol in symbol_strategy(),
        price_level in 0i64..1_000_000_000i64,
        quantity in 0i64..10_000_000_000i64,
        side in prop::sample::select(vec![Side::Buy, Side::Sell]),
        ts in valid_timestamp_strategy(),
    ) {
        let update = PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level,
            quantity,
            side,
            symbol: symbol.clone(),
        };

        let bytes = update.to_bytes().expect("SBE encode should succeed");
        let decoded = AnyMessage::try_decode(&bytes, 0).expect("SBE decode should succeed");

        match decoded {
            AnyMessage::PriceLevelUpdate(msg) => {
                let domain = PriceLevelUpdateDomain::try_from_decoder(msg)
                    .expect("decode to PriceLevelUpdateDomain");
                prop_assert_eq!(domain, update);
            }
            _ => prop_assert!(false, "Expected AnyMessage::PriceLevelUpdate"),
        }
    }

    #[test]
    fn test_prop_sbe_snapshot_roundtrip(
        symbol in symbol_strategy(),
        ts in valid_timestamp_strategy(),
        bids in prop::collection::vec(
            (0i64..1_000_000_000i64, 0i64..10_000_000_000i64),
            0..30,
        ),
        asks in prop::collection::vec(
            (0i64..1_000_000_000i64, 0i64..10_000_000_000i64),
            0..30,
        ),
    ) {
        let bids_entries: Vec<SnapshotBidsEntryDomain> = bids
            .into_iter()
            .map(|(p, q)| SnapshotBidsEntryDomain { price_level: p, quantity: q })
            .collect();
        let asks_entries: Vec<SnapshotAsksEntryDomain> = asks
            .into_iter()
            .map(|(p, q)| SnapshotAsksEntryDomain { price_level: p, quantity: q })
            .collect();

        let snapshot = SnapshotDomain {
            symbol: symbol.clone(),
            received_at: ts,
            exchange_timestamp: ts,
            bids: bids_entries.clone(),
            asks: asks_entries.clone(),
        };

        let bytes = snapshot.to_bytes().expect("SBE snapshot encode should succeed");
        let decoded = AnyMessage::try_decode(&bytes, 0).expect("SBE snapshot decode should succeed");

        match decoded {
            AnyMessage::Snapshot(msg) => {
                let domain = SnapshotDomain::try_from_decoder(msg)
                    .expect("decode to SnapshotDomain");
                prop_assert_eq!(domain, snapshot);
            }
            _ => prop_assert!(false, "Expected AnyMessage::Snapshot"),
        }
    }
}

// ── 6. SBE Chunk Framing & Boundary Streaming Properties ──────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn test_prop_aligned_sbe_chunk_framing_across_boundaries(
        chunk_size in prop::sample::select(vec![512usize, 1024, 2048, 4096]),
        message_count in 10usize..50,
        seed in any::<u64>(),
    ) {
        let temp_file = tempfile::NamedTempFile::new().expect("create temp file");
        let path = temp_file.path().to_path_buf();

        let base_ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();

        // Generate deterministic sequence of messages
        let mut expected_updates = Vec::new();
        let mut writer = AlignedSbeWriter::create(&path, chunk_size).expect("create writer");

        for i in 0..message_count {
            let update = PriceLevelUpdateDomain {
                backtest_timestamp: base_ts,
                exchange_timestamp: base_ts,
                price_level: 400_000 + (i as i64) * 100,
                quantity: 1_000_000 + (seed as i64).abs() % 100_000 + (i as i64),
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                symbol: format!("SYM{}", i % 5),
            };
            writer.write_msg(&update).expect("write_msg should succeed");
            expected_updates.push(update);
        }

        writer.flush().expect("flush writer");
        drop(writer);

        // Verify file length is exact non-zero multiple of chunk_size
        let metadata = std::fs::metadata(&path).expect("file metadata");
        let file_len = metadata.len() as usize;
        prop_assert!(file_len > 0, "Output file must not be empty");
        prop_assert_eq!(
            file_len % chunk_size,
            0,
            "File length {} is not an exact multiple of chunk size {}",
            file_len,
            chunk_size
        );

        // Read all messages back using AlignedSbeReader
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(&path, chunk_size)
            .expect("open reader");

        let mut read_count = 0;
        while let Some(msg) = reader.read_msg().expect("read_msg should succeed") {
            prop_assert!(
                read_count < expected_updates.len(),
                "Reader produced more messages ({}) than written ({})",
                read_count + 1,
                expected_updates.len()
            );

            match msg {
                AnyMessage::PriceLevelUpdate(read_update) => {
                    let domain = PriceLevelUpdateDomain::try_from_decoder(read_update)
                        .expect("decode to domain");
                    prop_assert_eq!(domain, expected_updates[read_count].clone());
                }
                _ => prop_assert!(false, "Expected AnyMessage::PriceLevelUpdate"),
            }
            read_count += 1;
        }

        prop_assert_eq!(
            read_count,
            expected_updates.len(),
            "Reader read {} messages, expected {}",
            read_count,
            expected_updates.len()
        );
    }
}
