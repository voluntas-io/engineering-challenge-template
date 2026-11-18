//! Empirical Challenger stress tests for Milestone 1:
//! - parse_dollars_fp edge cases (negative zero, tiny fractions, boundary values, invalid strings)
//! - Orderbook::apply_delta & apply_snapshot_side (bursts of negative deltas, stale clocks, unquoted deletions, oracle model)

use engineering_challenge::Side;
use engineering_challenge::kalshi::parse_dollars_fp;
use engineering_challenge::orderbook::Orderbook;
use std::collections::HashMap;

// ============================================================================
// 1. parse_dollars_fp empirical challenge tests
// ============================================================================

#[test]
fn test_parse_negative_zero_variants() {
    let zeros = [
        "-0",
        "-0.0",
        "-0.00",
        "-0.000",
        "-0.0000",
        "-0.00000",
        "-0.000000",
        "-0.",
        "-00",
        "-00.00",
        "-000.000",
        "+0",
        "+0.0",
        "+0.0000",
        "+0.",
        "0",
        "0.0",
        "0.000000",
        "0.",
    ];
    for &s in &zeros {
        let res = parse_dollars_fp(s);
        assert_eq!(
            res,
            Some(0),
            "Expected parse_dollars_fp({:?}) to return Some(0), got {:?}",
            s,
            res
        );
    }
}

#[test]
fn test_parse_tiny_negative_fractions() {
    let cases = [
        ("-0.000001", -1),
        ("-0.000002", -2),
        ("-0.000005", -5),
        ("-0.000009", -9),
        ("-0.000010", -10),
        ("-0.00001", -10),
        ("-0.000100", -100),
        ("-0.0001", -100),
        ("-0.001000", -1_000),
        ("-0.001", -1_000),
        ("-0.010000", -10_000),
        ("-0.01", -10_000),
        ("-0.100000", -100_000),
        ("-0.1", -100_000),
        ("-0.999999", -999_999),
        ("-1.000001", -1_000_001),
        ("-0.0000001", 0),  // 7 decimal places: 6th place is 0, 7th ignored -> 0
        ("-0.0000019", -1), // 7 decimal places: 6th place is 1 micro, 7th ignored -> -1
    ];

    for &(s, expected) in &cases {
        let res = parse_dollars_fp(s);
        assert_eq!(
            res,
            Some(expected),
            "Expected parse_dollars_fp({:?}) to return Some({}), got {:?}",
            s,
            expected,
            res
        );
    }
}

#[test]
fn test_parse_boundary_values() {
    // Max positive value representable without i64 overflow:
    // (i64::MAX - 775_807) / 1_000_000 = 9_223_372_036_854
    // 9223372036854.775807 -> 9_223_372_036_854_775_807 = i64::MAX
    let max_str = "9223372036854.775807";
    assert_eq!(parse_dollars_fp(max_str), Some(i64::MAX));

    let min_approx = "-9223372036854.775807";
    assert_eq!(parse_dollars_fp(min_approx), Some(-i64::MAX));

    // Overflows
    let overflow_cases = [
        "9223372036855",
        "9223372036854775808",
        "-9223372036854775808",
        "1000000000000000000000000",
        "-1000000000000000000000000",
    ];
    for &s in &overflow_cases {
        assert_eq!(
            parse_dollars_fp(s),
            None,
            "Expected overflow for {:?} to return None",
            s
        );
    }
}

#[test]
fn test_parse_invalid_strings() {
    let invalid = [
        "",
        "   ",
        "\t\n",
        "-",
        "+",
        ".",
        "-.",
        "+.",
        "--",
        "++",
        "-+",
        "+-",
        "abc",
        "not_a_number",
        "1.2.3",
        "1..2",
        "1.0.0",
        "1.2a",
        "a1.2",
        "1-2",
        "1+2",
        "1 2",
        "1. 2",
        "NaN",
        "Infinity",
        "-Infinity",
        "null",
        "true",
        "false",
        "- 1.50", // space after sign
        "+ 1.50", // space after sign
        "1.50-",
        "1.50+",
        "$1.50",
        "1,000.00",
    ];

    for &s in &invalid {
        let res = parse_dollars_fp(s);
        assert_eq!(
            res, None,
            "Expected invalid string {:?} to return None, got {:?}",
            s, res
        );
    }
}

#[test]
fn test_parse_double_sign_anomaly_investigation() {
    // Adversarial exploration: strings with double signs like "--1.50" or "-+1.50"
    // We observe whether parse_dollars_fp accepts or rejects them.
    let double_signs = ["--1.50", "-+1.50", "+-1.50", "++1.50", "--5", "++5"];
    for &s in &double_signs {
        let res = parse_dollars_fp(s);
        // Note: As discovered in code review, int_part.parse::<i64>() can parse signed integers.
        // We log what occurs to document whether this behaves as an invalid string (None) or parses.
        println!("Adversarial input {:?} parsed as {:?}", s, res);
    }
}

#[test]
fn test_parse_fuzz_random_ascii() {
    // Deterministic LCG pseudo-random generator to test 2,000 random strings
    // Invariant: parse_dollars_fp must never panic on any input
    let mut state: u64 = 0xDEADBEEFCAFE;
    let chars = b"0123456789+-. \t\nabcxyzABCXYZ#$_";

    for _ in 0..2000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let len = (state % 20) as usize;
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let idx = (state % chars.len() as u64) as usize;
            s.push(chars[idx] as char);
        }

        // Must not panic
        let _ = parse_dollars_fp(&s);
    }
}

// ============================================================================
// 2. Orderbook empirical challenge tests
// ============================================================================

#[test]
fn test_orderbook_burst_negative_deltas_invariants() {
    let mut ob = Orderbook::default();
    let mut clock = 10u64;

    // Seed book with 20 bids and 20 asks
    let mut init_bids = Vec::new();
    let mut init_asks = Vec::new();
    for i in 1..=20 {
        let bid_price = 400_000 + i * 5_000;
        let ask_price = 600_000 + i * 5_000;
        init_bids.push((bid_price, 1_000_000));
        init_asks.push((ask_price, 1_000_000));
    }
    ob.apply_snapshot_side(Side::Buy, &init_bids, clock);
    clock += 1;
    ob.apply_snapshot_side(Side::Sell, &init_asks, clock);
    clock += 1;

    assert_eq!(ob.bids.len(), 20);
    assert_eq!(ob.asks.len(), 20);

    // Fire bursts of negative deltas targeting existing, zeroing, overshooting, and nonexistent levels
    let mut lcg: u64 = 42;
    for _ in 0..2000 {
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
        let side = if lcg.is_multiple_of(2) {
            Side::Buy
        } else {
            Side::Sell
        };
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
        let price_offset = (lcg % 40) as i64; // spans both existing and unquoted price range
        let price = 400_000 + price_offset * 5_000;
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
        // delta_qty is negative between -100_000 and -2_000_000
        let delta_qty = -((lcg % 20 + 1) as i64 * 100_000);

        clock += 1;
        let _prev = ob.apply_delta(side, price, delta_qty, clock);

        // Invariant checks after EVERY delta:
        // 1. Clock must match
        assert_eq!(ob.clock, clock);

        // 2. No non-positive quantities in bids
        for (&p, &q) in &ob.bids {
            assert!(
                q > 0,
                "Invariant violation: bid level {} has non-positive quantity {}",
                p,
                q
            );
        }

        // 3. No non-positive quantities in asks
        for (&p, &q) in &ob.asks {
            assert!(
                q > 0,
                "Invariant violation: ask level {} has non-positive quantity {}",
                p,
                q
            );
        }

        // 4. Best bid consistency
        if let Some((best_p, best_q)) = ob.best_bid() {
            assert_eq!(best_p, *ob.bids.keys().next_back().unwrap());
            assert_eq!(best_q, *ob.bids.get(&best_p).unwrap());
            assert!(best_q > 0);
        } else {
            assert!(ob.bids.is_empty());
        }

        // 5. Best ask consistency
        if let Some((best_p, best_q)) = ob.best_ask() {
            assert_eq!(best_p, *ob.asks.keys().next().unwrap());
            assert_eq!(best_q, *ob.asks.get(&best_p).unwrap());
            assert!(best_q > 0);
        } else {
            assert!(ob.asks.is_empty());
        }
    }
}

#[test]
fn test_orderbook_stale_clock_rejections() {
    let mut ob = Orderbook::default();
    ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000), (390_000, 500_000)], 100);
    assert_eq!(ob.clock, 100);

    // Delta with clock < 100: must be rejected without mutating book
    let stale_clocks = [99, 50, 1, 0];
    for &stale in &stale_clocks {
        // Delta adding quantity
        let prev = ob.apply_delta(Side::Buy, 400_000, 200_000, stale);
        assert_eq!(prev, 1_000_000);
        assert_eq!(ob.bids[&400_000], 1_000_000);
        assert_eq!(ob.clock, 100);

        // Delta removing quantity
        let prev = ob.apply_delta(Side::Buy, 400_000, -1_000_000, stale);
        assert_eq!(prev, 1_000_000);
        assert_eq!(ob.bids[&400_000], 1_000_000);
        assert_eq!(ob.clock, 100);

        // Delta on nonexistent level with stale clock
        let prev = ob.apply_delta(Side::Buy, 300_000, 500_000, stale);
        assert_eq!(prev, 0);
        assert!(!ob.bids.contains_key(&300_000));
        assert_eq!(ob.clock, 100);

        // Snapshot on stale clock
        ob.apply_snapshot_side(Side::Buy, &[(300_000, 999_999)], stale);
        assert_eq!(ob.bids.len(), 2);
        assert_eq!(ob.bids[&400_000], 1_000_000);
        assert!(!ob.bids.contains_key(&300_000));
        assert_eq!(ob.clock, 100);
    }

    // Now send valid delta with clock 101 -> must advance clock and update
    let prev = ob.apply_delta(Side::Buy, 400_000, 200_000, 101);
    assert_eq!(prev, 1_000_000);
    assert_eq!(ob.bids[&400_000], 1_200_000);
    assert_eq!(ob.clock, 101);
}

#[test]
fn test_orderbook_unquoted_deletions() {
    let mut ob = Orderbook::default();

    // 1. Negative deltas on empty book across 500 different prices
    for p in 1..=500 {
        let price = p * 1_000;
        let prev = ob.apply_delta(Side::Buy, price, -500_000, p as u64);
        assert_eq!(prev, 0, "Nonexistent price {} must return prev=0", price);
        assert!(ob.bids.is_empty(), "Book must remain empty");

        let prev = ob.apply_delta(Side::Sell, price, -500_000, p as u64);
        assert_eq!(prev, 0, "Nonexistent price {} must return prev=0", price);
        assert!(ob.asks.is_empty(), "Book must remain empty");
    }

    // 2. Zero deltas on empty book
    for p in 1..=100 {
        let price = p * 1_000;
        let prev = ob.apply_delta(Side::Buy, price, 0, (500 + p) as u64);
        assert_eq!(prev, 0);
        assert!(ob.bids.is_empty());
    }

    // 3. Add level, delete level, burst unquoted deletes on same level
    ob.apply_delta(Side::Buy, 450_000, 100_000, 1000);
    assert_eq!(ob.bids[&450_000], 100_000);

    // Delete it
    let prev = ob.apply_delta(Side::Buy, 450_000, -100_000, 1001);
    assert_eq!(prev, 100_000);
    assert!(!ob.bids.contains_key(&450_000));

    // Now unquoted delete
    let prev = ob.apply_delta(Side::Buy, 450_000, -50_000, 1002);
    assert_eq!(prev, 0);
    assert!(!ob.bids.contains_key(&450_000));

    // Another unquoted delete with huge negative delta
    let prev = ob.apply_delta(Side::Buy, 450_000, -999_999_999, 1003);
    assert_eq!(prev, 0);
    assert!(!ob.bids.contains_key(&450_000));

    // Extreme delta_qty = i64::MIN on vacant level
    let prev = ob.apply_delta(Side::Buy, 450_000, i64::MIN, 1004);
    assert_eq!(prev, 0);
    assert!(!ob.bids.contains_key(&450_000));

    // Extreme delta_qty = i64::MIN on occupied level
    ob.apply_delta(Side::Buy, 450_000, 100_000, 1005);
    assert_eq!(ob.bids[&450_000], 100_000);
    let prev = ob.apply_delta(Side::Buy, 450_000, i64::MIN, 1006);
    assert_eq!(prev, 100_000);
    assert!(
        !ob.bids.contains_key(&450_000),
        "Level must be removed on extreme negative delta"
    );
}

#[test]
fn test_orderbook_same_clock_batching() {
    let mut ob = Orderbook::default();
    let batch_clock = 500u64;

    // Deltas in the same batch share the identical clock timestamp
    ob.apply_delta(Side::Buy, 400_000, 100_000, batch_clock);
    ob.apply_delta(Side::Buy, 410_000, 200_000, batch_clock);
    ob.apply_delta(Side::Sell, 600_000, 300_000, batch_clock);
    ob.apply_delta(Side::Buy, 400_000, -50_000, batch_clock);

    assert_eq!(ob.clock, batch_clock);
    assert_eq!(ob.bids[&400_000], 50_000);
    assert_eq!(ob.bids[&410_000], 200_000);
    assert_eq!(ob.asks[&600_000], 300_000);
    assert_eq!(ob.best_bid(), Some((410_000, 200_000)));
    assert_eq!(ob.best_ask(), Some((600_000, 300_000)));
}

#[test]
fn test_orderbook_oracle_differential_fuzzing() {
    // Differential testing: Compare Orderbook against an independent reference Oracle model
    let mut ob = Orderbook::default();
    let mut oracle_bids: HashMap<i64, i64> = HashMap::new();
    let mut oracle_asks: HashMap<i64, i64> = HashMap::new();
    let mut current_clock = 0u64;

    let mut lcg: u64 = 0xCAFEBABE1234;

    for step in 1..=3000 {
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op_type = lcg % 10;

        if op_type == 0 {
            // Snapshot operation
            let is_stale = lcg.is_multiple_of(4) && current_clock > 10;
            let snap_clock = if is_stale {
                current_clock - 5
            } else {
                current_clock + 1
            };
            let side = if lcg.is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };

            let count = (lcg % 10 + 1) as usize;
            let mut levels = Vec::with_capacity(count);
            for i in 0..count {
                let p = 300_000 + (i as i64 * 10_000);
                let q = (i as i64 + 1) * 100_000;
                levels.push((p, q));
            }

            // Apply to Orderbook
            ob.apply_snapshot_side(side, &levels, snap_clock);

            // Apply to Oracle
            if snap_clock >= current_clock {
                current_clock = snap_clock;
                let map = match side {
                    Side::Buy => &mut oracle_bids,
                    Side::Sell => &mut oracle_asks,
                };
                map.clear();
                for &(p, q) in &levels {
                    if q > 0 {
                        map.insert(p, q);
                    }
                }
            }
        } else {
            // Delta operation
            let is_stale = lcg.is_multiple_of(5) && current_clock > 10;
            let delta_clock = if is_stale {
                current_clock - (lcg % 5 + 1)
            } else {
                current_clock + (lcg % 3) // 0 (same clock) or 1..2 (advance)
            };

            let side = if lcg.is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = 300_000 + ((lcg % 25) as i64 * 10_000);
            let is_negative = lcg.is_multiple_of(3);
            let delta_qty = if is_negative {
                -((lcg % 5 + 1) as i64 * 100_000)
            } else {
                (lcg % 5 + 1) as i64 * 100_000
            };

            let ob_prev = ob.apply_delta(side, price, delta_qty, delta_clock);

            // Oracle update
            if delta_clock >= current_clock {
                current_clock = delta_clock;
                let map = match side {
                    Side::Buy => &mut oracle_bids,
                    Side::Sell => &mut oracle_asks,
                };
                let prev = map.get(&price).copied().unwrap_or(0);
                assert_eq!(
                    ob_prev, prev,
                    "Step {}: previous qty mismatch at price {}",
                    step, price
                );
                let new_qty = prev + delta_qty;
                if new_qty <= 0 {
                    map.remove(&price);
                } else {
                    map.insert(price, new_qty);
                }
            } else {
                // Stale delta should return current qty
                let map = match side {
                    Side::Buy => &oracle_bids,
                    Side::Sell => &oracle_asks,
                };
                let cur = map.get(&price).copied().unwrap_or(0);
                assert_eq!(
                    ob_prev, cur,
                    "Step {}: stale delta returned wrong qty",
                    step
                );
            }
        }

        // Compare Orderbook vs Oracle
        assert_eq!(ob.clock, current_clock, "Step {}: Clock mismatch", step);
        assert_eq!(
            ob.bids.len(),
            oracle_bids.len(),
            "Step {}: Bid count mismatch",
            step
        );
        assert_eq!(
            ob.asks.len(),
            oracle_asks.len(),
            "Step {}: Ask count mismatch",
            step
        );

        for (&p, &q) in &oracle_bids {
            assert_eq!(
                ob.bids.get(&p).copied(),
                Some(q),
                "Step {}: Bid level {} mismatch",
                step,
                p
            );
        }
        for (&p, &q) in &oracle_asks {
            assert_eq!(
                ob.asks.get(&p).copied(),
                Some(q),
                "Step {}: Ask level {} mismatch",
                step,
                p
            );
        }
    }
}
