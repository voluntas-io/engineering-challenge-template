//! Unit tests for Kalshi protocol parsing helpers.

#[cfg(test)]
mod tests {
    use engineering_challenge::kalshi::parse_dollars_fp;

    // ── Price parsing ─────────────────────────────────────────────────────────

    #[test]
    fn parse_cent_price() {
        assert_eq!(parse_dollars_fp("0.42"), Some(420_000));
    }

    #[test]
    fn parse_four_decimal_price() {
        assert_eq!(parse_dollars_fp("0.4200"), Some(420_000));
    }

    #[test]
    fn parse_one_dollar_price() {
        assert_eq!(parse_dollars_fp("1.0000"), Some(1_000_000));
    }

    #[test]
    fn parse_zero_price() {
        assert_eq!(parse_dollars_fp("0.0000"), Some(0));
    }

    #[test]
    fn parse_one_tenth_cent_price() {
        // $0.001 = 1000 in our units
        assert_eq!(parse_dollars_fp("0.001"), Some(1_000));
    }

    #[test]
    fn parse_sub_cent_price() {
        // $0.0100 = 10_000 in our units
        assert_eq!(parse_dollars_fp("0.0100"), Some(10_000));
    }

    #[test]
    fn parse_fractional_price_deci_cent() {
        // $0.0050 = 5_000
        assert_eq!(parse_dollars_fp("0.0050"), Some(5_000));
    }

    #[test]
    fn parse_max_price() {
        // $0.99 = 990_000
        assert_eq!(parse_dollars_fp("0.99"), Some(990_000));
    }

    #[test]
    fn parse_invalid_price_returns_none() {
        assert_eq!(parse_dollars_fp("not_a_number"), None);
    }

    #[test]
    fn parse_empty_returns_none() {
        assert_eq!(parse_dollars_fp(""), None);
    }

    // ── NO bid complementation ─────────────────────────────────────────────

    #[test]
    fn no_bid_complement_at_half_dollar() {
        // NO bid at $0.50 → YES ask at $0.50
        let no_price = parse_dollars_fp("0.50").unwrap();
        let yes_ask = 1_000_000 - no_price;
        assert_eq!(yes_ask, 500_000);
    }

    #[test]
    fn no_bid_complement_at_penny() {
        // NO bid at $0.01 → YES ask at $0.99
        let no_price = parse_dollars_fp("0.01").unwrap();
        let yes_ask = 1_000_000 - no_price;
        assert_eq!(yes_ask, 990_000);
    }

    #[test]
    fn no_bid_complement_at_ninety_nine_cents() {
        // NO bid at $0.99 → YES ask at $0.01
        let no_price = parse_dollars_fp("0.99").unwrap();
        let yes_ask = 1_000_000 - no_price;
        assert_eq!(yes_ask, 10_000);
    }

    #[test]
    fn complement_roundtrips() {
        for cents in 1..100u32 {
            let p = (cents * 10_000) as i64;
            let comp = 1_000_000 - p;
            assert_eq!(1_000_000 - comp, p, "complement of complement must be original price");
        }
    }

    // ── Quantity parsing ───────────────────────────────────────────────────

    #[test]
    fn parse_whole_contract_qty() {
        // "13.00" → 13 contracts = 13_000_000
        assert_eq!(parse_dollars_fp("13.00"), Some(13_000_000));
    }

    #[test]
    fn parse_fractional_contract_qty() {
        // "0.50" → 0.5 contracts = 500_000
        assert_eq!(parse_dollars_fp("0.50"), Some(500_000));
    }

    #[test]
    fn parse_large_contract_qty() {
        // "1000.00" → 1000 contracts = 1_000_000_000
        assert_eq!(parse_dollars_fp("1000.00"), Some(1_000_000_000));
    }

    // ── Level array parsing (snapshot ingestion) ───────────────────────────

    #[test]
    fn level_array_roundtrip() {
        // Simulate what decode_snapshot does for yes_dollars_fp
        let raw = serde_json::json!([
            ["0.4200", "13.00"],
            ["0.3100", "10.00"],
        ]);
        let levels: Vec<(i64, i64)> = raw
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| {
                let pair = e.as_array()?;
                let price = parse_dollars_fp(pair[0].as_str()?)?;
                let qty = parse_dollars_fp(pair[1].as_str()?)?;
                Some((price, qty))
            })
            .collect();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], (420_000, 13_000_000));
        assert_eq!(levels[1], (310_000, 10_000_000));
    }

    // ── Negative prices and quantities (cancellations / reductions) ────────

    #[test]
    fn parse_negative_fractional_price() {
        assert_eq!(parse_dollars_fp("-0.50"), Some(-500_000));
    }

    #[test]
    fn parse_negative_one_dollar_fifty() {
        assert_eq!(parse_dollars_fp("-1.50"), Some(-1_500_000));
    }

    #[test]
    fn parse_negative_sub_cent() {
        assert_eq!(parse_dollars_fp("-0.001"), Some(-1_000));
        assert_eq!(parse_dollars_fp("-0.0001"), Some(-100));
    }

    #[test]
    fn parse_negative_zero() {
        assert_eq!(parse_dollars_fp("-0.0000"), Some(0));
        assert_eq!(parse_dollars_fp("-0"), Some(0));
    }

    #[test]
    fn parse_negative_fractional_qty() {
        assert_eq!(parse_dollars_fp("-0.25"), Some(-250_000));
    }

    #[test]
    fn parse_negative_whole_qty() {
        assert_eq!(parse_dollars_fp("-13.00"), Some(-13_000_000));
    }

    #[test]
    fn parse_positive_explicit_sign() {
        assert_eq!(parse_dollars_fp("+0.50"), Some(500_000));
        assert_eq!(parse_dollars_fp("+13.00"), Some(13_000_000));
    }

    #[test]
    fn parse_whitespace_trimmed() {
        assert_eq!(parse_dollars_fp("  -0.50  "), Some(-500_000));
        assert_eq!(parse_dollars_fp("  0.4200 "), Some(420_000));
    }
}


