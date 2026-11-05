//! Unit tests for the orderbook state machine.

#[cfg(test)]
mod tests {
    use engineering_challenge::Side;
    use engineering_challenge::orderbook::Orderbook;

    // ── Snapshot tests ────────────────────────────────────────────────────────

    #[test]
    fn snapshot_populates_bids_and_asks() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000), (380_000, 500_000)], 1);
        ob.apply_snapshot_side(Side::Sell, &[(600_000, 2_000_000)], 1);

        assert_eq!(ob.bids.len(), 2);
        assert_eq!(ob.asks.len(), 1);
        assert_eq!(ob.best_bid(), Some((400_000, 1_000_000)));
        assert_eq!(ob.best_ask(), Some((600_000, 2_000_000)));
    }

    #[test]
    fn snapshot_clears_previous_state() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000), (350_000, 200_000)], 1);
        // Second snapshot should replace, not accumulate
        ob.apply_snapshot_side(Side::Buy, &[(410_000, 500_000)], 2);
        assert_eq!(ob.bids.len(), 1);
        assert_eq!(ob.best_bid(), Some((410_000, 500_000)));
    }

    #[test]
    fn snapshot_ignores_zero_qty_levels() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 0), (380_000, 1_000_000)], 1);
        assert_eq!(ob.bids.len(), 1);
        assert!(!ob.bids.contains_key(&400_000));
    }

    // ── Delta tests ───────────────────────────────────────────────────────────

    #[test]
    fn delta_adds_new_level() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 1);
        ob.apply_delta(Side::Buy, 420_000, 500_000, 2);
        assert_eq!(ob.bids.len(), 2);
        assert_eq!(ob.bids[&420_000], 500_000);
    }

    #[test]
    fn delta_updates_existing_level() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 1);
        ob.apply_delta(Side::Buy, 400_000, 250_000, 2);
        assert_eq!(ob.bids[&400_000], 1_250_000);
    }

    #[test]
    fn delta_removes_level_when_qty_reaches_zero() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 1);
        ob.apply_delta(Side::Buy, 400_000, -1_000_000, 2);
        assert!(!ob.bids.contains_key(&400_000));
    }

    #[test]
    fn delta_removes_level_when_qty_goes_negative() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 500_000)], 1);
        ob.apply_delta(Side::Buy, 400_000, -900_000, 2);
        assert!(!ob.bids.contains_key(&400_000));
    }

    #[test]
    fn delta_on_nonexistent_level_treated_as_new() {
        let mut ob = Orderbook::default();
        // Applying a delta to a price we haven't seen yet should create the level
        ob.apply_delta(Side::Buy, 300_000, 1_000_000, 1);
        assert_eq!(ob.bids[&300_000], 1_000_000);
    }

    // ── Clock tracking ────────────────────────────────────────────────────────

    #[test]
    fn clock_advances_monotonically_with_snapshots() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 10);
        assert_eq!(ob.clock, 10);
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 5); // older
        assert_eq!(ob.clock, 10, "clock must not go backwards");
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 20);
        assert_eq!(ob.clock, 20);
    }

    #[test]
    fn clock_advances_with_deltas() {
        let mut ob = Orderbook::default();
        ob.apply_delta(Side::Sell, 600_000, 1_000_000, 100);
        assert_eq!(ob.clock, 100);
        ob.apply_delta(Side::Sell, 600_000, 500_000, 50); // stale
        assert_eq!(ob.clock, 100);
    }

    // ── Price level ordering ──────────────────────────────────────────────────

    #[test]
    fn best_bid_is_highest_price() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(300_000, 1), (400_000, 1), (350_000, 1)], 1);
        assert_eq!(ob.best_bid().unwrap().0, 400_000);
    }

    #[test]
    fn best_ask_is_lowest_price() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Sell, &[(700_000, 1), (600_000, 1), (650_000, 1)], 1);
        assert_eq!(ob.best_ask().unwrap().0, 600_000);
    }

    // ── Empty book ────────────────────────────────────────────────────────────

    #[test]
    fn empty_book_has_no_best_bid_or_ask() {
        let ob = Orderbook::default();
        assert!(ob.best_bid().is_none());
        assert!(ob.best_ask().is_none());
    }

    #[test]
    fn book_emptied_by_deltas_has_no_best_bid() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 1);
        ob.apply_delta(Side::Buy, 400_000, -1_000_000, 2);
        assert!(ob.best_bid().is_none());
    }

    // ── Stale clock & edge cases ──────────────────────────────────────────────

    #[test]
    fn stale_delta_does_not_mutate_book() {
        let mut ob = Orderbook::default();
        ob.apply_delta(Side::Buy, 400_000, 1_000_000, 100);
        assert_eq!(ob.clock, 100);

        // Stale delta with clock = 50 should be ignored
        let prev = ob.apply_delta(Side::Buy, 400_000, 500_000, 50);
        assert_eq!(prev, 1_000_000);
        assert_eq!(
            ob.bids[&400_000], 1_000_000,
            "stale delta must not mutate quantity"
        );
        assert_eq!(ob.clock, 100, "clock must remain at 100");
    }

    #[test]
    fn stale_snapshot_does_not_mutate_book() {
        let mut ob = Orderbook::default();
        ob.apply_snapshot_side(Side::Buy, &[(400_000, 1_000_000)], 100);
        assert_eq!(ob.clock, 100);

        // Stale snapshot with clock = 50 should be ignored
        ob.apply_snapshot_side(Side::Buy, &[(300_000, 500_000)], 50);
        assert_eq!(ob.bids.len(), 1);
        assert_eq!(
            ob.bids[&400_000], 1_000_000,
            "stale snapshot must not overwrite book"
        );
        assert_eq!(ob.clock, 100, "clock must remain at 100");
    }

    #[test]
    fn delta_negative_on_nonexistent_level_does_not_insert() {
        let mut ob = Orderbook::default();
        let prev = ob.apply_delta(Side::Buy, 300_000, -500_000, 1);
        assert_eq!(prev, 0);
        assert!(
            ob.bids.is_empty(),
            "negative delta on nonexistent level must not insert node"
        );
    }

    #[test]
    fn delta_zero_qty_on_nonexistent_level_does_not_insert() {
        let mut ob = Orderbook::default();
        let prev = ob.apply_delta(Side::Buy, 300_000, 0, 1);
        assert_eq!(prev, 0);
        assert!(
            ob.bids.is_empty(),
            "zero delta on nonexistent level must not insert node"
        );
    }

    #[test]
    fn delta_fractional_cancellation() {
        let mut ob = Orderbook::default();
        ob.apply_delta(Side::Buy, 400_000, 500_000, 1);
        assert_eq!(ob.bids[&400_000], 500_000);

        // Partial reduction
        ob.apply_delta(Side::Buy, 400_000, -200_000, 2);
        assert_eq!(ob.bids[&400_000], 300_000);

        // Full cancellation
        ob.apply_delta(Side::Buy, 400_000, -300_000, 3);
        assert!(!ob.bids.contains_key(&400_000));
    }
}
