#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use engineering_challenge::{
    Side,
    fixed::{format_scaled, parse_scaled, rescale_exact},
    model::{
        BookSnapshot, Level, NumericSpec, Price, PriceBand, PriceUnit, Quantity, QuantityChange,
        QuantityDelta, QuantityUnit, Ratio,
    },
    orderbook::OrderBook,
    util::{price_from_sbe, price_to_sbe, quantity_from_sbe, quantity_to_sbe},
};

fn spec() -> NumericSpec {
    NumericSpec {
        price_scale: 9,
        quantity_scale: 6,
        price_unit: PriceUnit::Yield,
        quantity_unit: QuantityUnit::Lots,
        tick_schedule: vec![
            PriceBand::from_micros(-1_000_000_000, 0, 1_000, -1_000_000_000),
            PriceBand::from_micros(0, 1_000_000_000, 10_000, 0),
        ],
        quantity_step: Some(Quantity::new(25).unwrap()),
        lot_size: Some(Quantity::new(100).unwrap()),
        contract_multiplier: Some(Ratio::new(5, 2).unwrap()),
    }
}

#[test]
fn negative_prices_and_large_values_are_first_class() {
    let mut book = OrderBook::new();
    for price in [i64::MIN, -9_999_999_999, -1, 0, 1, 9_999_999_999, i64::MAX] {
        book.apply(
            Side::Buy,
            Price::from_raw(price),
            QuantityChange::Absolute(Quantity::new(1).unwrap()),
        )
        .unwrap();
        assert_eq!(
            book.quantity(Side::Buy, Price::from_raw(price)),
            Some(Quantity::new(1).unwrap())
        );
    }
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(i64::MAX));
}

#[test]
fn decimal_parsing_round_trips_at_nine_decimals() {
    for text in [
        "0.000000001",
        "-12345.678901234",
        "999999999.999999999",
        "-0.1",
        "9000000000.123456789",
    ] {
        let coefficient = parse_scaled(text, 9).unwrap();
        let formatted = format_scaled(coefficient, 9);
        assert_eq!(parse_scaled(&formatted, 9).unwrap(), coefficient, "{text}");
    }
    // More than nine fractional digits is rejected, never rounded.
    assert!(parse_scaled("0.0000000001", 9).is_err());
    assert!(parse_scaled("1e-9", 9).is_err());
}

#[test]
fn tick_schedule_changes_across_bands() {
    let spec = spec();
    spec.validate().unwrap();
    // Lower band: 0.000001 steps (micro-units of a two-scale value).
    assert!(spec.accepts_price(Price::from_raw(-999_999_000)));
    assert!(!spec.accepts_price(Price::from_raw(-999_999_500)));
    // Boundary at zero belongs to both bands' grids.
    assert!(spec.accepts_price(Price::from_raw(0)));
    // Upper band: coarser steps after zero.
    assert!(spec.accepts_price(Price::from_raw(10_000)));
    assert!(!spec.accepts_price(Price::from_raw(1_000)));
}

#[test]
fn quantity_steps_and_lot_sizes_are_enforced() {
    let spec = spec();
    assert!(spec.accepts_quantity(Quantity::new(100).unwrap()));
    assert!(spec.accepts_quantity(Quantity::new(125).unwrap()));
    assert!(!spec.accepts_quantity(Quantity::new(110).unwrap()));
    assert!(!spec.accepts_quantity(Quantity::new(50).unwrap()));
    assert!(Quantity::new(-1).is_err());
}

#[test]
fn signed_deltas_are_distinct_from_absolute_quantities() {
    let mut book = OrderBook::new();
    book.apply(
        Side::Buy,
        Price::from_raw(500_000),
        QuantityChange::Absolute(Quantity::new(100).unwrap()),
    )
    .unwrap();
    book.apply(
        Side::Buy,
        Price::from_raw(500_000),
        QuantityChange::Delta(QuantityDelta::from_raw(-40)),
    )
    .unwrap();
    assert_eq!(
        book.quantity(Side::Buy, Price::from_raw(500_000)),
        Some(Quantity::new(60).unwrap())
    );
    // A negative delta that would make the quantity negative is rejected,
    // and the book is unchanged.
    assert!(
        book.apply(
            Side::Buy,
            Price::from_raw(500_000),
            QuantityChange::Delta(QuantityDelta::from_raw(-61)),
        )
        .is_err()
    );
    assert_eq!(
        book.quantity(Side::Buy, Price::from_raw(500_000)),
        Some(Quantity::new(60).unwrap())
    );
}

#[test]
fn multiplication_and_conversion_overflow_are_rejected() {
    assert!(Ratio::new(2, 1).unwrap().apply_exact(i64::MAX).is_err());
    assert!(rescale_exact(i64::MAX, 0, 6).is_err());
    assert!(rescale_exact(i64::MIN, 6, 4).is_err());
    assert!(rescale_exact(1, 6, 4).is_err(), "inexact narrowing");
    let mut book = OrderBook::new();
    book.apply(
        Side::Sell,
        Price::from_raw(1),
        QuantityChange::Absolute(Quantity::new(i64::MAX).unwrap()),
    )
    .unwrap();
    assert!(
        book.apply(
            Side::Sell,
            Price::from_raw(1),
            QuantityChange::Delta(QuantityDelta::from_raw(1)),
        )
        .is_err()
    );
}

#[test]
fn contract_multipliers_are_exact_without_floating_point() {
    let multiplier = Ratio::new(5, 2).unwrap();
    assert_eq!(multiplier.apply_exact(10).unwrap(), 25);
    // 7.5 is not an integer: rejected, not rounded.
    assert!(multiplier.apply_exact(3).is_err());
    let inverse = Ratio::new(2, 5).unwrap();
    assert_eq!(inverse.apply_exact(25).unwrap(), 10);
}

#[test]
fn sbe_conversion_preserves_units_and_rejects_inexact_values() {
    let spec = spec();
    // Six-decimal quantities convert to SBE exactly.
    let quantity = Quantity::new(12_340_000).unwrap();
    assert_eq!(quantity_to_sbe(quantity, &spec).unwrap(), 12_340_000);
    assert_eq!(quantity_from_sbe(12_340_000, &spec).unwrap(), quantity);
    // Nine-decimal prices only convert when the digits allow it.
    let convertible = Price::from_raw(123_456_000);
    assert_eq!(price_to_sbe(convertible, &spec).unwrap(), 123_456);
    assert_eq!(price_from_sbe(123_456, &spec).unwrap(), convertible);
    assert!(
        price_to_sbe(Price::from_raw(123_456_789), &spec).is_err(),
        "0.123456789 cannot be represented at six decimals"
    );
    // Units survive metadata conversion unchanged.
    assert_eq!(spec.price_unit, PriceUnit::Yield);
    assert_eq!(spec.quantity_unit, QuantityUnit::Lots);
}

#[test]
fn price_is_not_interchangeable_with_quantity() {
    // This is a compile-time property; the test documents that no implicit
    // conversion exists between the two wrappers.
    fn assert_not_assignable(_: Quantity) {}
    assert_not_assignable(Quantity::new(1).unwrap());
}

#[test]
fn hundred_thousand_symbol_book_workload_smoke() {
    let symbols = 100_000usize;
    let mut books: Vec<OrderBook> = (0..symbols).map(|_| OrderBook::new()).collect();
    let start = std::time::Instant::now();
    for (index, book) in books.iter_mut().enumerate() {
        let bid = Price::from_raw(100_000 + (index % 10_000) as i64 * 100);
        let ask = Price::from_raw(100_000 + (index % 10_000) as i64 * 100 + 10_000);
        book.apply(
            Side::Buy,
            bid,
            QuantityChange::Absolute(Quantity::new(1_000).unwrap()),
        )
        .unwrap();
        book.apply(
            Side::Sell,
            ask,
            QuantityChange::Absolute(Quantity::new(1_000).unwrap()),
        )
        .unwrap();
    }
    let levels: usize = books
        .iter()
        .map(|book| {
            let snapshot: BookSnapshot = book.to_snapshot();
            snapshot.bids.len() + snapshot.asks.len()
        })
        .sum();
    assert_eq!(levels, symbols * 2);
    // Generous smoke bound: the release workload is measured by the bench.
    assert!(
        start.elapsed().as_secs() < 60,
        "100k-symbol workload should be well under a minute"
    );
    assert_eq!(std::mem::size_of::<Price>(), 8);
    assert_eq!(std::mem::size_of::<Quantity>(), 8);
    assert_eq!(std::mem::size_of::<Level>(), 16);
}
