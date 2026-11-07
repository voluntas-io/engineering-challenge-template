//! A normalized, aggregated two-sided order book.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::Side;
use crate::model::{BookSnapshot, Level, Price, Quantity, QuantityChange, QuantityDelta};

#[derive(Debug, Error)]
pub enum BookError {
    #[error("snapshot contains duplicate {side} level at price {price}")]
    DuplicateLevel { side: Side, price: Price },
    #[error("delta at price {price} would make quantity negative: {current} + {delta}")]
    NegativeResult {
        price: Price,
        current: Quantity,
        delta: QuantityDelta,
    },
    #[error("quantity overflow at price {price}: {current} + {delta}")]
    Overflow {
        price: Price,
        current: Quantity,
        delta: QuantityDelta,
    },
}

/// Aggregated bids and asks keyed by exact price coefficient.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
}

impl OrderBook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.last_key_value().map(|(price, quantity)| Level {
            price: *price,
            quantity: *quantity,
        })
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first_key_value().map(|(price, quantity)| Level {
            price: *price,
            quantity: *quantity,
        })
    }

    /// A book is crossed when the best bid is strictly above the best ask.
    /// A locked book (bid == ask) is valid and common in binary markets.
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price > ask.price,
            _ => false,
        }
    }

    /// Replace this book with an exchange snapshot.
    ///
    /// A valid snapshot contains at most one level per price per side;
    /// duplicate levels are rejected as malformed instead of being silently
    /// aggregated or overwritten, because either behavior would let a
    /// malformed snapshot change book state in a way the venue never
    /// described. Zero-quantity levels are ignored (the level is absent).
    /// On any error this book is left unchanged.
    pub fn replace(&mut self, snapshot: &BookSnapshot) -> Result<(), BookError> {
        let mut next = Self::new();
        for level in &snapshot.bids {
            insert_snapshot_level(&mut next.bids, Side::Buy, level)?;
        }
        for level in &snapshot.asks {
            insert_snapshot_level(&mut next.asks, Side::Sell, level)?;
        }
        *self = next;
        Ok(())
    }

    /// Apply an absolute or relative level update.
    ///
    /// Prices are signed and unrestricted: the instrument's [`crate::model::NumericSpec`]
    /// owns tick validation, not the shared book. Quantities remain non-negative
    /// and all arithmetic is checked.
    pub fn apply(
        &mut self,
        side: Side,
        price: Price,
        change: QuantityChange,
    ) -> Result<(), BookError> {
        let side_map = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        match change {
            QuantityChange::Absolute(quantity) => {
                if quantity.is_zero() {
                    side_map.remove(&price);
                } else {
                    side_map.insert(price, quantity);
                }
            }
            QuantityChange::Delta(delta) => {
                let current = side_map.get(&price).copied().unwrap_or_default();
                let next = current
                    .raw()
                    .checked_add(delta.raw())
                    .ok_or(BookError::Overflow {
                        price,
                        current,
                        delta,
                    })?;
                if next < 0 {
                    return Err(BookError::NegativeResult {
                        price,
                        current,
                        delta,
                    });
                }
                if next == 0 {
                    side_map.remove(&price);
                } else {
                    let quantity = Quantity::new(next).map_err(|_| BookError::NegativeResult {
                        price,
                        current,
                        delta,
                    })?;
                    side_map.insert(price, quantity);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn quantity(&self, side: Side, price: Price) -> Option<Quantity> {
        match side {
            Side::Buy => self.bids.get(&price).copied(),
            Side::Sell => self.asks.get(&price).copied(),
        }
    }

    /// Snapshot with best bids first and best asks first.
    #[must_use]
    pub fn to_snapshot(&self) -> BookSnapshot {
        BookSnapshot {
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(price, quantity)| Level {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, quantity)| Level {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
        }
    }
}

/// Insert one snapshot level into a side map, rejecting duplicates.
fn insert_snapshot_level(
    side_map: &mut BTreeMap<Price, Quantity>,
    side: Side,
    level: &Level,
) -> Result<(), BookError> {
    if level.quantity.is_zero() {
        // A zero-quantity level is absent by definition.
        return Ok(());
    }
    if side_map.insert(level.price, level.quantity).is_some() {
        return Err(BookError::DuplicateLevel {
            side,
            price: level.price,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(value: i64) -> Price {
        Price::from_raw(value)
    }

    fn q(value: i64) -> Quantity {
        Quantity::new(value).unwrap()
    }

    fn d(value: i64) -> QuantityDelta {
        QuantityDelta::from_raw(value)
    }

    fn level(price: i64, quantity: i64) -> Level {
        Level::new(price, quantity).unwrap()
    }

    #[test]
    fn applies_absolute_and_delta_updates() {
        let mut book = OrderBook::new();
        book.apply(
            Side::Buy,
            p(400_000),
            QuantityChange::Absolute(q(2_000_000)),
        )
        .unwrap();
        book.apply(Side::Buy, p(400_000), QuantityChange::Delta(d(500_000)))
            .unwrap();
        assert_eq!(book.quantity(Side::Buy, p(400_000)), Some(q(2_500_000)));
        book.apply(Side::Buy, p(400_000), QuantityChange::Delta(d(-2_500_000)))
            .unwrap();
        assert_eq!(book.quantity(Side::Buy, p(400_000)), None);
        assert!(book.is_empty());
    }

    #[test]
    fn accepts_negative_and_large_prices_but_not_negative_quantities() {
        let mut book = OrderBook::new();
        for price in [-9_000_000_000, -2_500_000, -1, 0, 1, 9_000_000_000] {
            book.apply(Side::Buy, p(price), QuantityChange::Absolute(q(1)))
                .unwrap();
            assert_eq!(book.quantity(Side::Buy, p(price)), Some(q(1)));
            book.apply(Side::Buy, p(price), QuantityChange::Absolute(q(0)))
                .unwrap();
        }
        assert!(Level::new(400_000, -1).is_err());
        assert!(Quantity::new(-1).is_err());
    }

    #[test]
    fn rejects_negative_results_and_overflow() {
        let mut book = OrderBook::new();
        assert!(
            book.apply(Side::Sell, p(600_000), QuantityChange::Delta(d(-1)))
                .is_err()
        );
        book.apply(
            Side::Sell,
            p(600_000),
            QuantityChange::Absolute(q(i64::MAX)),
        )
        .unwrap();
        assert!(matches!(
            book.apply(Side::Sell, p(600_000), QuantityChange::Delta(d(1))),
            Err(BookError::Overflow { .. })
        ));
        assert_eq!(book.quantity(Side::Sell, p(600_000)), Some(q(i64::MAX)));
    }

    #[test]
    fn detects_crossed_but_not_locked() {
        let mut book = OrderBook::new();
        book.apply(Side::Buy, p(500_000), QuantityChange::Absolute(q(1)))
            .unwrap();
        book.apply(Side::Sell, p(500_000), QuantityChange::Absolute(q(1)))
            .unwrap();
        assert!(!book.is_crossed());
        book.apply(Side::Buy, p(510_000), QuantityChange::Absolute(q(1)))
            .unwrap();
        assert!(book.is_crossed());
    }

    #[test]
    fn snapshot_order_is_best_first() {
        let mut snapshot = BookSnapshot::default();
        for (price, quantity) in [(100_000, 1), (400_000, 2), (200_000, 3)] {
            snapshot.bids.push(level(price, quantity));
        }
        for (price, quantity) in [(900_000, 4), (600_000, 5), (700_000, 6)] {
            snapshot.asks.push(level(price, quantity));
        }
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        let rendered = book.to_snapshot();
        assert_eq!(
            rendered
                .bids
                .iter()
                .map(|level| level.price.raw())
                .collect::<Vec<_>>(),
            [400_000, 200_000, 100_000]
        );
        assert_eq!(
            rendered
                .asks
                .iter()
                .map(|level| level.price.raw())
                .collect::<Vec<_>>(),
            [600_000, 700_000, 900_000]
        );
    }

    #[test]
    fn rejects_duplicate_snapshot_levels() {
        // Duplicate bids are malformed.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(level(400_000, 1_000_000));
        snapshot.bids.push(level(400_000, 2_000_000));
        snapshot.asks.push(level(600_000, 1_000_000));
        let mut book = OrderBook::new();
        let error = book.replace(&snapshot).unwrap_err();
        assert!(matches!(
            error,
            BookError::DuplicateLevel {
                side: Side::Buy,
                price
            } if price == p(400_000)
        ));
        assert!(
            book.is_empty(),
            "failed replace must leave the book unchanged"
        );

        // Duplicate asks are malformed too, with the same error.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(level(400_000, 1_000_000));
        snapshot.asks.push(level(600_000, 1_000_000));
        snapshot.asks.push(level(600_000, 5_000_000));
        let mut book = OrderBook::new();
        assert!(matches!(
            book.replace(&snapshot),
            Err(BookError::DuplicateLevel {
                side: Side::Sell,
                price
            }) if price == p(600_000)
        ));

        // The same price on both sides is a locked book, not a duplicate.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(level(500_000, 1_000_000));
        snapshot.asks.push(level(500_000, 1_000_000));
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        assert!(!book.is_crossed());
        assert_eq!(book.quantity(Side::Buy, p(500_000)), Some(q(1_000_000)));
        assert_eq!(book.quantity(Side::Sell, p(500_000)), Some(q(1_000_000)));

        // Zero-quantity levels are ignored, including repeated ones.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(level(400_000, 0));
        snapshot.bids.push(level(400_000, 0));
        snapshot.asks.push(level(600_000, 1_000_000));
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        assert_eq!(book.quantity(Side::Buy, p(400_000)), None);

        // Prices outside the former probability range are valid now.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(level(2_000_000, 1));
        snapshot.bids.push(level(-2_000_000, 1));
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        assert_eq!(book.best_bid().unwrap().price, p(2_000_000));
    }
}
