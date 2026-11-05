//! A normalized, aggregated two-sided order book.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::Side;
use crate::fixed::SCALE;
use crate::model::{BookSnapshot, Level, QuantityChange};

#[derive(Debug, Error)]
pub enum BookError {
    #[error("price {price} outside [0, {SCALE}]")]
    InvalidPrice { price: i64 },
    #[error("negative absolute quantity {quantity} at price {price}")]
    NegativeQuantity { price: i64, quantity: i64 },
    #[error("snapshot contains duplicate {side} level at price {price}")]
    DuplicateLevel { side: Side, price: i64 },
    #[error("delta at price {price} would make quantity negative: {current} + {delta}")]
    NegativeResult {
        price: i64,
        current: i64,
        delta: i64,
    },
    #[error("quantity overflow at price {price}: {current} + {delta}")]
    Overflow {
        price: i64,
        current: i64,
        delta: i64,
    },
}

/// Aggregated bids and asks keyed by fixed-point price.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
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
    pub fn apply(
        &mut self,
        side: Side,
        price: i64,
        change: QuantityChange,
    ) -> Result<(), BookError> {
        if !(0..=SCALE).contains(&price) {
            return Err(BookError::InvalidPrice { price });
        }
        let side_map = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        match change {
            QuantityChange::Absolute(quantity) => {
                if quantity < 0 {
                    return Err(BookError::NegativeQuantity { price, quantity });
                }
                if quantity == 0 {
                    side_map.remove(&price);
                } else {
                    side_map.insert(price, quantity);
                }
            }
            QuantityChange::Delta(delta) => {
                let current = side_map.get(&price).copied().unwrap_or(0);
                let next = current.checked_add(delta).ok_or(BookError::Overflow {
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
                    side_map.insert(price, next);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn quantity(&self, side: Side, price: i64) -> Option<i64> {
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
    side_map: &mut BTreeMap<i64, i64>,
    side: Side,
    level: &Level,
) -> Result<(), BookError> {
    if !(0..=SCALE).contains(&level.price) {
        return Err(BookError::InvalidPrice { price: level.price });
    }
    if level.quantity < 0 {
        return Err(BookError::NegativeQuantity {
            price: level.price,
            quantity: level.quantity,
        });
    }
    if level.quantity == 0 {
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

    #[test]
    fn applies_absolute_and_delta_updates() {
        let mut book = OrderBook::new();
        book.apply(Side::Buy, 400_000, QuantityChange::Absolute(2_000_000))
            .unwrap();
        book.apply(Side::Buy, 400_000, QuantityChange::Delta(500_000))
            .unwrap();
        assert_eq!(book.quantity(Side::Buy, 400_000), Some(2_500_000));
        book.apply(Side::Buy, 400_000, QuantityChange::Delta(-2_500_000))
            .unwrap();
        assert_eq!(book.quantity(Side::Buy, 400_000), None);
        assert!(book.is_empty());
    }

    #[test]
    fn rejects_negative_results() {
        let mut book = OrderBook::new();
        assert!(
            book.apply(Side::Sell, 600_000, QuantityChange::Delta(-1))
                .is_err()
        );
    }

    #[test]
    fn detects_crossed_but_not_locked() {
        let mut book = OrderBook::new();
        book.apply(Side::Buy, 500_000, QuantityChange::Absolute(1))
            .unwrap();
        book.apply(Side::Sell, 500_000, QuantityChange::Absolute(1))
            .unwrap();
        assert!(!book.is_crossed());
        book.apply(Side::Buy, 510_000, QuantityChange::Absolute(1))
            .unwrap();
        assert!(book.is_crossed());
    }

    #[test]
    fn snapshot_order_is_best_first() {
        let mut snapshot = BookSnapshot::default();
        for (price, quantity) in [(100_000, 1), (400_000, 2), (200_000, 3)] {
            snapshot.bids.push(Level { price, quantity });
        }
        for (price, quantity) in [(900_000, 4), (600_000, 5), (700_000, 6)] {
            snapshot.asks.push(Level { price, quantity });
        }
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        let rendered = book.to_snapshot();
        assert_eq!(
            rendered
                .bids
                .iter()
                .map(|level| level.price)
                .collect::<Vec<_>>(),
            [400_000, 200_000, 100_000]
        );
        assert_eq!(
            rendered
                .asks
                .iter()
                .map(|level| level.price)
                .collect::<Vec<_>>(),
            [600_000, 700_000, 900_000]
        );
    }

    #[test]
    fn rejects_duplicate_snapshot_levels() {
        // Duplicate bids are malformed.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: 1_000_000,
        });
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: 2_000_000,
        });
        snapshot.asks.push(Level {
            price: 600_000,
            quantity: 1_000_000,
        });
        let mut book = OrderBook::new();
        let error = book.replace(&snapshot).unwrap_err();
        assert!(matches!(
            error,
            BookError::DuplicateLevel {
                side: Side::Buy,
                price: 400_000
            }
        ));
        assert!(
            book.is_empty(),
            "failed replace must leave the book unchanged"
        );

        // Duplicate asks are malformed too, with the same error.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: 1_000_000,
        });
        snapshot.asks.push(Level {
            price: 600_000,
            quantity: 1_000_000,
        });
        snapshot.asks.push(Level {
            price: 600_000,
            quantity: 5_000_000,
        });
        let mut book = OrderBook::new();
        assert!(matches!(
            book.replace(&snapshot),
            Err(BookError::DuplicateLevel {
                side: Side::Sell,
                price: 600_000
            })
        ));

        // The same price on both sides is a locked book, not a duplicate.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 500_000,
            quantity: 1_000_000,
        });
        snapshot.asks.push(Level {
            price: 500_000,
            quantity: 1_000_000,
        });
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        assert!(!book.is_crossed());
        assert_eq!(book.quantity(Side::Buy, 500_000), Some(1_000_000));
        assert_eq!(book.quantity(Side::Sell, 500_000), Some(1_000_000));

        // Zero-quantity levels are ignored, including repeated ones.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: 0,
        });
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: 0,
        });
        snapshot.asks.push(Level {
            price: 600_000,
            quantity: 1_000_000,
        });
        let mut book = OrderBook::new();
        book.replace(&snapshot).unwrap();
        assert_eq!(book.quantity(Side::Buy, 400_000), None);

        // Invalid prices and negative quantities are still rejected.
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 2_000_000,
            quantity: 1,
        });
        let mut book = OrderBook::new();
        assert!(matches!(
            book.replace(&snapshot),
            Err(BookError::InvalidPrice { price: 2_000_000 })
        ));
        let mut snapshot = BookSnapshot::default();
        snapshot.bids.push(Level {
            price: 400_000,
            quantity: -1,
        });
        let mut book = OrderBook::new();
        assert!(matches!(
            book.replace(&snapshot),
            Err(BookError::NegativeQuantity {
                price: 400_000,
                quantity: -1
            })
        ));
    }
}
