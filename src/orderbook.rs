//! In-memory orderbook state machine.
//!
//! Each [`Orderbook`] holds the full set of active price levels for one symbol.
//! It applies [`BookUpdate`] events (from snapshots or deltas) and can emit
//! the current state as a canonical snapshot at any time.
//!
//! ## Price representation
//!
//! All prices and quantities use the SBE fixed-point unit:
//!   - `1_000_000` = $1.00 USDC (price)
//!   - `1_000_000` = 1 share / contract (quantity)
//!
//! Levels with quantity ≤ 0 are removed from the book.

use std::collections::{BTreeMap, btree_map::Entry};

use crate::Side;
use chrono::{DateTime, Utc};

// ── Public event types ────────────────────────────────────────────────────────

/// A single price-level update (applies to both snapshot ingestion and deltas).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub side: Side,
    /// Price in fixed-point units (1_000_000 = $1.00).
    pub price: i64,
    /// Absolute quantity after this update (snapshot); or signed delta.
    /// A delta of 0 removes the level.
    pub qty: i64,
    /// Exchange clock value accompanying this update.
    pub clock: u64,
}

/// A snapshot of one side of the book (all active price levels).
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: i64,
    pub qty: i64,
}

/// Full orderbook snapshot at a point in time.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub symbol: String,
    pub received_at: DateTime<Utc>,
    pub exchange_ts: DateTime<Utc>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

// ── Orderbook state ───────────────────────────────────────────────────────────

/// In-memory limit orderbook for a single symbol.
///
/// Uses [`BTreeMap`] for O(log n) level lookups and naturally sorted iteration
/// (ascending price). We track bids and asks separately.
#[derive(Debug, Default)]
pub struct Orderbook {
    /// Bid (Buy) price levels: price → qty. Sorted ascending.
    pub bids: BTreeMap<i64, i64>,
    /// Ask (Sell) price levels: price → qty. Sorted ascending.
    pub asks: BTreeMap<i64, i64>,
    /// Exchange clock of the last applied update.
    pub clock: u64,
}

impl Orderbook {
    /// Apply a snapshot: **replace** the entire specified side with new levels.
    ///
    /// We call this once per side when we receive an `orderbook_snapshot`.
    /// Because a Kalshi snapshot may omit a side entirely (no levels), we pass
    /// the full set of updates at once so we can clear first.
    pub fn apply_snapshot_side(&mut self, side: Side, levels: &[(i64, i64)], clock: u64) {
        if clock < self.clock {
            return;
        }

        let map = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        map.clear();
        for (price, qty) in levels {
            if *qty > 0 {
                map.insert(*price, *qty);
            }
        }
        if clock > self.clock {
            self.clock = clock;
        }
    }

    /// Apply a delta update to a single price level.
    ///
    /// A delta with `qty == 0` removes the level. Otherwise the quantity is
    /// set to the absolute value provided (Kalshi sends absolute quantities
    /// in snapshots and **net-change deltas** in delta messages). We store
    /// the running total and update it here.
    ///
    /// Returns the previous quantity at that price (0 if the level was absent).
    pub fn apply_delta(&mut self, side: Side, price: i64, delta_qty: i64, clock: u64) -> i64 {
        let map = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if clock < self.clock {
            return map.get(&price).copied().unwrap_or(0);
        }

        if clock > self.clock {
            self.clock = clock;
        }

        match map.entry(price) {
            Entry::Occupied(mut occ) => {
                let prev = *occ.get();
                let new_qty = prev + delta_qty;
                if new_qty <= 0 {
                    occ.remove();
                } else {
                    *occ.get_mut() = new_qty;
                }
                prev
            }
            Entry::Vacant(vac) => {
                if delta_qty > 0 {
                    vac.insert(delta_qty);
                }
                0
            }
        }
    }

    /// Return whether the book has any levels.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Return the best (highest) bid price and quantity.
    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (p, q))
    }

    /// Return the best (lowest) ask price and quantity.
    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(&p, &q)| (p, q))
    }

    /// Collect the current bids (ascending price order).
    pub fn bids_snapshot(&self) -> Vec<PriceLevel> {
        self.bids
            .iter()
            .map(|(&price, &qty)| PriceLevel { price, qty })
            .collect()
    }

    /// Collect the current asks (ascending price order).
    pub fn asks_snapshot(&self) -> Vec<PriceLevel> {
        self.asks
            .iter()
            .map(|(&price, &qty)| PriceLevel { price, qty })
            .collect()
    }
}
