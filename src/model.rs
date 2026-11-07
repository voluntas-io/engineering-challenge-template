//! Normalized market-data domain types shared by exchanges, the collector core,
//! replay, and inspection.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Stable identifier of one websocket source (a shard replica).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct SourceId {
    pub shard: u16,
    pub replica: u16,
}

impl SourceId {
    #[must_use]
    pub const fn new(shard: u16, replica: u16) -> Self {
        Self { shard, replica }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}r{}", self.shard, self.replica)
    }
}

/// Exact price coefficient.
///
/// The coefficient is interpreted using the owning instrument's
/// [`NumericSpec::price_scale`] and [`NumericSpec::price_unit`]. The scale and
/// unit live in instrument metadata, not in every book level, so a level stays
/// one machine word. Prices are signed: negative prices are valid for yield,
/// spread, and other non-probability instruments.
///
/// `Price` and [`Quantity`] are deliberately not interchangeable:
///
/// ```compile_fail
/// use engineering_challenge::model::{Price, Quantity};
/// let _: Quantity = Price::from_raw(1);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Price(i64);

impl Price {
    #[must_use]
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }

    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    #[must_use]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }
}

impl From<i64> for Price {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Price> for i64 {
    fn from(value: Price) -> Self {
        value.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Exact non-negative quantity coefficient.
///
/// Positive book quantities are non-negative by construction. Deltas use
/// [`QuantityDelta`] instead, so a signed delta can never be stored as an
/// absolute quantity by accident.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(try_from = "i64", into = "i64")]
pub struct Quantity(i64);

impl Quantity {
    pub const ZERO: Self = Self(0);

    pub fn new(value: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(value >= 0, "negative quantity {value}");
        Ok(Self(value))
    }

    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl TryFrom<i64> for Quantity {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Quantity> for i64 {
    fn from(value: Quantity) -> Self {
        value.0
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Signed change to a non-negative quantity.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct QuantityDelta(i64);

impl QuantityDelta {
    #[must_use]
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl From<i64> for QuantityDelta {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<QuantityDelta> for i64 {
    fn from(value: QuantityDelta) -> Self {
        value.0
    }
}

impl fmt::Display for QuantityDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Price denomination carried by instrument metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriceUnit {
    /// ISO currency code, for example `USD` or `USDC`.
    Currency(String),
    /// Index or contract points.
    Points,
    /// Interest rate.
    Rate,
    /// Yield.
    Yield,
    /// Probability in `[0, 1]`.
    Probability,
    /// Another explicitly named unit.
    Other(String),
}

impl Default for PriceUnit {
    fn default() -> Self {
        Self::Currency("USD".to_owned())
    }
}

/// Quantity denomination carried by instrument metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantityUnit {
    Shares,
    #[default]
    Contracts,
    Lots,
    Currency(String),
    Physical(String),
    Other(String),
}

/// One band of a tick schedule.
///
/// A price is on the grid when it lies in `[start, end]` and
/// `(price - origin) % step == 0`. The origin makes tick-phase validation
/// explicit rather than assuming every venue's grid starts at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceBand {
    pub start: Price,
    pub end: Price,
    pub step: Price,
    pub origin: Price,
}

impl PriceBand {
    #[must_use]
    pub const fn new(start: Price, end: Price, step: Price, origin: Price) -> Self {
        Self {
            start,
            end,
            step,
            origin,
        }
    }

    #[must_use]
    pub const fn from_micros(start_us: i64, end_us: i64, step_us: i64, origin_us: i64) -> Self {
        Self {
            start: Price::from_raw(start_us),
            end: Price::from_raw(end_us),
            step: Price::from_raw(step_us),
            origin: Price::from_raw(origin_us),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.start <= self.end, "price band start exceeds end");
        anyhow::ensure!(self.step.raw() > 0, "non-positive price band step");
        ensure_on_grid(self.start, self)?;
        ensure_on_grid(self.end, self)?;
        Ok(())
    }

    #[must_use]
    pub fn contains(&self, price: Price) -> bool {
        self.start <= price && price <= self.end
    }

    #[must_use]
    pub fn is_on_grid(&self, price: Price) -> bool {
        if !self.contains(price) {
            return false;
        }
        let offset = i128::from(price.raw()) - i128::from(self.origin.raw());
        offset % i128::from(self.step.raw()) == 0
    }
}

fn ensure_on_grid(price: Price, band: &PriceBand) -> anyhow::Result<()> {
    let offset = i128::from(price.raw()) - i128::from(band.origin.raw());
    anyhow::ensure!(
        offset % i128::from(band.step.raw()) == 0,
        "price band boundary {} is not on the tick grid",
        price
    );
    Ok(())
}

/// Exact rational multiplier such as a contract multiplier or lot conversion.
///
/// Arithmetic uses `i128` and rejects a result that is not an exact `i64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawRatio")]
pub struct Ratio {
    numerator: i64,
    denominator: i64,
}

#[derive(Serialize, Deserialize)]
struct RawRatio {
    numerator: i64,
    denominator: i64,
}

impl Ratio {
    pub fn new(numerator: i64, denominator: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(denominator != 0, "zero ratio denominator");
        let (numerator, denominator) = if denominator < 0 {
            (
                numerator
                    .checked_neg()
                    .ok_or_else(|| anyhow::anyhow!("ratio numerator overflow"))?,
                denominator
                    .checked_neg()
                    .ok_or_else(|| anyhow::anyhow!("ratio denominator overflow"))?,
            )
        } else {
            (numerator, denominator)
        };
        let divisor = gcd(numerator.unsigned_abs(), denominator.unsigned_abs());
        let divisor = i64::try_from(divisor).unwrap_or(1).max(1);
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    #[must_use]
    pub const fn numerator(self) -> i64 {
        self.numerator
    }

    #[must_use]
    pub const fn denominator(self) -> i64 {
        self.denominator
    }

    /// Multiply an integer by this ratio, returning an exact `i64` or an error.
    pub fn apply_exact(self, value: i64) -> anyhow::Result<i64> {
        let product = i128::from(value)
            .checked_mul(i128::from(self.numerator))
            .ok_or_else(|| anyhow::anyhow!("ratio multiplication overflows i128"))?;
        let divisor = i128::from(self.denominator);
        anyhow::ensure!(
            product % divisor == 0,
            "ratio {}/{} does not produce an exact integer for {value}",
            self.numerator,
            self.denominator
        );
        i64::try_from(product / divisor).map_err(|_| anyhow::anyhow!("ratio result outside i64"))
    }
}

impl TryFrom<RawRatio> for Ratio {
    type Error = anyhow::Error;

    fn try_from(value: RawRatio) -> Result<Self, Self::Error> {
        Self::new(value.numerator, value.denominator)
    }
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

/// Exact numeric semantics for one instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumericSpec {
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub price_unit: PriceUnit,
    pub quantity_unit: QuantityUnit,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tick_schedule: Vec<PriceBand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_step: Option<Quantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lot_size: Option<Quantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_multiplier: Option<Ratio>,
}

impl Default for NumericSpec {
    fn default() -> Self {
        Self {
            price_scale: u8::try_from(crate::fixed::DECIMALS).unwrap_or(6),
            quantity_scale: u8::try_from(crate::fixed::DECIMALS).unwrap_or(6),
            price_unit: PriceUnit::default(),
            quantity_unit: QuantityUnit::default(),
            tick_schedule: Vec::new(),
            quantity_step: None,
            lot_size: None,
            contract_multiplier: None,
        }
    }
}

impl NumericSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.price_scale <= crate::fixed::MAX_SCALE,
            "price scale {} exceeds maximum {}",
            self.price_scale,
            crate::fixed::MAX_SCALE
        );
        anyhow::ensure!(
            self.quantity_scale <= crate::fixed::MAX_SCALE,
            "quantity scale {} exceeds maximum {}",
            self.quantity_scale,
            crate::fixed::MAX_SCALE
        );
        validate_unit_text(&self.price_unit)?;
        validate_quantity_unit(&self.quantity_unit)?;
        for band in &self.tick_schedule {
            band.validate()?;
        }
        for pair in self.tick_schedule.windows(2) {
            anyhow::ensure!(
                pair[0].end <= pair[1].start,
                "price bands overlap or are out of order"
            );
        }
        if let Some(step) = self.quantity_step {
            anyhow::ensure!(!step.is_zero(), "zero quantity step");
        }
        if let Some(lot) = self.lot_size {
            anyhow::ensure!(!lot.is_zero(), "zero lot size");
            if let Some(step) = self.quantity_step {
                anyhow::ensure!(
                    lot.raw() % step.raw() == 0,
                    "lot size is not a multiple of the quantity step"
                );
            }
        }
        if let Some(multiplier) = self.contract_multiplier {
            anyhow::ensure!(
                multiplier.numerator() != 0,
                "zero contract multiplier numerator"
            );
        }
        Ok(())
    }

    /// Whether the price is valid under the configured tick schedule.
    #[must_use]
    pub fn accepts_price(&self, price: Price) -> bool {
        self.tick_schedule.is_empty()
            || self.tick_schedule.iter().any(|band| band.is_on_grid(price))
    }

    /// Whether the quantity satisfies the configured increment and lot size.
    #[must_use]
    pub fn accepts_quantity(&self, quantity: Quantity) -> bool {
        if let Some(step) = self.quantity_step
            && quantity.raw() % step.raw() != 0
        {
            return false;
        }
        if let Some(lot) = self.lot_size
            && quantity.raw() < lot.raw()
            && quantity.raw() != 0
        {
            return false;
        }
        true
    }
}

fn validate_unit_text(unit: &PriceUnit) -> anyhow::Result<()> {
    match unit {
        PriceUnit::Currency(code) => {
            anyhow::ensure!(!code.trim().is_empty(), "empty currency code")
        }
        PriceUnit::Other(name) => anyhow::ensure!(!name.trim().is_empty(), "empty price unit"),
        PriceUnit::Points | PriceUnit::Rate | PriceUnit::Yield | PriceUnit::Probability => {}
    }
    Ok(())
}

fn validate_quantity_unit(unit: &QuantityUnit) -> anyhow::Result<()> {
    match unit {
        QuantityUnit::Currency(code) => {
            anyhow::ensure!(!code.trim().is_empty(), "empty quantity currency code");
        }
        QuantityUnit::Physical(name) | QuantityUnit::Other(name) => {
            anyhow::ensure!(!name.trim().is_empty(), "empty quantity unit");
        }
        QuantityUnit::Shares | QuantityUnit::Contracts | QuantityUnit::Lots => {}
    }
    Ok(())
}

/// One exchange-defined band of valid prices.
///
/// Kalshi exposes `price_ranges` as fixed-point dollar strings. Keeping every
/// band (rather than collapsing to one tick size) preserves tapered grids such
/// as `$0.00-$0.10` at `$0.001` and `$0.10-$0.90` at `$0.01`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceRange {
    pub start_us: i64,
    pub end_us: i64,
    pub step_us: i64,
}

impl PriceRange {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.start_us <= self.end_us,
            "price range start exceeds end"
        );
        anyhow::ensure!(self.step_us > 0, "non-positive price range step");
        Ok(())
    }

    /// Validate the challenge's probability range in prediction-market
    /// adapters. This is not a universal price restriction.
    pub fn validate_probability(&self) -> anyhow::Result<()> {
        self.validate()?;
        anyhow::ensure!(
            (0..=crate::fixed::SCALE).contains(&self.start_us),
            "price range start outside [0, 1]"
        );
        anyhow::ensure!(
            (0..=crate::fixed::SCALE).contains(&self.end_us),
            "price range end outside [0, 1]"
        );
        Ok(())
    }

    #[must_use]
    pub const fn to_band(self) -> PriceBand {
        PriceBand::from_micros(self.start_us, self.end_us, self.step_us, 0)
    }
}

/// Exchange-level metadata for one subscribable symbol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketInfo {
    /// Exchange name (`kalshi` or `polymarket`).
    pub exchange: String,
    /// Identifier used on the exchange wire (ticker or asset id).
    pub native_symbol: String,
    /// Namespaced identifier written to SBE.
    pub symbol: String,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub exchange_market_id: Option<String>,
    pub close_time_us: Option<i64>,
    /// Finest valid tick across all bands, in millionths of a dollar.
    pub tick_size_us: Option<i64>,
    /// Complete exchange price grid when the exchange publishes one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub price_ranges: Vec<PriceRange>,
    /// Human-readable exchange grid label. Never use this for pricing logic;
    /// `price_ranges` is authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_level_structure: Option<String>,
    /// Exact price/quantity scales, units, tick schedule, and multipliers.
    #[serde(default)]
    pub numeric_spec: NumericSpec,
}

impl MarketInfo {
    /// Validate fields that every adapter must supply.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.exchange.is_empty(), "empty exchange name");
        anyhow::ensure!(!self.native_symbol.is_empty(), "empty native symbol");
        anyhow::ensure!(!self.symbol.is_empty(), "empty output symbol");
        if let Some(close_time_us) = self.close_time_us {
            anyhow::ensure!(close_time_us >= 0, "negative close timestamp");
        }
        if let Some(tick_size_us) = self.tick_size_us {
            anyhow::ensure!(tick_size_us > 0, "non-positive tick size");
        }
        for range in &self.price_ranges {
            range.validate()?;
        }
        self.numeric_spec.validate()?;
        Ok(())
    }
}

/// One aggregated price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    pub quantity: Quantity,
}

impl Level {
    pub fn new(price: i64, quantity: i64) -> anyhow::Result<Self> {
        Ok(Self {
            price: Price::from_raw(price),
            quantity: Quantity::new(quantity)?,
        })
    }
}

/// A complete two-sided book state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl BookSnapshot {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

/// Metadata carried by a decoded exchange action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventMeta {
    /// Local time captured immediately after the websocket frame was read.
    pub received_at_us: i64,
    /// Best exchange-provided timestamp, if the feed supplied one.
    pub exchange_ts_us: Option<i64>,
    /// Per-subscription sequence number, if the feed supplies one.
    pub seq: Option<u64>,
    /// Exchange-wide ordering clock, if the feed supplies one.
    pub clock: Option<i64>,
    /// Locally assigned connection generation. It increases on every successful
    /// reconnect and is stamped by the supervisor before the coordinator sees
    /// the event. Together with `receive_seq` it provides a monotonic receive
    /// fence that does not depend on wall-clock timestamps.
    pub connection_epoch: u64,
    /// Locally assigned, monotonically increasing frame number within
    /// `connection_epoch`. Frames are numbered in read order before decoding.
    pub receive_seq: u64,
}

impl EventMeta {
    #[must_use]
    pub const fn new(received_at_us: i64) -> Self {
        Self {
            received_at_us,
            exchange_ts_us: None,
            seq: None,
            clock: None,
            connection_epoch: 0,
            receive_seq: 0,
        }
    }

    /// Whether this event carries a supervisor-assigned receive fence.
    #[must_use]
    pub const fn has_receive_fence(&self) -> bool {
        self.connection_epoch != 0 || self.receive_seq != 0
    }

    #[must_use]
    pub fn exchange_ts_or_received(&self) -> i64 {
        self.exchange_ts_us.unwrap_or(self.received_at_us)
    }
}

/// How a level's quantity changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityChange {
    /// Replace the level with this non-negative absolute quantity.
    Absolute(Quantity),
    /// Add this signed quantity to the current level.
    Delta(QuantityDelta),
}

/// Exchange lifecycle notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleKind {
    Created,
    Activated,
    Deactivated,
    CloseDateUpdated,
    Determined,
    Settled,
    PriceStructureUpdated,
    MetadataUpdated,
    NewMarket,
    Resolved,
    Other(String),
}

impl LifecycleKind {
    /// Whether the market is no longer tradable and should be removed from the
    /// live collection until discovery/lifecycle says otherwise.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Determined | Self::Settled | Self::Resolved)
    }
}

/// Continuity state of a symbol's order book.
///
/// This tracks whether a book is seeded, live, stale, resyncing after a
/// discontinuity, or terminal. It is separate from source connectivity: a
/// connected socket does not imply a live book, and a self-consistent SBE
/// capture does not prove complete event receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityState {
    /// No accepted exchange snapshot has established the book.
    Unseeded,
    /// An accepted snapshot anchors the active source; no known gap is
    /// currently unresolved.
    Live,
    /// Freshness policy has fired; the book must not be considered eligible
    /// for downstream risk-increasing use.
    Stale,
    /// Continuity uncertain: active-source loss, reset, disconnect, malformed
    /// frame, or stale-probe escalation. Deltas cannot restore state by
    /// themselves. A fresh authoritative book is required.
    Resyncing,
    /// Venue lifecycle marks the instrument resolved/terminal. Do not
    /// silently re-enable from a late delta.
    Terminal,
}

impl ContinuityState {
    /// Whether the book may be used for downstream risk-increasing decisions.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }

    /// Whether the book is in a state that blocks risk-increasing actions.
    #[must_use]
    pub fn blocks_risk(&self) -> bool {
        matches!(
            self,
            Self::Unseeded | Self::Stale | Self::Resyncing | Self::Terminal
        )
    }
}

/// Reason for a continuity gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscontinuityReason {
    /// Active source lost (disconnect, protocol error)
    ActiveSourceLost,
    /// Sequence/clock gap detected
    SequenceGap,
    /// Malformed frame rejected
    MalformedFrame,
    /// Stale probe escalation (max probes exceeded)
    StaleProbeEscalation,
    /// Active source switched (failover)
    SourceSwitched,
    /// Stream reset
    StreamReset,
    /// Protocol error on the wire
    ProtocolError,
    /// Lifecycle terminal event
    Terminal,
}

impl fmt::Display for DiscontinuityReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ActiveSourceLost => "active_source_lost",
            Self::SequenceGap => "sequence_gap",
            Self::MalformedFrame => "malformed_frame",
            Self::StaleProbeEscalation => "stale_probe_escalation",
            Self::SourceSwitched => "source_switched",
            Self::StreamReset => "stream_reset",
            Self::ProtocolError => "protocol_error",
            Self::Terminal => "terminal",
        })
    }
}

/// Why a source's books must be considered invalid until fresh snapshots
/// arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetReason {
    Connected,
    SequenceGap,
    ProtocolError,
    SubscriptionRefresh,
    RequestedRefresh,
}

impl fmt::Display for ResetReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => f.write_str("connected"),
            Self::SequenceGap => f.write_str("sequence-gap"),
            Self::ProtocolError => f.write_str("protocol-error"),
            Self::SubscriptionRefresh => f.write_str("subscription-refresh"),
            Self::RequestedRefresh => f.write_str("requested-refresh"),
        }
    }
}

/// Normalized event emitted by an exchange adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeEvent {
    Snapshot {
        native_symbol: String,
        book: BookSnapshot,
        meta: EventMeta,
    },
    LevelChange {
        native_symbol: String,
        side: Side,
        price: Price,
        change: QuantityChange,
        meta: EventMeta,
    },
    Lifecycle {
        native_symbol: String,
        kind: LifecycleKind,
        close_time_us: Option<i64>,
        title: Option<String>,
        /// Updated finest tick size when the exchange publishes one.
        tick_size_us: Option<i64>,
        /// Updated full price grid when the exchange publishes one.
        price_ranges: Option<Vec<PriceRange>>,
        /// Updated human-readable grid label, never used for pricing.
        price_level_structure: Option<String>,
        meta: EventMeta,
    },
    /// Invalidate some or all source-local books and wait for snapshots.
    StreamReset {
        native_symbols: Vec<String>,
        reason: ResetReason,
        /// Local time at which the reset was observed.
        received_at_us: i64,
    },
}

/// Command sent from the collector core/coordinator to a source worker.
#[derive(Debug, Clone)]
pub enum Control {
    /// Replace this source's desired native symbol set.
    SetSubscriptions(BTreeSet<String>),
    /// Ask the source to emit fresh snapshots for these symbols without a
    /// recovery trigger. Used for non-incident probes.
    RequestSnapshot(Vec<String>),
    /// Explicit continuity recovery. The core creates the request before this
    /// control is sent and only accepts a snapshot whose supervisor-assigned
    /// receive fence is strictly newer than the acknowledgement sent by the
    /// worker for `generation`.
    Recover {
        generation: u64,
        symbols: Vec<String>,
        /// Drop the connection first. The replacement snapshot must then come
        /// from a later connection epoch.
        reconnect: bool,
    },
    /// Drop the connection and reconnect with exponential backoff.
    Reconnect(&'static str),
    /// Close the websocket and stop the worker.
    Shutdown,
}

/// One SBE message ready for the writer.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputMessage {
    Snapshot(SnapshotDomain),
    PriceLevelUpdate(PriceLevelUpdateDomain),
}

impl OutputMessage {
    #[must_use]
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot(snapshot) => &snapshot.symbol,
            Self::PriceLevelUpdate(update) => &update.symbol,
        }
    }
}

/// Per-source connectivity and protocol counters surfaced by a worker.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceStatus {
    pub source: SourceId,
    pub connected: bool,
    pub reconnects: u64,
    pub messages: u64,
    pub decode_errors: u64,
    pub protocol_errors: u64,
    pub bytes_received: u64,
    pub last_frame_us: Option<i64>,
    pub last_keepalive_us: Option<i64>,
    pub latency_ewma_us: Option<i64>,
    pub subscriptions: usize,
    pub last_error: Option<String>,
}

impl SourceStatus {
    #[must_use]
    pub fn new(source: SourceId) -> Self {
        Self {
            source,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod numeric_tests {
    use super::*;

    fn band(start: i64, end: i64, step: i64, origin: i64) -> PriceBand {
        PriceBand::from_micros(start, end, step, origin)
    }

    #[test]
    fn price_and_quantity_have_distinct_units_and_ranges() {
        assert_eq!(Price::from_raw(-2_500_000).raw(), -2_500_000);
        assert!(Price::from_raw(-1).is_negative());
        assert_eq!(Quantity::new(0).unwrap().raw(), 0);
        assert!(Quantity::new(-1).is_err());
        assert!(Quantity::try_from(-1).is_err());
        assert_eq!(Quantity::try_from(3).unwrap().raw(), 3);
        // Deltas are signed and separate from absolute quantities.
        assert_eq!(QuantityDelta::from_raw(-4).raw(), -4);
        assert!(QuantityDelta::from_raw(-4).raw() < 0);
    }

    #[test]
    fn negative_and_large_prices_round_trip_through_serde() {
        for price in [
            Price::from_raw(i64::MIN),
            Price::from_raw(-2_500_000),
            Price::from_raw(-1),
            Price::from_raw(0),
            Price::from_raw(i64::MAX),
        ] {
            let encoded = serde_json::to_string(&price).unwrap();
            let decoded: Price = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, price);
        }
        let level = Level::new(-2_500_000, 7).unwrap();
        let encoded = serde_json::to_string(&level).unwrap();
        assert_eq!(encoded, r#"{"price":-2500000,"quantity":7}"#);
        assert_eq!(serde_json::from_str::<Level>(&encoded).unwrap(), level);
        assert!(serde_json::from_str::<Level>(r#"{"price":1,"quantity":-1}"#).is_err());
    }

    #[test]
    fn validates_tick_bands_boundaries_and_origins() {
        let schedule = NumericSpec {
            tick_schedule: vec![
                band(0, 100_000, 1_000, 0),
                band(100_000, 1_000_000, 10_000, 0),
            ],
            ..NumericSpec::default()
        };
        schedule.validate().unwrap();
        assert!(schedule.accepts_price(Price::from_raw(0)));
        assert!(schedule.accepts_price(Price::from_raw(99_000)));
        assert!(schedule.accepts_price(Price::from_raw(100_000)));
        assert!(schedule.accepts_price(Price::from_raw(1_000_000)));
        assert!(!schedule.accepts_price(Price::from_raw(99_500)));
        assert!(!schedule.accepts_price(Price::from_raw(1_000_500)));

        // A non-zero origin makes tick phase explicit.
        let shifted = NumericSpec {
            tick_schedule: vec![band(1_000, 9_000, 2_000, 1_000)],
            ..NumericSpec::default()
        };
        shifted.validate().unwrap();
        assert!(shifted.accepts_price(Price::from_raw(1_000)));
        assert!(shifted.accepts_price(Price::from_raw(3_000)));
        assert!(!shifted.accepts_price(Price::from_raw(2_000)));

        // Boundaries that are not on the grid are rejected at configuration time.
        assert!(band(500, 9_000, 1_000, 0).validate().is_err());
        assert!(band(0, 9_000, 0, 0).validate().is_err());
        assert!(band(9_000, 0, 1_000, 0).validate().is_err());
        assert!(
            NumericSpec {
                tick_schedule: vec![band(0, 9_000, 1_000, 0), band(8_000, 10_000, 1_000, 0)],
                ..NumericSpec::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn validates_quantity_steps_and_lot_sizes() {
        let spec = NumericSpec {
            quantity_step: Some(Quantity::new(10).unwrap()),
            lot_size: Some(Quantity::new(100).unwrap()),
            ..NumericSpec::default()
        };
        spec.validate().unwrap();
        assert!(spec.accepts_quantity(Quantity::new(0).unwrap()));
        assert!(spec.accepts_quantity(Quantity::new(100).unwrap()));
        assert!(spec.accepts_quantity(Quantity::new(110).unwrap()));
        assert!(!spec.accepts_quantity(Quantity::new(105).unwrap()));
        assert!(!spec.accepts_quantity(Quantity::new(50).unwrap()));
        assert!(
            NumericSpec {
                quantity_step: Some(Quantity::new(10).unwrap()),
                lot_size: Some(Quantity::new(105).unwrap()),
                ..NumericSpec::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            NumericSpec {
                quantity_step: Some(Quantity::new(0).unwrap()),
                ..NumericSpec::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn ratios_are_exact_and_never_use_floating_point() {
        let multiplier = Ratio::new(3, 2).unwrap();
        assert_eq!(multiplier.numerator(), 3);
        assert_eq!(multiplier.denominator(), 2);
        assert_eq!(multiplier.apply_exact(10).unwrap(), 15);
        assert_eq!(Ratio::new(4, 2).unwrap().numerator(), 2);
        assert_eq!(Ratio::new(4, 2).unwrap().denominator(), 1);
        assert!(multiplier.apply_exact(5).is_err(), "7.5 is not exact");
        assert!(Ratio::new(1, 0).is_err());
        assert!(
            Ratio::new(i64::MAX, 1)
                .unwrap()
                .apply_exact(i64::MAX)
                .is_err()
        );
    }

    #[test]
    fn numeric_spec_preserves_units_and_scales_through_json() {
        let spec = NumericSpec {
            price_scale: 9,
            quantity_scale: 2,
            price_unit: PriceUnit::Yield,
            quantity_unit: QuantityUnit::Lots,
            tick_schedule: vec![band(-1_000_000, 0, 1, -1_000_000)],
            quantity_step: Some(Quantity::new(25).unwrap()),
            lot_size: Some(Quantity::new(100).unwrap()),
            contract_multiplier: Some(Ratio::new(5, 2).unwrap()),
        };
        spec.validate().unwrap();
        let encoded = serde_json::to_string(&spec).unwrap();
        let decoded: NumericSpec = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, spec);
        assert_eq!(format!("{:?}", decoded.price_unit), "Yield");
        assert_eq!(format!("{:?}", decoded.quantity_unit), "Lots");
    }

    #[test]
    fn price_range_probability_guard_is_separate_from_general_validation() {
        let negative = PriceRange {
            start_us: -2_000_000,
            end_us: 2_000_000,
            step_us: 1,
        };
        negative.validate().unwrap();
        assert!(negative.validate_probability().is_err());
        PriceRange {
            start_us: 0,
            end_us: 1_000_000,
            step_us: 1_000,
        }
        .validate_probability()
        .unwrap();
    }
}
