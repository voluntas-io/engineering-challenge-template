//! Exact conversion between instrument-scale numeric values and the supplied
//! challenge SBE representation.
//!
//! The wire schema is fixed at six decimal places. Instrument metadata may use
//! another scale, including more than six decimals. Conversion is exact and
//! fails rather than rounding, so a value that cannot be represented in the
//! capture is rejected before it reaches the writer.

use anyhow::Context;

use crate::fixed::{DECIMALS, rescale_exact};
use crate::model::{Level, NumericSpec, Price, Quantity};

fn sbe_scale() -> anyhow::Result<u8> {
    u8::try_from(DECIMALS).context("SBE decimal count is not a valid scale")
}

/// Convert an instrument-scale price to the canonical SBE coefficient.
pub fn price_to_sbe(price: Price, spec: &NumericSpec) -> anyhow::Result<i64> {
    rescale_exact(price.raw(), spec.price_scale, sbe_scale()?).with_context(|| {
        format!(
            "price {} at scale {} is not exactly representable at SBE scale {DECIMALS}",
            price, spec.price_scale
        )
    })
}

/// Convert an instrument-scale quantity to the canonical SBE coefficient.
pub fn quantity_to_sbe(quantity: Quantity, spec: &NumericSpec) -> anyhow::Result<i64> {
    rescale_exact(quantity.raw(), spec.quantity_scale, sbe_scale()?).with_context(|| {
        format!(
            "quantity {quantity} at scale {} is not exactly representable at SBE scale {DECIMALS}",
            spec.quantity_scale
        )
    })
}

/// Convert a canonical SBE price coefficient back to the instrument scale.
pub fn price_from_sbe(value: i64, spec: &NumericSpec) -> anyhow::Result<Price> {
    rescale_exact(value, sbe_scale()?, spec.price_scale)
        .map(Price::from_raw)
        .with_context(|| {
            format!(
                "SBE price {value} is not exactly representable at scale {}",
                spec.price_scale
            )
        })
}

/// Convert a canonical SBE quantity coefficient back to the instrument scale.
pub fn quantity_from_sbe(value: i64, spec: &NumericSpec) -> anyhow::Result<Quantity> {
    let quantity = rescale_exact(value, sbe_scale()?, spec.quantity_scale)
        .context("SBE quantity is not exactly representable at the instrument scale")?;
    Quantity::new(quantity).context("SBE quantity is negative")
}

/// Convert one book level to SBE `(price, quantity)` coefficients.
pub fn level_to_sbe(level: &Level, spec: &NumericSpec) -> anyhow::Result<(i64, i64)> {
    Ok((
        price_to_sbe(level.price, spec)?,
        quantity_to_sbe(level.quantity, spec)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PriceUnit, QuantityDelta, QuantityUnit};

    fn six_decimal_spec() -> NumericSpec {
        NumericSpec {
            price_scale: 6,
            quantity_scale: 6,
            price_unit: PriceUnit::Currency("USDC".to_owned()),
            quantity_unit: QuantityUnit::Contracts,
            ..NumericSpec::default()
        }
    }

    #[test]
    fn scale_six_conversion_is_identity_for_existing_captures() {
        let spec = six_decimal_spec();
        for value in [i64::MIN, -2_500_000, -1, 0, 1, 999_999, i64::MAX] {
            assert_eq!(price_to_sbe(Price::from_raw(value), &spec).unwrap(), value);
            assert_eq!(
                price_from_sbe(value, &spec).unwrap(),
                Price::from_raw(value)
            );
        }
        for value in [0, 1, 999_999, i64::MAX] {
            let quantity = Quantity::new(value).unwrap();
            assert_eq!(quantity_to_sbe(quantity, &spec).unwrap(), value);
            assert_eq!(quantity_from_sbe(value, &spec).unwrap(), quantity);
        }
    }

    #[test]
    fn converts_scales_exactly_and_rejects_inexact_values() {
        let spec = NumericSpec {
            price_scale: 9,
            quantity_scale: 2,
            ..NumericSpec::default()
        };
        assert_eq!(
            price_to_sbe(Price::from_raw(123_456_000), &spec).unwrap(),
            123_456
        );
        assert!(price_to_sbe(Price::from_raw(123_456_789), &spec).is_err());
        assert_eq!(price_from_sbe(123_456, &spec).unwrap().raw(), 123_456_000);
        assert_eq!(
            quantity_to_sbe(Quantity::new(123).unwrap(), &spec).unwrap(),
            1_230_000
        );
        assert_eq!(
            quantity_from_sbe(1_230_000, &spec).unwrap(),
            Quantity::new(123).unwrap()
        );
        assert!(quantity_from_sbe(-1, &spec).is_err());
    }

    #[test]
    fn preserves_units_while_converting() {
        let spec = NumericSpec {
            price_scale: 2,
            quantity_scale: 0,
            price_unit: PriceUnit::Yield,
            quantity_unit: QuantityUnit::Lots,
            ..NumericSpec::default()
        };
        let level = Level {
            price: Price::from_raw(-125),
            quantity: Quantity::new(3).unwrap(),
        };
        assert_eq!(
            level_to_sbe(&level, &spec).unwrap(),
            (-1_250_000, 3_000_000)
        );
        assert_eq!(spec.price_unit, PriceUnit::Yield);
        assert_eq!(spec.quantity_unit, QuantityUnit::Lots);
        // Deltas are not SBE quantities and cannot be passed as one.
        let delta = QuantityDelta::from_raw(-1);
        assert_eq!(delta.raw(), -1);
    }
}
