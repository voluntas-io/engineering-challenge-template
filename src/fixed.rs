//! Exact decimal parsing and rescaling for exchange wire values.
//!
//! The SBE challenge schema represents both dollars and contract counts as
//! signed 64-bit integers scaled by 1,000,000. Exchange APIs publish decimal
//! strings with venue-specific precision, and non-prediction instruments need
//! arbitrary scales and signed prices. All conversion therefore goes through a
//! configured scale here, using `i128` intermediates and checked conversions.
//! Nothing is rounded silently: an inexact conversion is an error.

use anyhow::{Context, bail};

/// Number of fixed-point units in one canonical SBE dollar or contract/share.
pub const SCALE: i64 = 1_000_000;
/// Number of decimal places represented by [`SCALE`].
pub const DECIMALS: usize = 6;
/// Maximum supported decimal scale for an `i64` coefficient.
///
/// `10^18` is representable in `i64`; `10^19` is not.
pub const MAX_SCALE: u8 = 18;

/// Return `10^scale` as an `i64`, rejecting unsupported scales.
pub fn scale_factor(scale: u8) -> anyhow::Result<i64> {
    anyhow::ensure!(
        scale <= MAX_SCALE,
        "decimal scale {scale} exceeds maximum {MAX_SCALE}"
    );
    Ok(10i64.pow(u32::from(scale)))
}

/// Parse an exact signed decimal string into an integer coefficient at
/// `scale` decimal places.
///
/// Accepts plain decimal notation only (no exponent, no thousands separators)
/// and rejects more fractional digits than `scale` rather than rounding.
pub fn parse_scaled(input: &str, scale: u8) -> anyhow::Result<i64> {
    let text = input.trim();
    if text.is_empty() {
        bail!("empty fixed-point value");
    }
    if scale > MAX_SCALE {
        bail!("decimal scale {scale} exceeds maximum {MAX_SCALE}");
    }

    let (negative, unsigned) = if let Some(unsigned) = text.strip_prefix('-') {
        (true, unsigned)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    if unsigned.is_empty() {
        bail!("missing digits in fixed-point value `{input}`");
    }

    let mut pieces = unsigned.split('.');
    let whole = pieces.next().unwrap_or_default();
    let fraction = pieces.next();
    if pieces.next().is_some() {
        bail!("multiple decimal points in fixed-point value `{input}`");
    }
    if whole.is_empty() && fraction.is_none_or(str::is_empty) {
        bail!("missing digits in fixed-point value `{input}`");
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid whole-number digits in fixed-point value `{input}`");
    }
    if let Some(fraction) = fraction {
        if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("invalid fractional digits in fixed-point value `{input}`");
        }
        if fraction.len() > usize::from(scale) {
            bail!(
                "fixed-point value `{input}` has {} fractional digits; maximum is {scale}",
                fraction.len()
            );
        }
    }

    let whole_value: i128 = if whole.is_empty() {
        0
    } else {
        whole
            .parse()
            .with_context(|| format!("fixed-point whole part out of range in `{input}`"))?
    };
    let mut fractional_value = 0i128;
    if let Some(fraction) = fraction
        && !fraction.is_empty()
    {
        let exponent = u32::from(scale) - u32::try_from(fraction.len())?;
        let factor = 10i128.pow(exponent);
        fractional_value = fraction
            .parse::<i128>()
            .with_context(|| format!("fixed-point fraction out of range in `{input}`"))?
            * factor;
    }

    let magnitude = whole_value
        .checked_mul(10i128.pow(u32::from(scale)))
        .and_then(|value| value.checked_add(fractional_value))
        .with_context(|| format!("fixed-point value `{input}` overflows"))?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).with_context(|| format!("fixed-point value `{input}` out of range"))
}

/// Parse a signed decimal string into millionths.
pub fn parse_micros(input: &str) -> anyhow::Result<i64> {
    parse_scaled(input, u8::try_from(DECIMALS)?)
}

/// Parse a prediction-market price in dollars and validate the challenge's
/// `[0, 1]` probability range.
///
/// This guard belongs to the prediction-market adapters. The general numeric
/// model uses [`parse_scaled`] and accepts any signed price.
pub fn parse_price(input: &str) -> anyhow::Result<i64> {
    let price = parse_micros(input)?;
    anyhow::ensure!(
        (0..=SCALE).contains(&price),
        "price `{input}` outside [0, 1]"
    );
    Ok(price)
}

/// Parse a non-negative quantity in canonical six-decimal units.
pub fn parse_quantity(input: &str) -> anyhow::Result<i64> {
    parse_quantity_scaled(input, u8::try_from(DECIMALS)?)
}

/// Parse a non-negative quantity at a configured scale.
pub fn parse_quantity_scaled(input: &str, scale: u8) -> anyhow::Result<i64> {
    let quantity = parse_scaled(input, scale)?;
    anyhow::ensure!(quantity >= 0, "negative quantity `{input}`");
    Ok(quantity)
}

/// Format an integer coefficient at `scale` decimal places as a stable string.
#[must_use]
pub fn format_scaled(value: i64, scale: u8) -> String {
    let Ok(scale_value) = scale_factor(scale) else {
        return value.to_string();
    };
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    let divisor = scale_value.unsigned_abs();
    let whole = magnitude / divisor;
    let fraction = magnitude % divisor;
    if fraction == 0 {
        format!("{sign}{whole}")
    } else {
        let width = usize::from(scale);
        let fraction = format!("{fraction:0width$}");
        format!("{sign}{whole}.{}", fraction.trim_end_matches('0'))
    }
}

/// Format millionths as a stable decimal string, primarily for diagnostics.
#[must_use]
pub fn format_micros(value: i64) -> String {
    format_scaled(value, u8::try_from(DECIMALS).unwrap_or(6))
}

/// Rescale an exact integer coefficient, rejecting inexact conversions and
/// overflow.
pub fn rescale_exact(value: i64, from_scale: u8, to_scale: u8) -> anyhow::Result<i64> {
    anyhow::ensure!(
        from_scale <= MAX_SCALE && to_scale <= MAX_SCALE,
        "decimal scale exceeds maximum {MAX_SCALE}"
    );
    let value = i128::from(value);
    let rescaled = if to_scale >= from_scale {
        let factor = 10i128.pow(u32::from(to_scale - from_scale));
        value
            .checked_mul(factor)
            .context("rescaling overflows i128")?
    } else {
        let divisor = 10i128.pow(u32::from(from_scale - to_scale));
        if value % divisor != 0 {
            bail!(
                "value {value} at scale {from_scale} is not exactly representable at scale {to_scale}"
            );
        }
        value / divisor
    };
    i64::try_from(rescaled).context("rescaled value outside i64 range")
}

/// Convert an instrument-scale coefficient to the canonical SBE scale.
pub fn to_sbe_micros(value: i64, scale: u8) -> anyhow::Result<i64> {
    rescale_exact(value, scale, u8::try_from(DECIMALS)?)
}

/// Convert a canonical SBE coefficient to an instrument scale.
pub fn from_sbe_micros(value: i64, scale: u8) -> anyhow::Result<i64> {
    rescale_exact(value, u8::try_from(DECIMALS)?, scale)
}

/// Parse Kalshi's legacy integer-cent price.
pub fn cents_to_micros(cents: i64) -> anyhow::Result<i64> {
    cents
        .checked_mul(10_000)
        .context("cent price overflows fixed-point range")
}

/// Parse a whole-contract legacy quantity.
pub fn contracts_to_micros(contracts: i64) -> anyhow::Result<i64> {
    contracts
        .checked_mul(SCALE)
        .context("contract quantity overflows fixed-point range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_decimals() {
        assert_eq!(parse_micros("0").unwrap(), 0);
        assert_eq!(parse_micros("1").unwrap(), 1_000_000);
        assert_eq!(parse_micros("0.08").unwrap(), 80_000);
        assert_eq!(parse_micros("0.0800").unwrap(), 80_000);
        assert_eq!(parse_micros(".5").unwrap(), 500_000);
        assert_eq!(parse_micros("1.").unwrap(), 1_000_000);
        assert_eq!(parse_micros("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse_micros("300.00").unwrap(), 300_000_000);
        assert_eq!(parse_micros("219.217767").unwrap(), 219_217_767);
    }

    #[test]
    fn parses_at_configured_scale_beyond_six_decimals() {
        assert_eq!(parse_scaled("0.123456789", 9).unwrap(), 123_456_789);
        assert_eq!(parse_scaled("-0.000000001", 9).unwrap(), -1);
        assert_eq!(
            parse_scaled("123456789012345678", 0).unwrap(),
            123_456_789_012_345_678
        );
        assert_eq!(parse_scaled("1.2300", 4).unwrap(), 12_300);
        assert!(parse_scaled("1.0000000001", 9).is_err());
        assert!(parse_scaled("1", 19).is_err());
    }

    #[test]
    fn rejects_inexact_or_malformed_values() {
        for value in [
            "",
            ".",
            "-",
            "+",
            "1.0000001",
            "1e-3",
            "1,000",
            "nan",
            "--1",
            "−1",
            "１２",
        ] {
            assert!(parse_micros(value).is_err(), "{value} should be rejected");
        }
    }

    #[test]
    fn validates_prediction_market_price_range() {
        assert_eq!(parse_price("0.0000").unwrap(), 0);
        assert_eq!(parse_price("1.0000").unwrap(), 1_000_000);
        assert!(parse_price("1.000001").is_err());
        assert!(parse_price("-0.1").is_err());
        // The general parser deliberately does not impose the range.
        assert_eq!(parse_scaled("-2.5", 6).unwrap(), -2_500_000);
    }

    #[test]
    fn formats_cleanly_at_any_scale() {
        assert_eq!(format_micros(80_000), "0.08");
        assert_eq!(format_micros(-54_000_000), "-54");
        assert_eq!(format_micros(219_217_767), "219.217767");
        assert_eq!(format_scaled(123_456_789, 9), "0.123456789");
        assert_eq!(format_scaled(-1, 9), "-0.000000001");
        assert_eq!(format_scaled(42, 0), "42");
    }

    #[test]
    fn rescales_exactly_or_rejects() {
        assert_eq!(rescale_exact(12_300, 4, 6).unwrap(), 1_230_000);
        assert_eq!(rescale_exact(1_230_000, 6, 4).unwrap(), 12_300);
        assert_eq!(rescale_exact(-1, 6, 6).unwrap(), -1);
        assert!(
            rescale_exact(1, 6, 4).is_err(),
            "1e-6 is not a 4-place value"
        );
        assert!(rescale_exact(12_345, 4, 2).is_err());
        assert!(
            to_sbe_micros(123_456_789, 9)
                .unwrap_err()
                .to_string()
                .contains("not exactly")
        );
        assert_eq!(to_sbe_micros(123_456_000, 9).unwrap(), 123_456);
    }

    #[test]
    fn round_trips_scale_conversion() {
        for value in [-123_456_000_i64, -1_000, 0, 1_000, 123_456_000] {
            let micros = rescale_exact(value, 9, 6).unwrap();
            assert_eq!(rescale_exact(micros, 6, 9).unwrap(), value);
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_micros("9223372036855").is_err());
        assert!(cents_to_micros(i64::MAX).is_err());
        assert!(rescale_exact(i64::MAX, 0, 6).is_err());
        assert!(parse_scaled("9223372036854775808", 0).is_err());
    }
}
