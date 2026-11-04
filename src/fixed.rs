//! Decimal strings to the schema's fixed point, without floating point.
//!
//! The schema uses `1_000_000` = 1 USDC for prices and 1 share for
//! quantities, so values carry at most six decimal places.

use std::fmt;

/// Fixed-point scale: `SCALE` = 1.0.
pub const SCALE: i64 = 1_000_000;
const DECIMALS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFixedError {
    input: String,
    reason: &'static str,
}

impl fmt::Display for ParseFixedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid decimal {:?}: {}", self.input, self.reason)
    }
}

impl std::error::Error for ParseFixedError {}

/// Parses `"0.523"`, `"-54.00"`, `"300"`, `".5"` into fixed point.
///
/// Rejects more than six decimal places instead of rounding, so a format
/// change at the exchange shows up as an error rather than silent precision
/// loss. Trailing zeros beyond six places are accepted.
pub fn parse_fixed(input: &str) -> Result<i64, ParseFixedError> {
    let err = |reason| ParseFixedError {
        input: input.to_owned(),
        reason,
    };
    let (negative, digits) = match input.as_bytes().first() {
        Some(b'-') => (true, &input[1..]),
        Some(b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(err("no digits"));
    }
    if !int_part.bytes().chain(frac_part.bytes()).all(|b| b.is_ascii_digit()) {
        return Err(err("not a plain decimal number"));
    }
    let frac_part = frac_part.trim_end_matches('0');
    if frac_part.len() > DECIMALS {
        return Err(err("more than 6 decimal places"));
    }

    let mut value: i64 = 0;
    for b in int_part.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(|| err("out of range"))?;
    }
    value = value.checked_mul(SCALE).ok_or_else(|| err("out of range"))?;
    let mut frac: i64 = 0;
    for b in frac_part.bytes() {
        frac = frac * 10 + i64::from(b - b'0');
    }
    frac *= 10_i64.pow((DECIMALS - frac_part.len()) as u32);
    value = value.checked_add(frac).ok_or_else(|| err("out of range"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prices_and_sizes() {
        assert_eq!(parse_fixed("0.523"), Ok(523_000));
        assert_eq!(parse_fixed("0.0800"), Ok(80_000));
        assert_eq!(parse_fixed("1"), Ok(SCALE));
        assert_eq!(parse_fixed("300.00"), Ok(300 * SCALE));
        assert_eq!(parse_fixed("1234.567891"), Ok(1_234_567_891));
        assert_eq!(parse_fixed(".5"), Ok(500_000));
        assert_eq!(parse_fixed("5."), Ok(5 * SCALE));
        assert_eq!(parse_fixed("0"), Ok(0));
    }

    #[test]
    fn parses_signs() {
        assert_eq!(parse_fixed("-54.00"), Ok(-54 * SCALE));
        assert_eq!(parse_fixed("+0.01"), Ok(10_000));
    }

    #[test]
    fn accepts_trailing_zeros_past_six_places() {
        assert_eq!(parse_fixed("0.12345600"), Ok(123_456));
    }

    #[test]
    fn rejects_bad_input() {
        for bad in ["", "-", ".", "abc", "1e5", "0.1234567", "1,5", " 1", "1.2.3", "--1"] {
            assert!(parse_fixed(bad).is_err(), "{bad:?} should fail");
        }
        assert!(parse_fixed("9999999999999999").is_err());
    }
}
