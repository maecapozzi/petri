//! `10s`, `5m`, `2h`, `500ms`, or a bare number of seconds.

use std::time::Duration;

pub(crate) fn parse(text: &str) -> Option<Duration> {
    let text = text.trim();
    if let Ok(secs) = text.parse::<u64>() {
        return secs.checked_mul(1_000).map(Duration::from_millis);
    }
    let split = text.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (number, unit) = text.split_at(split);
    let scale = match unit.trim() {
        "ms" => 1,
        "s" | "sec" | "secs" => 1_000,
        "m" | "min" | "mins" => 60_000,
        "h" | "hr" | "hrs" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    let millis = decimal_millis(number, scale)?;
    Some(Duration::from_millis(millis))
}

fn decimal_millis(number: &str, scale: u64) -> Option<u64> {
    let mut digits = Vec::with_capacity(number.len() + 8);
    let mut decimal_places = None;
    for byte in number.bytes() {
        match byte {
            b'0'..=b'9' => digits.push(byte - b'0'),
            b'.' if decimal_places.is_none() => decimal_places = Some(0),
            _ => return None,
        }
        if let Some(places) = &mut decimal_places
            && byte != b'.'
        {
            *places += 1;
        }
    }
    if digits.is_empty() {
        return None;
    }

    let mut product = Vec::with_capacity(digits.len() + 8);
    let mut carry = 0;
    for digit in digits.into_iter().rev() {
        let value = u64::from(digit) * scale + carry;
        product.push((value % 10) as u8);
        carry = value / 10;
    }
    while carry != 0 {
        product.push((carry % 10) as u8);
        carry /= 10;
    }

    let fractional = decimal_places.unwrap_or_default();
    product
        .iter()
        .skip(fractional)
        .rev()
        .try_fold(0_u64, |value, digit| {
            value.checked_mul(10)?.checked_add(u64::from(*digit))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffixed_decimals_are_converted_exactly() {
        assert_eq!(parse("1.5s"), Some(Duration::from_millis(1_500)));
        assert_eq!(parse(".5s"), Some(Duration::from_millis(500)));
        assert_eq!(parse("0.0009s"), Some(Duration::ZERO));
        assert_eq!(
            parse("18446744073709551615ms"),
            Some(Duration::from_millis(u64::MAX))
        );
        assert_eq!(parse("18446744073709551616ms"), None);
    }
}
