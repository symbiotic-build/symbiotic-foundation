//! Retry-After parsing and deterministic retry delays, without a runtime or HTTP client.
//!
//! Callers supply the current time and a jitter seed. Parsing preserves upstream
//! delays; only [`delay_ms`] applies the retry policy's 120 s clamp.

use std::time::UNIX_EPOCH;
use thiserror::Error;

/// A Retry-After value is neither unsigned seconds nor a supported HTTP date.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("invalid Retry-After value")]
pub struct InvalidRetryAfter;

/// The inclusive jitter range cannot be represented because its maximum is `u64::MAX`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("jitter maximum must be less than u64::MAX seconds")]
pub struct InvalidJitter;

/// Parse unsigned decimal seconds or an HTTP date into delay seconds.
///
/// `now_unix_seconds` is the current time in whole seconds since the Unix epoch.
/// Past dates return zero. HTTP dates include the two obsolete HTTP date formats.
/// Numeric delays are returned unchanged, without a retry-policy clamp.
///
/// # Errors
/// Returns [`InvalidRetryAfter`] for empty, invalid, or overflowing values.
pub fn parse_retry_after(text: &str, now_unix_seconds: i64) -> Result<u64, InvalidRetryAfter> {
    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        return text.parse().map_err(|_| InvalidRetryAfter);
    }
    let deadline = httpdate::parse_http_date(text).map_err(|_| InvalidRetryAfter)?;
    let deadline_seconds = deadline
        .duration_since(UNIX_EPOCH)
        .map_err(|_| InvalidRetryAfter)?
        .as_secs();
    let delay_seconds = (i128::from(deadline_seconds) - i128::from(now_unix_seconds)).max(0);
    u64::try_from(delay_seconds).map_err(|_| InvalidRetryAfter)
}

/// Double the base delay per attempt, up to 32 times the base.
///
/// Attempts zero and one use the base, with a minimum of 1 ms. The result is
/// capped at 30 s, or at the base if the base is longer. Arithmetic saturates.
pub fn backoff_ms(attempt: u32, base_ms: u64) -> u64 {
    let base_ms = base_ms.max(1);
    base_ms
        .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(5)))
        .min(30_000.max(base_ms))
}

/// Add deterministic whole-second jitter to backoff and clamp to 1 ms–120 s.
///
/// Jitter is `jitter_seed % (max_jitter_seconds + 1)`. Callers choose a stable
/// seed for each attempt. To reproduce the model queue's delays, concatenate
/// item-id bytes, request-hash bytes, the attempt as four little-endian bytes,
/// and error-kind bytes; SHA-256 that sequence, then interpret its first eight
/// bytes as a little-endian `u64`. This module does not hash caller data.
/// Error-kind bytes are the UTF-8 bytes of `rate_limited` for
/// `ModelError::RateLimited`, `unavailable` for `ModelError::Unavailable`,
/// `timeout` for `ModelError::Timeout`, and `other` for every other primary
/// variant. Select the primary error by recursively unwrapping
/// `ModelError::HttpNotSent`, `ModelError::Http`, and `ModelError::Diagnostics`.
/// Arithmetic saturates before the final clamp.
///
/// # Errors
/// Returns [`InvalidJitter`] if `max_jitter_seconds` is `u64::MAX`.
pub fn delay_ms(
    attempt: u32,
    base_ms: u64,
    max_jitter_seconds: u64,
    jitter_seed: u64,
) -> Result<u64, InvalidJitter> {
    let range = max_jitter_seconds.checked_add(1).ok_or(InvalidJitter)?;
    let jitter_seconds = jitter_seed % range;
    Ok(backoff_ms(attempt, base_ms)
        .saturating_add(jitter_seconds.saturating_mul(1_000))
        .clamp(1, 120_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2015-10-21 07:27:00 UTC, from the model's existing Retry-After fixtures.
    const NOW: i64 = 1_445_412_420;

    #[test]
    fn retry_after_seconds_are_not_clamped() {
        for (text, expected) in [
            ("0", 0),
            ("007", 7),
            ("583", 583),
            ("18446744073709551615", u64::MAX),
        ] {
            assert_eq!(parse_retry_after(text, NOW), Ok(expected), "{text}");
        }
    }

    #[test]
    fn retry_after_http_dates_return_seconds_until_deadline() {
        for (text, expected) in [
            ("Wed, 21 Oct 2015 07:28:00 GMT", 60),
            ("Wednesday, 21-Oct-15 07:28:00 GMT", 60),
            ("Wed Oct 21 07:28:00 2015", 60),
            ("Wed, 21 Oct 2015 07:27:00 GMT", 0),
            ("Wed, 21 Oct 2015 07:26:00 GMT", 0),
        ] {
            assert_eq!(parse_retry_after(text, NOW), Ok(expected), "{text}");
        }
    }

    #[test]
    fn retry_after_invalid_or_overflowing_text_is_rejected() {
        for text in [
            "",
            "later",
            "-1",
            "+1",
            "1.5",
            " 7",
            "7 ",
            "18446744073709551616",
        ] {
            assert_eq!(
                parse_retry_after(text, NOW),
                Err(InvalidRetryAfter),
                "{text}"
            );
        }
    }

    #[test]
    fn retry_after_dates_accept_extreme_current_times_without_overflow() {
        let epoch = "Thu, 01 Jan 1970 00:00:00 GMT";
        assert_eq!(parse_retry_after(epoch, i64::MIN), Ok(1_u64 << 63));
        assert_eq!(parse_retry_after(epoch, i64::MAX), Ok(0));
    }

    #[test]
    fn backoff_doubles_for_attempts_one_through_seven_and_caps() {
        assert_eq!(
            (1..=7)
                .map(|attempt| backoff_ms(attempt, 1_000))
                .collect::<Vec<_>>(),
            [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]
        );
        assert_eq!(backoff_ms(9, 250), 8_000);
        assert_eq!(backoff_ms(u32::MAX, 1_000), 30_000);
    }

    #[test]
    fn backoff_preserves_long_bases_and_saturates() {
        for (attempt, base, expected) in [
            (0, 0, 1),
            (0, 250, 250),
            (1, 0, 1),
            (7, 60_000, 60_000),
            (u32::MAX, u64::MAX, u64::MAX),
        ] {
            assert_eq!(backoff_ms(attempt, base), expected);
        }
    }

    #[test]
    fn delay_jitter_is_deterministic_and_covers_the_inclusive_range() {
        for seed in 0..=8 {
            assert_eq!(delay_ms(1, 1, 3, seed), Ok(1 + (seed % 4) * 1_000));
        }
        assert_eq!(delay_ms(1, 1, 0, u64::MAX), Ok(1));
    }

    #[test]
    fn delay_clamps_at_one_millisecond_and_two_minutes() {
        assert_eq!(delay_ms(0, 0, 0, 0), Ok(1));
        assert_eq!(delay_ms(7, 1_000, 100, 100), Ok(120_000));
        assert_eq!(delay_ms(1, u64::MAX, 0, 0), Ok(120_000));
        assert_eq!(delay_ms(1, 1, u64::MAX - 1, u64::MAX - 1), Ok(120_000));
    }

    #[test]
    fn unrepresentable_jitter_range_returns_an_error() {
        assert_eq!(delay_ms(1, 1_000, u64::MAX, 0), Err(InvalidJitter));
    }
}
