//! Exact passthrough money arithmetic. One stored unit is 10^-9 USD.
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use symbiotic_trace::UsageTrace;

/// Nonnegative decimal amount with nine fractional places, bounded by SQLite i64.
/// Configuration uses decimal strings; calculations never use binary floats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecimalAmount(pub(crate) u64);
impl DecimalAmount {
    /// Parse an exact nonnegative decimal string with at most nine fractional digits.
    pub fn parse(value: &str) -> Option<Self> {
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || fraction.len() > 9
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || (value.contains('.') && fraction.is_empty())
        {
            return None;
        }
        let whole: u64 = whole.parse().ok()?;
        let part = if fraction.is_empty() {
            0
        } else {
            fraction.parse::<u64>().ok()?
        };
        let units = whole
            .checked_mul(1_000_000_000)?
            .checked_add(part.checked_mul(10u64.pow(9 - fraction.len() as u32))?)?;
        (units <= i64::MAX as u64).then_some(Self(units))
    }
    /// Value in billionths of the configured currency (USD for prices and limits).
    pub fn nano_units(self) -> u64 {
        self.0
    }
}
impl Serialize for DecimalAmount {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!(
            "{}.{:09}",
            self.0 / 1_000_000_000,
            self.0 % 1_000_000_000
        ))
    }
}
impl<'de> Deserialize<'de> for DecimalAmount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).ok_or_else(|| de::Error::custom("invalid decimal amount"))
    }
}
/// USD per million tokens, before request-level rounding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenRates {
    /// Price of uncached input.
    pub input_cache_miss: DecimalAmount,
    /// Price of cached input.
    pub input_cache_hit: DecimalAmount,
    /// Price of output excluding separately priced reasoning.
    pub output: DecimalAmount,
    /// Missing means the output rate, because reasoning is included in output.
    #[serde(default)]
    pub reasoning: Option<DecimalAmount>,
}
impl TokenRates {
    /// Cost rounded up once per request, saturating at SQLite's largest integer.
    /// Incomplete token categories leave cost unknown rather than guessing usage.
    pub fn cost_nano_usd(&self, usage: &UsageTrace) -> Option<u64> {
        let (miss, hit) = match (
            usage.input_tokens,
            usage.cache_miss_tokens,
            usage.cache_hit_tokens,
        ) {
            (_, Some(miss), Some(hit)) => (miss, hit),
            (Some(total), Some(miss), None) => (miss, total.saturating_sub(miss)),
            (Some(total), None, Some(hit)) => (total.saturating_sub(hit), hit),
            (Some(total), None, None) => (total, 0),
            _ => return None,
        };
        let output = usage.output_tokens?;
        if output > 0
            && usage.reasoning_tokens.is_none()
            && self.reasoning.is_some_and(|rate| rate != self.output)
        {
            return None;
        }
        let reasoning = usage.reasoning_tokens.unwrap_or(0);
        let sum = [
            (miss, self.input_cache_miss),
            (hit, self.input_cache_hit),
            (output.saturating_sub(reasoning), self.output),
            (reasoning, self.reasoning.unwrap_or(self.output)),
        ]
        .into_iter()
        .fold(0u128, |sum, (tokens, rate)| {
            sum.saturating_add(u128::from(tokens).saturating_mul(u128::from(rate.0)))
        });
        Some(sum.div_ceil(1_000_000).min(i64::MAX as u128) as u64)
    }
}
/// A UTC weekday/time interval, start inclusive and end exclusive. No holidays.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeakWindow {
    /// ISO weekday numbers: Monday=1 through Sunday=7.
    pub weekdays: Vec<u32>,
    /// UTC HH:MM; must precede end on the same day.
    pub start: String,
    /// UTC HH:MM, including 24:00 for the end of a day.
    pub end: String,
    /// Rates used during this window.
    pub rates: TokenRates,
}
fn minute(value: &str) -> Option<u32> {
    if value.len() != 5
        || value.as_bytes()[2] != b':'
        || !value
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 2 || b.is_ascii_digit())
    {
        return None;
    }
    let h: u32 = value.get(..2)?.parse().ok()?;
    let m: u32 = value.get(3..)?.parse().ok()?;
    (h < 24 && m < 60 || h == 24 && m == 0).then_some(h * 60 + m)
}
/// Default rates with optional nonoverlapping UTC peak windows.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassthroughPrice {
    /// Off-peak rates in USD per million tokens.
    pub rates: TokenRates,
    /// Overrides; holidays use their weekday's scheduled price.
    #[serde(default)]
    pub peak: Vec<PeakWindow>,
}
impl PassthroughPrice {
    /// Reject malformed or overlapping intervals rather than select ambiguously.
    pub fn valid(&self) -> bool {
        for (i, window) in self.peak.iter().enumerate() {
            let (Some(start), Some(end)) = (minute(&window.start), minute(&window.end)) else {
                return false;
            };
            if start >= end
                || window.weekdays.is_empty()
                || window.weekdays.iter().any(|d| !(1..=7).contains(d))
            {
                return false;
            }
            if self.peak[..i].iter().any(|other| {
                window.weekdays.iter().any(|d| other.weekdays.contains(d))
                    && minute(&other.start).is_some_and(|s| s < end)
                    && minute(&other.end).is_some_and(|e| start < e)
            }) {
                return false;
            }
        }
        true
    }
    /// Freeze the rates at the actual send time, before streaming or settlement.
    pub fn rates_at(&self, sent: DateTime<Utc>) -> &TokenRates {
        let now = sent.hour() * 60 + sent.minute();
        self.peak
            .iter()
            .find(|w| {
                w.weekdays.contains(&sent.weekday().number_from_monday())
                    && minute(&w.start).is_some_and(|s| now >= s)
                    && minute(&w.end).is_some_and(|e| now < e)
            })
            .map_or(&self.rates, |w| &w.rates)
    }
}
/// Rolling account budget. Each unresolved receipt holds max_request_usd.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpendLimit {
    /// Total allowed USD in the rolling window.
    pub usd: DecimalAmount,
    /// Rolling duration, positive and representable in milliseconds.
    pub window_seconds: u64,
    /// Conservative hold for each request without a known settled cost.
    pub max_request_usd: DecimalAmount,
}
impl SpendLimit {
    /// Validate the rolling duration and positive per-request hold.
    pub fn valid(&self) -> bool {
        self.window_seconds > 0
            && self.window_seconds <= i64::MAX as u64 / 1000
            && self.max_request_usd.0 > 0
    }
}
/// Canonical ledger cost totals for one account and a half-open send-time window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SpendCost {
    /// Sum of measured, priced receipts in billionths of USD, saturating at u64::MAX.
    pub settled_nano_usd: u64,
    /// Unknown receipts plus settled priced receipts with unknown cost; excludes Released.
    pub unknown_usage_count: u64,
    /// Receipts without configured price, independently of whether usage arrived.
    pub unpriced_count: u64,
}
