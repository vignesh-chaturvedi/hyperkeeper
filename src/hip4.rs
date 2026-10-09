//! HIP-4 price-based outcomes: what a `binaryPrice` market should settle to.
//!
//! The template reads: the market resolves to Yes if the perp's price is above the
//! threshold at the given time, "according to the {seconds}-second TWAP of
//! {priceDescription} price ending at {time}". Venues use three price descriptions:
//!
//! - `1-minute candle` (300 s): the mean of the 1-minute candle closes in the window. A
//!   back-test of 35 settled mainnet markets on Oct 8, 2026 matched 35 of 35.
//! - `<PERP>-USDC perp mark` (60 s): the mark price, which has no public history, so the
//!   keeper records it live from the `activeAssetCtx` feed.
//! - `the Hyperliquid <PERP> perp trade` (90 s): trade prices, recorded live.

use std::collections::BTreeMap;

use anyhow::{Context, bail};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

/// Which price a market settles on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PriceKind {
    Candle1m,
    Mark,
    Trade,
}

/// A `binaryPrice` market, parsed from its `outcomeMeta` or `settledOutcome` description.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryPrice {
    pub outcome: u64,
    /// The perp's coin, e.g. `BTC` or `xyz:GOLD`.
    pub coin: String,
    pub kind: PriceKind,
    pub seconds: u64,
    pub threshold: Decimal,
    /// The window's end, in ms since the epoch.
    pub time_ms: u64,
    pub venue: Option<String>,
}

impl BinaryPrice {
    /// Parses `perp:BTC|priceDescription:…|seconds:60|threshold:79422|time:20260904-1400`.
    pub fn parse(
        outcome: u64,
        name: &str,
        description: &str,
        venue: Option<String>,
    ) -> anyhow::Result<Self> {
        if name != "template:binaryPrice" {
            bail!("not a binaryPrice market: {name}");
        }
        let fields: BTreeMap<&str, &str> = description
            .split('|')
            .filter_map(|kv| kv.split_once(':'))
            .collect();
        let field = |key: &str| {
            fields
                .get(key)
                .copied()
                .with_context(|| format!("no {key} in {description}"))
        };
        let coin = field("perp")?.to_string();
        let description_of_price = field("priceDescription")?;
        let kind = if description_of_price == "1-minute candle" {
            PriceKind::Candle1m
        } else if description_of_price.ends_with("perp mark") {
            PriceKind::Mark
        } else if description_of_price.ends_with("perp trade") {
            PriceKind::Trade
        } else {
            bail!("unknown price description: {description_of_price}");
        };
        let time = NaiveDateTime::parse_from_str(field("time")?, "%Y%m%d-%H%M")
            .with_context(|| format!("bad time in {description}"))?;
        Ok(Self {
            outcome,
            coin,
            kind,
            seconds: field("seconds")?.parse()?,
            threshold: field("threshold")?.parse()?,
            time_ms: time.and_utc().timestamp_millis() as u64,
            venue,
        })
    }

    pub fn window_start_ms(&self) -> u64 {
        self.time_ms.saturating_sub(self.seconds * 1000)
    }
}

/// Yes when the settlement price is strictly above the threshold.
pub fn resolves_yes(price: Decimal, threshold: Decimal) -> bool {
    price > threshold
}

/// Time-weighted average of a step function over `[start, end)`: each sample's price holds
/// until the next sample. The window must be covered: the first sample must be at or
/// before `start`, so the price at the window's opening is known.
pub fn step_twap(samples: &[(u64, Decimal)], start: u64, end: u64) -> Option<Decimal> {
    if end <= start {
        return None;
    }
    let opening = samples.iter().rposition(|(t, _)| *t <= start)?;
    let mut total = Decimal::ZERO;
    let mut at = start;
    let mut price = samples[opening].1;
    for &(t, next) in &samples[opening + 1..] {
        if t >= end {
            break;
        }
        total += price * Decimal::from(t - at);
        at = t;
        price = next;
    }
    total += price * Decimal::from(end - at);
    Some(total / Decimal::from(end - start))
}

/// Mean of the closes of the 1-minute candles opening in `[start, end)`, with every minute
/// present.
pub fn candle_twap(candles: &[(u64, Decimal)], start: u64, end: u64) -> Option<Decimal> {
    let closes: Vec<Decimal> = candles
        .iter()
        .filter(|(open, _)| *open >= start && *open < end)
        .map(|(_, close)| *close)
        .collect();
    let expected = ((end - start) / 60_000) as usize;
    if closes.is_empty() || closes.len() != expected {
        return None;
    }
    Some(closes.iter().sum::<Decimal>() / Decimal::from(closes.len()))
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn parses_the_three_price_descriptions() {
        let candle = BinaryPrice::parse(
            7349,
            "template:binaryPrice",
            "perp:HYPE|priceDescription:1-minute candle|seconds:300|threshold:98|time:20261030-2000",
            Some("txyz".into()),
        )
        .unwrap();
        assert_eq!(candle.kind, PriceKind::Candle1m);
        assert_eq!(candle.coin, "HYPE");
        assert_eq!(candle.threshold, dec!(98));
        assert_eq!(candle.time_ms, 1_793_390_400_000);
        assert_eq!(candle.window_start_ms(), 1_793_390_100_000);

        let mark = BinaryPrice::parse(
            1,
            "template:binaryPrice",
            "perp:xyz:GOLD|priceDescription:xyz:GOLD-USDC perp mark|seconds:60|threshold:4090|time:20261007-2000",
            None,
        )
        .unwrap();
        assert_eq!(
            (mark.kind, mark.coin.as_str()),
            (PriceKind::Mark, "xyz:GOLD")
        );

        let trade = BinaryPrice::parse(
            1471,
            "template:binaryPrice",
            "perp:BTC|priceDescription:the Hyperliquid BTC perp trade|seconds:90|threshold:79422|time:20260904-1400",
            None,
        )
        .unwrap();
        assert_eq!((trade.kind, trade.seconds), (PriceKind::Trade, 90));

        assert!(BinaryPrice::parse(1, "template:priceTouch", "perp:BTC", None).is_err());
    }

    #[test]
    fn step_twap_weights_each_price_by_how_long_it_held() {
        // 100 for the first 30 s of the window, 110 for the last 30 s.
        let samples = [(0, dec!(90)), (10_000, dec!(100)), (40_000, dec!(110))];
        assert_eq!(step_twap(&samples, 10_000, 70_000), Some(dec!(105)));
        // The price at the window's opening comes from the last sample before it.
        assert_eq!(step_twap(&samples, 5_000, 15_000), Some(dec!(95)));
        // A window opening before the first sample is not covered.
        assert_eq!(step_twap(&samples, 0, 10_000), Some(dec!(90)));
        assert_eq!(step_twap(&samples[1..], 0, 10_000), None);
    }

    #[test]
    fn candle_twap_needs_every_minute() {
        let candles = [(0, dec!(10)), (60_000, dec!(20)), (120_000, dec!(30))];
        assert_eq!(candle_twap(&candles, 0, 180_000), Some(dec!(20)));
        assert_eq!(candle_twap(&candles, 0, 240_000), None);
    }

    #[test]
    fn strictly_above_the_threshold_resolves_yes() {
        assert!(resolves_yes(dec!(59.7001), dec!(59.7)));
        assert!(!resolves_yes(dec!(59.7), dec!(59.7)));
        // Mainnet outcome 9552: xyz:SILVER, threshold 59.7, 5-minute candle TWAP 59.6496,
        // settled No.
        assert!(!resolves_yes(dec!(59.6496), dec!(59.7)));
    }
}
