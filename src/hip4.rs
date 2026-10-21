//! HIP-4 price-based outcomes: what a `binaryPrice` or `priceTouch` market should settle to.
//!
//! Both templates settle on "the {seconds}-second TWAP of {priceDescription} price":
//!
//! - `binaryPrice` resolves to Yes if that TWAP, ending at {time}, is above the threshold.
//! - `priceTouch` resolves to Yes as soon as that TWAP, taken over a rolling window, equals or
//!   crosses the target at any time up to {time}, and to No if it never does.
//!
//! If the perp is delisted before the market settles, both templates settle on the perp's
//! settlement price at delisting instead, and a touch market can settle No right then.
//!
//! Testnet carries numbered drafts of the same templates (`binaryPrice6`, `priceTouch2`), which
//! read the same way. Venues use three price descriptions:
//!
//! - `1-minute candle` (300 s): the mean of the 1-minute candle closes in the window. A
//!   back-test of 35 settled mainnet markets on Oct 8, 2026 matched 35 of 35.
//! - `<PERP>-USDC perp mark` (60 s): the mark price, which has no public history, so the
//!   keeper records it live from the `activeAssetCtx` feed.
//! - `the Hyperliquid <PERP> perp trade` (90 s): trade prices, recorded live.

use std::collections::{BTreeMap, HashMap};

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

/// How a market turns the TWAP into a result.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Rule {
    /// `binaryPrice`: Yes if the TWAP ending at the market's time is above this threshold.
    Above(Decimal),
    /// `priceTouch`: Yes once a rolling TWAP equals or crosses this target, up to the
    /// market's time.
    Touch(Decimal),
}

/// A price-based market, parsed from its `outcomeMeta` or `settledOutcome` description.
#[derive(Debug, Clone, PartialEq)]
pub struct PriceMarket {
    pub outcome: u64,
    /// The perp's coin, e.g. `BTC` or `xyz:GOLD`.
    pub coin: String,
    pub kind: PriceKind,
    pub seconds: u64,
    pub rule: Rule,
    /// The window's end, or for a touch market its deadline, in ms since the epoch.
    pub time_ms: u64,
    pub venue: Option<String>,
}

impl PriceMarket {
    /// Parses `perp:BTC|priceDescription:…|seconds:60|threshold:79422|time:20260904-1400`, or
    /// the same with `target` for a touch market.
    pub fn parse(
        outcome: u64,
        name: &str,
        description: &str,
        venue: Option<String>,
    ) -> anyhow::Result<Self> {
        let template = name
            .strip_prefix("template:")
            .with_context(|| format!("not a template market: {name}"))?;
        // Testnet numbers its drafts of a template: binaryPrice4, binaryPrice6, priceTouch2.
        let family = template.trim_end_matches(|c: char| c.is_ascii_digit());
        if family != "binaryPrice" && family != "priceTouch" {
            bail!("not a supported price template: {name}");
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
        let rule = if family == "priceTouch" {
            // A rolling candle TWAP would need a candle request every few seconds.
            if kind == PriceKind::Candle1m {
                bail!("touch markets on candles are not supported: {description}");
            }
            Rule::Touch(field("target")?.parse()?)
        } else {
            Rule::Above(field("threshold")?.parse()?)
        };
        let time = NaiveDateTime::parse_from_str(field("time")?, "%Y%m%d-%H%M")
            .with_context(|| format!("bad time in {description}"))?;
        Ok(Self {
            outcome,
            coin,
            kind,
            seconds: field("seconds")?.parse()?,
            rule,
            time_ms: time.and_utc().timestamp_millis() as u64,
            venue,
        })
    }

    pub fn window_start_ms(&self) -> u64 {
        self.time_ms.saturating_sub(self.seconds * 1000)
    }

    /// The threshold or target the TWAP is compared with.
    pub fn level(&self) -> Decimal {
        match self.rule {
            Rule::Above(level) | Rule::Touch(level) => level,
        }
    }

    /// The DEX the perp trades on, as `metaAndAssetCtxs` takes it: `None` for the main DEX.
    pub fn dex(&self) -> Option<String> {
        self.coin.split_once(':').map(|(dex, _)| dex.to_string())
    }

    /// What the market resolves to when its perp was delisted at `price`. A touch market is
    /// touched if that price reaches the target from the side its TWAP started on, so it
    /// can't be judged until that side is known.
    pub fn resolves_at_delisting(&self, price: Decimal, from_below: Option<bool>) -> Option<bool> {
        match self.rule {
            Rule::Above(threshold) => Some(resolves_yes(price, threshold)),
            Rule::Touch(target) => from_below.map(|below| {
                if below {
                    price >= target
                } else {
                    price <= target
                }
            }),
        }
    }
}

/// Yes when the settlement price is strictly above the threshold.
pub fn resolves_yes(price: Decimal, threshold: Decimal) -> bool {
    price > threshold
}

/// The delisted perps in a `metaAndAssetCtxs` reply, with their mark prices. A delisted
/// perp's mark stays frozen, and hyperkeeper takes it as the settlement price at delisting.
pub fn delisted_prices(reply: &serde_json::Value) -> HashMap<String, Decimal> {
    let (Some(universe), Some(ctxs)) = (reply[0]["universe"].as_array(), reply[1].as_array())
    else {
        return HashMap::new();
    };
    universe
        .iter()
        .zip(ctxs)
        .filter(|(asset, _)| asset["isDelisted"].as_bool() == Some(true))
        .filter_map(|(asset, ctx)| {
            let price = ctx["markPx"].as_str()?.parse().ok()?;
            Some((asset["name"].as_str()?.to_string(), price))
        })
        .collect()
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

/// Plain mean of the samples timed in `[start, end)`, each counted once however long its
/// price held. An alternative reading of "TWAP", kept to compare with the venues.
pub fn sample_mean(samples: &[(u64, Decimal)], start: u64, end: u64) -> Option<Decimal> {
    let prices: Vec<Decimal> = samples
        .iter()
        .filter(|(t, _)| *t >= start && *t < end)
        .map(|(_, price)| *price)
        .collect();
    if prices.is_empty() {
        return None;
    }
    Some(prices.iter().sum::<Decimal>() / Decimal::from(prices.len()))
}

/// Volume-weighted average price of the trades `(time, price, size)` in `[start, end)`.
pub fn vwap(trades: &[(u64, Decimal, Decimal)], start: u64, end: u64) -> Option<Decimal> {
    let (notional, volume) = trades
        .iter()
        .filter(|(t, _, _)| *t >= start && *t < end)
        .fold((Decimal::ZERO, Decimal::ZERO), |(n, v), (_, px, sz)| {
            (n + px * sz, v + sz)
        });
    (volume > Decimal::ZERO).then(|| notional / volume)
}

/// The first moment in `(from, to]` at which the `seconds`-second TWAP ending then reaches
/// `target` from the side it started on: from below means at or above the target, from above
/// means at or below it.
///
/// Between the moments a sample enters or leaves the window, the TWAP of a step function moves
/// in a straight line, so checking those moments finds every touch. The time returned can be
/// up to one such step after the exact crossing.
pub fn first_touch(
    samples: &[(u64, Decimal)],
    seconds: u64,
    target: Decimal,
    from_below: bool,
    from: u64,
    to: u64,
) -> Option<u64> {
    let window = seconds * 1000;
    let mut ends: Vec<u64> = samples
        .iter()
        .flat_map(|(t, _)| [*t, t + window])
        .chain([to])
        .filter(|end| *end > from && *end <= to)
        .collect();
    ends.sort_unstable();
    ends.dedup();
    ends.into_iter().find(|end| {
        step_twap(samples, end.saturating_sub(window), *end).is_some_and(|twap| {
            if from_below {
                twap >= target
            } else {
                twap <= target
            }
        })
    })
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
        let candle = PriceMarket::parse(
            7349,
            "template:binaryPrice",
            "perp:HYPE|priceDescription:1-minute candle|seconds:300|threshold:98|time:20261030-2000",
            Some("txyz".into()),
        )
        .unwrap();
        assert_eq!(candle.kind, PriceKind::Candle1m);
        assert_eq!(candle.coin, "HYPE");
        assert_eq!(candle.rule, Rule::Above(dec!(98)));
        assert_eq!(candle.time_ms, 1_793_390_400_000);
        assert_eq!(candle.window_start_ms(), 1_793_390_100_000);

        let mark = PriceMarket::parse(
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

        let trade = PriceMarket::parse(
            1471,
            "template:binaryPrice",
            "perp:BTC|priceDescription:the Hyperliquid BTC perp trade|seconds:90|threshold:79422|time:20260904-1400",
            None,
        )
        .unwrap();
        assert_eq!((trade.kind, trade.seconds), (PriceKind::Trade, 90));

        assert!(PriceMarket::parse(1, "template:priceTouch", "perp:BTC", None).is_err());
        assert!(PriceMarket::parse(1, "template:binaryPriceExternal", "perp:BTC", None).is_err());
    }

    #[test]
    fn parses_touch_markets_and_testnet_drafts() {
        // Mainnet outcome 7175 on Outcome's venue.
        let touch = PriceMarket::parse(
            7175,
            "template:priceTouch",
            "perp:BTC|priceDescription:BTC-USDC perp mark|seconds:3|target:92500|time:20261101-0000",
            Some("out".into()),
        )
        .unwrap();
        assert_eq!(touch.rule, Rule::Touch(dec!(92500)));
        assert_eq!((touch.kind, touch.seconds), (PriceKind::Mark, 3));
        assert_eq!(touch.level(), dec!(92500));

        let draft = PriceMarket::parse(
            1,
            "template:binaryPrice6",
            "perp:BTC|priceDescription:the Hyperliquid BTC perp trade|seconds:90|threshold:84912|time:20261020-1200",
            None,
        )
        .unwrap();
        assert_eq!(draft.rule, Rule::Above(dec!(84912)));
    }

    #[test]
    fn first_touch_finds_the_moment_a_rolling_twap_reaches_the_target() {
        // One-second marks climbing through 100: 98, 99, 100, 101 from t = 0 s.
        let marks = [
            (0, dec!(98)),
            (1_000, dec!(99)),
            (2_000, dec!(100)),
            (3_000, dec!(101)),
        ];
        // The 3 s TWAP ending at 5 s is (100 + 101 + 101) / 3 = 100.67, at 4 s it is
        // (99 + 100 + 101) / 3 = 100, which already counts as a touch.
        assert_eq!(
            first_touch(&marks, 3, dec!(100), true, 0, 10_000),
            Some(4_000)
        );
        // A higher target is never reached while the price sits at 101.
        assert_eq!(first_touch(&marks, 3, dec!(102), true, 0, 10_000), None);
        // From above, a target of 98.5 is never reached either: the price only rose.
        assert_eq!(
            first_touch(&marks, 3, dec!(98.5), false, 3_000, 10_000),
            None
        );
        // Only window ends after `from` count.
        assert_eq!(
            first_touch(&marks, 3, dec!(100), true, 4_000, 10_000),
            Some(5_000)
        );
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
    fn the_three_averages_disagree_when_trading_is_uneven() {
        // One trade at 100 opens the window, then a burst of three heavier trades at 90 in
        // its last 10 s: over time the price was mostly 100; per trade and per unit it was 90.
        let trades = [
            (0, dec!(100), dec!(1)),
            (50_000, dec!(90), dec!(2)),
            (55_000, dec!(90), dec!(2)),
            (58_000, dec!(90), dec!(2)),
        ];
        let points: Vec<(u64, Decimal)> = trades.iter().map(|(t, px, _)| (*t, *px)).collect();
        let six = |p: Option<Decimal>| p.map(|p| p.round_dp(6));
        assert_eq!(six(step_twap(&points, 0, 60_000)), Some(dec!(98.333333)));
        assert_eq!(sample_mean(&points, 0, 60_000), Some(dec!(92.5)));
        assert_eq!(six(vwap(&trades, 0, 60_000)), Some(dec!(91.428571)));
        // Nothing traded in the window.
        assert_eq!(sample_mean(&points, 60_000, 90_000), None);
        assert_eq!(vwap(&trades, 60_000, 90_000), None);
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

    #[test]
    fn a_delisted_perp_settles_on_its_frozen_mark() {
        // para:PAPER after its delisting on Oct 20, 2026, trimmed to two assets.
        let reply = serde_json::json!([
            { "universe": [{ "name": "para:LIVE" }, { "name": "para:PAPER", "isDelisted": true }] },
            [{ "markPx": "1.5" }, { "markPx": "0.0259", "oraclePx": "0.0259" }]
        ]);
        assert_eq!(
            delisted_prices(&reply),
            HashMap::from([("para:PAPER".to_string(), dec!(0.0259))])
        );

        // Outcome 14106, which the venue settled No at the delisting.
        let touch = PriceMarket::parse(
            14106,
            "template:priceTouch",
            "perp:para:PAPER|priceDescription:para:PAPER-USDC perp mark|seconds:3|target:0.1|time:20261101-0000",
            Some("out".into()),
        )
        .unwrap();
        assert_eq!(touch.dex().as_deref(), Some("para"));
        assert_eq!(
            touch.resolves_at_delisting(dec!(0.0259), Some(true)),
            Some(false)
        );
        assert_eq!(
            touch.resolves_at_delisting(dec!(0.1), Some(true)),
            Some(true)
        );
        assert_eq!(
            touch.resolves_at_delisting(dec!(0.0259), Some(false)),
            Some(true)
        );
        assert_eq!(touch.resolves_at_delisting(dec!(0.0259), None), None);

        let above = PriceMarket::parse(
            1,
            "template:binaryPrice",
            "perp:BTC|priceDescription:the Hyperliquid BTC perp trade|seconds:90|threshold:100000|time:20261030-2000",
            None,
        )
        .unwrap();
        assert_eq!(above.dex(), None);
        assert_eq!(above.resolves_at_delisting(dec!(100000), None), Some(false));
        assert_eq!(above.resolves_at_delisting(dec!(100001), None), Some(true));
    }
}
