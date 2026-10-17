//! One pricing round: fetch every source, keep a short cache, aggregate each asset.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use rust_decimal::Decimal;
use tracing::debug;

use crate::{
    aggregate::{Aggregate, AggregateError, aggregate},
    config::{Asset, Config, Convention, Quote, SourceKind, Sources},
    sources::Fetcher,
};

/// USDT/USD barely moves, so a rate this old is still good enough to convert with.
const RATE_MAX_AGE: Duration = Duration::from_secs(300);

/// An asset priced this round.
#[derive(Debug, Clone)]
pub struct Priced {
    pub coin: String,
    /// Spot aggregate; its price is the oracle price before the guard.
    pub spot: Aggregate,
    /// Perp aggregate price, or the spot price when perps are short of quotes.
    pub external: Decimal,
}

/// The result of one round.
#[derive(Debug, Default)]
pub struct Round {
    pub priced: Vec<Priced>,
    /// Assets without a price this round, and why.
    pub unpriced: Vec<(String, AggregateError)>,
    /// USD per USDT used this round, if a conversion was needed and a rate was fresh.
    pub usdt_usd: Option<Decimal>,
}

pub struct Pricer {
    fetcher: Fetcher,
    sources: Sources,
    kinds: Vec<SourceKind>,
    symbols: Vec<String>,
    assets: Vec<Asset>,
    cache: HashMap<(SourceKind, String), (Decimal, Instant)>,
    rate: Option<(Decimal, Instant)>,
    /// Failed fetches per source since start.
    pub source_errors: BTreeMap<SourceKind, usize>,
    /// Failed USDT/USD rate fetches since start.
    pub rate_errors: usize,
}

impl Pricer {
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        // A fetch has to finish inside the update interval, and the interval is at least 2.5 s.
        let timeout = Duration::from_millis(config.interval_ms.min(2_500));
        let kinds = all_kinds(&config.sources);
        let mut fetcher = Fetcher::new(timeout)?;
        if kinds.contains(&SourceKind::Pyth) {
            let feeds = config
                .assets
                .iter()
                .filter_map(|a| Some((a.symbol.clone(), a.pyth.clone()?)))
                .collect();
            fetcher = fetcher.with_pyth(feeds);
        }
        Ok(Self {
            fetcher,
            sources: config.sources.clone(),
            kinds,
            symbols: config.assets.iter().map(|a| a.symbol.clone()).collect(),
            assets: config.assets.clone(),
            cache: HashMap::new(),
            rate: None,
            source_errors: BTreeMap::new(),
            rate_errors: 0,
        })
    }

    pub async fn round(&mut self) -> Round {
        let convert = self.sources.convention != Convention::AsQuoted;
        let (results, rate) =
            tokio::join!(self.fetcher.fetch_all(&self.kinds, &self.symbols), async {
                if convert {
                    Some(self.fetcher.usdt_usd().await)
                } else {
                    None
                }
            });
        let now = Instant::now();
        match rate {
            Some(Ok(rate)) => self.rate = Some((rate, now)),
            Some(Err(err)) => {
                self.rate_errors += 1;
                debug!("USDT/USD rate fetch failed: {err:#}");
            }
            None => {}
        }
        let usdt_usd = self
            .rate
            .filter(|(_, at)| now.duration_since(*at) <= RATE_MAX_AGE)
            .map(|(rate, _)| rate);

        for (kind, result) in results {
            match result {
                Ok(prices) => {
                    for (symbol, price) in prices {
                        self.cache.insert((kind, symbol), (price, now));
                    }
                }
                Err(err) => {
                    *self.source_errors.entry(kind).or_default() += 1;
                    debug!(source = kind.name(), "fetch failed: {err:#}");
                }
            }
        }

        let max_age = Duration::from_millis(self.sources.max_age_ms);
        let convention = self.sources.convention;
        let fresh = |kinds: &[SourceKind], symbol: &str| -> Vec<(SourceKind, Decimal)> {
            kinds
                .iter()
                .filter_map(|&kind| {
                    let (price, at) = self.cache.get(&(kind, symbol.to_string()))?;
                    if now.duration_since(*at) > max_age {
                        return None;
                    }
                    Some((kind, express(*price, kind.quote(), convention, usdt_usd)?))
                })
                .collect()
        };

        let mut round = Round {
            usdt_usd: usdt_usd.filter(|_| convert),
            ..Round::default()
        };
        for asset in &self.assets {
            let spot = match aggregate(
                &fresh(&self.sources.spot, &asset.symbol),
                self.sources.max_deviation_bps,
                self.sources.min_sources,
            ) {
                Ok(spot) => spot,
                Err(err) => {
                    round.unpriced.push((asset.coin.clone(), err));
                    continue;
                }
            };
            let external = aggregate(
                &fresh(&self.sources.perp, &asset.symbol),
                self.sources.max_deviation_bps,
                self.sources.min_sources,
            )
            .map(|perp| perp.price)
            .unwrap_or(spot.price);
            round.priced.push(Priced {
                coin: asset.coin.clone(),
                spot,
                external,
            });
        }
        round
    }

    /// Fetches the USDT/USD rate once, for `hyperkeeper check`.
    pub async fn usdt_usd_once(&self) -> anyhow::Result<Decimal> {
        self.fetcher.usdt_usd().await
    }

    /// Fetches every source once, uncached, for `hyperkeeper check`.
    pub async fn fetch_once(&self) -> Vec<(SourceKind, anyhow::Result<crate::sources::Prices>)> {
        self.fetcher.fetch_all(&self.kinds, &self.symbols).await
    }
}

/// A venue's price in the configured convention. `None` when a conversion is needed but
/// no fresh USDT/USD rate is known: that quote is left out rather than mixed in.
pub fn express(
    price: Decimal,
    quote: Quote,
    convention: Convention,
    usdt_usd: Option<Decimal>,
) -> Option<Decimal> {
    match (convention, quote) {
        (Convention::AsQuoted, _)
        | (Convention::Usd, Quote::Usd)
        | (Convention::Usdt, Quote::Usdt) => Some(price),
        (Convention::Usd, Quote::Usdt) => usdt_usd.map(|rate| price * rate),
        (Convention::Usdt, Quote::Usd) => usdt_usd.map(|rate| price / rate),
    }
}

fn all_kinds(sources: &Sources) -> Vec<SourceKind> {
    let mut kinds: Vec<SourceKind> = sources.spot.iter().chain(&sources.perp).copied().collect();
    kinds.sort();
    kinds.dedup();
    kinds
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn usdt_quotes_are_converted_to_the_convention() {
        let rate = Some(dec!(0.99928));
        assert_eq!(
            express(dec!(100), Quote::Usdt, Convention::Usd, rate),
            Some(dec!(99.928))
        );
        assert_eq!(
            express(dec!(100), Quote::Usd, Convention::Usd, rate),
            Some(dec!(100))
        );
        assert_eq!(
            express(dec!(99.928), Quote::Usd, Convention::Usdt, rate),
            Some(dec!(100))
        );
        assert_eq!(
            express(dec!(100), Quote::Usdt, Convention::AsQuoted, None),
            Some(dec!(100))
        );
    }

    #[test]
    fn without_a_rate_only_quotes_needing_no_conversion_survive() {
        assert_eq!(express(dec!(100), Quote::Usdt, Convention::Usd, None), None);
        assert_eq!(
            express(dec!(100), Quote::Usd, Convention::Usd, None),
            Some(dec!(100))
        );
        assert_eq!(express(dec!(100), Quote::Usd, Convention::Usdt, None), None);
    }
}
