//! Public price sources. None needs an API key.
//!
//! Every source returns the mid of the best bid and ask per symbol. Venues quote against
//! USDT or USD; both are treated as dollars, as Hyperliquid's own validator oracle does.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use anyhow::{Context, anyhow};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::config::SourceKind;

/// Price per underlying symbol, e.g. `BTC`.
pub type Prices = BTreeMap<String, Decimal>;

pub struct Fetcher {
    http: reqwest::Client,
}

impl Fetcher {
    pub fn new(timeout: Duration) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(crate::USER_AGENT)
            .build()?;
        Ok(Self { http })
    }

    /// Fetches every source concurrently. A failed source comes back as an error and the
    /// others are unaffected. Both Hyperliquid sources share one request.
    pub async fn fetch_all(
        &self,
        kinds: &[SourceKind],
        symbols: &[String],
    ) -> Vec<(SourceKind, anyhow::Result<Prices>)> {
        let wants_hl = kinds.iter().any(|k| {
            matches!(
                k,
                SourceKind::HyperliquidOracle | SourceKind::HyperliquidPerp
            )
        });
        let others = kinds
            .iter()
            .filter(|k| {
                !matches!(
                    k,
                    SourceKind::HyperliquidOracle | SourceKind::HyperliquidPerp
                )
            })
            .map(|&kind| async move { (kind, self.fetch(kind, symbols).await) });

        let (hl, mut results) = tokio::join!(
            async {
                if wants_hl {
                    Some(self.hyperliquid(symbols).await)
                } else {
                    None
                }
            },
            futures::future::join_all(others)
        );

        if let Some(hl) = hl {
            for &kind in kinds {
                let result = match (&hl, kind) {
                    (Ok((oracle, _)), SourceKind::HyperliquidOracle) => Ok(oracle.clone()),
                    (Ok((_, mids)), SourceKind::HyperliquidPerp) => Ok(mids.clone()),
                    (Err(err), SourceKind::HyperliquidOracle | SourceKind::HyperliquidPerp) => {
                        Err(anyhow!("{err:#}"))
                    }
                    _ => continue,
                };
                results.push((kind, result));
            }
        }
        results
    }

    /// USD per USDT: the median of Kraken's and Coinbase's USDT/USD books. Either venue
    /// alone is enough. A rate outside 0.95 to 1.05 is treated as a bad read.
    pub async fn usdt_usd(&self) -> anyhow::Result<Decimal> {
        let kraken = async {
            let body = self
                .get("https://api.kraken.com/0/public/Ticker?pair=USDTUSD")
                .await?;
            let ticker = body["result"]
                .as_object()
                .and_then(|r| r.values().next())
                .context("no USDT/USD pair")?;
            let bid = ticker["b"][0]
                .as_str()
                .and_then(|s| Decimal::from_str(s).ok());
            let ask = ticker["a"][0]
                .as_str()
                .and_then(|s| Decimal::from_str(s).ok());
            match (bid, ask) {
                (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask >= bid => {
                    anyhow::Ok((bid + ask) / Decimal::TWO)
                }
                _ => anyhow::bail!("no USDT/USD bid and ask"),
            }
        };
        let coinbase = async {
            let ticker = self
                .get("https://api.exchange.coinbase.com/products/USDT-USD/ticker")
                .await?;
            mid(&ticker, "bid", "ask").context("no USDT/USD bid and ask")
        };
        let (kraken, coinbase) = tokio::join!(kraken, coinbase);
        let rates: Vec<Decimal> = [kraken, coinbase]
            .into_iter()
            .flatten()
            .filter(|r| (Decimal::new(95, 2)..=Decimal::new(105, 2)).contains(r))
            .collect();
        crate::aggregate::median(rates).context("no USDT/USD rate from Kraken or Coinbase")
    }

    async fn fetch(&self, kind: SourceKind, symbols: &[String]) -> anyhow::Result<Prices> {
        match kind {
            SourceKind::Binance => self.binance_spot(symbols).await,
            SourceKind::BinancePerp => self.binance_perp(symbols).await,
            SourceKind::Coinbase => self.coinbase(symbols).await,
            SourceKind::Bybit => self.bybit("spot", symbols).await,
            SourceKind::BybitPerp => self.bybit("linear", symbols).await,
            SourceKind::Kraken => self.kraken(symbols).await,
            SourceKind::Okx => self.okx("SPOT", "", symbols).await,
            SourceKind::OkxPerp => self.okx("SWAP", "-SWAP", symbols).await,
            SourceKind::HyperliquidOracle | SourceKind::HyperliquidPerp => {
                unreachable!("fetched together in fetch_all")
            }
        }
    }

    async fn get(&self, url: &str) -> anyhow::Result<Value> {
        let response = self.http.get(url).send().await?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("HTTP {status} from {url}");
        }
        Ok(response.json().await?)
    }

    async fn binance_spot(&self, symbols: &[String]) -> anyhow::Result<Prices> {
        let list = symbols
            .iter()
            .map(|s| format!("\"{s}USDT\""))
            .collect::<Vec<_>>()
            .join(",");
        let url = format!("https://api.binance.com/api/v3/ticker/bookTicker?symbols=[{list}]");
        let tickers = self.get(&url).await?;
        collect(
            symbols,
            tickers.as_array(),
            |s| format!("{s}USDT"),
            "symbol",
            "bidPrice",
            "askPrice",
        )
    }

    async fn binance_perp(&self, symbols: &[String]) -> anyhow::Result<Prices> {
        // The futures endpoint takes one symbol or none, so fetch all and filter.
        let tickers = self
            .get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
            .await?;
        collect(
            symbols,
            tickers.as_array(),
            |s| format!("{s}USDT"),
            "symbol",
            "bidPrice",
            "askPrice",
        )
    }

    async fn coinbase(&self, symbols: &[String]) -> anyhow::Result<Prices> {
        let requests = symbols.iter().map(|symbol| async move {
            let url = format!("https://api.exchange.coinbase.com/products/{symbol}-USD/ticker");
            let ticker = self.get(&url).await?;
            let price = mid(&ticker, "bid", "ask").context("no bid/ask")?;
            anyhow::Ok((symbol.clone(), price))
        });
        let mut prices = Prices::new();
        // A symbol Coinbase doesn't list is simply missing from this source.
        for (symbol, price) in futures::future::join_all(requests)
            .await
            .into_iter()
            .flatten()
        {
            prices.insert(symbol, price);
        }
        if prices.is_empty() {
            anyhow::bail!("no symbol answered");
        }
        Ok(prices)
    }

    async fn bybit(&self, category: &str, symbols: &[String]) -> anyhow::Result<Prices> {
        let url = format!("https://api.bybit.com/v5/market/tickers?category={category}");
        let body = self.get(&url).await?;
        collect(
            symbols,
            body["result"]["list"].as_array(),
            |s| format!("{s}USDT"),
            "symbol",
            "bid1Price",
            "ask1Price",
        )
    }

    async fn kraken(&self, symbols: &[String]) -> anyhow::Result<Prices> {
        // Kraken names BTC XBT and DOGE XDG, and returns canonical pair keys such as
        // XXBTZUSD, so match each key back to the symbol it contains.
        let alias = |s: &str| match s {
            "BTC" => "XBT".to_string(),
            "DOGE" => "XDG".to_string(),
            other => other.to_string(),
        };
        let pairs = symbols
            .iter()
            .map(|s| format!("{}USD", alias(s)))
            .collect::<Vec<_>>()
            .join(",");
        let body = self
            .get(&format!(
                "https://api.kraken.com/0/public/Ticker?pair={pairs}"
            ))
            .await?;
        let result = body["result"].as_object().context("no result")?;
        let mut prices = Prices::new();
        for symbol in symbols {
            let name = alias(symbol);
            let found = result
                .iter()
                .find(|(key, _)| key.ends_with("USD") && key.contains(&name));
            if let Some((_, ticker)) = found {
                let bid = ticker["b"][0]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok());
                let ask = ticker["a"][0]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok());
                if let (Some(bid), Some(ask)) = (bid, ask) {
                    prices.insert(symbol.clone(), (bid + ask) / Decimal::TWO);
                }
            }
        }
        Ok(prices)
    }

    async fn okx(
        &self,
        inst_type: &str,
        suffix: &str,
        symbols: &[String],
    ) -> anyhow::Result<Prices> {
        let url = format!("https://www.okx.com/api/v5/market/tickers?instType={inst_type}");
        let body = self.get(&url).await?;
        collect(
            symbols,
            body["data"].as_array(),
            |s| format!("{s}-USDT{suffix}"),
            "instId",
            "bidPx",
            "askPx",
        )
    }

    /// Oracle prices and perp mids from Hyperliquid's main DEX on mainnet, in one request.
    async fn hyperliquid(&self, symbols: &[String]) -> anyhow::Result<(Prices, Prices)> {
        let response = self
            .http
            .post("https://api.hyperliquid.xyz/info")
            .json(&serde_json::json!({"type": "metaAndAssetCtxs"}))
            .send()
            .await?;
        let body: Value = response.error_for_status()?.json().await?;
        let universe = body[0]["universe"].as_array().context("no universe")?;
        let ctxs = body[1].as_array().context("no asset contexts")?;
        let (mut oracle, mut mids) = (Prices::new(), Prices::new());
        // For perps, ctxs[i] belongs to universe[i].
        for (asset, ctx) in universe.iter().zip(ctxs) {
            let Some(name) = asset["name"].as_str() else {
                continue;
            };
            if !symbols.iter().any(|s| s == name) {
                continue;
            }
            if let Some(px) = decimal(&ctx["oraclePx"]) {
                oracle.insert(name.to_string(), px);
            }
            if let Some(px) = decimal(&ctx["midPx"]) {
                mids.insert(name.to_string(), px);
            }
        }
        Ok((oracle, mids))
    }
}

/// Picks each symbol's ticker out of a list by its venue name and takes the mid.
fn collect(
    symbols: &[String],
    tickers: Option<&Vec<Value>>,
    venue_name: impl Fn(&str) -> String,
    name_field: &str,
    bid_field: &str,
    ask_field: &str,
) -> anyhow::Result<Prices> {
    let tickers = tickers.context("unexpected response shape")?;
    let mut prices = Prices::new();
    for symbol in symbols {
        let wanted = venue_name(symbol);
        let ticker = tickers
            .iter()
            .find(|t| t[name_field].as_str() == Some(wanted.as_str()));
        if let Some(price) = ticker.and_then(|t| mid(t, bid_field, ask_field)) {
            prices.insert(symbol.clone(), price);
        }
    }
    Ok(prices)
}

fn mid(ticker: &Value, bid_field: &str, ask_field: &str) -> Option<Decimal> {
    let bid = decimal(&ticker[bid_field])?;
    let ask = decimal(&ticker[ask_field])?;
    (bid > Decimal::ZERO && ask >= bid).then(|| (bid + ask) / Decimal::TWO)
}

fn decimal(value: &Value) -> Option<Decimal> {
    value.as_str().and_then(|s| Decimal::from_str(s).ok())
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn collect_takes_the_mid_of_each_listed_symbol() {
        let tickers = vec![
            json!({"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "102"}),
            json!({"symbol": "ETHUSDT", "bidPrice": "10", "askPrice": "10.2"}),
        ];
        let symbols = vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()];
        let prices = collect(
            &symbols,
            Some(&tickers),
            |s| format!("{s}USDT"),
            "symbol",
            "bidPrice",
            "askPrice",
        )
        .unwrap();
        assert_eq!(prices["BTC"], dec!(101));
        assert_eq!(prices["ETH"], dec!(10.1));
        assert!(!prices.contains_key("SOL"));
    }

    #[test]
    fn mid_rejects_empty_and_crossed_books() {
        assert_eq!(mid(&json!({"b": "0", "a": "1"}), "b", "a"), None);
        assert_eq!(mid(&json!({"b": "2", "a": "1"}), "b", "a"), None);
        assert_eq!(mid(&json!({"b": "1"}), "b", "a"), None);
    }
}
