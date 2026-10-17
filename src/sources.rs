//! Price sources. Only Pyth needs an API key.
//!
//! Every exchange source returns the mid of the best bid and ask per symbol, and Pyth its
//! aggregate price. Venues quote against USDT or USD; `pricer` converts them to one currency.

use std::{
    collections::BTreeMap,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow, bail};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::config::SourceKind;

/// Price per underlying symbol, e.g. `BTC`.
pub type Prices = BTreeMap<String, Decimal>;

/// Where the Pyth API key is read from.
pub const PYTH_KEY_ENV: &str = "HYPERKEEPER_PYTH_API_KEY";
const HERMES: &str = "https://hermes.pyth.network";
/// Pyth prices older than this by their own publish time are dropped. A feed stops updating
/// outside its market's trading sessions, and a closed market should read as no price.
const PYTH_STALE_AFTER_SECS: u64 = 30;

pub struct Fetcher {
    http: reqwest::Client,
    pyth: Option<Pyth>,
}

/// Pyth's Hermes API, which has required an API key since August 2026.
struct Pyth {
    /// Without a key every Pyth fetch fails, and the other sources carry on.
    key: Option<String>,
    /// Feed name per symbol, e.g. `NVDA` to `Equity.US.NVDA/USD`.
    feeds: BTreeMap<String, String>,
    /// Feed id per symbol, looked up in Pyth's public catalog on first use.
    ids: tokio::sync::OnceCell<BTreeMap<String, String>>,
    /// Feed ids the key's plan doesn't cover, learned from Pyth's errors.
    denied: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Fetcher {
    pub fn new(timeout: Duration) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(crate::USER_AGENT)
            .build()?;
        Ok(Self { http, pyth: None })
    }

    /// Enables the `pyth` source for these feeds, keyed by symbol, with the API key from
    /// `HYPERKEEPER_PYTH_API_KEY`.
    pub fn with_pyth(mut self, feeds: BTreeMap<String, String>) -> Self {
        let key = std::env::var(PYTH_KEY_ENV)
            .ok()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty());
        if key.is_none() {
            tracing::warn!("{PYTH_KEY_ENV} is not set, so the pyth source will fail every round");
        }
        self.pyth = Some(Pyth {
            key,
            feeds,
            ids: tokio::sync::OnceCell::new(),
            denied: Default::default(),
        });
        self
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
            SourceKind::Bybit => self.bybit("spot", "USDT", symbols).await,
            SourceKind::BybitPerp => self.bybit("linear", "USDT", symbols).await,
            SourceKind::BybitXstock => self.bybit("spot", "XUSDT", symbols).await,
            SourceKind::Pyth => self.pyth(symbols).await,
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

    /// `suffix` turns a symbol into Bybit's name: `USDT` for `BTCUSDT`, or `XUSDT` for the
    /// xStock `NVDAXUSDT`.
    async fn bybit(
        &self,
        category: &str,
        suffix: &str,
        symbols: &[String],
    ) -> anyhow::Result<Prices> {
        let url = format!("https://api.bybit.com/v5/market/tickers?category={category}");
        let body = self.get(&url).await?;
        collect(
            symbols,
            body["result"]["list"].as_array(),
            |s| format!("{s}{suffix}"),
            "symbol",
            "bid1Price",
            "ask1Price",
        )
    }

    async fn pyth(&self, symbols: &[String]) -> anyhow::Result<Prices> {
        let pyth = self
            .pyth
            .as_ref()
            .context("the pyth source is not set up")?;
        let key = pyth
            .key
            .as_deref()
            .with_context(|| format!("no API key in {PYTH_KEY_ENV}"))?;
        let ids = pyth
            .ids
            .get_or_try_init(|| self.pyth_ids(&pyth.feeds))
            .await?;
        // Feed id to symbol, for the symbols wanted this round.
        let mut wanted: BTreeMap<String, String> = {
            let denied = pyth.denied.lock().unwrap();
            ids.iter()
                .filter(|(symbol, id)| symbols.contains(symbol) && !denied.contains(*id))
                .map(|(symbol, id)| (id.clone(), symbol.clone()))
                .collect()
        };
        loop {
            if wanted.is_empty() {
                return Ok(Prices::new());
            }
            let query = wanted
                .keys()
                .map(|id| format!("ids[]={id}"))
                .collect::<Vec<_>>()
                .join("&");
            let response = self
                .http
                .get(format!(
                    "{HERMES}/v2/updates/price/latest?{query}&parsed=true"
                ))
                .bearer_auth(key)
                .send()
                .await?;
            let status = response.status();
            if status == reqwest::StatusCode::FORBIDDEN {
                // Pyth rejects the whole request over one feed the plan doesn't cover, and
                // names it. Leave that feed out from now on and ask again without it.
                let body = response.text().await.unwrap_or_default();
                let denied = not_entitled_feed(&body);
                let Some(symbol) = denied.as_ref().and_then(|id| wanted.remove(id)) else {
                    bail!(
                        "HTTP {status} from Pyth: {}",
                        body.chars().take(200).collect::<String>()
                    );
                };
                tracing::warn!(
                    "the Pyth plan doesn't cover {} ({symbol}); leaving it out",
                    pyth.feeds.get(&symbol).map_or("this feed", String::as_str)
                );
                pyth.denied.lock().unwrap().extend(denied);
                continue;
            }
            if !status.is_success() {
                bail!("HTTP {status} from Pyth");
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            return parse_pyth(&response.json().await?, &wanted, now);
        }
    }

    /// Looks up each feed's id in Pyth's catalog, which is public. The catalog is about 1 MB,
    /// so this one request gets a longer timeout than price requests.
    async fn pyth_ids(
        &self,
        feeds: &BTreeMap<String, String>,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let response = self
            .http
            .get(format!("{HERMES}/v2/price_feeds"))
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            bail!("HTTP {status} from Pyth's feed catalog");
        }
        let catalog: Value = response.json().await?;
        let entries = catalog.as_array().context("unexpected catalog shape")?;
        let mut ids = BTreeMap::new();
        for (symbol, feed) in feeds {
            let id = entries
                .iter()
                .find(|e| e["attributes"]["symbol"].as_str() == Some(feed.as_str()))
                .and_then(|e| e["id"].as_str())
                .with_context(|| format!("no Pyth feed named {feed}"))?;
            ids.insert(symbol.clone(), id.trim_start_matches("0x").to_string());
        }
        Ok(ids)
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
/// The feed id in Pyth's "Not entitled: feed <id> (...)" error, without a `0x` prefix.
fn not_entitled_feed(body: &str) -> Option<String> {
    let rest = body.split("Not entitled: feed ").nth(1)?;
    let id: String = rest
        .trim_start_matches("0x")
        .chars()
        .take_while(char::is_ascii_hexdigit)
        .collect();
    (!id.is_empty()).then_some(id)
}

/// Reads Hermes's parsed price updates into a price per symbol, given each feed id's symbol.
/// Prices published more than `PYTH_STALE_AFTER_SECS` before `now_secs` are dropped.
fn parse_pyth(
    body: &Value,
    symbol_of: &BTreeMap<String, String>,
    now_secs: u64,
) -> anyhow::Result<Prices> {
    let updates = body["parsed"]
        .as_array()
        .context("no parsed price updates")?;
    let mut prices = Prices::new();
    for update in updates {
        let Some(symbol) = update["id"]
            .as_str()
            .and_then(|id| symbol_of.get(id.trim_start_matches("0x")))
        else {
            continue;
        };
        let price = &update["price"];
        let (Some(mantissa), Some(expo), Some(published)) = (
            price["price"].as_str().and_then(|p| p.parse::<i64>().ok()),
            price["expo"].as_i64(),
            price["publish_time"].as_u64(),
        ) else {
            continue;
        };
        if mantissa <= 0 || now_secs.saturating_sub(published) > PYTH_STALE_AFTER_SECS {
            continue;
        }
        let value = if expo <= 0 {
            Decimal::try_from_i128_with_scale(i128::from(mantissa), u32::try_from(-expo)?)?
        } else {
            Decimal::from(mantissa) * Decimal::from(10_i64.pow(u32::try_from(expo)?))
        };
        prices.insert(symbol.clone(), value);
    }
    Ok(prices)
}

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
    fn parse_pyth_scales_prices_and_drops_closed_markets() {
        // Shaped like Hermes's `parsed` updates. NVDA published 5 s ago, AAPL 2 hours ago.
        let body = json!({"parsed": [
            {"id": "aa", "price": {"price": "21874000000", "conf": "5000000", "expo": -8, "publish_time": 1_000}},
            {"id": "0xbb", "price": {"price": "34137000", "conf": "1000", "expo": -5, "publish_time": 1_000 - 7_200}},
            {"id": "cc", "price": {"price": "4170", "conf": "1", "expo": 0, "publish_time": 1_000}},
        ]});
        let symbol_of: BTreeMap<String, String> = [("aa", "NVDA"), ("bb", "AAPL"), ("cc", "GOLD")]
            .map(|(id, symbol)| (id.to_string(), symbol.to_string()))
            .into();
        let prices = parse_pyth(&body, &symbol_of, 1_005).unwrap();
        assert_eq!(prices["NVDA"], dec!(218.74));
        assert_eq!(prices["GOLD"], dec!(4170));
        assert!(!prices.contains_key("AAPL"));
    }

    #[test]
    fn not_entitled_feed_reads_the_feed_id_from_pyths_error() {
        // Pyth's reply on Oct 17, 2026 to a demo-plan key asking for NVDA.
        let body = "Not entitled: feed b1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593 (no grant accepts this feed (asset type 'equity', instrument type 'spot', exchange 1))";
        assert_eq!(
            not_entitled_feed(body).as_deref(),
            Some("b1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593")
        );
        assert_eq!(not_entitled_feed("unauthorized"), None);
    }

    #[test]
    fn mid_rejects_empty_and_crossed_books() {
        assert_eq!(mid(&json!({"b": "0", "a": "1"}), "b", "a"), None);
        assert_eq!(mid(&json!({"b": "2", "a": "1"}), "b", "a"), None);
        assert_eq!(mid(&json!({"b": "1"}), "b", "a"), None);
    }
}
