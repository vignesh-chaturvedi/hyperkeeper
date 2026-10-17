//! The keeper's TOML configuration.

use std::path::Path;

use anyhow::{Context, bail};
use hypersdk::hypercore::Chain;
use serde::Deserialize;

/// `setOracle` calls must be at least 2.5 s apart (HIP-3 deployer actions docs).
const MIN_INTERVAL_MS: u64 = 2_500;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Network the DEX lives on.
    pub network: Network,
    /// HIP-3 DEX name, e.g. `ignp`.
    pub dex: String,
    /// The DEX is a HIP-3* (permissioned) venue: send the `star.setOracle` operation,
    /// which takes spot oracle prices only, instead of the standard `setOracle`.
    #[serde(default)]
    pub star: bool,
    /// How often to compute, and in live mode send, an oracle update.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    pub sources: Sources,
    #[serde(default)]
    pub guards: Guards,
    /// Where `hyperkeeper run` gets its signing key. See `signer.rs`.
    #[serde(default)]
    pub signer: SignerConfig,
    #[serde(default)]
    pub failover: Failover,
    #[serde(rename = "asset")]
    pub assets: Vec<Asset>,
}

/// The signing key for live mode. `HYPERKEEPER_PRIVATE_KEY` in the environment takes
/// precedence; otherwise the keystore is decrypted with `HYPERKEEPER_KEYSTORE_PASSWORD`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignerConfig {
    /// Path to an encrypted JSON keystore, e.g. one made by `cast wallet import`.
    pub keystore: Option<std::path::PathBuf>,
}

/// Two keeper instances, each with its own key, both granted `setOracle`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failover {
    #[serde(default)]
    pub role: Role,
    /// The other instance's signing address. Required for a standby.
    pub peer: Option<alloy::primitives::Address>,
    /// A standby sends once the peer's last successful update is older than this.
    #[serde(default = "default_takeover_after_ms")]
    pub takeover_after_ms: u64,
    /// How often a standby asks the explorer for the peer's updates.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

impl Default for Failover {
    fn default() -> Self {
        Self {
            role: Role::default(),
            peer: None,
            takeover_after_ms: default_takeover_after_ms(),
            poll_ms: default_poll_ms(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Sends every interval.
    #[default]
    Primary,
    /// Sends only while the peer is silent.
    Standby,
}

/// Limits between an aggregated price and an oracle update. See `guard.rs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Guards {
    /// Largest move per update from the last accepted price; larger moves are capped.
    #[serde(default = "default_max_move_bps")]
    pub max_move_bps: u32,
    /// Raise an alarm after this many updates in a row that can't be sent. The exchange
    /// falls back to the local mark after 10 s without an update.
    #[serde(default = "default_unsent_alarm_ticks")]
    pub unsent_alarm_ticks: u32,
}

impl Default for Guards {
    fn default() -> Self {
        Self {
            max_move_bps: default_max_move_bps(),
            unsent_alarm_ticks: default_unsent_alarm_ticks(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn chain(self) -> Chain {
        match self {
            Network::Mainnet => Chain::Mainnet,
            Network::Testnet => Chain::Testnet,
        }
    }
}

/// One asset of the DEX and the underlying it tracks.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    /// Coin on the DEX, with its prefix, e.g. `ignp:BTC`.
    pub coin: String,
    /// Underlying symbol looked up on every source, e.g. `BTC`.
    pub symbol: String,
    /// The asset's Pyth feed, e.g. `Equity.US.NVDA/USD`. Only the `pyth` source reads it.
    #[serde(default)]
    pub pyth: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sources {
    /// Spot venues whose median becomes the oracle price.
    pub spot: Vec<SourceKind>,
    /// Perp venues whose median becomes the external perp price and the mark input.
    /// When none are fresh, the oracle price stands in.
    #[serde(default)]
    pub perp: Vec<SourceKind>,
    /// Quotes older than this are ignored.
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: u64,
    /// Fewer fresh, agreeing sources than this and the asset has no price.
    #[serde(default = "default_min_sources")]
    pub min_sources: usize,
    /// Sources further than this from the median are dropped before the final median.
    #[serde(default = "default_max_deviation_bps")]
    pub max_deviation_bps: u32,
    /// The currency every venue's price is expressed in before aggregation.
    #[serde(default)]
    pub convention: Convention,
}

/// The currency prices are expressed in before aggregation.
///
/// Venues quote either USD or USDT, and USDT rarely trades at exactly a dollar: on Oct 8,
/// 2026 it was 7.2 bp under. Mixing the two puts the median wherever the larger group of
/// venues sits, so the keeper converts every quote to one currency first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Convention {
    /// Dollars: USDT-quoted prices are multiplied by the USDT/USD rate.
    #[default]
    Usd,
    /// Tether: USD-quoted prices are divided by the USDT/USD rate. Hyperliquid's own
    /// validator oracle sits on this side.
    Usdt,
    /// No conversion: venues are mixed as quoted, as before Oct 8, 2026.
    AsQuoted,
}

/// The currency a venue quotes in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quote {
    Usd,
    Usdt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    Binance,
    Coinbase,
    Bybit,
    Kraken,
    Okx,
    /// Oracle price of the same coin on Hyperliquid's main DEX (mainnet).
    HyperliquidOracle,
    BinancePerp,
    BybitPerp,
    OkxPerp,
    /// Mid price of the same coin's perp on Hyperliquid's main DEX (mainnet).
    HyperliquidPerp,
    /// Pyth price feeds, named per asset with `pyth = "..."`. Covers stocks, metals and FX,
    /// and needs an API key in `HYPERKEEPER_PYTH_API_KEY`.
    Pyth,
    /// Bybit's tokenized stocks (xStocks), e.g. `NVDAXUSDT` for `NVDA`. They trade around the
    /// clock, at a premium to the stock outside its trading sessions.
    BybitXstock,
}

impl SourceKind {
    pub fn name(self) -> &'static str {
        match self {
            SourceKind::Binance => "binance",
            SourceKind::Coinbase => "coinbase",
            SourceKind::Bybit => "bybit",
            SourceKind::Kraken => "kraken",
            SourceKind::Okx => "okx",
            SourceKind::HyperliquidOracle => "hyperliquid-oracle",
            SourceKind::BinancePerp => "binance-perp",
            SourceKind::BybitPerp => "bybit-perp",
            SourceKind::OkxPerp => "okx-perp",
            SourceKind::HyperliquidPerp => "hyperliquid-perp",
            SourceKind::Pyth => "pyth",
            SourceKind::BybitXstock => "bybit-xstock",
        }
    }

    /// What the venue's prices are denominated in for the keeper's symbols.
    ///
    /// Hyperliquid's main-DEX oracle and perps count as USDT: the oracle is a median of
    /// mostly USDT-quoted venues, and on Oct 8, 2026 both read about 8 bp above
    /// USD-quoted venues while USDT traded 7.2 bp under a dollar.
    pub fn quote(self) -> Quote {
        match self {
            SourceKind::Coinbase | SourceKind::Kraken | SourceKind::Pyth => Quote::Usd,
            SourceKind::Binance
            | SourceKind::Bybit
            | SourceKind::Okx
            | SourceKind::BinancePerp
            | SourceKind::BybitPerp
            | SourceKind::OkxPerp
            | SourceKind::HyperliquidOracle
            | SourceKind::HyperliquidPerp
            | SourceKind::BybitXstock => Quote::Usdt,
        }
    }
}

fn default_interval_ms() -> u64 {
    3_000
}
fn default_max_age_ms() -> u64 {
    10_000
}
fn default_min_sources() -> usize {
    2
}
fn default_max_deviation_bps() -> u32 {
    100
}
fn default_max_move_bps() -> u32 {
    200
}
fn default_unsent_alarm_ticks() -> u32 {
    3
}
fn default_takeover_after_ms() -> u64 {
    // A primary sending every 3 s shows at most ~3 s plus the explorer's few seconds of
    // indexing delay at each poll; 10 s leaves a margin without a long gap on failure.
    10_000
}
fn default_poll_ms() -> u64 {
    5_000
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.interval_ms < MIN_INTERVAL_MS {
            bail!(
                "interval_ms is {}, but setOracle calls must be at least {MIN_INTERVAL_MS} ms apart",
                self.interval_ms
            );
        }
        if self.assets.is_empty() {
            bail!("no [[asset]] entries");
        }
        let prefix = format!("{}:", self.dex);
        for asset in &self.assets {
            if !asset.coin.starts_with(&prefix) {
                bail!("asset {} is not on dex {}", asset.coin, self.dex);
            }
        }
        if self.sources.spot.is_empty() {
            bail!("sources.spot is empty");
        }
        let uses_pyth = self.sources.spot.contains(&SourceKind::Pyth)
            || self.sources.perp.contains(&SourceKind::Pyth);
        if uses_pyth && self.assets.iter().all(|a| a.pyth.is_none()) {
            bail!("sources include pyth, but no [[asset]] names a pyth feed");
        }
        if self.sources.min_sources == 0 {
            bail!("sources.min_sources must be at least 1");
        }
        if self.guards.max_move_bps == 0 {
            bail!("guards.max_move_bps must be above 0");
        }
        if self.failover.role == Role::Standby {
            if self.failover.peer.is_none() {
                bail!("a standby needs failover.peer, the primary's signing address");
            }
            if self.failover.takeover_after_ms < 2 * self.interval_ms {
                bail!(
                    "failover.takeover_after_ms is {}, under two update intervals ({} ms): the standby would take over during normal operation",
                    self.failover.takeover_after_ms,
                    2 * self.interval_ms
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../hyperkeeper.example.toml");

    #[test]
    fn example_config_is_valid() {
        let config: Config = toml::from_str(EXAMPLE).unwrap();
        config.validate().unwrap();
        assert_eq!(config.network, Network::Testnet);
        assert!(config.sources.spot.contains(&SourceKind::Coinbase));
    }

    #[test]
    fn rejects_intervals_under_the_exchange_minimum() {
        let mut config: Config = toml::from_str(EXAMPLE).unwrap();
        config.interval_ms = 2_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn pyth_needs_a_feed_on_some_asset() {
        let mut config: Config = toml::from_str(EXAMPLE).unwrap();
        config.sources.spot.push(SourceKind::Pyth);
        assert!(config.validate().is_err());
        config.assets[0].pyth = Some("Crypto.BTC/USD".into());
        config.validate().unwrap();
    }

    #[test]
    fn rejects_assets_from_another_dex() {
        let mut config: Config = toml::from_str(EXAMPLE).unwrap();
        config.assets[0].coin = "xyz:BTC".into();
        assert!(config.validate().is_err());
    }
}
