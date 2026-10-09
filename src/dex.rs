//! What the keeper reads from, and checks against, the DEX itself.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

use alloy::primitives::Address;
use anyhow::Context;
use futures::StreamExt;
use hypersdk::hypercore::{
    self, Chain, HttpClient, PerpDexDetails, PrivateKeySigner, SubDeployerPermission,
    api::{Action, Response},
    types::{Incoming, Subscription},
    ws::Event,
};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config::{Config, Network};

/// Oracle-change gaps kept per asset: about a day at a 12 s cadence.
pub const MAX_GAPS: usize = 7_200;

/// One asset as the DEX shows it, from the `activeAssetCtx` WebSocket feed.
#[derive(Debug, Default, Clone)]
pub struct Observed {
    pub oracle: Option<Decimal>,
    pub prev_day: Option<Decimal>,
    pub changed_at: Option<Instant>,
    /// Seconds between oracle changes, the most recent `MAX_GAPS`.
    pub gaps: VecDeque<f64>,
}

/// The parts of `Observed` a round needs, without copying the gap history.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub oracle: Option<Decimal>,
    pub prev_day: Option<Decimal>,
}

pub fn snapshot(watch: &Watch) -> HashMap<String, Snapshot> {
    watch
        .lock()
        .unwrap()
        .iter()
        .map(|(coin, o)| {
            (
                coin.clone(),
                Snapshot {
                    oracle: o.oracle,
                    prev_day: o.prev_day,
                },
            )
        })
        .collect()
}

pub type Watch = Arc<Mutex<HashMap<String, Observed>>>;

/// Size decimals per coin, which fix how many price decimals the exchange accepts.
pub async fn sz_decimals(
    client: &HttpClient,
    config: &Config,
) -> anyhow::Result<BTreeMap<String, u32>> {
    let dex = client
        .perp_dexes()
        .await?
        .into_iter()
        .find(|d| d.name() == config.dex)
        .with_context(|| format!("DEX {} not found on {:?}", config.dex, config.network))?;
    let markets = client.perps_from(dex).await?;
    let mut decimals = BTreeMap::new();
    for asset in &config.assets {
        let market = markets
            .iter()
            .find(|m| m.name == asset.coin)
            .with_context(|| format!("{} is not listed on {}", asset.coin, config.dex))?;
        if market.delisted {
            warn!(coin = %asset.coin, "listed as delisted");
        }
        decimals.insert(asset.coin.clone(), u32::try_from(market.sz_decimals)?);
    }
    Ok(decimals)
}

/// Follows the DEX's own oracle over the WebSocket, recording when it changes.
pub fn watch(config: &Config) -> Watch {
    let state: Watch = Arc::default();
    let mut ws = match config.network {
        Network::Mainnet => hypercore::mainnet_ws(),
        Network::Testnet => hypercore::testnet_ws(),
    };
    for asset in &config.assets {
        ws.subscribe(Subscription::ActiveAssetCtx {
            coin: asset.coin.clone(),
        });
    }
    let shared = state.clone();
    tokio::spawn(async move {
        while let Some(event) = ws.next().await {
            let Event::Message(Incoming::ActiveAssetCtx { coin, ctx }) = event else {
                continue;
            };
            let mut map = shared.lock().unwrap();
            let observed = map.entry(coin).or_default();
            observed.prev_day = Some(ctx.prev_day_px);
            let Some(price) = ctx.oracle_px else { continue };
            if observed.oracle != Some(price) {
                let now = Instant::now();
                if let Some(previous) = observed.changed_at {
                    if observed.gaps.len() == MAX_GAPS {
                        observed.gaps.pop_front();
                    }
                    observed
                        .gaps
                        .push_back(now.duration_since(previous).as_secs_f64());
                }
                observed.oracle = Some(price);
                observed.changed_at = Some(now);
            }
        }
    });
    state
}

/// Signs the update with a throwaway key and sends it once. The exchange rejects it, and
/// the rejection names the address it recovered from the signature: if that is the
/// throwaway key's address, the action parsed and was hashed exactly as the exchange
/// hashes it, so a key with permission would be accepted.
pub async fn probe(client: &HttpClient, chain: Chain, update: Action) -> bool {
    let signer = PrivateKeySigner::random();
    let nonce = chrono::Utc::now().timestamp_millis() as u64;
    let request = match update.sign_sync(&signer, nonce, None, None, chain) {
        Ok(request) => request,
        Err(err) => {
            warn!("could not sign the probe update: {err:#}");
            return false;
        }
    };
    let me = format!("{:#x}", signer.address());
    match client.send(request).await {
        Ok(Response::Err(message)) if message.to_lowercase().contains(&me) => {
            info!(
                "signature check passed: the exchange parsed the update and recovered the signing key ({message})"
            );
            true
        }
        Ok(Response::Err(message)) => {
            warn!("signature check FAILED: {message}");
            false
        }
        Ok(other) => {
            warn!("unexpected reply to the probe update: {other:?}");
            false
        }
        Err(err) => {
            warn!("probe update not answered: {err:#}");
            false
        }
    }
}

/// How long ago any of the DEX's assets last changed its oracle, if one was seen.
pub fn oracle_quiet_for(watch: &Watch) -> Option<std::time::Duration> {
    watch
        .lock()
        .unwrap()
        .values()
        .filter_map(|o| o.changed_at)
        .max()
        .map(|at| at.elapsed())
}

/// Whether a key may send this DEX's oracle update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    Allowed(&'static str),
    /// Not allowed; these addresses are.
    Denied(Vec<Address>),
}

/// Checks a key against the DEX's live configuration before anything is sent.
pub async fn authorization(
    client: &HttpClient,
    config: &Config,
    key: Address,
) -> anyhow::Result<Authorization> {
    let details = client
        .perp_dex_details()
        .await?
        .into_iter()
        .find(|d| d.name == config.dex)
        .with_context(|| format!("DEX {} not found on {:?}", config.dex, config.network))?;
    Ok(authorize(&details, config.star, key))
}

/// The deployer may always send. On a standard DEX, so may the oracle updater and
/// `setOracle` sub-deployers. On a HIP-3* venue only `hip3Star:setOracle` sub-deployers
/// may: the regular grant, including the oracle updater's, doesn't cover the star
/// operation.
pub fn authorize(details: &PerpDexDetails, star: bool, key: Address) -> Authorization {
    if details.deployer == key {
        return Authorization::Allowed("deployer");
    }
    let mut allowed = vec![details.deployer];
    if star {
        let granted = details
            .sub_deployers
            .iter()
            .filter(|g| matches!(&g.permission, SubDeployerPermission::Hip3Star { action } if action == "setOracle"))
            .flat_map(|g| g.users.iter().copied());
        allowed.extend(granted);
        if allowed.contains(&key) {
            return Authorization::Allowed("hip3Star:setOracle sub-deployer");
        }
    } else {
        if details.oracle_updater == Some(key) {
            return Authorization::Allowed("oracle updater");
        }
        if details.sub_deployers_for("setOracle").contains(&key) {
            return Authorization::Allowed("setOracle sub-deployer");
        }
        allowed.extend(details.oracle_updater);
        allowed.extend(details.sub_deployers_for("setOracle"));
    }
    Authorization::Denied(allowed)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const DEPLOYER: &str = "0x1111111111111111111111111111111111111111";
    const UPDATER: &str = "0x2222222222222222222222222222222222222222";
    const SUB: &str = "0x3333333333333333333333333333333333333333";
    const STAR_SUB: &str = "0x4444444444444444444444444444444444444444";
    const STRANGER: &str = "0x5555555555555555555555555555555555555555";

    fn details() -> PerpDexDetails {
        serde_json::from_value(json!({
            "name": "test", "fullName": "Test", "deployer": DEPLOYER,
            "oracleUpdater": UPDATER, "feeRecipient": null,
            "subDeployers": [["setOracle", [SUB]], [{"hip3Star": "setOracle"}, [STAR_SUB]]]
        }))
        .unwrap()
    }

    fn key(s: &str) -> Address {
        s.parse().unwrap()
    }

    #[test]
    fn standard_dex_allows_deployer_updater_and_setoracle_sub_deployers() {
        let d = details();
        assert_eq!(
            authorize(&d, false, key(DEPLOYER)),
            Authorization::Allowed("deployer")
        );
        assert_eq!(
            authorize(&d, false, key(UPDATER)),
            Authorization::Allowed("oracle updater")
        );
        assert_eq!(
            authorize(&d, false, key(SUB)),
            Authorization::Allowed("setOracle sub-deployer")
        );
        assert!(matches!(
            authorize(&d, false, key(STAR_SUB)),
            Authorization::Denied(_)
        ));
    }

    #[test]
    fn star_venue_allows_only_the_deployer_and_star_grants() {
        let d = details();
        assert_eq!(
            authorize(&d, true, key(STAR_SUB)),
            Authorization::Allowed("hip3Star:setOracle sub-deployer")
        );
        // The regular grants, including the oracle updater's, don't cover star.setOracle.
        assert!(matches!(
            authorize(&d, true, key(UPDATER)),
            Authorization::Denied(_)
        ));
        assert!(matches!(
            authorize(&d, true, key(SUB)),
            Authorization::Denied(_)
        ));
        assert_eq!(
            authorize(&d, true, key(STRANGER)),
            Authorization::Denied(vec![key(DEPLOYER), key(STAR_SUB)])
        );
    }
}
