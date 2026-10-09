//! Builds the oracle update a DEX accepts from aggregated prices: the standard
//! `setOracle`, or the HIP-3* `star.setOracle` on permissioned venues.

use std::collections::BTreeMap;

use hypersdk::hypercore::{
    api::Action,
    deploy::{Hip3StarAction, Hip3StarOperation, Hip3StarSetOracle, PerpDeployAction, SetOracle},
};
use rust_decimal::{Decimal, RoundingStrategy};

/// Perp prices may carry at most `6 - szDecimals` decimals.
const MAX_PERP_DECIMALS: u32 = 6;

/// One asset's prices for an update.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetPrices {
    /// Coin on the DEX, e.g. `ignp:BTC`.
    pub coin: String,
    /// Spot reference price.
    pub oracle: Decimal,
    /// Perp reference price, used as the external perp price and as the mark input.
    pub external: Decimal,
}

/// Rounds to the decimals the exchange accepts for an asset and drops trailing zeros.
pub fn format_px(px: Decimal, sz_decimals: u32) -> String {
    let decimals = MAX_PERP_DECIMALS.saturating_sub(sz_decimals);
    px.round_dp_with_strategy(decimals, RoundingStrategy::MidpointNearestEven)
        .normalize()
        .to_string()
}

/// The `setOracle` action for every asset of the DEX.
///
/// The exchange requires every list sorted by coin before signing, and `externalPerpPxs`
/// to cover every asset, so callers pass all assets or none. `markPxs` carries one input,
/// the external perp price; the exchange takes the median of it and the local mark
/// (median of best bid, best ask and last trade).
pub fn set_oracle(
    dex: &str,
    prices: &[AssetPrices],
    sz_decimals: &BTreeMap<String, u32>,
) -> SetOracle {
    let external = entries(prices, sz_decimals, |p| p.external);
    SetOracle {
        dex: dex.to_string(),
        oracle_pxs: entries(prices, sz_decimals, |p| p.oracle),
        mark_pxs: vec![external.clone()],
        external_perp_pxs: external,
    }
}

/// The HIP-3* `star.setOracle` operation. Permissioned venues send only spot oracle
/// prices; the exchange derives external perp prices from the main DEX and marks onchain.
/// It needs its own grant, `{"hip3Star": "setOracle"}`.
pub fn star_set_oracle(
    dex: &str,
    prices: &[AssetPrices],
    sz_decimals: &BTreeMap<String, u32>,
) -> Hip3StarAction {
    Hip3StarAction {
        dex: dex.to_string(),
        operation: Hip3StarOperation::SetOracle(Hip3StarSetOracle {
            oracle_pxs: entries(prices, sz_decimals, |p| p.oracle),
        }),
    }
}

/// The update for a DEX: the star operation on HIP-3* venues, `setOracle` otherwise.
pub fn update(
    dex: &str,
    star: bool,
    prices: &[AssetPrices],
    sz_decimals: &BTreeMap<String, u32>,
) -> Action {
    if star {
        Action::PerpDeploy(PerpDeployAction::Star(star_set_oracle(
            dex,
            prices,
            sz_decimals,
        )))
    } else {
        Action::PerpDeploy(PerpDeployAction::SetOracle(set_oracle(
            dex,
            prices,
            sz_decimals,
        )))
    }
}

/// `(coin, price)` pairs sorted by coin, as every oracle list must be before signing.
fn entries(
    prices: &[AssetPrices],
    sz_decimals: &BTreeMap<String, u32>,
    pick: fn(&AssetPrices) -> Decimal,
) -> Vec<(String, String)> {
    let mut list: Vec<(String, String)> = prices
        .iter()
        .map(|p| {
            let decimals = sz_decimals.get(&p.coin).copied().unwrap_or(0);
            (p.coin.clone(), format_px(pick(p), decimals))
        })
        .collect();
    list.sort_by(|a, b| a.0.cmp(&b.0));
    list
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn prices_round_to_the_asset_decimals() {
        // BTC has 5 size decimals on the main DEX, so 1 price decimal.
        assert_eq!(format_px(dec!(85155.26), 5), "85155.3");
        assert_eq!(format_px(dec!(85155.0), 5), "85155");
        assert_eq!(format_px(dec!(0.0933512), 0), "0.093351");
        assert_eq!(format_px(dec!(2699.4549), 2), "2699.4549");
        assert_eq!(format_px(dec!(2699.45495), 2), "2699.455");
    }

    #[test]
    fn lists_are_sorted_by_coin_and_cover_every_asset() {
        let prices = [
            AssetPrices {
                coin: "ignp:ETH".into(),
                oracle: dec!(2700),
                external: dec!(2701),
            },
            AssetPrices {
                coin: "ignp:BTC".into(),
                oracle: dec!(85000),
                external: dec!(85010),
            },
        ];
        let decimals = BTreeMap::from([("ignp:BTC".to_string(), 5), ("ignp:ETH".to_string(), 4)]);
        let update = set_oracle("ignp", &prices, &decimals);
        assert_eq!(update.dex, "ignp");
        assert_eq!(
            update.oracle_pxs,
            vec![
                ("ignp:BTC".to_string(), "85000".to_string()),
                ("ignp:ETH".to_string(), "2700".to_string()),
            ]
        );
        assert_eq!(update.external_perp_pxs[0].1, "85010");
        assert_eq!(update.mark_pxs, vec![update.external_perp_pxs.clone()]);
    }

    #[test]
    fn serializes_to_the_documented_shape() {
        let update = set_oracle(
            "test",
            &[AssetPrices {
                coin: "test:BTC".into(),
                oracle: dec!(100000),
                external: dec!(100010),
            }],
            &BTreeMap::new(),
        );
        assert_eq!(
            serde_json::to_value(Action::PerpDeploy(PerpDeployAction::SetOracle(update))).unwrap(),
            serde_json::json!({
                "type": "perpDeploy",
                "setOracle": {
                    "dex": "test",
                    "oraclePxs": [["test:BTC", "100000"]],
                    "markPxs": [[["test:BTC", "100010"]]],
                    "externalPerpPxs": [["test:BTC", "100010"]]
                }
            })
        );
    }

    #[test]
    fn star_update_serializes_to_the_documented_shape() {
        let prices = [AssetPrices {
            coin: "test:BTC".into(),
            oracle: dec!(100000),
            external: dec!(100010),
        }];
        assert_eq!(
            serde_json::to_value(update("test", true, &prices, &BTreeMap::new())).unwrap(),
            serde_json::json!({
                "type": "perpDeploy",
                "star": {
                    "dex": "test",
                    "operation": {"setOracle": {"oraclePxs": [["test:BTC", "100000"]]}}
                }
            })
        );
    }
}
