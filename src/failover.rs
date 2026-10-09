//! Failover between two keeper instances, each with its own key granted `setOracle`.
//!
//! The primary sends every interval. The standby asks Hyperliquid's explorer for the
//! primary key's latest successful oracle update on our DEX and sends only while that is
//! older than `takeover_after_ms`. Looking at the primary's own transactions works when
//! prices are flat, where watching the oracle for changes would mistake a quiet market for
//! a dead primary, and needs no link between the two hosts. If the explorer stops
//! answering, the standby falls back to watching the DEX's oracle.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use alloy::primitives::Address;
use serde_json::Value;
use tracing::debug;

use crate::config::{Network, Role};

/// What the standby last learned about its peer.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerView {
    /// Exchange time (ms) of the peer's latest successful oracle update for our DEX, among
    /// its most recent transactions.
    pub last_update_ms: Option<u64>,
    /// When the explorer last answered.
    pub polled_at: Option<Instant>,
    /// The same moment as wall-clock ms, to measure the peer's silence against.
    pub polled_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Send,
    Hold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Primary,
    /// The peer updated this long ago, within the takeover window.
    PeerActive {
        age_ms: u64,
    },
    /// The peer's last update is this old, or none was found.
    PeerSilent {
        age_ms: Option<u64>,
    },
    /// No explorer answer recently; the DEX's oracle changed this long ago.
    OracleMoving {
        quiet_ms: u64,
    },
    OracleQuiet {
        quiet_ms: u64,
    },
    /// No explorer answer and no oracle observation yet.
    Unknown,
}

/// Whether to send this interval.
///
/// The peer's silence is measured as of the explorer's last answer, not now: between
/// polls the peer may well have sent, so measuring against the current time would add up
/// to a poll interval of imagined silence and make the standby flap.
///
/// `explorer_fresh` is whether the explorer answered recently enough to trust `view`.
/// `oracle_quiet_ms` is how long ago the DEX's oracle last changed, if seen.
pub fn decide(
    role: Role,
    view: PeerView,
    explorer_fresh: bool,
    oracle_quiet_ms: Option<u64>,
    takeover_after_ms: u64,
) -> (Decision, Reason) {
    if role == Role::Primary {
        return (Decision::Send, Reason::Primary);
    }
    if let (true, Some(polled_ms)) = (explorer_fresh, view.polled_at_ms) {
        return match view.last_update_ms {
            Some(last) => {
                let age_ms = polled_ms.saturating_sub(last);
                if age_ms >= takeover_after_ms {
                    (
                        Decision::Send,
                        Reason::PeerSilent {
                            age_ms: Some(age_ms),
                        },
                    )
                } else {
                    (Decision::Hold, Reason::PeerActive { age_ms })
                }
            }
            None => (Decision::Send, Reason::PeerSilent { age_ms: None }),
        };
    }
    match oracle_quiet_ms {
        Some(quiet_ms) if quiet_ms >= takeover_after_ms => {
            (Decision::Send, Reason::OracleQuiet { quiet_ms })
        }
        Some(quiet_ms) => (Decision::Hold, Reason::OracleMoving { quiet_ms }),
        None => (Decision::Hold, Reason::Unknown),
    }
}

/// The time (ms) of the newest successful oracle update for `dex` in an explorer
/// `userDetails` reply, which lists transactions newest first.
pub fn last_oracle_update(reply: &Value, dex: &str, star: bool) -> Option<u64> {
    reply["txs"].as_array()?.iter().find_map(|tx| {
        if !tx["error"].is_null() {
            return None;
        }
        let action = &tx["action"];
        if action["type"] != "perpDeploy" {
            return None;
        }
        let ours = if star {
            action["star"]["dex"] == dex && action["star"]["operation"].get("setOracle").is_some()
        } else {
            action["setOracle"]["dex"] == dex
        };
        if ours { tx["time"].as_u64() } else { None }
    })
}

fn explorer_url(network: Network) -> &'static str {
    match network {
        Network::Mainnet => "https://rpc.hyperliquid.xyz/explorer",
        Network::Testnet => "https://rpc.hyperliquid-testnet.xyz/explorer",
    }
}

/// Polls the explorer for the peer's updates in the background.
pub fn watch_peer(
    network: Network,
    peer: Address,
    dex: String,
    star: bool,
    poll: Duration,
) -> Arc<Mutex<PeerView>> {
    let view = Arc::new(Mutex::new(PeerView::default()));
    let shared = view.clone();
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .user_agent(crate::USER_AGENT)
            .build()
            .expect("HTTP client");
        let mut interval = tokio::time::interval(poll);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let reply = http
                .post(explorer_url(network))
                .json(&serde_json::json!({"type": "userDetails", "user": format!("{peer:#x}")}))
                .send()
                .await;
            let body: Result<Value, _> = match reply.and_then(|r| r.error_for_status()) {
                Ok(response) => response.json().await,
                Err(err) => Err(err),
            };
            match body {
                Ok(body) => {
                    let mut view = shared.lock().unwrap();
                    view.polled_at = Some(Instant::now());
                    view.polled_at_ms = Some(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.as_millis() as u64),
                    );
                    // Keep the last known update when it has scrolled out of the newest
                    // transactions, rather than reading that as silence.
                    if let Some(last) = last_oracle_update(&body, &dex, star) {
                        view.last_update_ms = Some(last);
                    }
                }
                Err(err) => debug!("explorer poll failed: {err:#}"),
            }
        }
    });
    view
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TAKEOVER: u64 = 10_000;

    fn view(polled_ms: u64, last: Option<u64>) -> PeerView {
        PeerView {
            last_update_ms: last,
            polled_at: Some(Instant::now()),
            polled_at_ms: Some(polled_ms),
        }
    }

    fn standby(view: PeerView, fresh: bool, quiet: Option<u64>) -> (Decision, Reason) {
        decide(Role::Standby, view, fresh, quiet, TAKEOVER)
    }

    #[test]
    fn the_primary_always_sends() {
        let primary = decide(Role::Primary, PeerView::default(), false, None, TAKEOVER);
        assert_eq!(primary.0, Decision::Send);
    }

    #[test]
    fn the_standby_holds_while_the_peer_updates_and_takes_over_when_it_stops() {
        let polled = 100_000;
        let active = Reason::PeerActive { age_ms: 3_000 };
        let silent = Reason::PeerSilent {
            age_ms: Some(10_000),
        };
        assert_eq!(
            standby(view(polled, Some(polled - 3_000)), true, None),
            (Decision::Hold, active)
        );
        assert_eq!(
            standby(view(polled, Some(polled - 10_000)), true, None),
            (Decision::Send, silent)
        );
        // A peer that has never sent leaves the standby in charge.
        let never = Reason::PeerSilent { age_ms: None };
        assert_eq!(
            standby(view(polled, None), true, None),
            (Decision::Send, never)
        );
    }

    #[test]
    fn silence_is_measured_at_the_poll_not_now() {
        // The peer updated 6 s before the explorer answered. However long ago that answer
        // was, the standby only knows the peer was silent for 6 s, so it holds.
        let held = (Decision::Hold, Reason::PeerActive { age_ms: 6_000 });
        assert_eq!(standby(view(50_000, Some(44_000)), true, None), held);
    }

    #[test]
    fn without_the_explorer_the_standby_watches_the_oracle() {
        let none = PeerView::default();
        let moving = (Decision::Hold, Reason::OracleMoving { quiet_ms: 4_000 });
        let quiet = (Decision::Send, Reason::OracleQuiet { quiet_ms: 12_000 });
        assert_eq!(standby(none, false, Some(4_000)), moving);
        assert_eq!(standby(none, false, Some(12_000)), quiet);
        assert_eq!(
            standby(none, false, None),
            (Decision::Hold, Reason::Unknown)
        );
        // A stale explorer answer is ignored in favour of the oracle.
        assert_eq!(standby(view(1, Some(0)), false, Some(4_000)), moving);
    }

    #[test]
    fn finds_the_newest_successful_update_for_our_dex() {
        let reply = json!({"txs": [
            {"time": 5, "error": "Invalid perp deployer or sub-deployer",
             "action": {"type": "perpDeploy", "star": {"dex": "ignp", "operation": {"setOracle": {}}}}},
            {"time": 4, "error": null, "action": {"type": "order"}},
            {"time": 3, "error": null,
             "action": {"type": "perpDeploy", "star": {"dex": "other", "operation": {"setOracle": {}}}}},
            {"time": 2, "error": null,
             "action": {"type": "perpDeploy", "star": {"dex": "ignp", "operation": {"setOracle": {}}}}},
            {"time": 1, "error": null, "action": {"type": "perpDeploy", "setOracle": {"dex": "ignp"}}}
        ]});
        assert_eq!(last_oracle_update(&reply, "ignp", true), Some(2));
        assert_eq!(last_oracle_update(&reply, "ignp", false), Some(1));
        assert_eq!(last_oracle_update(&reply, "xyz", false), None);
        assert_eq!(last_oracle_update(&json!({}), "ignp", true), None);
    }
}
