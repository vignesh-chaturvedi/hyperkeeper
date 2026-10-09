//! HIP-4 shadow settlement: predict what each `binaryPrice` market settles to, then check
//! the prediction against the venue's actual settlement. Sends nothing.
//!
//! Mark and trade prices have no public history, so the keeper records them over the
//! WebSocket from before each market's window opens. Candle-based markets are computed from
//! `candleSnapshot` once the window closes.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use futures::StreamExt;
use hypersdk::hypercore::{
    self, CandleInterval, HttpClient,
    types::{Incoming, Subscription},
    ws::Event,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
use serde_json::Value;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::{
    aggregate::deviation_bps,
    config::Network,
    hip4::{self, BinaryPrice, PriceKind},
};

pub struct Options {
    pub network: Network,
    /// Start following a market this long before its window closes. It must exceed the
    /// longest window (300 s) so recording starts before the window opens.
    pub lookahead_minutes: u64,
    /// Stop after this many minutes; run until interrupted when `None`.
    pub minutes: Option<u64>,
    pub out_dir: PathBuf,
}

/// Recorded prices per (coin, kind): `(ms since epoch, price)`, the last 15 minutes.
type Samples = Arc<Mutex<HashMap<(String, PriceKind), VecDeque<(u64, Decimal)>>>>;
const KEEP_MS: u64 = 15 * 60_000;
/// How long after a window closes to predict, so late samples and the last candle arrive.
const GRACE_MS: u64 = 10_000;
/// Give up on a settlement this long after the window closes.
const GIVE_UP_MS: u64 = 24 * 3_600_000;
/// Predictions closer to the threshold than this are reported as close calls.
const CLOSE_CALL_BPS: f64 = 2.0;

struct Tracked {
    market: BinaryPrice,
    predicted: Option<(Decimal, bool)>,
}

#[derive(Default)]
struct Stats {
    predicted: usize,
    unpredictable: usize,
    settled: usize,
    matched: usize,
    mismatched: Vec<u64>,
    close_calls: usize,
    gave_up: usize,
    /// Seconds from window close to first seeing the settlement, per venue.
    settle_lag: BTreeMap<String, Vec<f64>>,
}

#[derive(Serialize)]
struct Row<'a> {
    time: String,
    event: &'a str,
    outcome: u64,
    venue: Option<&'a str>,
    coin: &'a str,
    kind: String,
    threshold: String,
    expiry: String,
    price: Option<String>,
    predicted_yes: Option<bool>,
    margin_bps: Option<f64>,
    actual_yes: Option<bool>,
    settle_lag_s: Option<f64>,
    note: Option<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn info_url(network: Network) -> &'static str {
    match network {
        Network::Mainnet => "https://api.hyperliquid.xyz/info",
        Network::Testnet => "https://api.hyperliquid-testnet.xyz/info",
    }
}

pub async fn run(options: Options) -> anyhow::Result<()> {
    let network = options.network;
    let client = HttpClient::new(network.chain());
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(crate::USER_AGENT)
        .build()?;
    let post = |body: Value| {
        let http = http.clone();
        async move {
            let response = http.post(info_url(network)).json(&body).send().await?;
            anyhow::Ok(response.error_for_status()?.json::<Value>().await?)
        }
    };

    fs::create_dir_all(&options.out_dir)?;
    let log_path = options.out_dir.join(format!(
        "hip4-shadow-{}-{}.jsonl",
        match network {
            Network::Mainnet => "mainnet",
            Network::Testnet => "testnet",
        },
        chrono::Utc::now().format("%Y%m%d-%H%M")
    ));
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let samples: Samples = Arc::default();
    let ws = match network {
        Network::Mainnet => hypercore::mainnet_ws(),
        Network::Testnet => hypercore::testnet_ws(),
    };
    let (handle, mut stream) = ws.split();
    let recorder = samples.clone();
    tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            let Event::Message(message) = event else {
                continue;
            };
            let now = now_ms();
            let mut map = recorder.lock().unwrap();
            let mut push = |coin: String, kind: PriceKind, at: u64, price: Decimal| {
                let buffer = map.entry((coin, kind)).or_default();
                buffer.push_back((at, price));
                while buffer.front().is_some_and(|(t, _)| *t + KEEP_MS < now) {
                    buffer.pop_front();
                }
            };
            match message {
                Incoming::ActiveAssetCtx { coin, ctx } => {
                    if let Some(mark) = ctx.mark_px {
                        push(coin, PriceKind::Mark, now, mark);
                    }
                }
                Incoming::Trades(trades) => {
                    for trade in trades {
                        push(trade.coin, PriceKind::Trade, trade.time, trade.px);
                    }
                }
                _ => {}
            }
        }
    });

    info!(
        ?network,
        log = %log_path.display(),
        "HIP-4 shadow settlement: predicting binaryPrice markets and checking them against the venues; nothing is sent"
    );

    let mut tracked: BTreeMap<u64, Tracked> = BTreeMap::new();
    let mut subscribed: HashSet<(String, PriceKind)> = HashSet::new();
    let mut seen: HashSet<u64> = HashSet::new();
    let mut stats = Stats::default();
    let lookahead_ms = options.lookahead_minutes * 60_000;
    let started_ms = now_ms();
    let deadline = options
        .minutes
        .map(|m| Instant::now() + Duration::from_secs(m * 60));

    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_refresh: Option<Instant> = None;
    let mut last_settle_check = Instant::now();
    let mut last_summary = Instant::now();

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        let now = now_ms();

        // Follow markets whose window closes within the lookahead.
        if last_refresh.is_none_or(|at| at.elapsed() >= Duration::from_secs(60)) {
            last_refresh = Some(Instant::now());
            match post(serde_json::json!({"type": "outcomeMeta"})).await {
                Ok(meta) => {
                    for outcome in meta["outcomes"].as_array().into_iter().flatten() {
                        let (Some(id), Some(name), Some(description)) = (
                            outcome["outcome"].as_u64(),
                            outcome["name"].as_str(),
                            outcome["description"].as_str(),
                        ) else {
                            continue;
                        };
                        if name != "template:binaryPrice" || !seen.insert(id) {
                            continue;
                        }
                        let venue = outcome["venue"].as_str().map(str::to_string);
                        let market = match BinaryPrice::parse(id, name, description, venue) {
                            Ok(market) => market,
                            Err(err) => {
                                debug!(id, "skipping: {err:#}");
                                continue;
                            }
                        };
                        // A live-recorded market is only predictable if recording started
                        // before its window opened.
                        let recordable = market.kind == PriceKind::Candle1m
                            || market.window_start_ms() > started_ms;
                        if market.time_ms + GRACE_MS < now
                            || market.time_ms > now + lookahead_ms
                            || !recordable
                        {
                            seen.remove(&id);
                            continue;
                        }
                        if market.kind != PriceKind::Candle1m {
                            let key = (market.coin.clone(), market.kind);
                            if subscribed.insert(key) {
                                let coin = market.coin.clone();
                                handle.subscribe(match market.kind {
                                    PriceKind::Mark => Subscription::ActiveAssetCtx { coin },
                                    _ => Subscription::Trades { coin },
                                });
                            }
                        }
                        tracked.insert(
                            id,
                            Tracked {
                                market,
                                predicted: None,
                            },
                        );
                    }
                }
                Err(err) => warn!("outcomeMeta failed: {err:#}"),
            }
        }

        // Predict each market once its window has closed.
        let due: Vec<u64> = tracked
            .iter()
            .filter(|(_, t)| t.predicted.is_none() && now >= t.market.time_ms + GRACE_MS)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            let market = tracked[&id].market.clone();
            let price = match market.kind {
                PriceKind::Candle1m => client
                    .candle_snapshot(
                        market.coin.clone(),
                        CandleInterval::OneMinute,
                        market.window_start_ms(),
                        market.time_ms,
                    )
                    .await
                    .ok()
                    .and_then(|candles| {
                        let closes: Vec<(u64, Decimal)> =
                            candles.iter().map(|c| (c.open_time, c.close)).collect();
                        hip4::candle_twap(&closes, market.window_start_ms(), market.time_ms)
                    }),
                kind => {
                    let map = samples.lock().unwrap();
                    map.get(&(market.coin.clone(), kind)).and_then(|buffer| {
                        let buffer: Vec<(u64, Decimal)> = buffer.iter().copied().collect();
                        hip4::step_twap(&buffer, market.window_start_ms(), market.time_ms)
                    })
                }
            };
            let Some(price) = price else {
                stats.unpredictable += 1;
                let note = Some("no coverage of the window");
                write(
                    &mut log,
                    &market,
                    "unpredictable",
                    Fields {
                        note,
                        ..Fields::default()
                    },
                )?;
                tracked.remove(&id);
                continue;
            };
            let yes = hip4::resolves_yes(price, market.threshold);
            let margin = deviation_bps(price, market.threshold)
                .to_f64()
                .unwrap_or(f64::NAN);
            if margin.abs() < CLOSE_CALL_BPS {
                stats.close_calls += 1;
            }
            stats.predicted += 1;
            debug!(id, coin = %market.coin, %price, yes, "predicted");
            let fields = Fields {
                price: Some(price),
                predicted_yes: Some(yes),
                margin_bps: Some(margin),
                ..Fields::default()
            };
            write(&mut log, &market, "predicted", fields)?;
            tracked.get_mut(&id).expect("tracked").predicted = Some((price, yes));
        }

        // Check predictions against the venues' settlements.
        if last_settle_check.elapsed() >= Duration::from_secs(30) {
            last_settle_check = Instant::now();
            let waiting: Vec<u64> = tracked
                .iter()
                .filter(|(_, t)| t.predicted.is_some())
                .map(|(id, _)| *id)
                .collect();
            for id in waiting {
                let reply = match post(serde_json::json!({"type": "settledOutcome", "outcome": id}))
                    .await
                {
                    Ok(reply) => reply,
                    Err(err) => {
                        debug!(id, "settledOutcome failed: {err:#}");
                        continue;
                    }
                };
                let tracked_market = &tracked[&id];
                let market = tracked_market.market.clone();
                let (price, predicted_yes) = tracked_market.predicted.expect("predicted");
                if reply.is_null() {
                    if now > market.time_ms + GIVE_UP_MS {
                        stats.gave_up += 1;
                        let fields = Fields {
                            price: Some(price),
                            predicted_yes: Some(predicted_yes),
                            note: Some("not settled within 24 h"),
                            ..Fields::default()
                        };
                        write(&mut log, &market, "unsettled", fields)?;
                        tracked.remove(&id);
                    }
                    continue;
                }
                let fraction: Decimal = reply["settleFraction"]
                    .as_str()
                    .context("settledOutcome without settleFraction")?
                    .parse()?;
                let actual_yes = fraction == Decimal::ONE;
                let lag_s = (now.saturating_sub(market.time_ms)) as f64 / 1000.0;
                stats.settled += 1;
                if actual_yes == predicted_yes {
                    stats.matched += 1;
                } else {
                    stats.mismatched.push(id);
                    warn!(id, coin = %market.coin, %price, threshold = %market.threshold, predicted_yes, actual_yes, "MISMATCH");
                }
                stats
                    .settle_lag
                    .entry(market.venue.clone().unwrap_or_else(|| "protocol".into()))
                    .or_default()
                    .push(lag_s);
                let fields = Fields {
                    price: Some(price),
                    predicted_yes: Some(predicted_yes),
                    actual_yes: Some(actual_yes),
                    settle_lag_s: Some(lag_s),
                    ..Fields::default()
                };
                write(&mut log, &market, "settled", fields)?;
                tracked.remove(&id);
            }
        }

        if last_summary.elapsed() >= Duration::from_secs(600) {
            last_summary = Instant::now();
            summarize(&stats, tracked.len());
        }
    }

    summarize(&stats, tracked.len());
    info!(log = %log_path.display(), "HIP-4 shadow settlement stopped");
    Ok(())
}

/// What one log row says beyond the market itself.
#[derive(Default)]
struct Fields<'a> {
    price: Option<Decimal>,
    predicted_yes: Option<bool>,
    margin_bps: Option<f64>,
    actual_yes: Option<bool>,
    settle_lag_s: Option<f64>,
    note: Option<&'a str>,
}

fn write(log: &mut fs::File, market: &BinaryPrice, event: &str, f: Fields) -> anyhow::Result<()> {
    let row = Row {
        time: chrono::Utc::now().to_rfc3339(),
        event,
        outcome: market.outcome,
        venue: market.venue.as_deref(),
        coin: &market.coin,
        kind: format!("{:?}", market.kind),
        threshold: market.threshold.normalize().to_string(),
        expiry: chrono::DateTime::from_timestamp_millis(market.time_ms as i64)
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        price: f.price.map(|p| p.round_dp(6).normalize().to_string()),
        predicted_yes: f.predicted_yes,
        margin_bps: f.margin_bps.map(|m| (m * 100.0).round() / 100.0),
        actual_yes: f.actual_yes,
        settle_lag_s: f.settle_lag_s,
        note: f.note.map(str::to_string),
    };
    serde_json::to_writer(&mut *log, &row)?;
    log.write_all(b"\n")?;
    Ok(())
}

fn summarize(stats: &Stats, following: usize) {
    let lags = stats
        .settle_lag
        .iter()
        .map(|(venue, lags)| {
            let mut lags = lags.clone();
            lags.sort_by(f64::total_cmp);
            format!(
                "{venue} p50 {:.0} s (n={})",
                lags[lags.len() / 2],
                lags.len()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    info!(
        "--- following {following} | predicted {} | settled {} | matched {} | mismatched {:?} | close calls {} | unpredictable {} | gave up {} | settle lag: {} ---",
        stats.predicted,
        stats.settled,
        stats.matched,
        stats.mismatched,
        stats.close_calls,
        stats.unpredictable,
        stats.gave_up,
        if lags.is_empty() {
            "-".to_string()
        } else {
            lags
        }
    );
}
