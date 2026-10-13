//! HIP-4 shadow settlement: predict what each `binaryPrice` market settles to, then check
//! the prediction against the venue's actual settlement. Sends nothing.
//!
//! Mark and trade prices have no public history, so the keeper records them over the
//! WebSocket from before each market's window opens. Candle-based markets are computed from
//! `candleSnapshot` once the window closes.
//!
//! The templates say "TWAP" without saying how samples are weighted. Each prediction also
//! logs the plain mean of the samples and, for trades, the VWAP, and every recorded window's
//! raw samples go to a second log (`hip4-samples-*.jsonl`), so a disagreement with a venue
//! can be replayed under other definitions.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use hypersdk::hypercore::{
    self, CandleInterval, HttpClient,
    types::{Incoming, Subscription},
    ws::Event,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
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

/// One recorded price: when it happened (ms since epoch), the price, and a trade's size.
#[derive(Clone, Copy)]
struct Sample {
    at: u64,
    price: Decimal,
    size: Option<Decimal>,
}

/// Recorded prices per (coin, kind), the last 15 minutes.
type Samples = Arc<Mutex<HashMap<(String, PriceKind), VecDeque<Sample>>>>;
const KEEP_MS: u64 = 15 * 60_000;
/// Raw samples are saved from this long before a window opens, so other window boundaries
/// can be tested later.
const SAMPLE_MARGIN_MS: u64 = 30_000;
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
    /// Plain mean of the window's samples, an alternative reading of "TWAP".
    #[serde(skip_serializing_if = "Option::is_none")]
    mean_price: Option<String>,
    /// Volume-weighted average of the window's trades.
    #[serde(skip_serializing_if = "Option::is_none")]
    vwap_price: Option<String>,
}

/// One market's raw samples, as written to the samples log.
#[derive(Serialize)]
struct SamplesRow<'a> {
    outcome: u64,
    venue: Option<&'a str>,
    coin: &'a str,
    kind: String,
    window_start_ms: u64,
    expiry_ms: u64,
    /// `[ms since epoch, price, size]`, with no size for marks.
    samples: Vec<(u64, String, Option<String>)>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

pub async fn run(options: Options) -> anyhow::Result<()> {
    let network = options.network;
    let client = HttpClient::new(network.chain());

    fs::create_dir_all(&options.out_dir)?;
    let stem = format!(
        "{}-{}",
        match network {
            Network::Mainnet => "mainnet",
            Network::Testnet => "testnet",
        },
        chrono::Utc::now().format("%Y%m%d-%H%M")
    );
    let log_path = options.out_dir.join(format!("hip4-shadow-{stem}.jsonl"));
    let open = |path: &PathBuf| OpenOptions::new().create(true).append(true).open(path);
    let mut log = open(&log_path)?;
    let mut samples_log = open(&options.out_dir.join(format!("hip4-samples-{stem}.jsonl")))?;

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
            let mut push = |coin: String, kind: PriceKind, sample: Sample| {
                let buffer = map.entry((coin, kind)).or_default();
                buffer.push_back(sample);
                while buffer.front().is_some_and(|s| s.at + KEEP_MS < now) {
                    buffer.pop_front();
                }
            };
            match message {
                Incoming::ActiveAssetCtx { coin, ctx } => {
                    if let Some(mark) = ctx.mark_px {
                        let sample = Sample {
                            at: now,
                            price: mark,
                            size: None,
                        };
                        push(coin, PriceKind::Mark, sample);
                    }
                }
                Incoming::Trades(trades) => {
                    for trade in trades {
                        let sample = Sample {
                            at: trade.time,
                            price: trade.px,
                            size: Some(trade.sz),
                        };
                        push(trade.coin, PriceKind::Trade, sample);
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
            match client.outcome_meta().await {
                Ok(meta) => {
                    for outcome in meta.outcomes {
                        let id = u64::from(outcome.outcome);
                        if outcome.name != "template:binaryPrice" || !seen.insert(id) {
                            continue;
                        }
                        let market = match BinaryPrice::parse(
                            id,
                            &outcome.name,
                            &outcome.description,
                            outcome.venue,
                        ) {
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
            let (start, end) = (market.window_start_ms(), market.time_ms);
            let (mut mean_price, mut vwap_price) = (None, None);
            let price = match market.kind {
                PriceKind::Candle1m => client
                    .candle_snapshot(market.coin.clone(), CandleInterval::OneMinute, start, end)
                    .await
                    .ok()
                    .and_then(|candles| {
                        let closes: Vec<(u64, Decimal)> =
                            candles.iter().map(|c| (c.open_time, c.close)).collect();
                        hip4::candle_twap(&closes, start, end)
                    }),
                kind => {
                    let recorded: Vec<Sample> = samples
                        .lock()
                        .unwrap()
                        .get(&(market.coin.clone(), kind))
                        .map(|buffer| buffer.iter().copied().collect())
                        .unwrap_or_default();
                    write_samples(&mut samples_log, &market, &recorded)?;
                    let points: Vec<(u64, Decimal)> =
                        recorded.iter().map(|s| (s.at, s.price)).collect();
                    let trades: Vec<(u64, Decimal, Decimal)> = recorded
                        .iter()
                        .filter_map(|s| Some((s.at, s.price, s.size?)))
                        .collect();
                    mean_price = hip4::sample_mean(&points, start, end);
                    vwap_price = hip4::vwap(&trades, start, end);
                    hip4::step_twap(&points, start, end)
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
                mean_price,
                vwap_price,
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
                let settled = match client.settled_outcome(id).await {
                    Ok(settled) => settled,
                    Err(err) => {
                        debug!(id, "settledOutcome failed: {err:#}");
                        continue;
                    }
                };
                let tracked_market = &tracked[&id];
                let market = tracked_market.market.clone();
                let (price, predicted_yes) = tracked_market.predicted.expect("predicted");
                let Some(settled) = settled else {
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
                };
                // A binaryPrice outcome pays its first side, YES, in full or not at all.
                let actual_yes = settled.settle_fraction == Decimal::ONE;
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
    mean_price: Option<Decimal>,
    vwap_price: Option<Decimal>,
}

fn write(log: &mut fs::File, market: &BinaryPrice, event: &str, f: Fields) -> anyhow::Result<()> {
    let px = |p: Decimal| p.round_dp(6).normalize().to_string();
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
        price: f.price.map(px),
        predicted_yes: f.predicted_yes,
        margin_bps: f.margin_bps.map(|m| (m * 100.0).round() / 100.0),
        actual_yes: f.actual_yes,
        settle_lag_s: f.settle_lag_s,
        note: f.note.map(str::to_string),
        mean_price: f.mean_price.map(px),
        vwap_price: f.vwap_price.map(px),
    };
    serde_json::to_writer(&mut *log, &row)?;
    log.write_all(b"\n")?;
    Ok(())
}

/// Saves the samples a prediction was made from, from shortly before its window opened to
/// now.
fn write_samples(
    log: &mut fs::File,
    market: &BinaryPrice,
    recorded: &[Sample],
) -> anyhow::Result<()> {
    let from = market.window_start_ms().saturating_sub(SAMPLE_MARGIN_MS);
    // Keep the last sample before `from` too, so the price at that moment is known.
    let first = recorded.iter().rposition(|s| s.at <= from).unwrap_or(0);
    let row = SamplesRow {
        outcome: market.outcome,
        venue: market.venue.as_deref(),
        coin: &market.coin,
        kind: format!("{:?}", market.kind),
        window_start_ms: market.window_start_ms(),
        expiry_ms: market.time_ms,
        samples: recorded[first..]
            .iter()
            .map(|s| {
                let size = s.size.map(|z| z.normalize().to_string());
                (s.at, s.price.normalize().to_string(), size)
            })
            .collect(),
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
