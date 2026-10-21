//! HIP-4 shadow settlement: predict what each `binaryPrice` and `priceTouch` market settles
//! to, then check the prediction against the venue's actual settlement. Sends nothing.
//!
//! Mark and trade prices have no public history, so the keeper records them over the
//! WebSocket from before each market's window opens. Candle-based markets are computed from
//! `candleSnapshot` once the window closes. A touch market can settle any time before its
//! deadline, so it is watched from when it is first seen: its rolling TWAP is checked every
//! few seconds, and the prediction is Yes the moment it reaches the target.
//!
//! The templates say "TWAP" without saying how samples are weighted. Each prediction also
//! logs the plain mean of the samples and, for trades, the VWAP, and every recorded window's
//! raw samples go to a second log (`hip4-samples-*.jsonl`), so a disagreement with a venue
//! can be replayed under other definitions.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
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
    daily_log::DailyLog,
    hip4::{self, PriceKind, PriceMarket, Rule},
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
    market: PriceMarket,
    predicted: Option<(Decimal, bool)>,
    /// For a touch market: whether its TWAP started below the target, once known.
    from_below: Option<bool>,
    /// For a touch market: window ends up to here have been checked for a touch.
    checked_to: u64,
    /// For a touch market: when its TWAP first reached the target.
    touched_at: Option<u64>,
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
    /// `above` for `binaryPrice`, `touch` for `priceTouch`.
    rule: &'static str,
    /// The threshold, or a touch market's target.
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
    /// When a touch market's TWAP first reached its target.
    #[serde(skip_serializing_if = "Option::is_none")]
    touched_at: Option<String>,
}

/// One market's raw samples, as written to the samples log.
#[derive(Serialize)]
struct SamplesRow<'a> {
    outcome: u64,
    venue: Option<&'a str>,
    coin: &'a str,
    kind: String,
    window_start_ms: u64,
    window_end_ms: u64,
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
    let net = match network {
        Network::Mainnet => "mainnet",
        Network::Testnet => "testnet",
    };
    let mut log = DailyLog::open(&options.out_dir, format!("hip4-shadow-{net}"))?;
    let mut samples_log = DailyLog::open(&options.out_dir, format!("hip4-samples-{net}"))?;

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
        log = %log.path().display(),
        "HIP-4 shadow settlement: predicting binaryPrice and priceTouch markets and checking them against the venues; nothing is sent"
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

        // Follow markets whose window closes within the lookahead, and every touch market.
        if last_refresh.is_none_or(|at| at.elapsed() >= Duration::from_secs(60)) {
            last_refresh = Some(Instant::now());
            match client.outcome_meta().await {
                Ok(meta) => {
                    for outcome in meta.outcomes {
                        let id = u64::from(outcome.outcome);
                        if !outcome.name.starts_with("template:") || !seen.insert(id) {
                            continue;
                        }
                        let market = match PriceMarket::parse(
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
                        // before its window opened. A touch market is watched from now until
                        // its deadline instead.
                        let touch = matches!(market.rule, Rule::Touch(_));
                        let recordable = market.kind == PriceKind::Candle1m
                            || touch
                            || market.window_start_ms() > started_ms;
                        if market.time_ms + GRACE_MS < now
                            || (!touch && market.time_ms > now + lookahead_ms)
                            || !recordable
                        {
                            seen.remove(&id);
                            continue;
                        }
                        // A touch market that settled before hyperkeeper saw it can't be
                        // checked.
                        if touch {
                            match client.settled_outcome(id).await {
                                Ok(None) => {}
                                Ok(Some(_)) => continue,
                                Err(err) => {
                                    debug!(id, "settledOutcome failed: {err:#}");
                                    seen.remove(&id);
                                    continue;
                                }
                            }
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
                                from_below: None,
                                checked_to: now,
                                touched_at: None,
                            },
                        );
                    }
                }
                Err(err) => warn!("outcomeMeta failed: {err:#}"),
            }
        }

        // Watch each touch market for the first moment its TWAP reaches the target.
        for (id, t) in tracked.iter_mut() {
            let Rule::Touch(target) = t.market.rule else {
                continue;
            };
            if t.predicted.is_some() || t.touched_at.is_some() {
                continue;
            }
            let points: Vec<(u64, Decimal)> = samples
                .lock()
                .unwrap()
                .get(&(t.market.coin.clone(), t.market.kind))
                .map(|buffer| buffer.iter().map(|s| (s.at, s.price)).collect())
                .unwrap_or_default();
            let to = now.min(t.market.time_ms);
            let window = t.market.seconds * 1000;
            let Some(from_below) = t.from_below else {
                // The side of the target the first full window lands on.
                if let Some(twap) = hip4::step_twap(&points, to.saturating_sub(window), to) {
                    let side = if twap < target { "below" } else { "above" };
                    info!(id, coin = %t.market.coin, %target, %twap, "watching for a touch from {side}");
                    t.from_below = Some(twap < target);
                }
                t.checked_to = to;
                continue;
            };
            t.touched_at = hip4::first_touch(
                &points,
                t.market.seconds,
                target,
                from_below,
                t.checked_to,
                to,
            );
            if let Some(at) = t.touched_at {
                info!(id, coin = %t.market.coin, %target, touched_at = %rfc3339(at), "touched");
            }
            t.checked_to = to;
        }

        // Predict each market once its window has closed, or a touch market once it touched.
        let due: Vec<u64> = tracked
            .iter()
            .filter(|(_, t)| {
                t.predicted.is_none()
                    && (t.touched_at.is_some() || now >= t.market.time_ms + GRACE_MS)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            let market = tracked[&id].market.clone();
            let touched_at = tracked[&id].touched_at;
            // A touch market is judged on the window that touched, or else on its last one.
            let end = touched_at.unwrap_or(market.time_ms);
            let start = end.saturating_sub(market.seconds * 1000);
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
                    write_samples(&mut samples_log, &market, start, end, &recorded)?;
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
            let yes = match market.rule {
                Rule::Above(threshold) => hip4::resolves_yes(price, threshold),
                Rule::Touch(_) => touched_at.is_some(),
            };
            let margin = deviation_bps(price, market.level())
                .to_f64()
                .unwrap_or(f64::NAN);
            if matches!(market.rule, Rule::Above(_)) && margin.abs() < CLOSE_CALL_BPS {
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
                touched_at,
                ..Fields::default()
            };
            write(&mut log, &market, "predicted", fields)?;
            tracked.get_mut(&id).expect("tracked").predicted = Some((price, yes));
        }

        // Check predictions against the venues' settlements.
        if last_settle_check.elapsed() >= Duration::from_secs(30) {
            last_settle_check = Instant::now();
            // Touch markets are checked before any prediction too, to catch a touch the venue
            // saw and hyperkeeper didn't.
            let waiting: Vec<u64> = tracked
                .iter()
                .filter(|(_, t)| t.predicted.is_some() || matches!(t.market.rule, Rule::Touch(_)))
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
                let predicted = tracked_market.predicted;
                let touched_at = tracked_market.touched_at;
                // Lag counts from when the result was known: the expiry, or the touch.
                let known_at = touched_at.unwrap_or(market.time_ms);
                let Some(settled) = settled else {
                    if let Some((price, predicted_yes)) = predicted
                        && now > known_at + GIVE_UP_MS
                    {
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
                // These outcomes pay their first side, YES, in full or not at all.
                let actual_yes = settled.settle_fraction == Decimal::ONE;
                stats.settled += 1;
                let Some((price, predicted_yes)) = predicted else {
                    // Only a touch market gets here: the venue settled it before hyperkeeper
                    // saw its TWAP reach the target.
                    stats.mismatched.push(id);
                    let target = market.level();
                    warn!(id, coin = %market.coin, %target, actual_yes, "settled before hyperkeeper saw a touch");
                    let fields = Fields {
                        actual_yes: Some(actual_yes),
                        note: Some("settled before hyperkeeper saw a touch"),
                        ..Fields::default()
                    };
                    write(&mut log, &market, "settled", fields)?;
                    tracked.remove(&id);
                    continue;
                };
                let lag_s = (now.saturating_sub(known_at)) as f64 / 1000.0;
                if actual_yes == predicted_yes {
                    stats.matched += 1;
                } else {
                    stats.mismatched.push(id);
                    let level = market.level();
                    warn!(id, coin = %market.coin, %price, %level, predicted_yes, actual_yes, "MISMATCH");
                }
                let mut bucket = market.venue.clone().unwrap_or_else(|| "protocol".into());
                if matches!(market.rule, Rule::Touch(_)) {
                    bucket.push_str(" touch");
                }
                stats.settle_lag.entry(bucket).or_default().push(lag_s);
                let fields = Fields {
                    price: Some(price),
                    predicted_yes: Some(predicted_yes),
                    actual_yes: Some(actual_yes),
                    settle_lag_s: Some(lag_s),
                    touched_at,
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
    info!(log = %log.path().display(), "HIP-4 shadow settlement stopped");
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
    touched_at: Option<u64>,
}

/// Milliseconds since the epoch as an RFC 3339 time.
fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

fn write(log: &mut DailyLog, market: &PriceMarket, event: &str, f: Fields) -> anyhow::Result<()> {
    let px = |p: Decimal| p.round_dp(6).normalize().to_string();
    let row = Row {
        time: chrono::Utc::now().to_rfc3339(),
        event,
        outcome: market.outcome,
        venue: market.venue.as_deref(),
        coin: &market.coin,
        kind: format!("{:?}", market.kind),
        rule: match market.rule {
            Rule::Above(_) => "above",
            Rule::Touch(_) => "touch",
        },
        threshold: market.level().normalize().to_string(),
        expiry: rfc3339(market.time_ms),
        price: f.price.map(px),
        predicted_yes: f.predicted_yes,
        margin_bps: f.margin_bps.map(|m| (m * 100.0).round() / 100.0),
        actual_yes: f.actual_yes,
        settle_lag_s: f.settle_lag_s,
        note: f.note.map(str::to_string),
        mean_price: f.mean_price.map(px),
        vwap_price: f.vwap_price.map(px),
        touched_at: f.touched_at.map(rfc3339),
    };
    log.write(&row)
}

/// Saves the samples a prediction was made from, from shortly before its window opened to
/// now.
fn write_samples(
    log: &mut DailyLog,
    market: &PriceMarket,
    start: u64,
    end: u64,
    recorded: &[Sample],
) -> anyhow::Result<()> {
    let from = start.saturating_sub(SAMPLE_MARGIN_MS);
    // Keep the last sample before `from` too, so the price at that moment is known.
    let first = recorded.iter().rposition(|s| s.at <= from).unwrap_or(0);
    let row = SamplesRow {
        outcome: market.outcome,
        venue: market.venue.as_deref(),
        coin: &market.coin,
        kind: format!("{:?}", market.kind),
        window_start_ms: start,
        window_end_ms: end,
        samples: recorded[first..]
            .iter()
            .map(|s| {
                let size = s.size.map(|z| z.normalize().to_string());
                (s.at, s.price.normalize().to_string(), size)
            })
            .collect(),
    };
    log.write(&row)
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
