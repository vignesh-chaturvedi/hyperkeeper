//! Shadow mode: price a live DEX, build and sign the update the keeper would send, and
//! compare it with the oracle the DEX's deployer actually pushes. Nothing can take effect:
//! the one update sent, at startup, is signed by a throwaway key with no permissions.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

use hypersdk::hypercore::HttpClient;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    aggregate::deviation_bps,
    config::Config,
    dex::{self, Watch},
    guard::{Guard, Verdict},
    oracle::{self, AssetPrices},
    pricer::{Priced, Pricer},
};

/// Deviation samples kept per asset for the summary: 24 hours of 3 s ticks. Counters
/// stay exact; percentiles cover this rolling window, so memory stays flat on long runs.
const WINDOW: usize = 28_800;

pub struct Options {
    /// Stop after this many minutes; run until interrupted when `None`.
    pub minutes: Option<u64>,
    /// Directory for the per-tick JSONL log.
    pub out_dir: PathBuf,
    /// Write a log row every this many ticks. The summary statistics still count every
    /// tick.
    pub log_every: usize,
}

#[derive(Default)]
struct CoinStats {
    ticks: usize,
    /// Absolute deviation from their oracle in basis points, over the last `WINDOW` ticks.
    deviations: VecDeque<f64>,
    sources_used: usize,
    unpriced: usize,
    clamped: usize,
    outside_band: usize,
}

#[derive(Serialize)]
struct Row<'a> {
    time: String,
    coin: &'a str,
    /// Aggregated spot price, before the guard.
    oracle: String,
    /// Price the guard would send; absent when it refused the price.
    sent: Option<String>,
    guard: &'static str,
    external: String,
    theirs: Option<String>,
    deviation_bps: Option<f64>,
    /// USD per USDT this round used to convert quotes, if any.
    usdt_usd: Option<String>,
    sources: Vec<&'static str>,
    outliers: Vec<&'static str>,
    /// Every spot quote this tick, after conversion, by source.
    quotes: BTreeMap<&'static str, String>,
}

/// Fetches every source once and prints what each one says about each asset.
pub async fn check(config: &Config) -> anyhow::Result<()> {
    let pricer = Pricer::new(config)?;
    match pricer.usdt_usd_once().await {
        Ok(rate) => println!("{:<20} {rate} (USD per USDT)", "usdt-usd"),
        Err(err) => println!("{:<20} ERROR {err:#}", "usdt-usd"),
    }
    println!(
        "{:<20} {:?}; venue prices below are as quoted",
        "convention", config.sources.convention
    );
    for (kind, result) in pricer.fetch_once().await {
        match result {
            Ok(prices) => println!(
                "{:<20} {}",
                kind.name(),
                prices
                    .iter()
                    .map(|(s, p)| format!("{s} {p}"))
                    .collect::<Vec<_>>()
                    .join("  ")
            ),
            Err(err) => println!("{:<20} ERROR {err:#}", kind.name()),
        }
    }
    Ok(())
}

pub async fn run(config: Config, options: Options) -> anyhow::Result<()> {
    let chain = config.network.chain();
    let client = HttpClient::new(chain);
    let sz_decimals = dex::sz_decimals(&client, &config).await?;
    let mut pricer = Pricer::new(&config)?;
    let mut guard = Guard::new(config.guards.max_move_bps);

    fs::create_dir_all(&options.out_dir)?;
    let log_path = options.out_dir.join(format!(
        "shadow-{}-{}.jsonl",
        config.dex,
        chrono::Utc::now().format("%Y%m%d-%H%M")
    ));
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let watch = dex::watch(&config);

    info!(
        dex = %config.dex,
        network = ?config.network,
        assets = config.assets.len(),
        log = %log_path.display(),
        "shadow mode: pricing the DEX every {} ms; nothing will be sent that can take effect",
        config.interval_ms
    );

    let mut stats: BTreeMap<String, CoinStats> = BTreeMap::new();
    let mut ticks = 0usize;
    let mut unsent = 0usize;
    let mut unsent_run = 0u32;
    let mut alarms = 0usize;
    let mut probed = false;
    let mut last_unpriced: Vec<String> = Vec::new();

    let deadline = options
        .minutes
        .map(|m| Instant::now() + Duration::from_secs(m * 60));
    let mut interval = tokio::time::interval(Duration::from_millis(config.interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_summary = Instant::now();

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        ticks += 1;

        let round = pricer.round().await;
        let observed = dex::snapshot(&watch);

        let mut prices = Vec::new();
        let mut unpriced: Vec<String> = round
            .unpriced
            .iter()
            .map(|(coin, err)| format!("{coin} ({err})"))
            .collect();
        for (coin, _) in &round.unpriced {
            let stat = stats.entry(coin.clone()).or_default();
            stat.ticks += 1;
            stat.unpriced += 1;
        }

        for Priced {
            coin,
            spot,
            external,
        } in &round.priced
        {
            let stat = stats.entry(coin.clone()).or_default();
            stat.ticks += 1;
            stat.sources_used += spot.used.len();

            let theirs = observed.get(coin);
            let their_oracle = theirs.and_then(|t| t.oracle);
            if let Some(price) = their_oracle {
                guard.seed(coin, price);
            }
            let verdict = guard.check(coin, spot.price, theirs.and_then(|t| t.prev_day));
            let label = match &verdict {
                Verdict::Accepted(_) => "accepted",
                Verdict::Clamped { wanted, sent } => {
                    stat.clamped += 1;
                    warn!(%coin, %wanted, %sent, "move capped at {} bp", config.guards.max_move_bps);
                    "clamped"
                }
                Verdict::OutsideBand { price, reference } => {
                    stat.outside_band += 1;
                    warn!(%coin, %price, %reference, "refused: outside 10x of the previous-day price");
                    "outside-band"
                }
            };

            let deviation = their_oracle.map(|t| deviation_bps(spot.price, t));
            if let Some(d) = deviation {
                if stat.deviations.len() == WINDOW {
                    stat.deviations.pop_front();
                }
                stat.deviations
                    .push_back(d.abs().to_f64().unwrap_or(f64::NAN));
            }
            if ticks.is_multiple_of(options.log_every.max(1)) {
                serde_json::to_writer(
                    &mut log,
                    &Row {
                        time: chrono::Utc::now().to_rfc3339(),
                        coin,
                        oracle: spot.price.normalize().to_string(),
                        sent: verdict.price().map(|p| p.normalize().to_string()),
                        guard: label,
                        external: external.normalize().to_string(),
                        theirs: their_oracle.map(|t| t.normalize().to_string()),
                        deviation_bps: deviation.and_then(|d| d.round_dp(3).to_f64()),
                        usdt_usd: round.usdt_usd.map(|r| r.normalize().to_string()),
                        sources: spot.used.iter().map(|k| k.name()).collect(),
                        outliers: spot.outliers.iter().map(|(k, _)| k.name()).collect(),
                        quotes: spot
                            .quotes
                            .iter()
                            .map(|(k, p)| (k.name(), p.normalize().to_string()))
                            .collect(),
                    },
                )?;
                log.write_all(b"\n")?;
            }

            match verdict.price() {
                Some(oracle) => prices.push(AssetPrices {
                    coin: coin.clone(),
                    oracle,
                    external: *external,
                }),
                None => unpriced.push(format!("{coin} (refused by the guard)")),
            }
        }

        // externalPerpPxs must cover every asset, so a tick with an unpriced asset sends
        // nothing. The exchange falls back to the local mark after 10 s without an update.
        if unpriced.is_empty() {
            unsent_run = 0;
            if !probed {
                probed = true;
                let update = oracle::update(&config.dex, config.star, &prices, &sz_decimals);
                dex::probe(&client, chain, update).await;
            }
        } else {
            unsent += 1;
            unsent_run += 1;
            // Warn when the set of unpriced assets changes, not on every tick of a long run,
            // such as stocks while their market is closed.
            if unpriced != last_unpriced {
                warn!(
                    "would not send this update; unpriced: {}",
                    unpriced.join(", ")
                );
            }
            if unsent_run == config.guards.unsent_alarm_ticks {
                alarms += 1;
                warn!(
                    "ALARM: {unsent_run} updates in a row could not be sent (~{} s); the exchange falls back to the local mark after 10 s",
                    u64::from(unsent_run) * config.interval_ms / 1000
                );
            }
        }
        last_unpriced = unpriced;

        if last_summary.elapsed() >= Duration::from_secs(60) {
            last_summary = Instant::now();
            summarize(&stats, &watch, ticks, unsent, alarms, &pricer);
        }
    }

    summarize(&stats, &watch, ticks, unsent, alarms, &pricer);
    info!(log = %log_path.display(), "shadow run finished");
    Ok(())
}

fn summarize(
    stats: &BTreeMap<String, CoinStats>,
    watch: &Watch,
    ticks: usize,
    unsent: usize,
    alarms: usize,
    pricer: &Pricer,
) {
    let observed = watch.lock().unwrap();
    info!("--- {ticks} ticks, {unsent} would not have been sent, {alarms} alarms ---");
    for (coin, stat) in stats {
        let mut deviations: Vec<f64> = stat.deviations.iter().copied().collect();
        deviations.sort_by(f64::total_cmp);
        let their_gaps = observed.get(coin).map(|o| {
            let mut gaps: Vec<f64> = o.gaps.iter().copied().collect();
            gaps.sort_by(f64::total_cmp);
            gaps
        });
        let priced = stat.ticks - stat.unpriced;
        info!(
            "{coin:<10} |deviation| p50 {} p95 {} bps over {} ticks | {:.1} sources/tick | {} clamped, {} refused | their oracle changed every p50 {} s, max {} s",
            fmt_opt(percentile(&deviations, 0.5)),
            fmt_opt(percentile(&deviations, 0.95)),
            deviations.len(),
            if priced == 0 {
                0.0
            } else {
                stat.sources_used as f64 / priced as f64
            },
            stat.clamped,
            stat.outside_band,
            fmt_opt(their_gaps.as_deref().and_then(|g| percentile(g, 0.5))),
            fmt_opt(their_gaps.as_deref().and_then(|g| g.last().copied())),
        );
    }
    if pricer.rate_errors > 0 {
        info!("USDT/USD rate failures: {}", pricer.rate_errors);
    }
    if !pricer.source_errors.is_empty() {
        info!(
            "source failures: {}",
            pricer
                .source_errors
                .iter()
                .map(|(k, n)| format!("{} {n}", k.name()))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let index = ((sorted.len() - 1) as f64 * p).round() as usize;
    Some(sorted[index])
}

fn fmt_opt(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.2}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_picks_the_nearest_rank() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(&values, 0.5), Some(3.0));
        assert_eq!(percentile(&values, 0.95), Some(5.0));
        assert_eq!(percentile(&[], 0.5), None);
    }
}
