//! Live mode: send the oracle update every interval, or, as a standby, only while the
//! primary is silent. `--dry-run` does everything except send.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::bail;
use hypersdk::hypercore::{
    HttpClient,
    api::{OkResponse, Response},
};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::{
    config::{Config, Role},
    dex::{self, Authorization},
    failover::{self, Decision},
    guard::{Guard, Verdict},
    oracle::{self, AssetPrices},
    pricer::{Priced, Pricer},
    signer,
};

/// `setOracle` calls must be at least 2.5 s apart.
const MIN_SPACING: Duration = Duration::from_millis(2_500);

pub struct Options {
    /// Do everything except send.
    pub dry_run: bool,
    /// Start even if the key isn't allowed to send this DEX's oracle update.
    pub force: bool,
    /// Stop after this many minutes; run until interrupted when `None`.
    pub minutes: Option<u64>,
}

#[derive(Default)]
struct Counters {
    rounds: usize,
    sent: usize,
    failed: usize,
    /// Rounds a standby left to the primary.
    held: usize,
    /// Rounds with an asset that had no usable price.
    unpriced: usize,
    /// Rounds skipped to keep updates 2.5 s apart.
    spacing: usize,
    takeovers: usize,
    alarms: usize,
}

pub async fn run(config: Config, options: Options) -> anyhow::Result<()> {
    let chain = config.network.chain();
    let client = HttpClient::new(chain);
    let (signer, throwaway) = signer::load(&config.signer, options.dry_run)?;
    let me = signer.address();
    let role = config.failover.role;
    info!(
        key = %me,
        throwaway,
        dry_run = options.dry_run,
        ?role,
        dex = %config.dex,
        network = ?config.network,
        "hyperkeeper run starting"
    );

    let grant = if config.star {
        "hip3Star:setOracle"
    } else {
        "setOracle"
    };
    match dex::authorization(&client, &config, me).await {
        Ok(Authorization::Allowed(how)) => {
            info!("this key may send the DEX's oracle update, as its {how}")
        }
        Ok(Authorization::Denied(allowed)) => {
            let list = allowed
                .iter()
                .map(|a| format!("{a:#x}"))
                .collect::<Vec<_>>()
                .join(", ");
            if options.dry_run || options.force {
                warn!(
                    "this key may not send the DEX's oracle update (allowed: {list}); continuing because of --{}",
                    if options.dry_run { "dry-run" } else { "force" }
                );
            } else {
                bail!(
                    "key {me:#x} may not send {}'s oracle update. Allowed: {list}. The deployer can grant it with \
                     `hypecli dex sub-deployer --dex {} --user {me:#x} --permission {grant}`, or run with --dry-run",
                    config.dex,
                    config.dex
                );
            }
        }
        Err(err) if options.dry_run || options.force => {
            warn!("could not check the key's permission: {err:#}")
        }
        Err(err) => return Err(err.context("checking the key's permission")),
    }

    let sz_decimals = dex::sz_decimals(&client, &config).await?;
    let mut pricer = Pricer::new(&config)?;
    let mut guard = Guard::new(config.guards.max_move_bps);
    let watch = dex::watch(&config);
    let poll = Duration::from_millis(config.failover.poll_ms);
    let peer = match (role, config.failover.peer) {
        (Role::Standby, Some(peer)) => {
            info!(%peer, "standby: following the primary's updates through the explorer");
            Some(failover::watch_peer(
                config.network,
                peer,
                config.dex.clone(),
                config.star,
                poll,
            ))
        }
        _ => None,
    };

    let mut c = Counters::default();
    let mut last_send: Option<Instant> = None;
    let mut last_nonce = 0u64;
    let mut unsent_run = 0u32;
    let mut sending: Option<bool> = None;

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
        c.rounds += 1;
        if last_summary.elapsed() >= Duration::from_secs(60) {
            last_summary = Instant::now();
            summarize(&c, options.dry_run);
        }

        let round = pricer.round().await;
        let observed = dex::snapshot(&watch);
        let mut prices = Vec::new();
        let mut unpriced: Vec<String> = round
            .unpriced
            .iter()
            .map(|(coin, err)| format!("{coin} ({err})"))
            .collect();
        for Priced {
            coin,
            spot,
            external,
        } in &round.priced
        {
            let theirs = observed.get(coin);
            if let Some(price) = theirs.and_then(|t| t.oracle) {
                guard.seed(coin, price);
            }
            let verdict = guard.check(coin, spot.price, theirs.and_then(|t| t.prev_day));
            match &verdict {
                Verdict::Clamped { wanted, sent } => {
                    warn!(%coin, %wanted, %sent, "move capped at {} bp", config.guards.max_move_bps)
                }
                Verdict::OutsideBand { price, reference } => {
                    warn!(%coin, %price, %reference, "refused: outside 10x of the previous-day price")
                }
                Verdict::Accepted(_) => {}
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

        // externalPerpPxs must cover every asset, so a round with an unpriced asset sends
        // nothing; the exchange falls back to the local mark after 10 s.
        if !unpriced.is_empty() {
            c.unpriced += 1;
            warn!("not sending; unpriced: {}", unpriced.join(", "));
            note_unsent(&mut unsent_run, &mut c, &config);
            continue;
        }

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let (view, explorer_fresh) = match &peer {
            Some(view) => {
                let view = *view.lock().unwrap();
                let fresh = view.polled_at.is_some_and(|at| at.elapsed() <= 3 * poll);
                (view, fresh)
            }
            None => (failover::PeerView::default(), false),
        };
        let quiet_ms = dex::oracle_quiet_for(&watch).map(|d| d.as_millis() as u64);
        let (decision, reason) = failover::decide(
            role,
            view,
            explorer_fresh,
            quiet_ms,
            config.failover.takeover_after_ms,
        );
        let send_now = decision == Decision::Send;
        if role == Role::Standby && sending != Some(send_now) {
            if send_now {
                c.takeovers += 1;
                info!(?reason, "standby taking over");
            } else {
                info!(?reason, "standby holding; the primary is updating");
            }
        }
        sending = Some(send_now);
        if !send_now {
            c.held += 1;
            unsent_run = 0;
            continue;
        }
        if last_send.is_some_and(|at| at.elapsed() < MIN_SPACING) {
            c.spacing += 1;
            continue;
        }

        let action = oracle::update(&config.dex, config.star, &prices, &sz_decimals);
        let nonce = now_ms.max(last_nonce + 1);
        last_nonce = nonce;
        last_send = Some(Instant::now());
        if options.dry_run {
            c.sent += 1;
            unsent_run = 0;
            debug!(nonce, "dry run: would send the update");
            continue;
        }
        let request = match action.sign_sync(&signer, nonce, None, None, chain) {
            Ok(request) => request,
            Err(err) => {
                c.failed += 1;
                warn!("could not sign the update: {err:#}");
                note_unsent(&mut unsent_run, &mut c, &config);
                continue;
            }
        };
        match client.send(request).await {
            Ok(Response::Ok(reply)) => {
                c.sent += 1;
                unsent_run = 0;
                if !matches!(reply, OkResponse::Default) {
                    warn!("unexpected reply to an accepted update: {reply:?}");
                }
            }
            Ok(Response::Err(message)) => {
                c.failed += 1;
                warn!("update rejected: {message}");
                note_unsent(&mut unsent_run, &mut c, &config);
            }
            Err(err) => {
                c.failed += 1;
                warn!("update not answered: {err:#}");
                note_unsent(&mut unsent_run, &mut c, &config);
            }
        }
    }

    summarize(&c, options.dry_run);
    info!("hyperkeeper run stopped");
    Ok(())
}

/// Counts a round that should have updated the oracle and didn't, and raises the alarm
/// once that has happened `unsent_alarm_ticks` times in a row.
fn note_unsent(unsent_run: &mut u32, c: &mut Counters, config: &Config) {
    *unsent_run += 1;
    if *unsent_run == config.guards.unsent_alarm_ticks {
        c.alarms += 1;
        warn!(
            "ALARM: {} updates in a row not delivered (~{} s); the exchange falls back to the local mark after 10 s",
            unsent_run,
            u64::from(*unsent_run) * config.interval_ms / 1000
        );
    }
}

fn summarize(c: &Counters, dry_run: bool) {
    info!(
        "--- {} rounds | {} {} | {} failed | {} held for the primary | {} unpriced | {} spacing skips | {} takeovers | {} alarms ---",
        c.rounds,
        c.sent,
        if dry_run { "would have sent" } else { "sent" },
        c.failed,
        c.held,
        c.unpriced,
        c.spacing,
        c.takeovers,
        c.alarms
    );
}
