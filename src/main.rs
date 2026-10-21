//! hyperkeeper: open-source operations for Hyperliquid HIP-3 and HIP-4 deployers.

mod aggregate;
mod config;
mod daily_log;
mod dex;
mod failover;
mod guard;
mod hip4;
mod oracle;
mod pricer;
mod run;
mod settle;
mod shadow;
mod signer;
mod sources;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// Sent with every HTTP request, so venues can tell who is calling.
pub const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch every price source once and print what each says about each asset
    Check {
        #[arg(long, default_value = "hyperkeeper.toml")]
        config: PathBuf,
    },
    /// Price the DEX every interval, sign the update the keeper would send, and compare it
    /// with the DEX's live oracle. Nothing is sent that can take effect.
    Shadow {
        #[arg(long, default_value = "hyperkeeper.toml")]
        config: PathBuf,
        /// Stop after this many minutes. Runs until Ctrl-C otherwise.
        #[arg(long)]
        minutes: Option<u64>,
        /// Directory for the per-tick JSONL log.
        #[arg(long, default_value = "data")]
        out: PathBuf,
        /// Write a log row every this many ticks, to keep the log small for many assets.
        #[arg(long, default_value_t = 1)]
        log_every: usize,
    },
    /// HIP-4: predict what each binaryPrice market settles to and check it against the
    /// venue's settlement. Sends nothing.
    Hip4Shadow {
        /// mainnet or testnet.
        #[arg(long, default_value = "mainnet")]
        network: String,
        /// Follow markets whose window closes within this many minutes.
        #[arg(long, default_value_t = 30)]
        lookahead_minutes: u64,
        /// Stop after this many minutes. Runs until Ctrl-C otherwise.
        #[arg(long)]
        minutes: Option<u64>,
        /// Directory for the JSONL log.
        #[arg(long, default_value = "data")]
        out: PathBuf,
    },
    /// Send the oracle update every interval (or, as a standby, while the primary is
    /// silent). Refuses to start unless the key may send this DEX's oracle update.
    Run {
        #[arg(long, default_value = "hyperkeeper.toml")]
        config: PathBuf,
        /// Do everything except send. Works without a key.
        #[arg(long)]
        dry_run: bool,
        /// Start even if the permission check fails.
        #[arg(long)]
        force: bool,
        /// Stop after this many minutes. Runs until Ctrl-C otherwise.
        #[arg(long)]
        minutes: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "hyperkeeper=info".into()),
        )
        .with_target(false)
        .init();

    match Cli::parse().command {
        Command::Check { config } => shadow::check(&config::Config::load(&config)?).await,
        Command::Shadow {
            config,
            minutes,
            out,
            log_every,
        } => {
            shadow::run(
                config::Config::load(&config)?,
                shadow::Options {
                    minutes,
                    out_dir: out,
                    log_every,
                },
            )
            .await
        }
        Command::Hip4Shadow {
            network,
            lookahead_minutes,
            minutes,
            out,
        } => {
            let network = match network.as_str() {
                "mainnet" => config::Network::Mainnet,
                "testnet" => config::Network::Testnet,
                other => anyhow::bail!("--network must be mainnet or testnet, not {other}"),
            };
            anyhow::ensure!(
                lookahead_minutes > 5,
                "--lookahead-minutes must exceed the longest window, 5 minutes"
            );
            settle::run(settle::Options {
                network,
                lookahead_minutes,
                minutes,
                out_dir: out,
            })
            .await
        }
        Command::Run {
            config,
            dry_run,
            force,
            minutes,
        } => {
            run::run(
                config::Config::load(&config)?,
                run::Options {
                    dry_run,
                    force,
                    minutes,
                },
            )
            .await
        }
    }
}
