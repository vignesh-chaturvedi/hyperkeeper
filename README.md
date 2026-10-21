# hyperkeeper

Open-source operations for Hyperliquid deployers, in Rust on [hypersdk](https://github.com/infinitefield/hypersdk).

HIP-3 and HIP-4 deployers run write-side jobs around the clock: a HIP-3 DEX must update its oracle every 3 seconds, and a HIP-4 venue must settle every outcome it lists. A deployer can hand that work to a sub-deployer key that holds only the one permission it needs, without giving up the stake. hyperkeeper is the process that holds that key.

- **HIP-3 oracle.** Prices every asset from several public venues every 3 seconds and sends `setOracle`, or `star.setOracle` on HIP-3\* venues. Two instances with separate keys fail over to each other.
- **HIP-4 settlement.** Computes what each `binaryPrice` and `priceTouch` outcome settles to from the price its template names, seconds after the window closes or the target is touched.

**Status: pre-release, testnet.** Everything up to the send is built and tested. Sending needs a deployer to grant a key; until then hyperkeeper runs in shadow and dry-run modes, which sign but send nothing that can take effect.

## Results so far

| What | Result | Report |
|---|---|---|
| HIP-4 settlement, 3 mainnet venues, Oct 8–13 | **1,863 of 1,866** outcomes predicted correctly; the 3 misses were within 2.7 bp of the threshold | [HIP-4 report](reports/2026-10-13-hip4.md) |
| HIP-3 oracle, testnet `ignp`, 27 h | Within ~1 bp of the deployer's own oracle after USD conversion; no missed rounds | [shadow run](reports/2026-10-08-shadow-ignp.md) |
| Failover rehearsal, 24.7 h | A standby would have covered 34 silences of the primary key, 14.2 min in all, the longest 96 s | [dry runs](reports/2026-10-09-dry-runs.md) |

## Commands

| Command | What it does | Needs |
|---|---|---|
| `check` | Fetches every price source once and prints each one's prices | Nothing |
| `shadow` | Every 3 s: prices the DEX, builds and signs the oracle update, and compares it with the DEX's live oracle. Writes every tick to `data/shadow-<dex>-<date>.jsonl`. | Nothing |
| `run --dry-run` | Everything `run` does except the send, including failover decisions | Nothing |
| `run` | Sends the oracle update every 3 s, as a primary or a standby | A key the DEX has granted `setOracle` |
| `hip4-shadow` | Follows every `binaryPrice` and `priceTouch` market, predicts its settlement, and checks it against the venue's | Nothing |

## Quick start

Requires Rust 1.94.1 or newer.

```bash
cp hyperkeeper.example.toml hyperkeeper.toml
cargo run -- check                    # one fetch from every source
cargo run -- shadow --minutes 5       # price ignp on testnet and compare with its oracle
cargo run -- hip4-shadow --minutes 30 # follow mainnet HIP-4 markets closing in the next 30 min
```

Shadow mode logs a summary every minute:

```
ignp:BTC  |deviation| p50 1.33 p95 2.84 bps over 59 ticks | 5.0 sources/tick | their oracle changed every p50 11.56 s, max 13.68 s
```

At startup it sends one update signed by a throwaway key. The exchange rejects it but names the address it recovered from the signature. If that matches the throwaway key, the update was encoded exactly as the exchange hashes it.

## Safety model

- **One permission, one key.** hyperkeeper signs with a sub-deployer key granted only `setOracle` (or `hip3Star:setOracle` on a HIP-3\* venue). The deployer's own key and stake stay with the deployer, and the grant can be revoked at any time.
- **Preflight.** `run` reads the DEX's live permissions and refuses to start unless its key may send the update. The error names the keys that are allowed and the `hypecli dex sub-deployer` command that grants one.
- **Guards on every update.**
  - A price may move at most `max_move_bps` from the last accepted one; bigger moves converge over several updates.
  - It must stay within 10× of the DEX's previous-day price.
  - If any asset has no price, nothing is sent that tick, because `externalPerpPxs` must cover every asset.
  - An alarm fires after `unsent_alarm_ticks` updates in a row could not be sent.
- **Rate.** Never two updates within 2.5 s, with strictly rising nonces.
- **Keys.** Read from an encrypted keystore (`[signer] keystore`, password in `HYPERKEEPER_KEYSTORE_PASSWORD`) or, for test keys, `HYPERKEEPER_PRIVATE_KEY`. Only the address is ever logged.

```bash
export HYPERKEEPER_KEYSTORE_PASSWORD=...   # with [signer] keystore = "/path/to/keystore"
cargo run --release -- run
cargo run --release -- run --dry-run       # every step but the send; works without a key
```

## Failover

Two instances run, each with its own key, both granted `setOracle`:

```toml
[failover]
role = "standby"                 # the other instance is role = "primary" (the default)
peer = "0x..."                   # the primary's signing address
takeover_after_ms = 10000
poll_ms = 5000
```

The standby asks Hyperliquid's explorer for the primary key's latest successful oracle update. It sends only while that update is older than `takeover_after_ms`, measured at the moment the explorer answered.

Watching the primary's own transactions works even when prices are flat; watching the oracle for changes would mistake a quiet market for a dead primary. It also needs no link between the hosts. If the explorer stops answering, the standby falls back to watching the DEX's oracle.

When the primary recovers, both instances may send until the standby's next poll, up to `poll_ms`. Their prices come from the same sources and agree within a few basis points. The exchange may reject one of two updates that land within 2.5 s of each other.

## How a price is made

1. Every source is fetched concurrently. Quotes stay usable for `max_age_ms`, so one slow venue doesn't drop out.
2. Each quote is converted to the DEX's currency (`convention = "usd"`, `"usdt"` or `"as-quoted"`). USDT-quoted venues are converted with a USDT/USD rate from public order books. USDT trades a few basis points off the dollar, and that is enough to bias an oracle.
3. The oracle price is the median of the spot sources, after dropping any more than `max_deviation_bps` from the first median. Fewer than `min_sources` agreeing sources means no price.
4. The external perp price is the same over perp sources, falling back to the oracle price.
5. Lists are sorted by coin and prices rounded to `6 - szDecimals` decimals.

Sources: Binance, Coinbase, Bybit, Kraken and OKX spot; Binance, Bybit and OKX perps; Hyperliquid's own oracle and perp mids. For stocks, metals and FX:

- `pyth`: Pyth price feeds, named per asset (`pyth = "Equity.US.NVDA/USD"`). Pyth has required an API key since August 2026; put it in `HYPERKEEPER_PYTH_API_KEY`. A Pyth price whose own publish time is over 30 s old is dropped, so a market outside its trading session reads as unpriced rather than frozen.
- `bybit-xstock`: Bybit's tokenized stocks (`NVDAXUSDT` for `NVDA`), free and around the clock, but at a premium to the stock outside regular US hours.

`deploy/shadow-xyz.toml` shadows trade[XYZ]'s stocks, metals and FX with both. Each tick logs every source's quote under `quotes`, so the sources can be compared one by one.

## HIP-4 settlement

A `binaryPrice` outcome settles YES if the `{seconds}`-second TWAP of the named price, ending at `{time}`, is strictly above the threshold. `hip4-shadow` computes that TWAP at expiry:

- from 1-minute candle closes, when the template names candles;
- from mark prices or trades it records live, when it names those. There is no public history for them, so recording starts before each window opens.

A `priceTouch` outcome settles YES as soon as the `{seconds}`-second TWAP equals or crosses the target, at any time up to `{time}`, and NO if it never does. `hip4-shadow` watches each one from when it first sees it: every few seconds it checks the rolling TWAP, and predicts YES the moment it reaches the target. Between the moments a sample enters or leaves the window, that TWAP moves in a straight line, so checking those moments finds every touch. If the venue settles a touch that hyperkeeper didn't see, it counts as a miss.

It then polls the venue's settlement and logs each prediction, the venue's result and the settle lag to `data/hip4-shadow-*.jsonl`. For a touch, the lag counts from the touch.

Testnet also lists numbered drafts of these templates, such as `binaryPrice6` and `priceTouch2`. They read the same way and are handled the same. `scalarPrice` and `binaryPriceExternal` are not supported yet.

The templates say "TWAP" without saying how samples are weighted. So each prediction also logs the plain mean and, for trades, the volume-weighted average, and every recorded window's raw samples go to `data/hip4-samples-*.jsonl`. A disagreement with a venue can then be replayed under other definitions.

## Deploying

`deploy/` has systemd units for the shadow, dry-run standby and HIP-4 shadow services, plus the configs they ran with. They expect a `keeper` user and the repo at `/home/keeper/hyperkeeper`.

The JSONL logs start a new file each UTC day, so finished days can be compressed while the services run. The `ignp` shadow log is about 70 MB a day before compression and 6 MB after.

## Roadmap

- Settle HIP-4 outcomes for real (`settleOutcome`) once a venue grants a key
- Register recurring HIP-4 markets from a template, within the venue's limits
- Telegram and webhook alerts, and a `/metrics` endpoint
- A Docker image
- A public 7-day testnet run with uptime stats

## Disclaimer

hyperkeeper is experimental software that signs transactions that move markets. Run it on testnet first, grant it the narrowest permission that works, and watch it. It comes with no warranty; see the license.

## License

Apache-2.0
