# Shadow run: HIP-3\* test DEX `ignp`, Oct 7–8, 2026

The keeper priced `ignp`'s five assets every 3 s for **27.1 hours** (Oct 7 05:29 → Oct 8 08:36 UTC) and compared each price with the oracle `ignp` actually shows. It ran in shadow mode on a GCP e2-small in Tokyo: it built and signed the HIP-3\* `star.setOracle` update, but sent nothing that could take effect. The one probe update, signed by a throwaway key, passed the exchange's signature check.

## Results

| Asset | Rounds | \|deviation\| p50 / p95 / p99 / max (bp) | Bias (bp) | `ignp` price-change gap p50 / p95 / max (s) | Gaps >30 s / >60 s | Time its price sat unchanged ≥10 s / ≥30 s / ≥60 s |
|---|---|---|---|---|---|---|
| AVAX | 32,528 | 4.5 / 13.0 / 19.1 / 62.3 | +3.9 | 12.0 / 24.0 / 81 | 76 / 2 | 14% / 0.6% / 0.02% |
| BTC | 32,528 | 4.5 / 7.7 / 9.9 / 22.3 | +4.4 | 12.0 / 15.1 / 81 | 17 / 1 | 8% / 0.1% / 0.02% |
| DOGE | 32,528 | 4.5 / 9.3 / 13.3 / 207.4 | +4.4 | 12.1 / 36.0 / 177 | 436 / 55 | 30% / 6.2% / 1.17% |
| ETH | 32,528 | 4.5 / 8.3 / 11.2 / 82.0 | +4.3 | 12.0 / 15.3 / 105 | 46 / 3 | 10% / 0.5% / 0.07% |
| SOL | 32,528 | 4.7 / 8.8 / 12.1 / 42.3 | +4.4 | 12.0 / 24.0 / 81 | 87 / 4 | 13% / 0.8% / 0.02% |

## Findings

**1. `ignp` updates its oracle about every 12 s, and its price sometimes goes up to 3 minutes without changing.** The explorer confirms one key sends `star.setOracle` at that cadence. The standard HIP-3 guidance is every 3 s. DOGE was the worst: its oracle price sat unchanged for 10 s or more **30% of the time**, and 60 s or more 1.2% of the time (55 stretches over a minute, the longest 177 s). Those count price changes, not transactions, so they overstate the update gaps when an update repeats the last price; the explorer's transaction-level cadence (11.6–12.8 s) is the firmer number.

**2. Our ~4.5 bp deviation is a constant bias from quote currency, not noise or their staleness.**
- The deviation is the same whether `ignp`'s oracle is fresh (<3.5 s old) or stale (≥10 s): 4.4–4.7 bp median either way.
- Every asset shows the same signed bias, **+3.9 to +4.4 bp**.
- A live check on Oct 8 explains it. USDT traded at $0.99928, 7.2 bp under a dollar, and 4 of our 6 spot sources quote in USDT (Binance, Bybit, OKX, and Hyperliquid's own oracle, which reads like a USDT venue), so our median leaned USDT. That single snapshot also suggested `ignp` sits between USD and USDT venues; the run after the fix (below) shows it is effectively USD-referenced.
- **Fix:** convert USDT quotes to USD (a median of Kraken's and Coinbase's USDT/USD), so every source prices in one currency, and make the target convention configurable.

**3. The keeper's own pipeline was clean.**
- 0 missed rounds out of 32,528.
- 6.0 sources per round on average, never fewer than 5.
- 2 outliers dropped, both Hyperliquid's oracle on DOGE.
- 0 guard clamps or refusals, and 0 alarms.
- Source failures: Kraken 97 (0.3% of rounds), OKX 4, Binance perp 3, OKX perp 3. All were covered by the quote cache.
- 0 restarts.
- One flaw: memory grew from 11 MB to 60 MB because the stats keep every sample. It would have reached the 300 MB cap in about 5 days, so the stats need to be bounded.

## What changes next

1. Convert USDT-quoted prices to USD, with a configurable target convention.
2. Bound the in-memory stats.
3. Re-run, expecting a bias near zero against a USD-referenced oracle.
4. Then the live `run` mode with explorer-based failover.

## Method

- **Source:** the per-tick log, `data/shadow-ignp-20261007-0529.jsonl` (45 MB, not committed).
- **Deviation:** our aggregated spot price against `ignp`'s oracle at the same tick.
- **Update gaps and staleness:** measured from when `ignp`'s oracle value changed between 3 s rounds. That's an upper bound on its real update gaps, since an update that leaves the price unchanged is invisible here; the explorer gives the transaction-level cadence.
- **Live USDT check:** public order books on Oct 8, around 08:40 UTC.

## After the fix (Oct 8, 08:41 UTC on)

The keeper now converts every quote to USD with a USDT/USD rate (the median of Kraken's and Coinbase's books), redeployed at 08:41 UTC. First 68 rounds per asset:

| Asset | Signed bias (bp), before → after | \|deviation\| p50 (bp), before → after |
|---|---|---|
| AVAX | +3.9 → **+0.06** | 4.5 → 2.0 |
| BTC | +4.4 → **−0.56** | 4.5 → 1.2 |
| DOGE | +4.4 → **+0.23** | 4.5 → 0.9 |
| ETH | +4.3 → **+0.27** | 4.5 → 0.6 |
| SOL | +4.4 → **+0.06** | 4.7 → 0.8 |

The constant bias is gone, so `ignp`'s oracle is USD-referenced. What remains is mostly `ignp`'s own ~12 s update cadence. Memory stays at about 11 MB, with percentiles over a rolling 24-hour window.

