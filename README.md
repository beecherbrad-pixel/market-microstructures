# spark-microstructure

A PySpark pipeline that turns tick-level cryptocurrency trades into minute-level
liquidity and price-impact measures.

Source data is Binance's public spot trade history
([data.binance.vision](https://data.binance.vision)): every executed trade, with
a flag for which side initiated it.

## Status

| Stage | What it does | State |
|---|---|---|
| Ingest | Download, checksum-verify and store daily trade files | Done |
| Bronze | Raw trades in a Delta table with an explicit schema and lineage columns, loaded incrementally with Auto Loader | Done |
| Silver | Deduplicated trades, consistent timestamps, signed order flow; liquid clustering on pair and day | Done |
| Gold | One-minute bars and daily summaries: VWAP, realized volatility, order-flow imbalance, Roll spread, Amihud illiquidity | Done |
| Price impact | Kyle's lambda by pair and day, estimated in Spark; second-stage regressions on volatility; CSV exports for SAS | Done |
| SAS replication | Daily lambdas and every second-stage model re-estimated in SAS; parity table against Python ([docs/sas_parity.md](docs/sas_parity.md)) | Done |

## Scale

Measured on Databricks Free Edition serverless compute. Times are wall-clock
seconds from the `pipeline_runs` table, which every run appends to, and vary by
roughly 10% between runs.

| Step | August 2026, BTCUSDT | 1 Aug to 29 Oct 2026, BTCUSDT + ETHUSDT |
|---|---|---|
| Trades | 91.9 million | 480.4 million |
| Bronze, loading new files | 49 s | 124 s (388.5 million new rows) |
| Bronze, no new files | 4 s | 7 s |
| Silver, full rebuild | 39 to 53 s | 145 to 158 s |
| Gold bars | 68 s (44,640 bars) | 123 s (259,200 bars) |
| Gold daily | 14 s (31 pair-days) | 44 s (180 pair-days) |
| Kyle's lambda, one regression per pair-day | | 7 s (259,198 bars, 180 regressions) |

- With 5.2 times the trades, the silver rebuild took about 3 times as long and
  the gold bars about 1.8 times as long, most likely because serverless compute scales out
  for larger jobs.
- A full run over all 480 million trades (bronze check, silver, both gold
  tables) takes about 5.3 minutes.
- Auto Loader only reads files it has not seen, so a rerun with no new data
  finishes bronze in seconds.
- Every minute of every day had at least one trade (1,440 bars per pair per
  day), and silver dropped no rows: the source files had no duplicates or
  invalid trades over this period.

## Layout

```
src/microstructure/
    ingest/binance.py   downloader (standard library only)
    schema.py           explicit schema for raw trade files
    io.py               read raw files into a Spark DataFrame
    smoke.py            sanity check on downloaded data
    bronze.py           raw rows plus source file and load time
    silver.py           validate, deduplicate, normalize time, sign trades
    gold.py             time bars and daily liquidity measures
    impact.py           Kyle's lambda: one regression per group, run in Spark
    analysis.py         daily panel and volatility regressions (pandas, statsmodels)
    parity.py           Python vs SAS parity report
notebooks/
    01_bronze_silver.py Databricks notebook: volume -> bronze -> silver
    02_gold.py          Databricks notebook: silver -> gold bars and daily
    03_price_impact.py  Databricks notebook: lambda tables, regressions, CSV exports
sas/
    00_setup.sas        paths and data import
    01_daily_lambda.sas PROC REG, one regression per pair-day, HC1 errors
    02_volatility.sas   SURVEYREG, AUTOREG, IML, PANEL, SYSLIN: the second stage
    run_all.sas         runs the three in order
tests/                  pytest suite; runs offline on a local Spark session
.github/workflows/      runs the tests on every push
```

Transformation logic lives in the package as plain functions that take and
return DataFrames, so it can be unit-tested on small fixtures. Notebooks only
call those functions.

## Setup

Requires Python 3.10+ and, for anything that touches Spark, Java 17 or 21
(`java -version` to check).

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
```

On Windows, local Spark also needs Hadoop's native libraries (`hadoop.dll` and
`winutils.exe`, found through `HADOOP_HOME`). Without them the Spark tests are
skipped locally and run in CI instead; the downloader and its tests work
everywhere. WSL avoids the issue entirely.

## Getting data

```powershell
python -m microstructure.ingest.binance --start 2026-08-01 --end 2026-08-31 --out C:\data\binance
```

One file per symbol per day, written as gzip CSV under
`<out>/symbol=<SYMBOL>/`. Each download is verified against Binance's SHA-256
checksum. Re-running skips files that are already there, and days Binance has
not published are reported as `missing`.

Options: `--symbol BTCUSDT ETHUSDT` for several pairs, `--freq monthly` for one
file per month. August 2026 for BTCUSDT is about 660 MB.

Then check that Spark reads it (Linux, macOS, WSL, or Windows with
`HADOOP_HOME` set):

```powershell
python -m microstructure.smoke C:\data\binance
```

## Running on Databricks

Built for Databricks Free Edition (serverless compute, Unity Catalog).

1. **Workspace → Create → Git folder**, pointing at this repository. The
   notebook imports the package straight from the folder's `src/`, so nothing
   needs installing.
2. Open `notebooks/01_bronze_silver.py` and run the first cells. They create
   the `workspace.microstructure` schema and two volumes, `raw` and
   `checkpoints`.
3. Upload the downloaded `.csv.gz` files to
   `/Volumes/workspace/microstructure/raw/trades/symbol=<SYMBOL>/`
   (Catalog → the `raw` volume → *Upload to this volume*). Free Edition compute
   can only reach a short list of trusted domains, so files are downloaded
   locally rather than inside the notebook.
4. Run the whole notebook. Bronze is loaded with Auto Loader, so after
   uploading more files a rerun reads only the new ones; silver is rebuilt
   from bronze. Row counts and runtimes are appended to `pipeline_runs`.
5. Run `notebooks/02_gold.py`. It rebuilds `gold_bars` and `gold_daily` from
   silver, checks that the bars account for every trade, and logs its own
   runtimes. The `bar_seconds` widget switches to other bar lengths (300 for
   five-minute bars).
6. Run `notebooks/03_price_impact.py`. It builds `gold_lambda_daily` and
   `gold_lambda_hourly`, runs the volatility regressions, and writes CSVs and a
   chart to the `exports` volume.

## Deploying the job

The pipeline job is defined as code in a Databricks Asset Bundle
(`databricks.yml` and `jobs/microstructure_pipeline.yml`). It runs
`01_bronze_silver`, `02_gold` and `03_price_impact` in order on serverless compute whenever new files
arrive in the raw volume, and emails the deploying user if a task fails.

With the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
installed:

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks bundle validate
databricks bundle deploy
databricks bundle run microstructure_pipeline   # optional: run once now
```

`deploy` uploads the notebooks and `src/` to the workspace and creates or
updates the job, so the job always runs the code in the commit that was
deployed. Nothing in the bundle names a workspace or user; both come from the
CLI login.

## Gold measures

| Column | Meaning |
|---|---|
| `vwap` | Quote volume over base volume |
| `ofi` | Order-flow imbalance: net signed volume over total volume, from -1 (all sells) to +1 (all buys) |
| `roll_spread`, `roll_spread_bps` | Roll (1984) effective spread, `2 * sqrt(-cov(dp_t, dp_t-1))` from trade-to-trade price changes; null when the autocovariance is not negative |
| `log_return` | Close-to-close log return over one bar; null when the previous bar had no trades |
| `amihud` | Amihud (2002) illiquidity: absolute return per million USDT traded |
| `realized_vol` | Square root of summed squared bar returns: trailing 30 minutes on bars, the whole day on the daily table; not annualized |

Bars are cut on epoch time, so they do not depend on the session timezone. The
daily Roll spread is estimated from all of a day's trades rather than averaged
across bars, because a single minute can hold too few trades for a stable
covariance.

## Price impact

Kyle's (1985) lambda is the slope of a regression of returns on net order flow:
how far the price moves per unit of net buying. One regression per pair per day
on one-minute bars:

```
ret_bps_t = alpha + lambda * flow_musd_t + e_t
```

`ret_bps` is the bar's log return in basis points and `flow_musd` its net signed
quote volume in millions of USDT, so lambda is **basis points per $1 million of
net buying**.

| Column | Meaning |
|---|---|
| `lambda`, `alpha` | OLS slope and intercept |
| `lambda_se` | classical standard error |
| `lambda_se_hc1`, `lambda_t_hc1` | White (HC1) robust standard error and t-statistic; the same as SAS `PROC REG ... / hcc hccmethod=1` |
| `r2`, `n_obs` | fit and number of bars used (bars without a return drop out) |

The regressions run in Spark: coefficients come from the `regr_*` aggregates in
one pass, and the robust standard error from a second pass over the residuals.
`kyle_lambda(bars, by=[...])` takes any grouping, so the notebook also
estimates one lambda per pair for the whole sample and one per UTC hour of day.

The second stage asks whether lambda rises on volatile days. On the daily
panel, lambda is regressed on realized volatility with volume as a control
(volatile days are also busy days, and a deeper market pushes lambda down), in
levels, in logs (elasticities), and against a dummy for each pair's top fifth
of days by volatility. Robustness checks repeat the log model with the
previous day's volatility, with two-stage least squares using the previous
day's values as instruments, and on split samples of even and odd minutes
(see the results below). Pooled models have pair fixed effects and standard
errors clustered by date; per-pair models use Newey-West.

### Results, 1 Aug to 29 Oct 2026 (180 pair-days)

Lambda is clearly positive every day. The median is 3.0 bps per $1M of net
buying for BTCUSDT (interquartile range 2.3 to 3.9) and 7.7 for ETHUSDT
(5.9 to 10.6), so the same flow moves ETH about 2.5 times as far. The slope is
significant at 5% on 89 of 90 BTC days and all 90 ETH days, and order flow
explains about a quarter of the variance in minute returns (mean R-squared
0.26 and 0.22).

Across days, price impact rises with volatility and falls with volume. The
result survives every check that removes the overlap between lambda and
volatility:

| Log lambda on | Same day | Split sample | Split sample, residual vol | IV (yesterday's values) | Lagged vol, today's volume |
|---|---|---|---|---|---|
| log volatility | 1.63 (0.15) | 1.45 (0.15) | 1.25 (0.16) | 1.36 (0.28) | 0.12 (0.12) |
| log volume | -1.23 (0.13) | -1.14 (0.14) | -1.00 (0.15) | -0.98 (0.19) | -0.13 (0.10) |
| Pair-days | 180 | 180 | 180 | 178 | 178 |

Pooled models with pair fixed effects; standard errors in parentheses,
clustered by date. First-stage F statistics for the IV are 17.8 (volatility)
and 17.1 (volume). Per pair, the split-sample residual-volatility slope is 1.33
(0.13) for BTCUSDT and 1.10 (0.33) for ETHUSDT, and the IV slope is 1.24 (0.48)
and 1.34 (0.48), with Newey-West errors.

What each column removes:

| Model | What it does | Removes shared sampling noise | Removes impact creating volatility |
|---|---|---|---|
| Same day | Lambda, volatility and volume from the same minutes | No | No |
| Split sample | Lambda from even-numbered minutes, volatility and volume from odd-numbered minutes, so no trade or return feeds both | Yes | No |
| Split sample, residual vol | As above, with the odd minutes' residual volatility: the part of returns order flow does not explain | Yes | Mostly |
| IV | Two-stage least squares: today's log volatility and log volume instrumented with yesterday's | Yes | No: lambda is persistent, so yesterday's volatility carries yesterday's lambda, which biases the slope upward |

The two sources of mechanical correlation: lambda is estimated as
`corr(r, q) * sd(r) / sd(q)`, so a day on which flow and returns happen to
line up raises both the lambda estimate and volatility (shared sampling
noise); and when lambda really is high, the same flow moves prices further,
which raises volatility (impact creating volatility).

- **Most of the same-day effect is real.** Removing the shared minutes lowers
  the volatility elasticity from 1.63 to 1.45, and also removing flow-driven
  volatility lowers it to 1.25. The IV, a different method, gives 1.36. So
  sampling noise and impact feeding back into volatility account for roughly a
  quarter of the same-day estimate.
- **It matches Kyle's model.** In Kyle (1985), lambda equals the volatility of
  informed traders' information divided by the size of noise trading, so the
  model predicts an elasticity of +1 on volatility and -1 on noise-trading
  volume. The cleanest test gives +1.25 and -1.00. Residual volatility and
  total volume are only rough proxies for those two quantities, so this is a
  consistency check, not a test of the model.
- **Why the lagged check missed it.** That model put yesterday's volatility
  next to *today's* volume. Daily volatility and volume move together, so once
  today's volume is held fixed, yesterday's volatility adds little information
  about today's volatility, and its slope says little about the effect. The IV
  instruments both variables with their lags and does not have this problem.
- **High-volatility days.** A dummy for each pair's top fifth of days by
  volatility, without a volume control, is only marginally significant
  (pooled: +1.0 bps per $1M, p = 0.06), because volatile days are also busy
  days and the extra depth offsets part of the extra impact.
- **Not causal.** Information arriving probably raises volatility, volume and
  price impact together. That is what Kyle's model describes, but these
  regressions measure association, not the effect of one on another.

## SAS replication

Every estimate in the price-impact section is re-estimated in SAS (SAS
OnDemand for Academics) from the CSV exports, and a parity report puts the two
side by side.

| Python | SAS |
|---|---|
| Daily lambda, Spark `regr_*` aggregates with White (HC1) errors | `PROC REG`, `BY symbol trade_date`, `/ hcc hccmethod=1` |
| Pooled panel: OLS with pair dummies, errors clustered by date | `PROC SURVEYREG`, `CLASS symbol`, `CLUSTER trade_date` |
| Per pair: Newey-West, 5 lags | `PROC AUTOREG`, `COVEST=HAC(KERNEL=BARTLETT, BANDWIDTH=6)` |
| Two-stage least squares, first-stage F | first stages, second stage and a residual pass in SURVEYREG or AUTOREG; F statistic from `CONTRAST` (pooled) and `PROC IML` (per pair, Newey-West Wald test) |
| (coefficient cross-checks) | `PROC PANEL` with `FIXONE`; `PROC SYSLIN` with `2SLS` |

The standard errors are set up to be the same estimator, not just a similar
one: SURVEYREG's default cluster adjustment is the one statsmodels uses, and
SAS's Bartlett kernel weights lag *j* by 1 - *j*/*b*, so a bandwidth of 6 is
Newey-West with 5 lags. `tests/test_parity.py` checks both, and the IV
residual pass, against statsmodels. P-values are not compared: SAS uses t
distributions for robust errors and statsmodels the normal.

**Result:** all 219 estimates agree. The 180 daily lambdas (slope,
intercept, classical and HC1 standard errors, R-squared) and all 39
second-stage slopes and standard errors match to relative differences of
1e-11 or smaller, as do the six first-stage F statistics and the PROC PANEL
and PROC SYSLIN coefficient cross-checks. The full table is in
[docs/sas_parity.md](docs/sas_parity.md).

To run it:

1. Download the exports:
   `databricks fs cp -r dbfs:/Volumes/workspace/microstructure/exports ./exports`
2. In SAS Studio, create `microstructure/exports` and `microstructure/sas` in
   your home folder, upload the CSVs and the `sas/` programs, open
   `run_all.sas` and run it.
3. Download `sas_results.csv` and `sas_lambda_daily.csv` from
   `microstructure/sas_output` into a local `sas_output` folder, then
   `python -m microstructure.parity --exports exports --sas sas_output`, which
   writes `docs/sas_parity.md`.

## Data notes

- Files have no header. Column order: trade id, price, quantity, quote quantity,
  time, is-buyer-maker, is-best-match.
- `time` is epoch **milliseconds** before 2025-01-01 and epoch **microseconds**
  from then on.
- `is_buyer_maker = True` means the buyer's order was resting in the book, so
  the trade was sell-initiated. This gives signed order flow without quote data.
- The raw files carry no symbol column; it comes from the `symbol=` directory
  name.
