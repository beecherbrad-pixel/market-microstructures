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
| Price impact | Kyle's lambda by pair and day, estimated in Spark; second-stage regressions on volatility; CSV exports for SAS | Built; first full run pending |

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
notebooks/
    01_bronze_silver.py Databricks notebook: volume -> bronze -> silver
    02_gold.py          Databricks notebook: silver -> gold bars and daily
    03_price_impact.py  Databricks notebook: lambda tables, regressions, CSV exports
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
of days by volatility. Pooled models have pair fixed effects and standard
errors clustered by date; per-pair models use Newey-West.

## Data notes

- Files have no header. Column order: trade id, price, quantity, quote quantity,
  time, is-buyer-maker, is-best-match.
- `time` is epoch **milliseconds** before 2025-01-01 and epoch **microseconds**
  from then on.
- `is_buyer_maker = True` means the buyer's order was resting in the book, so
  the trade was sell-initiated. This gives signed order flow without quote data.
- The raw files carry no symbol column; it comes from the `symbol=` directory
  name.
