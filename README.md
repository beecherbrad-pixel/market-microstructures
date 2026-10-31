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
| Analysis | Kyle's lambda by pair and day | Planned |

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
notebooks/
    01_bronze_silver.py Databricks notebook: volume -> bronze -> silver
    02_gold.py          Databricks notebook: silver -> gold bars and daily
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

To run both notebooks in order on a schedule or after each upload, create a
job under **Jobs & Pipelines** with two notebook tasks, `02_gold` depending on
`01_bronze_silver`.

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

## Data notes

- Files have no header. Column order: trade id, price, quantity, quote quantity,
  time, is-buyer-maker, is-best-match.
- `time` is epoch **milliseconds** before 2025-01-01 and epoch **microseconds**
  from then on.
- `is_buyer_maker = True` means the buyer's order was resting in the book, so
  the trade was sell-initiated. This gives signed order flow without quote data.
- The raw files carry no symbol column; it comes from the `symbol=` directory
  name.
