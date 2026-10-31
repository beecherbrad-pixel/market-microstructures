# Databricks notebook source
# MAGIC %md
# MAGIC # Price impact: Kyle's lambda
# MAGIC
# MAGIC How far does $1 million of net buying move the price, and is that larger on volatile days?
# MAGIC
# MAGIC For each pair and UTC day, regress one-minute returns on net signed order flow:
# MAGIC
# MAGIC $$ret\_bps_t = \alpha + \lambda \cdot flow\_musd_t + \varepsilon_t$$
# MAGIC
# MAGIC so $\lambda$ is basis points per $1M of net buying. Then, across days, relate $\lambda$ to realized volatility
# MAGIC with volume as a control.
# MAGIC
# MAGIC | Output | One row per | Contents |
# MAGIC |---|---|---|
# MAGIC | `gold_lambda_daily` | pair and day | lambda, classical and White (HC1) standard errors, R-squared |
# MAGIC | `gold_lambda_hourly` | pair and UTC hour | lambda pooled over all days, for the intraday profile |
# MAGIC | `gold_lambda_split` | pair and day | lambda from even minutes; volatility, residual volatility and volume from odd minutes |
# MAGIC | `exports` volume | | CSVs for the SAS replication and the Tableau dashboard |
# MAGIC
# MAGIC Run `02_gold` first. The estimator is in `src/microstructure/impact.py` and the second stage in
# MAGIC `src/microstructure/analysis.py`; both are tested in CI.

# COMMAND ----------

# MAGIC %pip install -q statsmodels

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "microstructure")
dbutils.widgets.text("min_obs", "1000")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MIN_OBS = int(dbutils.widgets.get("min_obs"))
NS = f"{CATALOG}.{SCHEMA}"

BARS = f"{NS}.gold_bars"
DAILY = f"{NS}.gold_daily"
LAMBDA_DAILY = f"{NS}.gold_lambda_daily"
LAMBDA_HOURLY = f"{NS}.gold_lambda_hourly"
LAMBDA_SPLIT = f"{NS}.gold_lambda_split"
RUNS = f"{NS}.pipeline_runs"
EXPORTS = f"/Volumes/{CATALOG}/{SCHEMA}/exports"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.exports")

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from microstructure.analysis import high_vol_comparison, impact_panel, volatility_regressions
from microstructure.impact import kyle_lambda, regression_observations, split_sample

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily lambda
# MAGIC
# MAGIC One regression per pair-day, run inside Spark: the coefficients come from Spark's `regr_*` aggregates and
# MAGIC the White standard error from a second pass over the residuals.

# COMMAND ----------

import time

from pyspark.sql import functions as F

bars = spark.table(BARS)

started = time.perf_counter()
kyle_lambda(bars).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LAMBDA_DAILY)
lambda_seconds = time.perf_counter() - started

lam = spark.table(LAMBDA_DAILY)
bar_rows = bars.count()
lambda_rows = lam.count()
daily_rows = spark.table(DAILY).count()
print(f"gold_lambda_daily: {bar_rows:,} bars -> {lambda_rows:,} pair-day regressions in {lambda_seconds:,.1f}s")
assert lambda_rows == daily_rows, f"{lambda_rows} regressions but {daily_rows} pair-days in gold_daily"

# COMMAND ----------

display(
    lam.groupBy("symbol")
    .agg(
        F.count("*").alias("days"),
        F.round(F.expr("percentile(lambda, 0.5)"), 4).alias("lambda_median"),
        F.round(F.expr("percentile(lambda, 0.25)"), 4).alias("lambda_p25"),
        F.round(F.expr("percentile(lambda, 0.75)"), 4).alias("lambda_p75"),
        F.round(F.avg((F.col("lambda") > 0).cast("double")), 3).alias("share_positive"),
        F.round(F.avg((F.col("lambda_t_hc1") > 1.96).cast("double")), 3).alias("share_significant"),
        F.round(F.avg("r2"), 3).alias("r2_mean"),
        F.round(F.avg("n_obs"), 0).alias("bars_per_day"),
    )
    .orderBy("symbol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pooled and intraday lambda
# MAGIC
# MAGIC The same regression on each pair's whole sample, and by UTC hour of day pooled across days.

# COMMAND ----------

display(kyle_lambda(bars, by=["symbol"]).orderBy("symbol"))

started = time.perf_counter()
(
    kyle_lambda(bars.withColumn("hour_utc", F.hour("bar_start")), by=["symbol", "hour_utc"])
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(LAMBDA_HOURLY)
)
hourly_seconds = time.perf_counter() - started
hourly_rows = spark.table(LAMBDA_HOURLY).count()

display(spark.table(LAMBDA_HOURLY).select("symbol", "hour_utc", "lambda", "lambda_se_hc1", "r2").orderBy("symbol", "hour_utc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split sample
# MAGIC
# MAGIC Lambda from even-numbered minutes, volatility and volume from odd-numbered minutes, so the two never share a
# MAGIC trade or a return. `resid_vol_odd` is the volatility of the odd minutes that order flow does not explain.

# COMMAND ----------

started = time.perf_counter()
split_sample(bars).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LAMBDA_SPLIT)
split_seconds = time.perf_counter() - started
split_rows = spark.table(LAMBDA_SPLIT).count()
print(f"gold_lambda_split: {split_rows:,} pair-days in {split_seconds:,.1f}s")
assert split_rows == lambda_rows, f"{split_rows} split rows but {lambda_rows} pair-days"

display(
    spark.table(LAMBDA_SPLIT)
    .join(lam.select("symbol", "trade_date", "lambda"), ["symbol", "trade_date"])
    .groupBy("symbol")
    .agg(
        F.round(F.corr("lambda", "lambda_even"), 3).alias("corr_full_vs_even_lambda"),
        F.round(F.expr("percentile(lambda_even, 0.5)"), 4).alias("lambda_even_median"),
        F.round(F.avg("n_obs_even"), 0).alias("bars_even"),
        F.round(F.avg("n_obs_odd"), 0).alias("bars_odd"),
    )
    .orderBy("symbol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does price impact rise on volatile days?
# MAGIC
# MAGIC The daily panel is a few hundred rows, so it moves to pandas. Each model runs pooled (pair fixed effects,
# MAGIC standard errors clustered by date) and per pair (Newey-West):
# MAGIC
# MAGIC * `levels`: lambda on realized volatility (%) and volume ($bn)
# MAGIC * `logs`: log lambda on log realized volatility and log volume (elasticities)
# MAGIC * `high_vol_day`: lambda on a dummy for the pair's top fifth of days by realized volatility
# MAGIC * `logs_lagged_vol`: as `logs`, with the previous day's realized volatility. Same-day lambda and volatility
# MAGIC   are built from the same minute returns; yesterday's volatility is not, so a positive slope here means the
# MAGIC   link is not only mechanical. Expect it to be smaller, since yesterday's volatility only partly predicts today's.
# MAGIC * `iv_lagged`: two-stage least squares, with today's log volatility and log volume instrumented by yesterday's.
# MAGIC   Same scale as `logs`. `first_stage_f` below about 10 means weak instruments. Lambda is persistent, so
# MAGIC   yesterday's volatility also carries yesterday's lambda, which biases this slope upward.
# MAGIC * `split_sample`: log lambda from even minutes on log volatility and log volume from odd minutes.
# MAGIC * `split_sample_resid`: as `split_sample`, with the odd minutes' residual (non-flow) volatility. Because high
# MAGIC   impact itself raises total volatility, this is the cleanest test of whether impact rises when the market is
# MAGIC   volatile for other reasons.
# MAGIC
# MAGIC Volume is controlled for because volatile days are also busy days, and a deeper market pushes lambda down.

# COMMAND ----------

lam_pd = lam.toPandas()
daily_pd = spark.table(DAILY).toPandas()
split_pd = spark.table(LAMBDA_SPLIT).toPandas()

panel = impact_panel(lam_pd, daily_pd, split=split_pd, min_obs=MIN_OBS)
print(f"{len(panel)} pair-days in the panel ({len(lam_pd) - len(panel)} dropped with fewer than {MIN_OBS} bars)")

results = volatility_regressions(panel)
comparison = high_vol_comparison(panel)

display(results.round(4))

# COMMAND ----------

display(comparison.round(4))

# COMMAND ----------

import matplotlib.pyplot as plt

symbols = sorted(panel["symbol"].unique())
fig, axes = plt.subplots(len(symbols), 1, figsize=(11, 3.2 * len(symbols)), sharex=True, squeeze=False)
for ax, sym in zip(axes[:, 0], symbols):
    d = panel[panel["symbol"] == sym]
    ax.plot(d["trade_date"], d["lambda"], color="#1f5f99", lw=1.4, label="lambda (bps per $1M)")
    ax.set_ylabel("lambda", color="#1f5f99")
    ax.set_title(sym, loc="left", fontsize=11)
    ax2 = ax.twinx()
    ax2.plot(d["trade_date"], d["realized_vol_pct"], color="#c0632b", lw=1.0, alpha=0.8, label="realized vol (%)")
    ax2.set_ylabel("realized vol, %", color="#c0632b")
    hv = d[d["high_vol"] == 1]
    ax.scatter(hv["trade_date"], hv["lambda"], s=14, color="#1f5f99", zorder=3)
axes[0, 0].legend(loc="upper left", frameon=False, fontsize=9)
fig.suptitle("Daily price impact and volatility (dots: top-fifth volatility days)", x=0.01, ha="left")
fig.tight_layout()
fig.savefig(f"{EXPORTS}/lambda_vs_volatility.png", dpi=150)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exports for SAS and Tableau
# MAGIC
# MAGIC Plain CSVs with a header row, written to the `exports` volume. Download them from
# MAGIC **Catalog → microstructure → exports**, or with the CLI:
# MAGIC `databricks fs cp -r dbfs:/Volumes/workspace/microstructure/exports ./exports`.
# MAGIC
# MAGIC | File | Use |
# MAGIC |---|---|
# MAGIC | `regression_bars.csv` | minute-level `ret_bps` and `flow_musd`, to re-run the daily regressions in SAS (`PROC REG; BY symbol trade_date; MODEL ret_bps = flow_musd / hcc hccmethod=1;`) |
# MAGIC | `lambda_daily.csv` | the Python/Spark estimates the SAS ones are compared with |
# MAGIC | `impact_panel.csv` | the daily panel behind the volatility regressions |
# MAGIC | `python_results.csv` | second-stage coefficients, for the parity table |
# MAGIC | `high_vol_comparison.csv`, `lambda_hourly.csv`, `gold_daily.csv` | dashboard inputs |
# MAGIC
# MAGIC Timestamps are UTC.

# COMMAND ----------

regression_bars = (
    regression_observations(bars)
    .select(
        "symbol",
        "trade_date",
        F.date_format("bar_start", "yyyy-MM-dd HH:mm:ss").alias("bar_start_utc"),
        "ret_bps",
        "flow_musd",
        "n_trades",
        F.round(F.col("quote_volume") / 1e6, 6).alias("volume_musd"),
    )
    .orderBy("symbol", "bar_start")
    .toPandas()
)

exports = {
    "regression_bars.csv": regression_bars,
    "lambda_daily.csv": lam_pd.sort_values(["symbol", "trade_date"]),
    "impact_panel.csv": panel,
    "python_results.csv": results,
    "high_vol_comparison.csv": comparison,
    "lambda_hourly.csv": spark.table(LAMBDA_HOURLY).orderBy("symbol", "hour_utc").toPandas(),
    "lambda_split.csv": split_pd.sort_values(["symbol", "trade_date"]),
    "gold_daily.csv": daily_pd.sort_values(["symbol", "trade_date"]),
}
for name, frame in exports.items():
    frame.to_csv(f"{EXPORTS}/{name}", index=False)
    size_mb = os.path.getsize(f"{EXPORTS}/{name}") / 1e6
    print(f"{name:28s} {len(frame):>9,} rows  {size_mb:7.2f} MB")

# COMMAND ----------

# MAGIC %md ## Run log

# COMMAND ----------

from datetime import datetime, timezone

symbols = ",".join(symbols)
now = datetime.now(timezone.utc)
spark.createDataFrame(
    [
        (now, "gold_lambda_daily", bar_rows, lambda_rows, symbols, lambda_seconds),
        (now, "gold_lambda_hourly", bar_rows, hourly_rows, symbols, hourly_seconds),
        (now, "gold_lambda_split", bar_rows, split_rows, symbols, split_seconds),
    ],
    spark.table(RUNS).schema,
).write.mode("append").saveAsTable(RUNS)

display(spark.table(RUNS).orderBy(F.desc("run_at"), "layer"))
