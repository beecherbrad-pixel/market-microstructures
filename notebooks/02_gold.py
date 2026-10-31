# Databricks notebook source
# MAGIC %md
# MAGIC # Gold bars and daily measures
# MAGIC
# MAGIC Builds two Delta tables from `silver_trades`:
# MAGIC
# MAGIC | Table | One row per | Contents |
# MAGIC |---|---|---|
# MAGIC | `gold_bars` | pair and minute | OHLC, VWAP, buy and sell volume, order-flow imbalance, Roll spread, log return, Amihud illiquidity, 30-minute realized volatility |
# MAGIC | `gold_daily` | pair and UTC day | the same measures over the whole day, with the Roll spread estimated from all of the day's trades |
# MAGIC
# MAGIC Both are rebuilt in full on each run and their row counts and runtimes are appended to `pipeline_runs`.
# MAGIC Run `01_bronze_silver` first. The formulas are in `src/microstructure/gold.py` and tested in CI.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "microstructure")
dbutils.widgets.text("bar_seconds", "60")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BAR_SECONDS = int(dbutils.widgets.get("bar_seconds"))
NS = f"{CATALOG}.{SCHEMA}"

SILVER = f"{NS}.silver_trades"
BARS = f"{NS}.gold_bars"
DAILY = f"{NS}.gold_daily"
RUNS = f"{NS}.pipeline_runs"

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from microstructure.gold import to_gold_bars, to_gold_daily

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bars
# MAGIC
# MAGIC The trade-level step (price change since the previous trade, for the Roll spread) sorts each pair's day
# MAGIC separately, so days are processed in parallel. Everything after that works on bars, which are small:
# MAGIC about 1,440 rows per pair per day.

# COMMAND ----------

import time

from pyspark.sql import functions as F

silver = spark.table(SILVER)

started = time.perf_counter()
(
    to_gold_bars(silver, bar_seconds=BAR_SECONDS)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BARS)
)
bars_seconds = time.perf_counter() - started

silver_rows = silver.count()
bar_rows = spark.table(BARS).count()
print(f"gold_bars: {silver_rows:,} trades -> {bar_rows:,} bars in {bars_seconds:,.1f}s")

# COMMAND ----------

# MAGIC %md ## Daily

# COMMAND ----------

started = time.perf_counter()
(
    to_gold_daily(silver, spark.table(BARS))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DAILY)
)
daily_seconds = time.perf_counter() - started

daily_rows = spark.table(DAILY).count()
print(f"gold_daily: {bar_rows:,} bars -> {daily_rows:,} pair-days in {daily_seconds:,.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC
# MAGIC Bars must account for every silver trade and every unit of volume, and each bar's prices must be consistent.

# COMMAND ----------

bars = spark.table(BARS)

totals = bars.agg(F.sum("n_trades").alias("trades"), F.sum("volume").alias("volume")).first()
silver_volume = silver.agg(F.sum("qty")).first()[0]
assert totals["trades"] == silver_rows, f"bars hold {totals['trades']:,} trades, silver has {silver_rows:,}"
assert abs(totals["volume"] - silver_volume) <= 1e-9 * silver_volume, "bar volume does not add up to silver volume"

bad = bars.where(
    "low > high OR open NOT BETWEEN low AND high OR close NOT BETWEEN low AND high "
    "OR vwap < low - 1e-9 OR vwap > high + 1e-9 OR abs(ofi) > 1 + 1e-12"
).count()
assert bad == 0, f"{bad} bars with inconsistent prices or imbalance"

dupes = bars.groupBy("symbol", "bar_start").count().where("count > 1").count()
assert dupes == 0, f"{dupes} duplicate bars"

coverage = bars.groupBy("symbol").agg(
    F.count("*").alias("bars"),
    F.round(F.avg(F.col("log_return").isNotNull().cast("int")), 4).alias("share_with_return"),
    F.round(F.avg(F.col("roll_spread").isNotNull().cast("int")), 4).alias("share_with_roll"),
)
display(coverage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily measures
# MAGIC
# MAGIC Realized volatility is the square root of the sum of squared one-minute returns over the day (not annualized).
# MAGIC Roll spread is in basis points of VWAP; Amihud is absolute return per million USDT traded.

# COMMAND ----------

display(
    spark.table(DAILY)
    .select(
        "symbol",
        "trade_date",
        "n_trades",
        F.round(F.col("quote_volume") / 1e6, 1).alias("volume_musd"),
        F.round("vwap", 2).alias("vwap"),
        F.round("ofi", 4).alias("ofi"),
        F.round(F.col("realized_vol") * 100, 3).alias("realized_vol_pct"),
        F.round("roll_spread_bps", 3).alias("roll_bps"),
        F.round("amihud", 6).alias("amihud"),
    )
    .orderBy("symbol", "trade_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC The most volatile day per pair, minute by minute. Use the chart button on the result to plot `realized_vol`
# MAGIC and `ofi` against `bar_start`.

# COMMAND ----------

from pyspark.sql.window import Window

top_day = (
    spark.table(DAILY)
    .withColumn("rank", F.row_number().over(Window.partitionBy("symbol").orderBy(F.desc("realized_vol"))))
    .where("rank = 1")
    .select("symbol", "trade_date")
)
display(
    bars.join(top_day, ["symbol", "trade_date"])
    .select("symbol", "bar_start", "close", "n_trades", "ofi", "log_return", "realized_vol", "roll_spread_bps", "amihud")
    .orderBy("symbol", "bar_start")
)

# COMMAND ----------

# MAGIC %md ## Run log

# COMMAND ----------

from datetime import datetime, timezone

symbols = ",".join(r["symbol"] for r in bars.select("symbol").distinct().orderBy("symbol").collect())
now = datetime.now(timezone.utc)
spark.createDataFrame(
    [
        (now, "gold_bars", silver_rows, bar_rows, symbols, bars_seconds),
        (now, "gold_daily", bar_rows, daily_rows, symbols, daily_seconds),
    ],
    spark.table(RUNS).schema,
).write.mode("append").saveAsTable(RUNS)

display(spark.table(RUNS).orderBy(F.desc("run_at"), "layer"))
