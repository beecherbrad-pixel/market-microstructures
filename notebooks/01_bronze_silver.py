# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze and silver trades
# MAGIC
# MAGIC Loads Binance trade files from a Unity Catalog volume into two Delta tables:
# MAGIC
# MAGIC | Table | Contents | How it is filled |
# MAGIC |---|---|---|
# MAGIC | `bronze_trades` | Raw rows, unchanged, plus `source_file` and `ingested_at` | Auto Loader, incremental: only new files are read |
# MAGIC | `silver_trades` | Valid, deduplicated rows with `event_time`, `trade_date` and signed volume | Rebuilt from bronze on each run |
# MAGIC
# MAGIC Each run also appends its row counts and runtimes to `pipeline_runs`, which is where the numbers for the write-up come from.
# MAGIC
# MAGIC The logic lives in `src/microstructure/` and is unit-tested in CI; this notebook only wires it together.
# MAGIC
# MAGIC **Before the first run:** upload the `.csv.gz` files to
# MAGIC `/Volumes/<catalog>/<schema>/raw/trades/symbol=<SYMBOL>/` (the setup cell below creates the volume).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "microstructure")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NS = f"{CATALOG}.{SCHEMA}"

RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/trades"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_trades"
BRONZE = f"{NS}.bronze_trades"
SILVER = f"{NS}.silver_trades"
RUNS = f"{NS}.pipeline_runs"

# COMMAND ----------

# Make the package in this Git folder importable. The notebook runs from
# notebooks/, so the package is one level up under src/.
import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from microstructure.bronze import to_bronze
from microstructure.schema import RAW_TRADE_SCHEMA
from microstructure.silver import to_silver

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.raw COMMENT 'Binance trade files as downloaded'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.checkpoints COMMENT 'Auto Loader state'")
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {RUNS} (
        run_at TIMESTAMP, layer STRING, rows_in BIGINT, rows_out BIGINT,
        symbols STRING, seconds DOUBLE)"""
)

dbutils.fs.mkdirs(RAW_DIR)
files = [f.path for d in dbutils.fs.ls(RAW_DIR) if d.isDir() for f in dbutils.fs.ls(d.path)]
print(f"{len(files)} files under {RAW_DIR}")
assert files, f"No files yet: upload them to {RAW_DIR}/symbol=<SYMBOL>/ and rerun"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Auto Loader
# MAGIC
# MAGIC Auto Loader remembers which files it has already loaded (in the checkpoint volume), so uploading another month and
# MAGIC rerunning this cell loads only the new files. `availableNow` processes everything waiting and then stops, which is
# MAGIC the trigger serverless compute supports.
# MAGIC
# MAGIC To reload from scratch, drop `bronze_trades` and delete the checkpoint folder.

# COMMAND ----------

import time

from pyspark.sql import functions as F

started = time.perf_counter()

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "false")
    .option("pathGlobFilter", "*.csv*")
    .schema(RAW_TRADE_SCHEMA)
    .load(RAW_DIR)
)

query = (
    to_bronze(raw_stream)
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE)
)
query.awaitTermination()

bronze_seconds = time.perf_counter() - started
bronze_new_rows = sum(p["numInputRows"] for p in query.recentProgress)
print(f"bronze: {bronze_new_rows:,} new rows in {bronze_seconds:,.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC
# MAGIC Rebuilt in full from bronze: drop impossible rows, keep one row per `(symbol, trade_id)`, convert timestamps
# MAGIC (milliseconds before 2025, microseconds after) and sign each trade by its aggressor.

# COMMAND ----------

started = time.perf_counter()

bronze = spark.table(BRONZE)
(
    bronze.transform(to_silver)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

silver_seconds = time.perf_counter() - started
bronze_rows = bronze.count()
silver_rows = spark.table(SILVER).count()
print(f"silver: {bronze_rows:,} bronze rows -> {silver_rows:,} silver rows in {silver_seconds:,.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC Liquid clustering on `(symbol, trade_date)` keeps each pair's days together on disk, so the gold layer's
# MAGIC per-pair, per-day reads skip most files. `OPTIMIZE` applies it to existing data.

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER} CLUSTER BY (symbol, trade_date)")
spark.sql(f"OPTIMIZE {SILVER}")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

dupes = spark.table(SILVER).groupBy("symbol", "trade_id").count().where("count > 1").count()
assert dupes == 0, f"{dupes} duplicate (symbol, trade_id) keys in silver"

bad_side = spark.table(SILVER).where("side NOT IN (-1, 1) OR event_time IS NULL").count()
assert bad_side == 0, f"{bad_side} rows with a bad side or missing time"

print(f"dropped as invalid or duplicate: {bronze_rows - silver_rows:,}")

# COMMAND ----------

display(
    spark.table(SILVER)
    .groupBy("symbol", "trade_date")
    .agg(
        F.count("*").alias("trades"),
        F.round(F.sum("quote_qty") / 1e6, 1).alias("volume_musd"),
        F.round(F.avg((F.col("side") == -1).cast("int")), 3).alias("sell_share"),
        F.round(F.sum("signed_qty"), 2).alias("net_order_flow"),
    )
    .orderBy("symbol", "trade_date")
)

# COMMAND ----------

# MAGIC %md ## Run log

# COMMAND ----------

from datetime import datetime, timezone

symbols = ",".join(r["symbol"] for r in spark.table(SILVER).select("symbol").distinct().orderBy("symbol").collect())
now = datetime.now(timezone.utc)
spark.createDataFrame(
    [
        (now, "bronze", bronze_new_rows, bronze_rows, symbols, bronze_seconds),
        (now, "silver", bronze_rows, silver_rows, symbols, silver_seconds),
    ],
    spark.table(RUNS).schema,
).write.mode("append").saveAsTable(RUNS)

display(spark.table(RUNS).orderBy(F.desc("run_at"), "layer"))

# COMMAND ----------

# Delta keeps a version for every write, so earlier states can be queried with
# SELECT ... FROM silver_trades VERSION AS OF <n>.
display(spark.sql(f"DESCRIBE HISTORY {SILVER}").select("version", "timestamp", "operation", "operationMetrics"))
