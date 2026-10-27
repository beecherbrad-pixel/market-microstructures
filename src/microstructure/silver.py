"""Silver layer: clean, deduplicated trades with a timestamp and a trade sign.

Each step is a separate function that takes and returns a DataFrame, so it can
be tested on its own and chained with ``DataFrame.transform``:

    silver = bronze.transform(to_silver)

What silver guarantees:
    * one row per (symbol, trade_id), even if a file was loaded twice
    * no rows with a missing or non-positive price or quantity
    * ``time_us``: epoch microseconds for every row, whatever unit the file used
    * ``event_time``: a UTC timestamp, and ``trade_date`` for daily grouping
    * ``side``: +1 for buyer-initiated trades, -1 for seller-initiated ones
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MICROS_PER_DAY = 86_400_000_000

# Binance switched spot trade files from epoch milliseconds to microseconds on
# 2025-01-01. Millisecond values stay below 1e14 until the year 5138 and every
# microsecond value after 1973 is above it, so one threshold tells them apart.
MICROS_THRESHOLD = 10**14

SILVER_COLUMNS = [
    "symbol",
    "trade_id",
    "event_time",
    "trade_date",
    "time_us",
    "price",
    "qty",
    "quote_qty",
    "is_buyer_maker",
    "side",
    "signed_qty",
    "signed_quote_qty",
    "source_file",
]


def drop_invalid(trades: DataFrame) -> DataFrame:
    """Remove rows that cannot be a real trade: missing keys, or price or size not above zero."""
    return trades.where(
        F.col("trade_id").isNotNull()
        & F.col("time").isNotNull()
        & F.col("is_buyer_maker").isNotNull()
        & (F.col("price") > 0)
        & (F.col("qty") > 0)
    )


def dedupe(trades: DataFrame) -> DataFrame:
    """Keep one row per (symbol, trade_id).

    Binance trade ids are unique within a pair, so a repeated id means the same
    trade was loaded twice (for example, a daily file and the monthly file that
    covers the same day). The copy from the alphabetically first source file is
    kept, with the remaining columns as tie-breakers, so reruns give the same
    result.
    """
    if "source_file" not in trades.columns:
        return trades.dropDuplicates(["symbol", "trade_id"])
    first = Window.partitionBy("symbol", "trade_id").orderBy(
        F.col("source_file").asc_nulls_last(), F.col("time"), F.col("price"), F.col("qty")
    )
    return trades.withColumn("_rank", F.row_number().over(first)).where("_rank = 1").drop("_rank")


def normalize_time(trades: DataFrame) -> DataFrame:
    """Convert the raw ``time`` column to microseconds, a timestamp, and a date."""
    time_us = F.when(F.col("time") >= MICROS_THRESHOLD, F.col("time")).otherwise(F.col("time") * 1000)
    return (
        trades.withColumn("time_us", time_us)
        .withColumn("event_time", F.timestamp_micros("time_us"))
        # The UTC date, worked out from the integer so it does not depend on the
        # session timezone. UTC is also how Binance splits its daily files.
        .withColumn("trade_date", F.date_add(F.lit("1970-01-01").cast("date"), F.floor(F.col("time_us") / MICROS_PER_DAY).cast("int")))
    )


def sign_trades(trades: DataFrame) -> DataFrame:
    """Add the trade sign and signed volume.

    ``is_buyer_maker`` is True when the buyer's order was resting in the book,
    so the seller crossed the spread: a sell-initiated trade (side -1).
    Otherwise the buyer was the aggressor (side +1). Summing ``signed_qty`` over
    a bar gives net order flow, the input to Kyle's lambda.
    """
    side = F.when(F.col("is_buyer_maker"), F.lit(-1)).otherwise(F.lit(1)).cast("int")
    return (
        trades.withColumn("side", side)
        .withColumn("signed_qty", F.col("side") * F.col("qty"))
        .withColumn("signed_quote_qty", F.col("side") * F.col("quote_qty"))
    )


def to_silver(bronze: DataFrame) -> DataFrame:
    """Bronze rows to silver rows: the four steps above, in order."""
    if "source_file" not in bronze.columns:
        bronze = bronze.withColumn("source_file", F.lit(None).cast("string"))
    return (
        bronze.transform(drop_invalid)
        .transform(dedupe)
        .transform(normalize_time)
        .transform(sign_trades)
        .select(*SILVER_COLUMNS)
    )
