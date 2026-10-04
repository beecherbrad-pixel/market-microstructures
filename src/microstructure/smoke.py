"""Sanity check for downloaded trades: does Spark read them, and do they look right?

Usage:
    python -m microstructure.smoke C:\\data\\binance
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from microstructure.io import read_raw_trades


def time_unit(epoch: int) -> str:
    """Guess whether an epoch value is in milliseconds or microseconds.

    Millisecond timestamps stay below 1e14 until the year 5138, and every
    microsecond timestamp after 1973 is above it.
    """
    return "us" if epoch >= 10**14 else "ms"


def to_utc(epoch: int) -> datetime:
    divisor = 1_000_000 if time_unit(epoch) == "us" else 1_000
    return datetime.fromtimestamp(epoch / divisor, tz=timezone.utc)


def summarize(trades: DataFrame) -> dict:
    """One pass over the data: row count, time span, and share of sell-initiated trades."""
    row = trades.agg(
        F.count("*").alias("rows"),
        F.countDistinct("symbol").alias("symbols"),
        F.min("time").alias("first"),
        F.max("time").alias("last"),
        F.avg(F.col("is_buyer_maker").cast("int")).alias("sell_share"),
        F.sum(F.col("price").isNull().cast("int")).alias("null_prices"),
    ).first()
    return {
        "rows": row["rows"],
        "symbols": row["symbols"],
        "first_trade_utc": to_utc(row["first"]) if row["first"] is not None else None,
        "last_trade_utc": to_utc(row["last"]) if row["last"] is not None else None,
        "sell_initiated_share": row["sell_share"],
        "null_prices": row["null_prices"],
    }


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print(__doc__)
        return 2
    spark = SparkSession.builder.master("local[*]").appName("microstructure-smoke").getOrCreate()
    try:
        trades = read_raw_trades(spark, args[0])
        trades.printSchema()
        for key, value in summarize(trades).items():
            print(f"{key:>22}: {value}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
