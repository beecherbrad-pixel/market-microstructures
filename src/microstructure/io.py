"""Reading raw files into Spark DataFrames."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from microstructure.schema import RAW_TRADE_SCHEMA


def read_raw_trades(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read every trade file under ``path`` (plain or gzip CSV).

    ``path`` is the folder that contains the ``symbol=<SYMBOL>`` directories.
    Spark turns those directory names into a ``symbol`` column.
    """
    return spark.read.schema(RAW_TRADE_SCHEMA).csv(str(path), header=False)
