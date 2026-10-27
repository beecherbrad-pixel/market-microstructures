"""Bronze layer: raw trades, unchanged, plus lineage columns.

Bronze keeps every row exactly as Binance published it. The only additions say
where a row came from and when it was loaded, so any later number can be traced
back to a source file. Cleaning belongs in silver.

On Databricks the bronze table is filled incrementally with Auto Loader (see
``notebooks/01_bronze_silver.py``); this module holds the parts that are the
same everywhere, so they can be tested locally.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from microstructure.schema import RAW_TRADE_SCHEMA

# Column order for the bronze table: the raw columns, the pair, then lineage.
BRONZE_COLUMNS = [f.name for f in RAW_TRADE_SCHEMA.fields] + ["symbol", "source_file", "ingested_at"]


def with_lineage(raw: DataFrame) -> DataFrame:
    """Add ``source_file`` and ``ingested_at`` to a DataFrame read straight from files.

    ``_metadata`` is a hidden column that Spark exposes on file-based reads, so
    this must be applied before any join or aggregation drops it.
    """
    return raw.withColumn("source_file", F.col("_metadata.file_path")).withColumn(
        "ingested_at", F.current_timestamp()
    )


def with_symbol(df: DataFrame) -> DataFrame:
    """Make sure there is a ``symbol`` column, taking it from the ``symbol=`` folder if needed.

    A batch read turns ``symbol=BTCUSDT`` folders into a column by itself.
    Auto Loader only does so when told which folders are partitions, so this
    makes bronze independent of that setting.
    """
    if "symbol" in df.columns:
        return df
    return df.withColumn("symbol", F.regexp_extract("source_file", r"symbol=([^/]+)", 1))


def to_bronze(raw: DataFrame) -> DataFrame:
    """Raw trade files to bronze rows: lineage added, columns in a fixed order."""
    return with_lineage(raw).transform(with_symbol).select(*BRONZE_COLUMNS)
