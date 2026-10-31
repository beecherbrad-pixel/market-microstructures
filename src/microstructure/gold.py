"""Gold layer: time bars and daily liquidity measures built from silver trades.

Two tables come out of this module:

``to_gold_bars``   one row per symbol per bar (one minute by default)
``to_gold_daily``  one row per symbol per UTC day

Measures, and where they come from:

* **VWAP**: quote volume over base volume, the average price paid per unit.
* **Order-flow imbalance (OFI)**: net signed volume over total volume. It runs
  from -1 (every trade seller-initiated) to +1 (every trade buyer-initiated).
* **Roll spread** (Roll, 1984): bid-ask bounce makes consecutive trade-to-trade
  price changes negatively correlated, and the effective spread is
  ``2 * sqrt(-cov(dp_t, dp_t-1))``. When the covariance is not negative the
  estimator is undefined and is left null rather than forced to zero.
* **Log return**: close-to-close between adjacent bars. It is null when the
  previous bar is missing (no trades in that interval), so every return covers
  exactly one bar length.
* **Amihud illiquidity** (Amihud, 2002): absolute return per million units of
  quote volume (USDT for the pairs used here). Higher means a given amount of
  trading moves the price more.
* **Realized volatility**: square root of the sum of squared bar returns, over
  a trailing window on the bar table and over the whole day on the daily table.
  It is not annualized.

Bars are cut on epoch time, so they do not depend on the session timezone, and
the bar length must divide a day so that no bar crosses UTC midnight.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MICROS_PER_SECOND = 1_000_000
SECONDS_PER_DAY = 86_400

# Amihud ratios are tiny per unit of volume, so they are quoted per million.
AMIHUD_SCALE = 1e6

BAR_COLUMNS = [
    "symbol",
    "trade_date",
    "bar_start",
    "n_trades",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "vwap",
    "buy_volume",
    "sell_volume",
    "net_order_flow",
    "net_quote_flow",
    "ofi",
    "roll_spread",
    "roll_spread_bps",
    "log_return",
    "amihud",
    "realized_vol",
]

DAILY_COLUMNS = [
    "symbol",
    "trade_date",
    "n_bars",
    "n_trades",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "vwap",
    "net_order_flow",
    "ofi",
    "roll_spread",
    "roll_spread_bps",
    "daily_return",
    "realized_vol",
    "amihud",
]


def _check_bar_seconds(bar_seconds: int) -> None:
    if bar_seconds <= 0 or SECONDS_PER_DAY % bar_seconds:
        raise ValueError(f"bar_seconds must be a positive divisor of {SECONDS_PER_DAY}, got {bar_seconds}")


def _roll(cov: F.Column) -> F.Column:
    """Roll's estimator from the first-order autocovariance; null when it is not negative."""
    return F.when(cov < 0, 2 * F.sqrt(-cov))


def with_price_changes(trades: DataFrame) -> DataFrame:
    """Add ``dp`` (price change since the previous trade) and ``dp_prev`` (the change before that).

    Trades are ordered by time, then trade id, within each symbol and day.
    Partitioning by day as well as symbol lets Spark sort each day in parallel
    instead of pushing a whole pair through one task; the cost is that the first
    two trades of each day have no lagged change.
    """
    order = Window.partitionBy("symbol", "trade_date").orderBy("time_us", "trade_id")
    dp = F.col("price") - F.lag("price").over(order)
    return trades.withColumn("dp", dp).withColumn("dp_prev", F.lag("dp").over(order))


def minute_bars(trades: DataFrame, bar_seconds: int = 60) -> DataFrame:
    """Aggregate trades into fixed-length bars. Expects the ``dp`` columns from ``with_price_changes``.

    Only bars with at least one trade appear; an interval with no trades has
    no row.
    """
    _check_bar_seconds(bar_seconds)
    bar_us = bar_seconds * MICROS_PER_SECOND
    # Ties in time are broken by trade id, so open and close are well defined.
    order = F.struct("time_us", "trade_id")
    bar_start_us = F.floor(F.col("time_us") / bar_us) * bar_us

    return (
        trades.withColumn("bar_start_us", bar_start_us)
        .groupBy("symbol", "trade_date", "bar_start_us")
        .agg(
            F.count("*").alias("n_trades"),
            F.min_by("price", order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order).alias("close"),
            F.sum("qty").alias("volume"),
            F.sum("quote_qty").alias("quote_volume"),
            F.sum(F.when(F.col("side") == 1, F.col("qty")).otherwise(0.0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == -1, F.col("qty")).otherwise(0.0)).alias("sell_volume"),
            F.sum("signed_qty").alias("net_order_flow"),
            F.sum("signed_quote_qty").alias("net_quote_flow"),
            # covar_samp skips rows where either value is null.
            F.covar_samp("dp", "dp_prev").alias("roll_cov"),
        )
        .withColumn("bar_start", F.timestamp_micros("bar_start_us"))
        .withColumn("vwap", F.col("quote_volume") / F.col("volume"))
        .withColumn("ofi", F.col("net_order_flow") / F.col("volume"))
        .withColumn("roll_spread", _roll(F.col("roll_cov")))
        .withColumn("roll_spread_bps", F.col("roll_spread") / F.col("vwap") * 1e4)
    )


def add_bar_returns(bars: DataFrame, bar_seconds: int = 60, window_bars: int = 30) -> DataFrame:
    """Add the close-to-close log return, Amihud illiquidity and trailing realized volatility.

    ``realized_vol`` covers the last ``window_bars`` bar lengths of clock time
    (30 minutes by default), counting only bars that have a return.

    The bar table is small (about 1,440 rows per pair per day), so ordering a
    whole symbol in one window is cheap here, unlike at the trade level.
    """
    _check_bar_seconds(bar_seconds)
    bar_us = bar_seconds * MICROS_PER_SECOND
    by_time = Window.partitionBy("symbol").orderBy("bar_start_us")
    trailing = by_time.rangeBetween(-(window_bars - 1) * bar_us, 0)

    prev_close = F.lag("close").over(by_time)
    prev_start = F.lag("bar_start_us").over(by_time)
    log_return = F.when(F.col("bar_start_us") - prev_start == bar_us, F.log(F.col("close") / prev_close))

    return (
        bars.withColumn("log_return", log_return)
        .withColumn("amihud", F.abs("log_return") / F.col("quote_volume") * AMIHUD_SCALE)
        .withColumn("realized_vol", F.sqrt(F.sum(F.col("log_return") ** 2).over(trailing)))
    )


def to_gold_bars(silver: DataFrame, bar_seconds: int = 60, window_bars: int = 30) -> DataFrame:
    """Silver trades to gold bars."""
    return (
        silver.transform(with_price_changes)
        .transform(minute_bars, bar_seconds)
        .transform(add_bar_returns, bar_seconds, window_bars)
        .select(*BAR_COLUMNS)
    )


def to_gold_daily(silver: DataFrame, bars: DataFrame) -> DataFrame:
    """One row per symbol per day, from silver trades and the gold bars built from them.

    The Roll spread is re-estimated from all of the day's trades rather than
    averaged across bars, since a one-minute bar can hold too few trades for a
    stable covariance. Realized volatility and the daily return are built from
    bar returns, and Amihud is the day's average bar ratio.
    """
    if "dp" not in silver.columns:
        silver = with_price_changes(silver)

    roll = (
        silver.groupBy("symbol", "trade_date")
        .agg(F.covar_samp("dp", "dp_prev").alias("roll_cov"))
        .withColumn("roll_spread", _roll(F.col("roll_cov")))
    )

    order = F.col("bar_start")
    daily = (
        bars.groupBy("symbol", "trade_date")
        .agg(
            F.count("*").alias("n_bars"),
            F.sum("n_trades").alias("n_trades"),
            F.min_by("open", order).alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", order).alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("quote_volume").alias("quote_volume"),
            F.sum("net_order_flow").alias("net_order_flow"),
            # Sum of the day's one-bar returns, so it matches realized_vol exactly.
            # Crypto trades round the clock, so the first bar's return reaches back
            # across midnight to the previous day's last bar when that day is loaded.
            F.sum("log_return").alias("daily_return"),
            F.sqrt(F.sum(F.col("log_return") ** 2)).alias("realized_vol"),
            F.avg("amihud").alias("amihud"),
        )
        .withColumn("vwap", F.col("quote_volume") / F.col("volume"))
        .withColumn("ofi", F.col("net_order_flow") / F.col("volume"))
    )

    return (
        daily.join(roll, ["symbol", "trade_date"], "left")
        .withColumn("roll_spread_bps", F.col("roll_spread") / F.col("vwap") * 1e4)
        .select(*DAILY_COLUMNS)
    )
