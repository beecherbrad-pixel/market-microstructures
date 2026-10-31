"""Price impact: Kyle's lambda, estimated from gold bars.

In Kyle (1985) a market maker who cannot tell informed orders from uninformed
ones sets the price as a linear function of net order flow, so the price moves
by ``lambda`` for each unit of net buying. A higher lambda means a less liquid
market: the same order flow moves the price further.

The estimating equation, one observation per bar:

    ret_bps_t = alpha + lambda * flow_musd_t + e_t

``ret_bps`` is the bar's close-to-close log return in basis points and
``flow_musd`` is its net signed quote volume (buyer-initiated minus
seller-initiated) in millions of USDT. So lambda reads as *basis points of price
move per $1 million of net buying*.

The regression runs inside Spark, one per group (by default one per pair per
day), so it scales with the bar table rather than being pulled onto the driver:

* coefficients, R-squared and the classical standard error come from Spark's
  ``regr_*`` aggregates in a single pass;
* the heteroskedasticity-robust (White, HC1) standard error needs residuals, so
  a second pass joins each group's coefficients back onto its bars.

``split_sample`` supports a robustness check in the second stage: it estimates
lambda from even-numbered minutes and realized volatility from odd-numbered
ones, so the two never share data.

HC1 matches what SAS reports with ``PROC REG ... / hcc hccmethod=1``, which is
what the SAS replication uses for the parity check.
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RETURN_TO_BPS = 1e4
FLOW_UNIT = 1e6  # quote currency (USDT) per unit of flow_musd

DEFAULT_GROUPS = ("symbol", "trade_date")
MICROS_PER_SECOND = 1_000_000

LAMBDA_COLUMNS = [
    "n_obs",
    "alpha",
    "lambda",
    "lambda_se",
    "lambda_se_hc1",
    "lambda_t_hc1",
    "r2",
    "resid_sd",
    "flow_mean",
    "flow_sd",
]


SPLIT_COLUMNS = [
    "n_obs_even",
    "lambda_even",
    "lambda_even_se_hc1",
    "n_obs_odd",
    "realized_vol_odd",
    "resid_vol_odd",
    "quote_volume_odd",
]


def regression_observations(bars: DataFrame) -> DataFrame:
    """Bars in regression units: ``ret_bps`` and ``flow_musd``, keeping only bars that have both.

    A bar has no return when the previous bar is missing, so those bars drop out
    here rather than being treated as zero.
    """
    return (
        bars.withColumn("ret_bps", F.col("log_return") * RETURN_TO_BPS)
        .withColumn("flow_musd", F.col("net_quote_flow") / FLOW_UNIT)
        .where(F.col("ret_bps").isNotNull() & F.col("flow_musd").isNotNull())
    )


def kyle_lambda(bars: DataFrame, by: Sequence[str] = DEFAULT_GROUPS) -> DataFrame:
    """One price-impact regression per group of bars.

    ``by`` picks the groups: the default gives one lambda per pair per day;
    ``("symbol",)`` pools each pair's whole sample, and adding an hour column
    gives an intraday profile.

    ``resid_sd`` is the standard deviation of the residuals in bps: the part of
    each bar's return that order flow does not explain.

    Returns the ``by`` columns followed by ``LAMBDA_COLUMNS``. Standard errors
    are null when a group has fewer than three observations or no variation in
    flow.
    """
    by = list(by)
    obs = regression_observations(bars)
    y, x = "ret_bps", "flow_musd"

    fit = (
        obs.groupBy(*by)
        .agg(
            F.count("*").alias("n_obs"),
            F.regr_slope(y, x).alias("lambda"),
            F.regr_intercept(y, x).alias("alpha"),
            F.regr_r2(y, x).alias("r2"),
            F.regr_sxx(y, x).alias("_sxx"),
            F.regr_syy(y, x).alias("_syy"),
            F.regr_sxy(y, x).alias("_sxy"),
            F.avg(x).alias("flow_mean"),
            F.stddev_samp(x).alias("flow_sd"),
        )
    )

    dof = F.col("n_obs") - 2
    usable = (dof > 0) & (F.col("_sxx") > 0)
    # Residual sum of squares from the moment sums; clipped at zero so rounding
    # on a perfect fit cannot give the square root a tiny negative number.
    rss = F.greatest(F.col("_syy") - F.col("_sxy") ** 2 / F.col("_sxx"), F.lit(0.0))
    fit = fit.withColumn("resid_sd", F.when(dof > 0, F.sqrt(rss / dof))).withColumn(
        "lambda_se", F.when(usable, F.col("resid_sd") / F.sqrt(F.col("_sxx")))
    )

    # White (1980) sandwich with the HC1 small-sample factor n / (n - 2):
    # var(lambda) = n/(n-2) * sum((x - xbar)^2 * e^2) / Sxx^2
    resid = F.col(y) - F.col("alpha") - F.col("lambda") * F.col(x)
    meat = (
        obs.join(fit.select(*by, "alpha", "lambda", "flow_mean"), by)
        .groupBy(*by)
        .agg(F.sum(((F.col(x) - F.col("flow_mean")) * resid) ** 2).alias("_meat"))
    )

    hc1 = F.sqrt(F.col("n_obs") / dof * F.col("_meat")) / F.col("_sxx")
    return (
        fit.join(meat, by, "left")
        .withColumn("lambda_se_hc1", F.when(usable, hc1))
        .withColumn("lambda_t_hc1", F.col("lambda") / F.col("lambda_se_hc1"))
        .select(*by, *LAMBDA_COLUMNS)
    )


def split_sample(bars: DataFrame, by: Sequence[str] = DEFAULT_GROUPS, bar_seconds: int = 60) -> DataFrame:
    """Lambda from even-numbered bars; volatility and volume from odd-numbered bars.

    Bars are numbered from the epoch, so every pair and day is split the same
    way and both halves cover the whole day. Each bar's return runs from the
    previous bar's close to its own, so even and odd returns cover disjoint
    intervals, and no trade or return feeds both halves.

    Returns, per group:

    * ``lambda_even``, ``lambda_even_se_hc1``, ``n_obs_even``: Kyle's lambda on the even bars
    * ``realized_vol_odd``: square root of summed squared log returns over the odd bars
    * ``resid_vol_odd``: residual standard deviation (bps) of the same regression on
      the odd bars, which is the volatility that order flow does not explain
    * ``quote_volume_odd``, ``n_obs_odd``: odd-bar quote volume and number of bars with a return
    """
    by = list(by)
    index = F.floor(F.unix_micros("bar_start") / (bar_seconds * MICROS_PER_SECOND))
    numbered = bars.withColumn("_odd", (index % 2).cast("int"))
    even = numbered.where("_odd = 0")
    odd = numbered.where("_odd = 1")

    lam = kyle_lambda(even, by).select(
        *by,
        F.col("lambda").alias("lambda_even"),
        F.col("lambda_se_hc1").alias("lambda_even_se_hc1"),
        F.col("n_obs").alias("n_obs_even"),
    )
    odd_resid = kyle_lambda(odd, by).select(*by, F.col("resid_sd").alias("resid_vol_odd"))
    odd_stats = odd.groupBy(*by).agg(
        F.sqrt(F.sum(F.col("log_return") ** 2)).alias("realized_vol_odd"),
        F.sum("quote_volume").alias("quote_volume_odd"),
        F.count("log_return").alias("n_obs_odd"),
    )
    return (
        lam.join(odd_stats, by, "full")
        .join(odd_resid, by, "left")
        .select(*by, *SPLIT_COLUMNS)
    )
