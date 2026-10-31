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

LAMBDA_COLUMNS = [
    "n_obs",
    "alpha",
    "lambda",
    "lambda_se",
    "lambda_se_hc1",
    "lambda_t_hc1",
    "r2",
    "flow_mean",
    "flow_sd",
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
    fit = fit.withColumn("lambda_se", F.when(usable, F.sqrt(rss / dof / F.col("_sxx"))))

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
