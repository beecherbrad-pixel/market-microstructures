"""Tests for Kyle's lambda: Spark estimates checked against OLS worked out by hand."""

import math
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType

from microstructure.impact import LAMBDA_COLUMNS, kyle_lambda, regression_observations

BARS = StructType(
    [
        StructField("symbol", StringType()),
        StructField("trade_date", DateType()),
        StructField("bar_start", TimestampType()),
        StructField("log_return", DoubleType()),
        StructField("net_quote_flow", DoubleType()),
    ]
)

DAY = date(2026, 8, 1)

# Flow in $ millions and returns in bps, as the regression sees them.
FLOW = [1.0, -2.0, 0.5, 3.0, -1.0, 2.0]
RET = [3.0, -5.0, 2.0, 7.0, -1.0, 4.0]


def ols_by_hand(x, y):
    """Slope, intercept, classical SE and HC1 SE, from the textbook formulas."""
    n = len(x)
    xbar, ybar = sum(x) / n, sum(y) / n
    sxx = sum((xi - xbar) ** 2 for xi in x)
    sxy = sum((xi - xbar) * (yi - ybar) for xi, yi in zip(x, y))
    slope = sxy / sxx
    alpha = ybar - slope * xbar
    e = [yi - alpha - slope * xi for xi, yi in zip(x, y)]
    se = math.sqrt(sum(ei**2 for ei in e) / (n - 2) / sxx)
    hc1 = math.sqrt(n / (n - 2) * sum(((xi - xbar) * ei) ** 2 for xi, ei in zip(x, e))) / sxx
    return slope, alpha, se, hc1


@pytest.fixture
def bars(spark):
    def make(flow, ret, symbol="BTCUSDT", day=DAY):
        start = datetime(day.year, day.month, day.day)
        rows = [
            (symbol, day, start + timedelta(minutes=i), None if r is None else r / 1e4, None if f is None else f * 1e6)
            for i, (f, r) in enumerate(zip(flow, ret))
        ]
        return spark.createDataFrame(rows, BARS)

    return make


def test_matches_ols_by_hand(bars):
    row = kyle_lambda(bars(FLOW, RET)).first()
    slope, alpha, se, hc1 = ols_by_hand(FLOW, RET)

    assert row["n_obs"] == 6
    assert row["lambda"] == pytest.approx(slope)
    assert row["alpha"] == pytest.approx(alpha)
    assert row["lambda_se"] == pytest.approx(se)
    assert row["lambda_se_hc1"] == pytest.approx(hc1)
    assert row["lambda_t_hc1"] == pytest.approx(slope / hc1)
    assert row["flow_mean"] == pytest.approx(sum(FLOW) / 6)


def test_units_are_bps_per_million(bars):
    # Every $1M of net buying moves the price 2.5 bps, exactly.
    flow = [-2.0, -1.0, 0.0, 1.0, 2.0]
    row = kyle_lambda(bars(flow, [2.5 * f + 1 for f in flow])).first()

    assert row["lambda"] == pytest.approx(2.5)
    assert row["alpha"] == pytest.approx(1.0)
    assert row["r2"] == pytest.approx(1.0)
    assert row["lambda_se"] == pytest.approx(0.0, abs=1e-9)


def test_bars_without_a_return_are_dropped(bars):
    df = bars(FLOW + [10.0], RET + [None])
    assert regression_observations(df).count() == 6
    assert kyle_lambda(df).first()["lambda"] == pytest.approx(ols_by_hand(FLOW, RET)[0])


def test_one_regression_per_pair_and_day(bars):
    df = (
        bars(FLOW, RET)
        .unionByName(bars(FLOW, [2 * r for r in RET], symbol="ETHUSDT"))
        .unionByName(bars(FLOW, [-r for r in RET], day=date(2026, 8, 2)))
    )
    out = {(r["symbol"], r["trade_date"]): r["lambda"] for r in kyle_lambda(df).collect()}
    slope = ols_by_hand(FLOW, RET)[0]

    assert kyle_lambda(df).columns == ["symbol", "trade_date", *LAMBDA_COLUMNS]
    assert out[("BTCUSDT", DAY)] == pytest.approx(slope)
    assert out[("ETHUSDT", DAY)] == pytest.approx(2 * slope)
    assert out[("BTCUSDT", date(2026, 8, 2))] == pytest.approx(-slope)


def test_pooled_groups(bars):
    df = bars(FLOW, RET).unionByName(bars(FLOW, RET, day=date(2026, 8, 2)))
    row = kyle_lambda(df, by=["symbol"]).first()
    assert row["n_obs"] == 12
    assert row["lambda"] == pytest.approx(ols_by_hand(FLOW, RET)[0])


def test_too_few_observations_leave_errors_null(bars):
    row = kyle_lambda(bars([1.0, 2.0], [1.0, 3.0])).first()
    assert row["lambda"] == pytest.approx(2.0)
    assert row["lambda_se"] is None
    assert row["lambda_se_hc1"] is None
