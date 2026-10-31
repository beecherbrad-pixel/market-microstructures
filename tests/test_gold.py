"""Tests for the gold layer: bars and daily measures checked against hand-worked values."""

import math
import statistics
from datetime import date, datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from microstructure.bronze import to_bronze
from microstructure.gold import (
    BAR_COLUMNS,
    DAILY_COLUMNS,
    minute_bars,
    to_gold_bars,
    to_gold_daily,
    with_price_changes,
)
from microstructure.io import read_raw_trades
from microstructure.schema import RAW_TRADE_SCHEMA
from microstructure.silver import to_silver

BRONZE_LIKE = StructType(
    RAW_TRADE_SCHEMA.fields + [StructField("symbol", StringType()), StructField("source_file", StringType())]
)

T0_US = 1785542400000000  # 2026-08-01 00:00:00 UTC
SEC = 1_000_000


def trade(trade_id, price, qty=1.0, sec=0.0, sell=False, symbol="BTCUSDT"):
    """One bronze-shaped row, ``sec`` seconds after T0."""
    return (trade_id, price, qty, price * qty, T0_US + int(sec * SEC), sell, True, symbol, "a.csv.gz")


@pytest.fixture
def silver(spark):
    def make(*rows):
        return to_silver(spark.createDataFrame(list(rows), BRONZE_LIKE))

    return make


@pytest.fixture
def three_bars(silver):
    """Trades in minutes 0, 1 and 3 after T0; minute 2 has none."""
    return silver(
        trade(3, 101.0, 1.0, sec=3),  # deliberately out of order
        trade(1, 100.0, 1.0, sec=1),
        trade(2, 102.0, 2.0, sec=2, sell=True),
        trade(4, 103.0, 1.0, sec=61),
        trade(5, 104.0, 1.0, sec=61),  # same time as 4: higher id is later
        trade(6, 105.0, 1.0, sec=185),
    )


def by_minute(bars):
    return {int((r["bar_start"] - datetime(2026, 8, 1)).total_seconds() // 60): r for r in bars.collect()}


# --- bars --------------------------------------------------------------------


def test_bar_prices_volumes_and_flow(three_bars):
    bars = by_minute(to_gold_bars(three_bars))
    b0 = bars[0]

    assert sorted(bars) == [0, 1, 3]  # no row for a minute without trades
    assert (b0["open"], b0["high"], b0["low"], b0["close"]) == (100.0, 102.0, 100.0, 101.0)
    assert b0["n_trades"] == 3
    assert b0["volume"] == 4.0
    assert b0["quote_volume"] == 405.0
    assert b0["vwap"] == pytest.approx(101.25)
    assert (b0["buy_volume"], b0["sell_volume"]) == (2.0, 2.0)
    assert b0["net_order_flow"] == 0.0
    assert b0["ofi"] == 0.0
    assert bars[1]["close"] == 104.0  # tie on time broken by trade id
    assert bars[1]["ofi"] == 1.0


def test_returns_amihud_and_realized_vol(three_bars):
    bars = by_minute(to_gold_bars(three_bars))
    r1 = math.log(104 / 101)

    assert bars[0]["log_return"] is None  # first bar has nothing to compare with
    assert bars[1]["log_return"] == pytest.approx(r1)
    assert bars[3]["log_return"] is None  # minute 2 is missing, so no one-minute return
    assert bars[1]["amihud"] == pytest.approx(r1 / 207.0 * 1e6)
    assert bars[3]["amihud"] is None
    # Trailing 30 minutes: minute 3's window still holds minute 1's return.
    assert bars[3]["realized_vol"] == pytest.approx(r1)


def test_realized_vol_window_is_clock_time(silver):
    trades = silver(trade(1, 100.0, sec=0), trade(2, 110.0, sec=60), trade(3, 121.0, sec=120))
    bars = by_minute(to_gold_bars(trades, window_bars=1))
    # A one-bar window is just the absolute return of that bar.
    assert bars[2]["realized_vol"] == pytest.approx(math.log(1.1))


def test_roll_spread_on_bid_ask_bounce(silver):
    prices = [100.0, 101.0, 100.0, 101.0, 100.0]
    bars = to_gold_bars(silver(*[trade(i, p, sec=i) for i, p in enumerate(prices, 1)])).first()

    dp = [b - a for a, b in zip(prices, prices[1:])]
    cov = statistics.covariance(dp[1:], dp[:-1])  # sample covariance, as in Spark
    assert cov < 0
    assert bars["roll_spread"] == pytest.approx(2 * math.sqrt(-cov))
    assert bars["roll_spread_bps"] == pytest.approx(2 * math.sqrt(-cov) / bars["vwap"] * 1e4)


def test_roll_spread_is_null_without_negative_autocovariance(silver):
    trending = [trade(i, 100.0 + i, sec=i) for i in range(1, 6)]
    assert to_gold_bars(silver(*trending)).first()["roll_spread"] is None


def test_price_changes_restart_each_day(silver):
    trades = silver(trade(1, 100.0, sec=-1), trade(2, 101.0, sec=0), trade(3, 103.0, sec=1))
    rows = with_price_changes(trades).orderBy("trade_id").collect()
    assert [r["dp"] for r in rows] == [None, None, 2.0]  # trade 2 opens 2026-08-01


def test_bar_length_must_divide_a_day(three_bars):
    with pytest.raises(ValueError):
        minute_bars(with_price_changes(three_bars), bar_seconds=7)


def test_five_minute_bars(three_bars):
    bars = to_gold_bars(three_bars, bar_seconds=300).collect()
    assert len(bars) == 1
    assert (bars[0]["open"], bars[0]["close"], bars[0]["n_trades"]) == (100.0, 105.0, 6)


def test_pairs_are_kept_apart(silver):
    trades = silver(trade(1, 100.0, sec=1), trade(1, 2000.0, sec=1, symbol="ETHUSDT"))
    bars = {r["symbol"]: r["close"] for r in to_gold_bars(trades).collect()}
    assert bars == {"BTCUSDT": 100.0, "ETHUSDT": 2000.0}


# --- daily -------------------------------------------------------------------


def test_daily_summary(silver, three_bars):
    next_day = silver(trade(7, 106.0, sec=86_400 + 5))
    trades = three_bars.unionByName(next_day)
    bars = to_gold_bars(trades)
    daily = {r["trade_date"]: r for r in to_gold_daily(trades, bars).collect()}
    d1 = daily[date(2026, 8, 1)]
    r1 = math.log(104 / 101)

    assert to_gold_daily(trades, bars).columns == DAILY_COLUMNS
    assert sorted(daily) == [date(2026, 8, 1), date(2026, 8, 2)]
    assert (d1["n_bars"], d1["n_trades"]) == (3, 6)
    assert (d1["open"], d1["high"], d1["low"], d1["close"]) == (100.0, 105.0, 100.0, 105.0)
    assert d1["volume"] == 7.0
    assert d1["vwap"] == pytest.approx((405 + 207 + 105) / 7)
    assert d1["ofi"] == pytest.approx(3 / 7)  # buys 5, sells 2
    assert d1["daily_return"] == pytest.approx(r1)
    assert d1["realized_vol"] == pytest.approx(r1)
    assert d1["amihud"] == pytest.approx(r1 / 207.0 * 1e6)
    assert daily[date(2026, 8, 2)]["n_trades"] == 1


# --- end to end --------------------------------------------------------------


def test_gold_conserves_trades_and_volume(spark, raw_trades_dir):
    silver = to_silver(to_bronze(read_raw_trades(spark, raw_trades_dir)))
    bars = to_gold_bars(silver)
    daily = to_gold_daily(silver, bars)

    assert bars.columns == BAR_COLUMNS
    totals = bars.agg({"n_trades": "sum", "volume": "sum"}).first()
    assert totals["sum(n_trades)"] == silver.count()
    assert totals["sum(volume)"] == pytest.approx(silver.agg({"qty": "sum"}).first()[0])
    assert daily.first()["n_trades"] == silver.count()
