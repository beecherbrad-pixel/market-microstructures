"""Tests for the bronze and silver layers, on small hand-made DataFrames."""

from datetime import date, datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from microstructure.bronze import BRONZE_COLUMNS, to_bronze
from microstructure.io import read_raw_trades
from microstructure.schema import RAW_TRADE_SCHEMA
from microstructure.silver import (
    SILVER_COLUMNS,
    dedupe,
    drop_invalid,
    normalize_time,
    sign_trades,
    to_silver,
)

BRONZE_LIKE = StructType(
    RAW_TRADE_SCHEMA.fields + [StructField("symbol", StringType()), StructField("source_file", StringType())]
)

# 2026-08-01 00:00:00 UTC
T0_US = 1785542400000000
T0_MS = T0_US // 1000


def trade(trade_id, price=100.0, qty=1.0, time=T0_US, buyer_maker=False, symbol="BTCUSDT", source="a.csv.gz"):
    """One bronze-shaped row; only the fields a test cares about need to be given."""
    return (trade_id, price, qty, price * qty, time, buyer_maker, True, symbol, source)


@pytest.fixture
def frame(spark):
    def make(*rows):
        return spark.createDataFrame(list(rows), BRONZE_LIKE)

    return make


# --- bronze ------------------------------------------------------------------


def test_bronze_keeps_raw_rows_and_adds_lineage(spark, raw_trades_dir):
    bronze = to_bronze(read_raw_trades(spark, raw_trades_dir))
    rows = bronze.orderBy("trade_id").collect()

    assert bronze.columns == BRONZE_COLUMNS
    assert len(rows) == 5
    assert rows[0]["price"] == 113250.01  # values untouched
    assert rows[0]["time"] == 1785542400000123  # still the raw integer
    assert rows[0]["source_file"].endswith("symbol=BTCUSDT/BTCUSDT-trades-2026-08-01.csv")
    assert all(r["ingested_at"] is not None for r in rows)


def test_bronze_takes_symbol_from_the_path_when_the_reader_does_not(spark, raw_trades_dir):
    # Pointing at the symbol folder itself means Spark sees no partition column.
    raw = read_raw_trades(spark, raw_trades_dir / "symbol=BTCUSDT")
    assert "symbol" not in raw.columns

    bronze = to_bronze(raw)
    assert bronze.columns == BRONZE_COLUMNS
    assert {r["symbol"] for r in bronze.collect()} == {"BTCUSDT"}


# --- silver steps ------------------------------------------------------------


def test_drop_invalid_removes_impossible_trades(frame):
    trades = frame(
        trade(1),
        trade(2, price=0.0),
        trade(3, qty=-0.5),
        trade(None),
        (4, None, 1.0, None, T0_US, False, True, "BTCUSDT", "a"),  # missing price
    )
    assert [r["trade_id"] for r in drop_invalid(trades).collect()] == [1]


def test_dedupe_drops_repeats_but_keeps_other_pairs(frame):
    trades = frame(
        trade(1, source="b.csv.gz"),
        trade(1, source="a.csv.gz"),  # same trade loaded from a second file
        trade(1, source="a.csv.gz"),  # exact duplicate
        trade(1, symbol="ETHUSDT"),  # same id, different pair: a different trade
        trade(2),
    )
    out = {(r["symbol"], r["trade_id"]): r["source_file"] for r in dedupe(trades).collect()}

    assert out == {("BTCUSDT", 1): "a.csv.gz", ("ETHUSDT", 1): "a.csv.gz", ("BTCUSDT", 2): "a.csv.gz"}


def test_dedupe_handles_missing_source_file(frame):
    trades = frame(trade(1, source=None), trade(1, source=None), trade(2, source=None))
    assert dedupe(trades).count() == 2


def test_normalize_time_treats_milliseconds_and_microseconds_alike(frame):
    trades = frame(trade(1, time=T0_MS), trade(2, time=T0_US))
    rows = normalize_time(trades).orderBy("trade_id").collect()

    assert rows[0]["time_us"] == rows[1]["time_us"] == T0_US
    assert rows[0]["event_time"] == rows[1]["event_time"] == datetime(2026, 8, 1)  # session tz is UTC


def test_trade_date_is_utc_whatever_the_session_timezone(spark, frame):
    trades = frame(trade(1, time=T0_US - 1), trade(2, time=T0_US))  # either side of UTC midnight
    spark.conf.set("spark.sql.session.timeZone", "America/Vancouver")
    try:
        dates = [r["trade_date"] for r in normalize_time(trades).orderBy("trade_id").collect()]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    assert dates == [date(2026, 7, 31), date(2026, 8, 1)]


def test_sign_trades_follows_the_aggressor(frame):
    trades = frame(trade(1, qty=2.0, buyer_maker=True), trade(2, qty=3.0, buyer_maker=False))
    rows = sign_trades(trades).orderBy("trade_id").collect()

    assert [r["side"] for r in rows] == [-1, 1]  # buyer resting = seller aggressed
    assert [r["signed_qty"] for r in rows] == [-2.0, 3.0]
    assert [r["signed_quote_qty"] for r in rows] == [-200.0, 300.0]


# --- end to end --------------------------------------------------------------


def test_to_silver_on_the_fixture_file(spark, raw_trades_dir):
    bronze = to_bronze(read_raw_trades(spark, raw_trades_dir))
    loaded_twice = bronze.unionByName(bronze)  # simulate a double load
    silver = to_silver(loaded_twice)

    assert silver.columns == SILVER_COLUMNS
    assert silver.count() == 5
    net = silver.agg({"signed_qty": "sum"}).first()[0]
    # buys 0.01 + 0.25, sells 0.00012 + 0.0045 + 0.00087
    assert net == pytest.approx(0.25451)
    assert {r["trade_date"] for r in silver.collect()} == {date(2026, 8, 1)}


def test_to_silver_accepts_frames_without_lineage(spark, raw_trades_dir):
    silver = to_silver(read_raw_trades(spark, raw_trades_dir))
    assert silver.count() == 5
    assert silver.where("source_file IS NOT NULL").count() == 0
