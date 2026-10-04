"""Spark tests: reading raw trade files and the smoke-test summary.

The fixture under tests/fixtures/trades is synthetic. It follows Binance's
documented column order and microsecond timestamps, but the values are made up.
"""

from datetime import datetime, timezone

from microstructure.io import read_raw_trades
from microstructure.smoke import summarize, time_unit, to_utc


def test_read_raw_trades_applies_the_schema(spark, raw_trades_dir):
    trades = read_raw_trades(spark, raw_trades_dir)

    assert dict(trades.dtypes) == {
        "trade_id": "bigint",
        "price": "double",
        "qty": "double",
        "quote_qty": "double",
        "time": "bigint",
        "is_buyer_maker": "boolean",
        "is_best_match": "boolean",
        "symbol": "string",  # from the symbol=BTCUSDT directory name
    }
    assert trades.count() == 5


def test_read_raw_trades_parses_values(spark, raw_trades_dir):
    first = read_raw_trades(spark, raw_trades_dir).orderBy("trade_id").first()

    assert first["trade_id"] == 5120000001
    assert first["price"] == 113250.01
    assert first["qty"] == 0.00012
    assert first["time"] == 1785542400000123
    assert first["is_buyer_maker"] is True
    assert first["symbol"] == "BTCUSDT"


def test_summarize(spark, raw_trades_dir):
    summary = summarize(read_raw_trades(spark, raw_trades_dir))

    assert summary["rows"] == 5
    assert summary["symbols"] == 1
    assert summary["null_prices"] == 0
    assert summary["sell_initiated_share"] == 0.6  # 3 of 5 rows have is_buyer_maker = True
    assert summary["first_trade_utc"] == datetime(2026, 8, 1, 0, 0, 0, 123, tzinfo=timezone.utc)


def test_time_unit_separates_milliseconds_from_microseconds():
    assert time_unit(1735689599999) == "ms"  # 2024-12-31 23:59:59.999, last ms-era instant
    assert time_unit(1735689600000000) == "us"  # 2025-01-01 00:00:00, first us-era instant
    assert to_utc(1735689599999).year == 2024
    assert to_utc(1735689600000000) == datetime(2025, 1, 1, tzinfo=timezone.utc)
