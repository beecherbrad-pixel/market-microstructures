"""Explicit schema for Binance spot trade files.

Declaring the schema, instead of letting Spark infer it, avoids a full extra
pass over the data and stops a malformed file from silently changing a column
type.

Column order is fixed by Binance; the files have no header row:
    trade id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch

``time`` is kept as the raw integer. It is epoch milliseconds before
2025-01-01 and epoch microseconds from then on, so converting it to a timestamp
is a cleaning decision that belongs in the silver layer, not here.
"""

from pyspark.sql.types import BooleanType, DoubleType, LongType, StructField, StructType

RAW_TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", LongType()),
        StructField("price", DoubleType()),
        StructField("qty", DoubleType()),  # base asset, e.g. BTC
        StructField("quote_qty", DoubleType()),  # quote asset, e.g. USDT
        StructField("time", LongType()),
        # True when the buyer's order was resting in the book, i.e. the seller
        # crossed the spread: a sell-initiated trade.
        StructField("is_buyer_maker", BooleanType()),
        StructField("is_best_match", BooleanType()),
    ]
)
