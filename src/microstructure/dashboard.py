"""Tableau-ready tables for the public dashboard.

Reads the CSVs that ``notebooks/03_price_impact.py`` writes to the ``exports``
volume and writes three flat files that Tableau Public can load directly:

``dash_daily.csv``   one row per pair per day: Kyle's lambda with a 95% HC1
                     confidence interval, liquidity measures, volatility, the
                     high-volatility-day flag from ``analysis.impact_panel``,
                     and volume-adjusted logs for an added-variable plot
``dash_bars.csv``    one row per one-minute bar: return, net order flow, and the
                     cumulative return within the day, for the drill-down
``dash_hourly.csv``  lambda by UTC hour of day, per pair

Added-variable plot
-------------------
A plain scatter of lambda against volatility hides most of the relationship:
volatile days are also busy days, and volume pushes lambda down. The two
``... volume-adjusted`` columns are log lambda and log realized volatility,
each with its within-pair regression on log volume taken out. By the
Frisch-Waugh-Lovell theorem, a least-squares line through them, per pair, has
the same slope as the per-pair ``logs`` model in ``analysis`` (about 1.6), so
a Tableau linear trend line on that scatter shows the regression result.

Columns get readable names with units, so the dashboard needs no renaming.
``dash_bars`` and ``dash_daily`` share ``Pair`` and ``Date``, which is the key
for a Tableau relationship between them.

Usage::

    python -m microstructure.dashboard --exports exports --out exports/tableau
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

Z_95 = 1.959963984540054

KEYS = {"symbol": "Pair", "trade_date": "Date"}

DAILY_COLUMNS = {
    "lambda": "Lambda (bps per $1M)",
    "lambda_ci_low": "Lambda 95% CI low",
    "lambda_ci_high": "Lambda 95% CI high",
    "lambda_t_hc1": "Lambda t-stat (HC1)",
    "lambda_rel_median": "Lambda vs pair median",
    "r2": "R-squared",
    "realized_vol_pct": "Realized vol (%)",
    "vol_percentile": "Vol percentile in pair",
    "high_vol_day": "High-vol day",
    "volume_musd": "Volume ($M)",
    "n_trades": "Trades",
    "ofi": "Order-flow imbalance",
    "roll_spread_bps": "Roll spread (bps)",
    "amihud": "Amihud illiquidity",
    "daily_return_pct": "Daily return (%)",
    "n_obs": "Bars in regression",
    "log_lambda_adj": "Log lambda, volume-adjusted",
    "log_vol_adj": "Log vol, volume-adjusted",
}

BAR_COLUMNS = {
    "bar_start_utc": "Bar start (UTC)",
    "hour_utc": "Hour (UTC)",
    "ret_bps": "Return (bps)",
    "cum_ret_bps": "Cumulative return (bps)",
    "flow_musd": "Net flow ($M)",
    "volume_musd": "Volume ($M)",
    "n_trades": "Trades",
}

HOURLY_COLUMNS = {
    "hour_utc": "Hour (UTC)",
    "lambda": "Lambda (bps per $1M)",
    "lambda_ci_low": "Lambda 95% CI low",
    "lambda_ci_high": "Lambda 95% CI high",
    "r2": "R-squared",
    "n_obs": "Bars in regression",
}


def _add_ci(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["lambda_ci_low"] = out["lambda"] - Z_95 * out["lambda_se_hc1"]
    out["lambda_ci_high"] = out["lambda"] + Z_95 * out["lambda_se_hc1"]
    return out


def _rename(frame: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
    names = {**KEYS, **columns}
    return frame[[c for c in names if c in frame.columns]].rename(columns=names)


def partial_out(frame: pd.DataFrame, y: str, x: str, by: str) -> pd.Series:
    """Residual of ``y`` from an OLS regression on a constant and ``x``, within each ``by`` group."""

    def resid(g: pd.DataFrame) -> pd.Series:
        xc = g[x] - g[x].mean()
        yc = g[y] - g[y].mean()
        return yc - (xc * yc).sum() / (xc * xc).sum() * xc

    return pd.concat([resid(g) for _, g in frame.groupby(by)]).reindex(frame.index)


def daily_table(panel: pd.DataFrame) -> pd.DataFrame:
    """One row per pair-day, from ``impact_panel.csv``."""
    out = _add_ci(panel)
    by_pair = out.groupby("symbol")
    out["lambda_rel_median"] = out["lambda"] / by_pair["lambda"].transform("median")
    out["vol_percentile"] = by_pair["realized_vol"].rank(pct=True)
    out["realized_vol_pct"] = out["realized_vol"] * 100
    out["daily_return_pct"] = out["daily_return"] * 100
    out["volume_musd"] = out["quote_volume"] / 1e6
    for col, adj in [("log_lambda", "log_lambda_adj"), ("log_realized_vol", "log_vol_adj")]:
        out[adj] = partial_out(out, col, "log_volume", by="symbol")
    out["high_vol_day"] = out["high_vol"].map({1: "High-vol day", 0: "Other day"})
    return _rename(out.sort_values(["symbol", "trade_date"]), DAILY_COLUMNS).reset_index(drop=True)


def bars_table(bars: pd.DataFrame) -> pd.DataFrame:
    """One row per one-minute bar, from ``regression_bars.csv``.

    The cumulative return restarts at zero each day, so a line of it against
    bar time is the intraday price path in basis points.
    """
    out = bars.sort_values(["symbol", "bar_start_utc"]).copy()
    out["cum_ret_bps"] = out.groupby(["symbol", "trade_date"])["ret_bps"].cumsum()
    out["hour_utc"] = pd.to_datetime(out["bar_start_utc"]).dt.hour
    return _rename(out, BAR_COLUMNS).reset_index(drop=True)


def hourly_table(hourly: pd.DataFrame) -> pd.DataFrame:
    """Lambda by UTC hour of day, from ``lambda_hourly.csv``."""
    out = _add_ci(hourly).sort_values(["symbol", "hour_utc"])
    return _rename(out, HOURLY_COLUMNS).reset_index(drop=True)


def build(exports: Path) -> dict[str, pd.DataFrame]:
    exports = Path(exports)
    return {
        "dash_daily.csv": daily_table(pd.read_csv(exports / "impact_panel.csv")),
        "dash_bars.csv": bars_table(pd.read_csv(exports / "regression_bars.csv")),
        "dash_hourly.csv": hourly_table(pd.read_csv(exports / "lambda_hourly.csv")),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exports", type=Path, default=Path("exports"))
    parser.add_argument("--out", type=Path, default=Path("exports/tableau"))
    args = parser.parse_args(argv)

    args.out.mkdir(parents=True, exist_ok=True)
    for name, frame in build(args.exports).items():
        frame.to_csv(args.out / name, index=False)
        print(f"{name}: {len(frame):,} rows")


if __name__ == "__main__":
    main()
