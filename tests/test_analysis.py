"""Tests for the second stage, on a synthetic panel with known coefficients."""

from datetime import date, timedelta

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("statsmodels")

from microstructure.analysis import RESULT_COLUMNS, high_vol_comparison, impact_panel, volatility_regressions


def synthetic(days=120, seed=0):
    """Two pairs where lambda = base * (1 + 2 * realized vol in %) - 0.3 * volume ($bn), plus noise."""
    rng = np.random.default_rng(seed)
    lam_rows, daily_rows = [], []
    for symbol, base in [("BTCUSDT", 1.0), ("ETHUSDT", 4.0)]:
        for i in range(days):
            day = date(2026, 8, 1) + timedelta(days=i)
            rv = rng.uniform(0.005, 0.04)
            vol = rng.uniform(1e9, 5e9)
            lam = base + 2.0 * rv * 100 - 0.3 * vol / 1e9 + rng.normal(0, 0.05)
            lam_rows.append({"symbol": symbol, "trade_date": day, "n_obs": 1439, "lambda": lam})
            daily_rows.append(
                {
                    "symbol": symbol,
                    "trade_date": day,
                    "realized_vol": rv,
                    "quote_volume": vol,
                    "n_trades": 1,
                    "ofi": 0.0,
                    "roll_spread_bps": 0.1,
                    "amihud": 0.0,
                    "daily_return": 0.0,
                }
            )
    return pd.DataFrame(lam_rows), pd.DataFrame(daily_rows)


@pytest.fixture
def panel():
    return impact_panel(*synthetic())


def test_panel_variables(panel):
    assert len(panel) == 240
    assert panel["realized_vol_pct"].between(0.5, 4).all()
    # Top fifth of days within each pair.
    assert panel.groupby("symbol")["high_vol"].mean().round(2).tolist() == [0.2, 0.2]


def test_short_days_are_dropped():
    lambdas, daily = synthetic(days=10)
    lambdas.loc[0, "n_obs"] = 200
    assert len(impact_panel(lambdas, daily)) == 19


def test_non_positive_lambda_is_missing_in_logs():
    lambdas, daily = synthetic(days=10)
    lambdas.loc[0, "lambda"] = -0.5
    panel = impact_panel(lambdas, daily)
    assert panel["log_lambda"].isna().sum() == 1
    assert len(panel) == 20  # kept for the levels models


def test_levels_regression_recovers_slopes(panel):
    res = volatility_regressions(panel)
    pooled = res[(res["model"] == "levels") & (res["sample"] == "pooled")].set_index("term")

    assert list(res.columns) == RESULT_COLUMNS
    assert set(res["sample"]) == {"pooled", "BTCUSDT", "ETHUSDT"}
    assert pooled.loc["realized_vol_pct", "coef"] == pytest.approx(2.0, abs=0.02)
    assert pooled.loc["volume_busd", "coef"] == pytest.approx(-0.3, abs=0.02)
    assert pooled.loc["realized_vol_pct", "cov_type"] == "cluster(date)"
    assert (res.loc[res["sample"] != "pooled", "cov_type"] == "HAC(5)").all()


def test_high_vol_days_have_higher_lambda(panel):
    cmp = high_vol_comparison(panel).set_index("symbol")
    assert (cmp["days_high_vol"] == 24).all()
    assert (cmp["lambda_ratio"] > 1).all()
    assert (cmp["realized_vol_pct_high_vol"] > cmp["realized_vol_pct_other"]).all()


def test_lagged_volatility_is_previous_calendar_day():
    lambdas, daily = synthetic(days=5)
    daily = daily.drop(index=2)  # BTCUSDT loses 2026-08-03
    panel = impact_panel(lambdas, daily).set_index(["symbol", "trade_date"])
    btc = panel.loc["BTCUSDT"]
    rv = daily.set_index(["symbol", "trade_date"])["realized_vol"]["BTCUSDT"]

    assert np.isnan(btc.loc[date(2026, 8, 1), "realized_vol_lag"])  # first day of the sample
    assert btc.loc[date(2026, 8, 2), "realized_vol_lag"] == rv[date(2026, 8, 1)]
    assert np.isnan(btc.loc[date(2026, 8, 4), "realized_vol_lag"])  # previous day missing: no lag
    assert btc.loc[date(2026, 8, 5), "realized_vol_lag"] == rv[date(2026, 8, 4)]


def test_lagged_model_runs_on_days_with_a_lag(panel):
    res = volatility_regressions(panel)
    lagged = res[(res["model"] == "logs_lagged_vol") & (res["sample"] == "pooled")].set_index("term")
    assert set(lagged.index) == {"log_realized_vol_lag", "log_volume"}
    assert lagged.loc["log_realized_vol_lag", "n_obs"] == 238  # first day of each pair drops
