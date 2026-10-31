"""Tests for the second stage, on a synthetic panel with known coefficients."""

from datetime import date, timedelta

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("statsmodels")

from microstructure.analysis import (
    RESULT_COLUMNS,
    _controls,
    high_vol_comparison,
    impact_panel,
    volatility_regressions,
)


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


# --- instrumental variables ---------------------------------------------------


def endogenous(days=400, seed=3):
    """A panel where same-day OLS is biased and lagged instruments are valid.

    log lambda = 1.0 * log rv - 0.5 * log volume + u, and today's volatility
    shock is correlated with u (as with shared sampling noise), while yesterday's
    values are not.
    """
    rng = np.random.default_rng(seed)
    lam_rows, daily_rows = [], []
    for symbol, fe in [("BTCUSDT", 0.0), ("ETHUSDT", 1.0)]:
        lrv, lvol = np.log(0.02), np.log(2e9)
        for i in range(days):
            u = rng.normal(0, 0.3)
            lrv = np.log(0.02) + 0.8 * (lrv - np.log(0.02)) + 0.6 * u + rng.normal(0, 0.15)
            lvol = np.log(2e9) + 0.7 * (lvol - np.log(2e9)) + 0.5 * (lrv - np.log(0.02)) + rng.normal(0, 0.15)
            day = date(2026, 1, 1) + timedelta(days=i)
            lam = np.exp(fe + 1.0 * lrv - 0.5 * lvol + 15 + u)
            lam_rows.append({"symbol": symbol, "trade_date": day, "n_obs": 1440, "lambda": lam})
            daily_rows.append(
                {
                    "symbol": symbol,
                    "trade_date": day,
                    "realized_vol": np.exp(lrv),
                    "quote_volume": np.exp(lvol),
                    "n_trades": 1,
                    "ofi": 0.0,
                    "roll_spread_bps": 0.1,
                    "amihud": 0.0,
                    "daily_return": 0.0,
                }
            )
    return impact_panel(pd.DataFrame(lam_rows), pd.DataFrame(daily_rows))


def test_iv_removes_same_day_bias():
    res = volatility_regressions(endogenous()).set_index(["model", "sample", "term"])
    ols = res.loc[("logs", "pooled", "log_realized_vol"), "coef"]
    iv = res.loc[("iv_lagged", "pooled", "log_realized_vol")]

    assert ols > 1.3  # biased upward by the shared shock
    assert iv["coef"] == pytest.approx(1.0, abs=0.15)
    assert res.loc[("iv_lagged", "pooled", "log_volume"), "coef"] == pytest.approx(-0.5, abs=0.15)
    assert iv["first_stage_f"] > 10
    assert np.isnan(iv["r2"])
    assert np.isnan(res.loc[("logs", "pooled", "log_realized_vol"), "first_stage_f"])


def test_iv_matches_textbook_2sls():
    """Coefficients against statsmodels' own 2SLS; clustered errors against the sandwich formula."""
    from statsmodels.sandbox.regression.gmm import IV2SLS

    panel = endogenous(days=150)
    ours = volatility_regressions(panel).set_index(["model", "sample", "term"])

    data = panel.dropna(subset=["log_lambda", "log_realized_vol_lag", "log_volume_lag"]).sort_values("trade_date")
    controls = _controls(data, pooled=True)
    X = data[["log_realized_vol", "log_volume"]].join(controls).to_numpy()
    Z = data[["log_realized_vol_lag", "log_volume_lag"]].join(controls).to_numpy()
    y = data["log_lambda"].to_numpy()

    ref = IV2SLS(y, X, Z).fit()
    for i, term in enumerate(["log_realized_vol", "log_volume"]):
        assert ours.loc[("iv_lagged", "pooled", term), "coef"] == pytest.approx(ref.params[i], rel=1e-8)

    # V = c * A^-1 (sum_g Xh_g' u_g u_g' Xh_g) A^-1, with A = Xh'Xh and Stata's small-sample factor c.
    Xh = Z @ np.linalg.lstsq(Z, X, rcond=None)[0]
    u = y - X @ ref.params
    groups = pd.factorize(data["trade_date"])[0]
    A_inv = np.linalg.inv(Xh.T @ Xh)
    meat = sum(np.outer(Xh[groups == g].T @ u[groups == g], Xh[groups == g].T @ u[groups == g]) for g in np.unique(groups))
    n, k, G = len(y), X.shape[1], len(np.unique(groups))
    V = (G / (G - 1)) * ((n - 1) / (n - k)) * A_inv @ meat @ A_inv
    for i, term in enumerate(["log_realized_vol", "log_volume"]):
        assert ours.loc[("iv_lagged", "pooled", term), "se"] == pytest.approx(np.sqrt(V[i, i]), rel=1e-6)


# --- split sample ------------------------------------------------------------


def test_split_sample_models_need_split_data(panel):
    assert "split_sample" not in set(volatility_regressions(panel)["model"])

    lambdas, daily = synthetic()
    rng = np.random.default_rng(5)
    split = daily[["symbol", "trade_date"]].assign(
        n_obs_even=720,
        lambda_even=lambdas["lambda"] * np.exp(rng.normal(0, 0.05, len(daily))),
        lambda_even_se_hc1=0.1,
        n_obs_odd=720,
        realized_vol_odd=daily["realized_vol"] / np.sqrt(2),
        resid_vol_odd=daily["realized_vol"] * 1e4 / np.sqrt(1440),
        quote_volume_odd=daily["quote_volume"] / 2,
    )
    split.loc[0, "n_obs_even"] = 100  # too short a half-day: drops from the split models only

    res = volatility_regressions(impact_panel(lambdas, daily, split=split))
    pooled = res[(res["model"] == "split_sample") & (res["sample"] == "pooled")].set_index("term")
    assert {"split_sample", "split_sample_resid"} <= set(res["model"])
    assert pooled.loc["log_realized_vol_odd", "n_obs"] == 239
    assert pooled.loc["log_realized_vol_odd", "coef"] > 0
