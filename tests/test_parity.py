"""Tests for the Python/SAS parity report, and for the equivalences the SAS code relies on.

SAS itself cannot run in CI, so the second half re-derives, in numpy, what
each SAS option computes and checks it against statsmodels:

* PROC SURVEYREG's clustered variance, ``G/(G-1) * (n-1)/(n-p)`` times the
  sandwich, against statsmodels' ``cov_type="cluster"``;
* PROC AUTOREG's Bartlett HAC with kernel weight ``1 - j/b`` and
  ``BANDWIDTH=6`` against statsmodels' Newey-West with ``maxlags=5``;
* the third IV pass in ``sas/02_volatility.sas`` (regress
  ``y - sum (x - xhat) * b`` on the fitted values) against ``analysis._fit_iv``.
"""

from datetime import date, timedelta

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
sm = pytest.importorskip("statsmodels.api")

from microstructure.analysis import _fit_iv, volatility_regressions
from microstructure.parity import (
    CROSS_CHECKS,
    cross_check_parity,
    lambda_parity,
    rel_diff,
    report,
    results_parity,
)

from test_analysis import endogenous


def test_rel_diff():
    assert rel_diff([1.0, 0.0, -2.0], [1.0, 0.0, -2.0 * (1 + 1e-9)]).max() < 1e-8
    assert rel_diff([1.0], [1.1])[0] == pytest.approx(0.1 / 1.1)


# --- daily lambda -------------------------------------------------------------


def lambda_frames(days=5):
    rng = np.random.default_rng(1)
    rows = []
    for sym in ["BTCUSDT", "ETHUSDT"]:
        for i in range(days):
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": (date(2026, 8, 1) + timedelta(days=i)).isoformat(),
                    "n_obs": 1439,
                    "alpha": rng.normal(),
                    "lambda": rng.uniform(2, 10),
                    "lambda_se": rng.uniform(0.1, 0.2),
                    "lambda_se_hc1": rng.uniform(0.1, 0.3),
                    "r2": rng.uniform(0.1, 0.3),
                }
            )
    py = pd.DataFrame(rows)
    # SAS layout: PROC REG names, dates as SAS writes them, tiny rounding noise.
    sas = py.rename(columns={"lambda_se_hc1": "HCStdErr"}).copy()
    for col in ["alpha", "lambda", "lambda_se", "HCStdErr", "r2"]:
        sas[col] = sas[col] * (1 + 1e-12)
    sas["lambda_t"] = sas["lambda"] / sas["lambda_se"]
    return py, sas


def test_lambda_parity_matches():
    py, sas = lambda_frames()
    rows, summary = lambda_parity(py, sas)
    assert len(rows) == 10
    assert summary["pair_days"].tolist() == [5, 5]
    assert (summary[["lambda_max_rel", "se_hc1_max_rel"]] < 1e-9).all().all()
    assert (summary["n_obs_mismatches"] == 0).all()
    assert (summary["unmatched_days"] == 0).all()


def test_lambda_parity_flags_differences():
    py, sas = lambda_frames()
    sas.loc[0, "lambda"] *= 1.01
    sas.loc[1, "n_obs"] -= 1
    sas = sas.drop(index=9)
    _, summary = lambda_parity(py, sas)
    s = summary.set_index("symbol")
    assert s.loc["BTCUSDT", "lambda_max_rel"] == pytest.approx(0.01 / 1.01, rel=1e-6)
    assert s.loc["BTCUSDT", "n_obs_mismatches"] == 1
    assert s.loc["ETHUSDT", "unmatched_days"] == 1


def test_missing_hc_column_is_an_error():
    py, sas = lambda_frames()
    with pytest.raises(KeyError):
        lambda_parity(py, sas.drop(columns="HCStdErr"))


# --- second stage -------------------------------------------------------------


@pytest.fixture(scope="module")
def py_results():
    return volatility_regressions(endogenous(days=200))


def fake_sas(py_results):
    """What sas_results.csv looks like when SAS agrees, plus the cross-check rows."""
    sas = py_results.copy()
    sas["coef"] *= 1 + 1e-10
    extra = []
    for sas_model, (py_model, _, _) in CROSS_CHECKS.items():
        rows = py_results[py_results["model"] == py_model].copy()
        # PROC PANEL runs on the pooled panel only, PROC SYSLIN per pair only.
        pooled = rows["sample"] == "pooled"
        rows = rows[pooled] if sas_model == "logs_panel_fixone" else rows[~pooled]
        rows["model"] = sas_model
        rows["se"] *= 0.8  # classical errors: not compared
        extra.append(rows)
    return pd.concat([sas, *extra], ignore_index=True)


def test_results_parity_and_report(py_results):
    sas = fake_sas(py_results)
    res = results_parity(py_results, sas)
    assert len(res) == len(py_results)
    assert (res["_merge"] == "both").all()
    assert res["coef_rel"].max() < 1e-9
    assert res["n_obs_match"].all()

    cross = cross_check_parity(py_results, sas)
    assert set(cross["check"]) == {label for _, label, _ in CROSS_CHECKS.values()}
    assert cross["coef_rel"].max() < 1e-9

    _, lam_summary = lambda_parity(*lambda_frames())
    text = report(lam_summary, res, cross)
    assert "**differs**" not in text
    assert "0 second-stage rows differ; 0 cross-check rows differ." in text
    # IV rows carry the first-stage F statistic for both sides.
    assert text.count(" / ") >= 6


def test_report_flags_a_different_standard_error(py_results):
    sas = fake_sas(py_results)
    sas.loc[0, "se"] *= 1.05
    res = results_parity(py_results, sas)
    _, lam_summary = lambda_parity(*lambda_frames())
    text = report(lam_summary, res, cross_check_parity(py_results, sas))
    assert "1 second-stage rows differ" in text
    assert "**differs**" in text


def test_report_flags_misplaced_cross_check_and_missing_f(py_results):
    sas = fake_sas(py_results)
    # What the first SAS run produced: PANEL rows under the pairs instead of the pooled sample.
    panel = sas["model"] == "logs_panel_fixone"
    sas.loc[panel, "sample"] = "BTCUSDT"
    iv_pairs = (sas["model"] == "iv_lagged") & (sas["sample"] != "pooled")
    sas.loc[iv_pairs, "first_stage_f"] = np.nan
    res = results_parity(py_results, sas)
    _, lam_summary = lambda_parity(*lambda_frames())
    text = report(lam_summary, res, cross_check_parity(py_results, sas))
    assert "0 second-stage rows differ" in text
    assert "4 cross-check rows differ" in text
    assert "4 first-stage F statistics missing from SAS" in text
    assert "| missing |" in text


# --- what the SAS options compute ---------------------------------------------


def design(n=300, seed=0):
    rng = np.random.default_rng(seed)
    X = np.column_stack([np.ones(n), rng.normal(size=n), rng.normal(size=n)])
    e = rng.normal(size=n) * (1 + np.abs(X[:, 1]))
    e = np.convolve(e, [1, 0.6, 0.3], mode="same")  # serial correlation
    y = X @ np.array([1.0, 2.0, -0.5]) + e
    return y, X


def test_surveyreg_cluster_adjustment_matches_statsmodels():
    y, X = design()
    groups = np.repeat(np.arange(100), 3)
    n, p = X.shape
    G = len(np.unique(groups))

    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    u = y - X @ beta
    bread = np.linalg.inv(X.T @ X)
    scores = np.vstack([(X[groups == g] * u[groups == g, None]).sum(axis=0) for g in range(G)])
    V = (G / (G - 1)) * ((n - 1) / (n - p)) * bread @ scores.T @ scores @ bread

    res = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": groups})
    np.testing.assert_allclose(np.sqrt(np.diag(V)), res.bse, rtol=1e-10)


def test_autoreg_bartlett_bandwidth_6_is_newey_west_5_lags():
    y, X = design()
    lags, bandwidth = 5, 6

    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    g = X * (y - X @ beta)[:, None]
    S = g.T @ g
    for j in range(1, len(y)):
        w = max(0.0, 1 - j / bandwidth)  # SAS: Bartlett k(j/b) = 1 - j/b
        if w == 0:
            break
        G = g[j:].T @ g[:-j]
        S += w * (G + G.T)
    bread = np.linalg.inv(X.T @ X)
    V = bread @ S @ bread  # no ADJUSTDF

    res = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    np.testing.assert_allclose(np.sqrt(np.diag(V)), res.bse, rtol=1e-10)


@pytest.mark.parametrize("pooled", [True, False])
def test_sas_iv_passes_match_fit_iv(pooled):
    panel = endogenous(days=300)
    data = panel if pooled else panel[panel["symbol"] == "BTCUSDT"]
    y, endog, instr = "log_lambda", ["log_realized_vol", "log_volume"], ["log_realized_vol_lag", "log_volume_lag"]
    expected, _, _ = _fit_iv(data, y, endog, instr, pooled)

    d = data.dropna(subset=[y, *endog, *instr]).sort_values("trade_date").copy()
    controls = pd.DataFrame({"const": 1.0}, index=d.index)
    if pooled:
        controls = controls.join(pd.get_dummies(d["symbol"], prefix="pair", drop_first=True, dtype=float))
    if pooled:
        kw = {"cov_type": "cluster", "cov_kwds": {"groups": pd.factorize(d["trade_date"])[0]}}
    else:
        kw = {"cov_type": "HAC", "cov_kwds": {"maxlags": 5}}

    # Pass 1: first stages, keep fitted values (OUTPUT P= / PM=).
    Z = d[instr].join(controls)
    for e in endog:
        d[f"hat_{e}"] = sm.OLS(d[e], Z).fit().fittedvalues
    hats = [f"hat_{e}" for e in endog]
    # Pass 2: coefficients.
    b = sm.OLS(d[y], d[hats].join(controls)).fit().params
    # Pass 3: y* = y - sum (x - xhat) * b on the fitted values.
    d["y_star"] = d[y] - sum((d[e] - d[f"hat_{e}"]) * b[f"hat_{e}"] for e in endog)
    third = sm.OLS(d["y_star"], d[hats].join(controls)).fit(**kw)

    for e in endog:
        assert third.params[f"hat_{e}"] == pytest.approx(expected.params[e], rel=1e-10)
        assert third.bse[f"hat_{e}"] == pytest.approx(expected.bse[e], rel=1e-10)
