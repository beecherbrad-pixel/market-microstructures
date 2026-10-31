"""Second stage: does price impact rise on high-volatility days?

Works on the daily panel, one row per pair per day, which is small enough for
pandas (about 90 days per pair). The inputs are two Spark tables brought to the
driver with ``toPandas()``:

* ``gold_lambda_daily`` from ``impact.kyle_lambda``
* ``gold_daily`` from ``gold.to_gold_daily``

Each specification runs on the pooled panel (with a fixed effect per pair) and
on each pair alone:

``levels``        lambda on realized volatility (%) and volume ($bn)
``logs``          log lambda on log realized volatility and log volume, so the
                  slopes are elasticities; days with lambda <= 0 drop out
``high_vol_day``  lambda on a dummy for the pair's most volatile days
``logs_lagged_vol`` as ``logs``, but with the previous day's realized
                  volatility in place of the same day's

Robustness checks
-----------------
Same-day lambda and realized volatility are built from the same minute returns,
which links them in two ways that are not the economics we want:

1. *Shared sampling noise*: a day on which flow and returns happen to line up
   gives both a higher lambda estimate and a higher volatility.
2. *Impact creates volatility*: when lambda really is high, order flow moves
   prices further, which raises realized volatility. This runs from lambda to
   volatility, not the other way.

The checks, and what each removes:

``logs_lagged_vol``     yesterday's volatility in place of today's (reduced
                        form). Removes 1; 2 survives through persistent lambda.
``iv_lagged``           two-stage least squares: today's log volatility and log
                        volume instrumented with yesterday's. Removes 1 and gives a
                        slope on the same scale as ``logs``. Not 2: lambda is
                        persistent, so yesterday's volatility also carries
                        yesterday's lambda, which biases the slope upward.
``split_sample``        log lambda from even minutes on log volatility and log
                        volume from odd minutes. Removes 1; not 2.
``split_sample_resid``  as ``split_sample``, with the odd minutes' residual
                        volatility, the part order flow does not explain. Removes
                        1 and most of 2: the closest test of whether impact rises
                        when the market is volatile for reasons other than flow.

Volume is a control because volatile days are also busy days, and a deeper
market on busy days pushes lambda the other way; leaving it out would mix the
two effects.

Standard errors: on the pooled panel they are clustered by date, because both
pairs share each day's market-wide news. On a single pair they are
Newey-West (HAC), because daily lambda is persistent.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

KEYS = ["symbol", "trade_date"]

# A full day has 1,440 one-minute bars. Days with far fewer usable bars (a
# partial first or last day of the sample) give noisy estimates.
MIN_OBS = 1_000

HIGH_VOL_QUANTILE = 0.8  # top fifth of days by realized volatility, within each pair
HAC_LAGS = 5

DAILY_FIELDS = ["realized_vol", "quote_volume", "n_trades", "ofi", "roll_spread_bps", "amihud", "daily_return"]

SPECS = {
    "levels": ("lambda", ["realized_vol_pct", "volume_busd"]),
    "logs": ("log_lambda", ["log_realized_vol", "log_volume"]),
    "high_vol_day": ("lambda", ["high_vol"]),
    "logs_lagged_vol": ("log_lambda", ["log_realized_vol_lag", "log_volume"]),
    "split_sample": ("log_lambda_even", ["log_realized_vol_odd", "log_volume_odd"]),
    "split_sample_resid": ("log_lambda_even", ["log_resid_vol_odd", "log_volume_odd"]),
}

# Instrumental-variables models: (outcome, endogenous regressors, instruments).
IV_SPECS = {
    "iv_lagged": ("log_lambda", ["log_realized_vol", "log_volume"], ["log_realized_vol_lag", "log_volume_lag"]),
}

RESULT_COLUMNS = [
    "model", "sample", "term", "coef", "se", "t", "p_value", "n_obs", "r2", "cov_type", "first_stage_f",
]


def impact_panel(
    lambdas: pd.DataFrame,
    daily: pd.DataFrame,
    split: pd.DataFrame | None = None,
    min_obs: int = MIN_OBS,
    high_vol_quantile: float = HIGH_VOL_QUANTILE,
) -> pd.DataFrame:
    """Join daily lambdas to the daily measures and add the regression variables.

    ``split`` is the optional output of ``impact.split_sample``; without it the
    split-sample models are skipped.
    """
    daily = add_previous_day(daily)
    panel = lambdas.merge(daily[KEYS + DAILY_FIELDS + LAG_FIELDS], on=KEYS, how="inner")
    panel = panel[(panel["n_obs"] >= min_obs) & panel["lambda"].notna()].copy()

    panel["realized_vol_pct"] = panel["realized_vol"] * 100
    panel["volume_busd"] = panel["quote_volume"] / 1e9
    positive = panel["lambda"] > 0
    panel["log_lambda"] = np.log(panel["lambda"].where(positive))
    panel["log_realized_vol"] = np.log(panel["realized_vol"])
    panel["log_volume"] = np.log(panel["quote_volume"])
    panel["log_realized_vol_lag"] = np.log(panel["realized_vol_lag"])
    panel["log_volume_lag"] = np.log(panel["quote_volume_lag"])

    if split is not None:
        panel = panel.merge(split, on=KEYS, how="left")
        # Even minutes are half the day, so they need half the bars.
        ok = (panel["n_obs_even"] >= min_obs // 2) & (panel["lambda_even"] > 0)
        panel["log_lambda_even"] = np.log(panel["lambda_even"].where(ok))
        panel["log_realized_vol_odd"] = np.log(panel["realized_vol_odd"])
        panel["log_resid_vol_odd"] = np.log(panel["resid_vol_odd"])
        panel["log_volume_odd"] = np.log(panel["quote_volume_odd"])

    cutoff = panel.groupby("symbol")["realized_vol"].transform(lambda s: s.quantile(high_vol_quantile))
    panel["high_vol"] = (panel["realized_vol"] >= cutoff).astype(int)
    return panel.sort_values(KEYS).reset_index(drop=True)


LAG_FIELDS = ["realized_vol_lag", "quote_volume_lag"]


def add_previous_day(daily: pd.DataFrame) -> pd.DataFrame:
    """Add ``realized_vol_lag`` and ``quote_volume_lag``: the pair's values on the previous calendar day.

    It is taken from the full daily table, before any days are filtered out of
    the panel, and is missing when the previous day is not loaded, so a gap in
    the data never pairs a day with an older one.
    """
    daily = daily.copy()
    dates = pd.to_datetime(daily["trade_date"])
    prev = daily.assign(_date=dates + pd.Timedelta(days=1))[["symbol", "_date", "realized_vol", "quote_volume"]]
    prev = prev.rename(columns={"realized_vol": "realized_vol_lag", "quote_volume": "quote_volume_lag"})
    out = daily.assign(_date=dates).merge(prev, on=["symbol", "_date"], how="left")
    return out.drop(columns="_date")


def _controls(data: pd.DataFrame, pooled: bool) -> pd.DataFrame:
    """Constant, plus pair dummies on the pooled panel."""
    X = pd.DataFrame({"const": 1.0}, index=data.index)
    if pooled:
        X = X.join(pd.get_dummies(data["symbol"], prefix="pair", drop_first=True, dtype=float))
    return X


def _ols(y: pd.Series, X: pd.DataFrame, data: pd.DataFrame, pooled: bool):
    import statsmodels.api as sm

    model = sm.OLS(y.astype(float), X)
    if pooled:
        groups = pd.factorize(data["trade_date"])[0]
        return model.fit(cov_type="cluster", cov_kwds={"groups": groups}), "cluster(date)"
    return model.fit(cov_type="HAC", cov_kwds={"maxlags": HAC_LAGS}), f"HAC({HAC_LAGS})"


def _fit(data: pd.DataFrame, y: str, xs: list[str], pooled: bool):
    data = data.dropna(subset=[y, *xs]).sort_values("trade_date")
    X = data[xs].astype(float).join(_controls(data, pooled))
    return _ols(data[y], X, data, pooled)


def _fit_iv(data: pd.DataFrame, y: str, endog: list[str], instruments: list[str], pooled: bool):
    """Two-stage least squares with the same standard errors as the OLS models.

    First stage: each endogenous regressor on the instruments and controls.
    Second stage: the outcome on the first-stage fitted values and controls.
    The second-stage OLS gets the right coefficients but the wrong residuals
    (it uses fitted values, not the actual regressors), so it is re-run on
    ``fitted part + structural residual``. 2SLS residuals are orthogonal to the
    fitted regressors, so that re-run returns the same coefficients and its
    sandwich standard errors use the structural residuals, which is the
    correct 2SLS covariance.

    Also returns each first stage's F statistic on the excluded instruments
    (robust, same covariance type). Below about 10 the instruments are weak and
    the 2SLS estimate is unreliable.
    """
    data = data.dropna(subset=[y, *endog, *instruments]).sort_values("trade_date")
    controls = _controls(data, pooled)
    Z = data[instruments].astype(float).join(controls)

    fitted, first_f = {}, {}
    for col in endog:
        first, _ = _ols(data[col], Z, data, pooled)
        fitted[col] = first.fittedvalues
        restriction = np.zeros((len(instruments), Z.shape[1]))
        for i, name in enumerate(instruments):
            restriction[i, Z.columns.get_loc(name)] = 1.0
        first_f[col] = float(np.squeeze(first.f_test(restriction).fvalue))

    X_hat = pd.DataFrame(fitted).join(controls)
    beta = _ols(data[y], X_hat, data, pooled)[0].params
    X = data[endog].astype(float).join(controls)
    resid = data[y].astype(float) - X @ beta
    res, cov = _ols(X_hat @ beta + resid, X_hat, data, pooled)
    return res, cov, first_f


def volatility_regressions(panel: pd.DataFrame) -> pd.DataFrame:
    """Every specification on the pooled panel and on each pair, as one tidy table.

    Only the slopes of interest are reported; the constant and pair effects are
    left out.
    """
    samples = [("pooled", panel, True)]
    if panel["symbol"].nunique() > 1:
        samples += [(sym, grp, False) for sym, grp in panel.groupby("symbol", sort=True)]
    else:
        samples = [(panel["symbol"].iloc[0], panel, False)]

    def row(model_name, sample, term, res, cov, r2, first_f=np.nan):
        return {
            "model": model_name,
            "sample": sample,
            "term": term,
            "coef": res.params[term],
            "se": res.bse[term],
            "t": res.tvalues[term],
            "p_value": res.pvalues[term],
            "n_obs": int(res.nobs),
            "r2": r2,
            "cov_type": cov,
            "first_stage_f": first_f,
        }

    rows = []
    for model_name, (y, xs) in SPECS.items():
        if not set([y, *xs]) <= set(panel.columns):
            continue  # split-sample models without split-sample data
        for sample, data, pooled in samples:
            res, cov = _fit(data, y, xs, pooled)
            rows += [row(model_name, sample, term, res, cov, res.rsquared) for term in xs]

    for model_name, (y, endog, instruments) in IV_SPECS.items():
        for sample, data, pooled in samples:
            res, cov, first_f = _fit_iv(data, y, endog, instruments, pooled)
            # R-squared has no useful meaning for 2SLS, so it is left blank.
            rows += [row(model_name, sample, term, res, cov, np.nan, first_f[term]) for term in endog]
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def high_vol_comparison(panel: pd.DataFrame) -> pd.DataFrame:
    """Average lambda on each pair's high-volatility days against its other days."""
    stats = (
        panel.groupby(["symbol", "high_vol"])
        .agg(
            days=("lambda", "size"),
            lambda_mean=("lambda", "mean"),
            lambda_median=("lambda", "median"),
            realized_vol_pct=("realized_vol_pct", "mean"),
        )
        .unstack("high_vol")
    )
    out = pd.DataFrame(
        {
            "days_high_vol": stats[("days", 1)],
            "days_other": stats[("days", 0)],
            "realized_vol_pct_high_vol": stats[("realized_vol_pct", 1)],
            "realized_vol_pct_other": stats[("realized_vol_pct", 0)],
            "lambda_mean_high_vol": stats[("lambda_mean", 1)],
            "lambda_mean_other": stats[("lambda_mean", 0)],
            "lambda_median_high_vol": stats[("lambda_median", 1)],
            "lambda_median_other": stats[("lambda_median", 0)],
        }
    )
    out["lambda_ratio"] = out["lambda_mean_high_vol"] / out["lambda_mean_other"]
    return out.reset_index()
