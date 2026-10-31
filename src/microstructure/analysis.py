"""Second stage: does price impact rise on high-volatility days?

Works on the daily panel, one row per pair per day, which is small enough for
pandas (about 90 days per pair). The inputs are two Spark tables brought to the
driver with ``toPandas()``:

* ``gold_lambda_daily`` from ``impact.kyle_lambda``
* ``gold_daily`` from ``gold.to_gold_daily``

Three specifications, each run on the pooled panel (with a fixed effect per
pair) and on each pair alone:

``levels``        lambda on realized volatility (%) and volume ($bn)
``logs``          log lambda on log realized volatility and log volume, so the
                  slopes are elasticities; days with lambda <= 0 drop out
``high_vol_day``  lambda on a dummy for the pair's most volatile days
``logs_lagged_vol`` as ``logs``, but with the previous day's realized
                  volatility in place of the same day's

The lagged model is a robustness check. Same-day lambda and realized
volatility are both built from the same minute returns, so part of their
correlation is mechanical. Yesterday's volatility shares no data with today's
regression but still predicts today's volatility, because volatility is
persistent. If its slope stays positive, the link is not only mechanical. The
slope will be smaller than the same-day one, because yesterday's volatility is
only a partial proxy for today's.

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
}

RESULT_COLUMNS = ["model", "sample", "term", "coef", "se", "t", "p_value", "n_obs", "r2", "cov_type"]


def impact_panel(
    lambdas: pd.DataFrame,
    daily: pd.DataFrame,
    min_obs: int = MIN_OBS,
    high_vol_quantile: float = HIGH_VOL_QUANTILE,
) -> pd.DataFrame:
    """Join daily lambdas to the daily measures and add the regression variables."""
    daily = add_lagged_volatility(daily)
    panel = lambdas.merge(daily[KEYS + DAILY_FIELDS + ["realized_vol_lag"]], on=KEYS, how="inner")
    panel = panel[(panel["n_obs"] >= min_obs) & panel["lambda"].notna()].copy()

    panel["realized_vol_pct"] = panel["realized_vol"] * 100
    panel["volume_busd"] = panel["quote_volume"] / 1e9
    positive = panel["lambda"] > 0
    panel["log_lambda"] = np.log(panel["lambda"].where(positive))
    panel["log_realized_vol"] = np.log(panel["realized_vol"])
    panel["log_volume"] = np.log(panel["quote_volume"])
    panel["log_realized_vol_lag"] = np.log(panel["realized_vol_lag"])

    cutoff = panel.groupby("symbol")["realized_vol"].transform(lambda s: s.quantile(high_vol_quantile))
    panel["high_vol"] = (panel["realized_vol"] >= cutoff).astype(int)
    return panel.sort_values(KEYS).reset_index(drop=True)


def add_lagged_volatility(daily: pd.DataFrame) -> pd.DataFrame:
    """Add ``realized_vol_lag``: the pair's realized volatility on the previous calendar day.

    It is taken from the full daily table, before any days are filtered out of
    the panel, and is missing when the previous day is not loaded, so a gap in
    the data never pairs a day with an older one.
    """
    daily = daily.copy()
    dates = pd.to_datetime(daily["trade_date"])
    prev = daily.assign(_date=dates + pd.Timedelta(days=1))[["symbol", "_date", "realized_vol"]]
    prev = prev.rename(columns={"realized_vol": "realized_vol_lag"})
    out = daily.assign(_date=dates).merge(prev, on=["symbol", "_date"], how="left")
    return out.drop(columns="_date")


def _fit(data: pd.DataFrame, y: str, xs: list[str], pooled: bool):
    import statsmodels.api as sm

    data = data.dropna(subset=[y, *xs]).sort_values("trade_date")
    X = data[xs].astype(float)
    if pooled:
        X = X.join(pd.get_dummies(data["symbol"], prefix="pair", drop_first=True, dtype=float))
    X = sm.add_constant(X, has_constant="add")
    model = sm.OLS(data[y].astype(float), X)

    if pooled:
        groups = pd.factorize(data["trade_date"])[0]
        return model.fit(cov_type="cluster", cov_kwds={"groups": groups}), "cluster(date)"
    return model.fit(cov_type="HAC", cov_kwds={"maxlags": HAC_LAGS}), f"HAC({HAC_LAGS})"


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

    rows = []
    for model_name, (y, xs) in SPECS.items():
        for sample, data, pooled in samples:
            res, cov = _fit(data, y, xs, pooled)
            for term in xs:
                rows.append(
                    {
                        "model": model_name,
                        "sample": sample,
                        "term": term,
                        "coef": res.params[term],
                        "se": res.bse[term],
                        "t": res.tvalues[term],
                        "p_value": res.pvalues[term],
                        "n_obs": int(res.nobs),
                        "r2": res.rsquared,
                        "cov_type": cov,
                    }
                )
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
