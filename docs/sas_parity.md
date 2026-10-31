# Python and SAS parity

Relative difference is `|python - sas| / max(|python|, |sas|)`; **match** means at most 1e-06 on every compared number. P-values are not compared: SAS uses t distributions for the robust standard errors and statsmodels the normal.

## Daily Kyle's lambda

One regression per pair-day of one-minute returns on net order flow. Python/Spark: `impact.kyle_lambda` (`regr_*` aggregates plus a residual pass). SAS: `PROC REG ... / hcc hccmethod=1` with `BY symbol trade_date`. Columns are the largest relative difference over all days.

| Pair | Pair-days | Days with different bars | Lambda | Intercept | SE | HC1 SE | R² | Result |
|---|---|---|---|---|---|---|---|---|
| BTCUSDT | 90 | 0 | 8e-15 | 1e-11 | 7e-14 | 3e-14 | 3e-14 | match |
| ETHUSDT | 90 | 0 | 7e-15 | 1e-11 | 4e-14 | 2e-14 | 1e-13 | match |

## Second stage: lambda and volatility

Pooled models: Python `statsmodels` OLS with pair dummies and errors clustered by date; SAS `PROC SURVEYREG` with `CLASS symbol` and `CLUSTER trade_date`. Per pair: Newey-West with 5 lags; SAS `PROC AUTOREG` with `COVEST=HAC(KERNEL=BARTLETT, BANDWIDTH=6)`. IV: two-stage least squares, with the first stage's F statistic on the excluded instruments in the last columns.

| Model | Sample | Term | Coef, Python | Coef, SAS | SE, Python | SE, SAS | Coef diff | SE diff | First-stage F, Python / SAS | Result |
|---|---|---|---|---|---|---|---|---|---|---|
| levels | pooled | `realized_vol_pct` | 2.0087 | 2.0087 | 0.4634 | 0.4634 | 2e-16 | 1e-14 |  | match |
| levels | pooled | `volume_busd` | -3.3194 | -3.3194 | 0.7707 | 0.7707 | 1e-14 | 7e-15 |  | match |
| levels | BTCUSDT | `realized_vol_pct` | 2.6964 | 2.6964 | 0.2819 | 0.2819 | 3e-15 | 3e-15 |  | match |
| levels | BTCUSDT | `volume_busd` | -2.5496 | -2.5496 | 0.4236 | 0.4236 | 2e-14 | 1e-14 |  | match |
| levels | ETHUSDT | `realized_vol_pct` | 4.3226 | 4.3226 | 0.6130 | 0.6130 | 2e-14 | 9e-15 |  | match |
| levels | ETHUSDT | `volume_busd` | -12.1611 | -12.1611 | 1.7833 | 1.7833 | 2e-15 | 3e-15 |  | match |
| logs | pooled | `log_realized_vol` | 1.6304 | 1.6304 | 0.1533 | 0.1533 | 4e-12 | 1e-11 |  | match |
| logs | pooled | `log_volume` | -1.2336 | -1.2336 | 0.1326 | 0.1326 | 5e-12 | 1e-11 |  | match |
| logs | BTCUSDT | `log_realized_vol` | 1.5944 | 1.5944 | 0.1196 | 0.1196 | 8e-12 | 3e-12 |  | match |
| logs | BTCUSDT | `log_volume` | -1.1630 | -1.1630 | 0.1288 | 0.1288 | 1e-11 | 4e-13 |  | match |
| logs | ETHUSDT | `log_realized_vol` | 1.6132 | 1.6132 | 0.3036 | 0.3036 | 3e-12 | 3e-12 |  | match |
| logs | ETHUSDT | `log_volume` | -1.2473 | -1.2473 | 0.2257 | 0.2257 | 3e-12 | 3e-12 |  | match |
| high_vol_day | pooled | `high_vol` | 1.0041 | 1.0041 | 0.5275 | 0.5275 | 2e-15 | 8e-15 |  | match |
| high_vol_day | BTCUSDT | `high_vol` | 0.5951 | 0.5951 | 0.3433 | 0.3433 | 1e-14 | 1e-14 |  | match |
| high_vol_day | ETHUSDT | `high_vol` | 1.4131 | 1.4131 | 1.0149 | 1.0149 | 3e-15 | 4e-15 |  | match |
| logs_lagged_vol | pooled | `log_realized_vol_lag` | 0.1238 | 0.1238 | 0.1208 | 0.1208 | 3e-13 | 2e-13 |  | match |
| logs_lagged_vol | pooled | `log_volume` | -0.1284 | -0.1284 | 0.0982 | 0.0982 | 1e-12 | 1e-12 |  | match |
| logs_lagged_vol | BTCUSDT | `log_realized_vol_lag` | 0.0074 | 0.0074 | 0.1017 | 0.1017 | 2e-11 | 1e-12 |  | match |
| logs_lagged_vol | BTCUSDT | `log_volume` | 0.1694 | 0.1694 | 0.1164 | 0.1164 | 2e-12 | 1e-12 |  | match |
| logs_lagged_vol | ETHUSDT | `log_realized_vol_lag` | 0.1911 | 0.1911 | 0.1934 | 0.1934 | 1e-13 | 2e-13 |  | match |
| logs_lagged_vol | ETHUSDT | `log_volume` | -0.2987 | -0.2987 | 0.1085 | 0.1085 | 3e-13 | 7e-13 |  | match |
| split_sample | pooled | `log_realized_vol_odd` | 1.4452 | 1.4452 | 0.1518 | 0.1518 | 8e-12 | 8e-12 |  | match |
| split_sample | pooled | `log_volume_odd` | -1.1405 | -1.1405 | 0.1366 | 0.1366 | 9e-12 | 8e-12 |  | match |
| split_sample | BTCUSDT | `log_realized_vol_odd` | 1.4056 | 1.4056 | 0.1050 | 0.1050 | 1e-11 | 1e-11 |  | match |
| split_sample | BTCUSDT | `log_volume_odd` | -1.0759 | -1.0759 | 0.1267 | 0.1267 | 1e-11 | 8e-12 |  | match |
| split_sample | ETHUSDT | `log_realized_vol_odd` | 1.4425 | 1.4425 | 0.3180 | 0.3180 | 4e-13 | 2e-12 |  | match |
| split_sample | ETHUSDT | `log_volume_odd` | -1.1575 | -1.1575 | 0.2403 | 0.2403 | 6e-13 | 2e-12 |  | match |
| split_sample_resid | pooled | `log_resid_vol_odd` | 1.2528 | 1.2528 | 0.1619 | 0.1619 | 7e-12 | 4e-12 |  | match |
| split_sample_resid | pooled | `log_volume_odd` | -0.9985 | -0.9985 | 0.1461 | 0.1461 | 8e-12 | 5e-12 |  | match |
| split_sample_resid | BTCUSDT | `log_resid_vol_odd` | 1.3262 | 1.3262 | 0.1272 | 0.1272 | 5e-12 | 6e-12 |  | match |
| split_sample_resid | BTCUSDT | `log_volume_odd` | -0.9972 | -0.9972 | 0.1477 | 0.1477 | 8e-12 | 5e-12 |  | match |
| split_sample_resid | ETHUSDT | `log_resid_vol_odd` | 1.0958 | 1.0958 | 0.3260 | 0.3260 | 8e-14 | 2e-12 |  | match |
| split_sample_resid | ETHUSDT | `log_volume_odd` | -0.9397 | -0.9397 | 0.2448 | 0.2448 | 9e-14 | 2e-12 |  | match |
| iv_lagged | pooled | `log_realized_vol` | 1.3577 | 1.3577 | 0.2807 | 0.2807 | 7e-13 | 3e-12 | 17.8 / 17.8 | match |
| iv_lagged | pooled | `log_volume` | -0.9812 | -0.9812 | 0.1890 | 0.1890 | 1e-12 | 7e-13 | 17.1 / 17.1 | match |
| iv_lagged | BTCUSDT | `log_realized_vol` | 1.2377 | 1.2377 | 0.4846 | 0.4846 | 2e-11 | 6e-11 | 24.2 / 24.2 | match |
| iv_lagged | BTCUSDT | `log_volume` | -0.8562 | -0.8562 | 0.4313 | 0.4313 | 3e-11 | 5e-11 | 23.0 / 23.0 | match |
| iv_lagged | ETHUSDT | `log_realized_vol` | 1.3391 | 1.3391 | 0.4820 | 0.4820 | 2e-12 | 1e-11 | 16.6 / 16.6 | match |
| iv_lagged | ETHUSDT | `log_volume` | -0.9889 | -0.9889 | 0.2476 | 0.2476 | 2e-12 | 2e-11 | 18.2 / 18.2 | match |

## Cross-checks with other SAS procedures

Coefficients only: these procedures report classical standard errors, so their errors are not expected to match.

| SAS procedure | Python model | Sample | Term | Coef, Python | Coef, SAS | Diff | Result |
|---|---|---|---|---|---|---|---|
| PROC PANEL, FIXONE | logs | pooled | `log_realized_vol` | 1.6304 | 1.6304 | 2e-12 | match |
| PROC PANEL, FIXONE | logs | pooled | `log_volume` | -1.2336 | -1.2336 | 2e-12 | match |
| PROC SYSLIN, 2SLS | iv_lagged | BTCUSDT | `log_realized_vol` | 1.2377 | 1.2377 | 3e-11 | match |
| PROC SYSLIN, 2SLS | iv_lagged | BTCUSDT | `log_volume` | -0.8562 | -0.8562 | 4e-11 | match |
| PROC SYSLIN, 2SLS | iv_lagged | ETHUSDT | `log_realized_vol` | 1.3391 | 1.3391 | 7e-12 | match |
| PROC SYSLIN, 2SLS | iv_lagged | ETHUSDT | `log_volume` | -0.9889 | -0.9889 | 7e-12 | match |

219 estimates compared; 0 second-stage rows differ; 0 cross-check rows differ.
