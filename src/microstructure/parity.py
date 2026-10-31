"""Parity check: the Python/Spark estimates against the SAS replication.

Inputs, all CSV:

* from the Databricks ``exports`` volume (``notebooks/03_price_impact.py``):
  ``lambda_daily.csv`` and ``python_results.csv``
* from SAS (``sas/run_all.sas``): ``sas_lambda_daily.csv`` and
  ``sas_results.csv``

Output is a Markdown report with three tables:

1. daily Kyle's lambda, one regression per pair-day, summarized by pair;
2. every second-stage slope, Python next to SAS;
3. coefficient cross-checks from PROC PANEL and PROC SYSLIN against the
   matching Python models.

Usage::

    python -m microstructure.parity --exports exports --sas sas_output --out docs/sas_parity.md

Coefficients and standard errors are compared by relative difference,
``|a - b| / max(|a|, |b|)``. Both sides are double precision and the CSVs
carry at least 15 significant digits, so anything above about 1e-6 is a real
difference in method, not rounding. P-values are not compared: SAS uses t
distributions for the robust errors and statsmodels the normal.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RTOL = 1e-6

# SAS-only cross-check models and the Python model each one should reproduce.
# The last entry is the sample each procedure runs on.
CROSS_CHECKS = {
    "logs_panel_fixone": ("logs", "PROC PANEL, FIXONE", "pooled"),
    "iv_lagged_syslin": ("iv_lagged", "PROC SYSLIN, 2SLS", "pairs"),
}

KEYS = ["model", "sample", "term"]


def rel_diff(a, b) -> np.ndarray:
    """``|a - b| / max(|a|, |b|)``, and 0 where both are 0."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    scale = np.maximum(np.abs(a), np.abs(b))
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.abs(a - b) / scale
    return np.where(scale == 0, 0.0, out)


def _lower(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rename(columns=str.lower)


def _sas_hc1_column(columns) -> str:
    """The HC1 standard error column from PROC REG, ``HCStdErr`` in current releases."""
    for col in columns:
        c = col.lower()
        if c.startswith("hc") and "stderr" in c:
            return col
    raise KeyError(f"no HC standard error column in the SAS daily lambda file: {list(columns)}")


def _dates(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    return frame


def lambda_parity(python: pd.DataFrame, sas: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join the daily lambdas on pair and day.

    Returns the row-level comparison and a per-pair summary.
    """
    py = _dates(_lower(python))
    sa = _dates(_lower(sas))
    sa = sa.rename(columns={_sas_hc1_column(sa.columns): "lambda_se_hc1"})

    fields = ["lambda", "alpha", "lambda_se", "lambda_se_hc1", "r2"]
    joined = py[["symbol", "trade_date", "n_obs", *fields]].merge(
        sa[["symbol", "trade_date", "n_obs", *fields]],
        on=["symbol", "trade_date"],
        how="outer",
        suffixes=("_py", "_sas"),
        indicator=True,
    )
    for f in fields:
        joined[f"{f}_rel"] = rel_diff(joined[f"{f}_py"], joined[f"{f}_sas"])
    joined["n_obs_match"] = joined["n_obs_py"] == joined["n_obs_sas"]

    both = joined[joined["_merge"] == "both"]
    summary = both.groupby("symbol").agg(
        pair_days=("trade_date", "size"),
        n_obs_mismatches=("n_obs_match", lambda s: int((~s).sum())),
        lambda_max_rel=("lambda_rel", "max"),
        alpha_max_rel=("alpha_rel", "max"),
        se_max_rel=("lambda_se_rel", "max"),
        se_hc1_max_rel=("lambda_se_hc1_rel", "max"),
        r2_max_rel=("r2_rel", "max"),
    )
    only = joined[joined["_merge"] != "both"].groupby("symbol").size()
    summary["unmatched_days"] = only.reindex(summary.index, fill_value=0).astype(int)
    return joined, summary.reset_index()


def results_parity(python: pd.DataFrame, sas: pd.DataFrame) -> pd.DataFrame:
    """Second-stage slopes side by side, for every model both sides estimated."""
    py = _lower(python)
    sa = _lower(sas)
    sa = sa[~sa["model"].isin(CROSS_CHECKS)]
    cols = ["coef", "se", "n_obs", "first_stage_f"]
    joined = py[KEYS + cols].merge(
        sa[KEYS + cols], on=KEYS, how="outer", suffixes=("_py", "_sas"), indicator=True
    )
    joined["coef_rel"] = rel_diff(joined["coef_py"], joined["coef_sas"])
    joined["se_rel"] = rel_diff(joined["se_py"], joined["se_sas"])
    joined["f_rel"] = rel_diff(joined["first_stage_f_py"], joined["first_stage_f_sas"])
    joined["n_obs_match"] = joined["n_obs_py"] == joined["n_obs_sas"]
    order = {m: i for i, m in enumerate(py["model"].drop_duplicates())}
    joined["_order"] = joined["model"].map(order).fillna(len(order))
    joined["_pooled_first"] = joined["sample"] != "pooled"
    keys = ["_order", "_pooled_first", "sample", "term"]
    return joined.sort_values(keys).drop(columns=["_order", "_pooled_first"]).reset_index(drop=True)


def cross_check_parity(python: pd.DataFrame, sas: pd.DataFrame) -> pd.DataFrame:
    """Coefficients from the SAS-only cross-check procedures against their Python counterparts."""
    py = _lower(python)
    sa = _lower(sas)
    rows = []
    for sas_model, (py_model, label, samples) in CROSS_CHECKS.items():
        s = sa[sa["model"] == sas_model][["sample", "term", "coef"]]
        p = py[py["model"] == py_model][["sample", "term", "coef"]]
        pooled = p["sample"] == "pooled"
        p = p[pooled] if samples == "pooled" else p[~pooled]
        # Outer join: a missing or misplaced SAS row shows up as a difference.
        j = p.merge(s, on=["sample", "term"], how="outer", suffixes=("_py", "_sas"))
        j.insert(0, "check", label)
        j.insert(1, "python_model", py_model)
        rows.append(j)
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not out.empty:
        out["coef_rel"] = rel_diff(out["coef_py"], out["coef_sas"])
    return out


def _fmt(x, digits=4) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{x:.{digits}f}"


def _sci(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return "0" if x == 0 else f"{x:.0e}"


def _status(*rels, rtol=RTOL) -> str:
    vals = [r for r in rels if not (r is None or np.isnan(r))]
    if not vals:
        return "missing"
    return "match" if max(vals) <= rtol else "**differs**"


def _table(header: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(lines)


def report(lam_summary: pd.DataFrame, results: pd.DataFrame, cross: pd.DataFrame, rtol: float = RTOL) -> str:
    out = ["# Python and SAS parity", ""]
    out.append(
        f"Relative difference is `|python - sas| / max(|python|, |sas|)`; **match** means at most {rtol:g} "
        "on every compared number. P-values are not compared: SAS uses t distributions for the robust "
        "standard errors and statsmodels the normal."
    )

    out += ["", "## Daily Kyle's lambda", ""]
    out.append(
        "One regression per pair-day of one-minute returns on net order flow. Python/Spark: "
        "`impact.kyle_lambda` (`regr_*` aggregates plus a residual pass). SAS: "
        "`PROC REG ... / hcc hccmethod=1` with `BY symbol trade_date`. Columns are the largest "
        "relative difference over all days."
    )
    out.append("")
    rows = []
    for r in lam_summary.itertuples():
        rels = [r.lambda_max_rel, r.alpha_max_rel, r.se_max_rel, r.se_hc1_max_rel, r.r2_max_rel]
        ok = r.n_obs_mismatches == 0 and r.unmatched_days == 0
        status = _status(*rels, rtol=rtol) if ok else "**differs**"
        rows.append(
            [r.symbol, str(r.pair_days), str(r.n_obs_mismatches + r.unmatched_days)]
            + [_sci(v) for v in rels]
            + [status]
        )
    out.append(
        _table(
            ["Pair", "Pair-days", "Days with different bars", "Lambda", "Intercept", "SE", "HC1 SE", "R²", "Result"],
            rows,
        )
    )

    out += ["", "## Second stage: lambda and volatility", ""]
    out.append(
        "Pooled models: Python `statsmodels` OLS with pair dummies and errors clustered by date; SAS "
        "`PROC SURVEYREG` with `CLASS symbol` and `CLUSTER trade_date`. Per pair: Newey-West with 5 lags; "
        "SAS `PROC AUTOREG` with `COVEST=HAC(KERNEL=BARTLETT, BANDWIDTH=6)`. IV: two-stage least squares, "
        "with the first stage's F statistic on the excluded instruments in the last columns."
    )
    out.append("")
    rows = []
    for r in results.itertuples():
        rels = [r.coef_rel, r.se_rel]
        status = _status(*rels, rtol=rtol) if r.n_obs_match else "**differs**"
        f = ""
        if not np.isnan(r.first_stage_f_py):
            f = f"{_fmt(r.first_stage_f_py, 1)} / {_fmt(r.first_stage_f_sas, 1)}"
        rows.append(
            [
                r.model,
                r.sample,
                f"`{r.term}`",
                _fmt(r.coef_py),
                _fmt(r.coef_sas),
                _fmt(r.se_py),
                _fmt(r.se_sas),
                _sci(r.coef_rel),
                _sci(r.se_rel),
                f,
                status,
            ]
        )
    out.append(
        _table(
            [
                "Model",
                "Sample",
                "Term",
                "Coef, Python",
                "Coef, SAS",
                "SE, Python",
                "SE, SAS",
                "Coef diff",
                "SE diff",
                "First-stage F, Python / SAS",
                "Result",
            ],
            rows,
        )
    )

    if not cross.empty:
        out += ["", "## Cross-checks with other SAS procedures", ""]
        out.append(
            "Coefficients only: these procedures report classical standard errors, so their errors are "
            "not expected to match."
        )
        out.append("")
        rows = [
            [
                r.check,
                r.python_model,
                r.sample,
                f"`{r.term}`",
                _fmt(r.coef_py),
                _fmt(r.coef_sas),
                _sci(r.coef_rel),
                _status(r.coef_rel, rtol=rtol),
            ]
            for r in cross.itertuples()
        ]
        out.append(
            _table(["SAS procedure", "Python model", "Sample", "Term", "Coef, Python", "Coef, SAS", "Diff", "Result"], rows)
        )

    n_compared = len(results) + int(lam_summary["pair_days"].sum())
    differs = int((results.apply(lambda r: _status(r.coef_rel, r.se_rel, rtol=rtol) != "match" or not r.n_obs_match, axis=1)).sum())
    cross_differs = 0 if cross.empty else int((cross["coef_rel"].isna() | (cross["coef_rel"] > rtol)).sum())
    f_missing = int((results["first_stage_f_py"].notna() & results["first_stage_f_sas"].isna()).sum())
    summary = f"{n_compared} estimates compared; {differs} second-stage rows differ"
    summary += f"; {cross_differs} cross-check rows differ"
    if f_missing:
        summary += f"; {f_missing} first-stage F statistics missing from SAS"
    out += ["", summary + ".", ""]
    return "\n".join(out)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exports", type=Path, required=True, help="folder with lambda_daily.csv and python_results.csv")
    parser.add_argument("--sas", type=Path, required=True, help="folder with sas_lambda_daily.csv and sas_results.csv")
    parser.add_argument("--out", type=Path, default=Path("docs/sas_parity.md"))
    parser.add_argument("--rtol", type=float, default=RTOL)
    args = parser.parse_args(argv)

    _, lam_summary = lambda_parity(
        pd.read_csv(args.exports / "lambda_daily.csv"), pd.read_csv(args.sas / "sas_lambda_daily.csv")
    )
    py_results = pd.read_csv(args.exports / "python_results.csv")
    sas_results = pd.read_csv(args.sas / "sas_results.csv")
    results = results_parity(py_results, sas_results)
    cross = cross_check_parity(py_results, sas_results)

    text = report(lam_summary, results, cross, rtol=args.rtol)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    results.to_csv(args.out.with_suffix(".csv"), index=False)
    print(text)
    print(f"Wrote {args.out} and {args.out.with_suffix('.csv')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
