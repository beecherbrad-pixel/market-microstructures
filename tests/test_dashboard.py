"""Tests for the Tableau dashboard tables."""

import pytest

pd = pytest.importorskip("pandas")

np = pytest.importorskip("numpy")

from microstructure.dashboard import Z_95, bars_table, daily_table, hourly_table, partial_out


@pytest.fixture
def panel():
    return pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 3 + ["ETHUSDT"] * 3,
            "trade_date": ["2026-08-01", "2026-08-02", "2026-08-03"] * 2,
            "n_obs": [1440] * 6,
            "lambda": [2.0, 3.0, 4.0, 6.0, 8.0, 10.0],
            "lambda_se_hc1": [0.5] * 6,
            "lambda_t_hc1": [4.0, 6.0, 8.0, 12.0, 16.0, 20.0],
            "r2": [0.2] * 6,
            "realized_vol": [0.01, 0.03, 0.02, 0.02, 0.01, 0.04],
            "quote_volume": [5e8] * 6,
            "n_trades": [1_000_000] * 6,
            "ofi": [0.0] * 6,
            "roll_spread_bps": [None, 0.1, 0.2, 0.3, None, 0.4],
            "amihud": [0.001] * 6,
            "daily_return": [0.01, -0.02, 0.0, 0.0, 0.01, 0.02],
            "high_vol": [0, 1, 0, 0, 0, 1],
        }
    ).assign(
        log_lambda=lambda d: np.log(d["lambda"]),
        log_realized_vol=lambda d: np.log(d["realized_vol"]),
        log_volume=np.log([5e8, 6e8, 7e8, 4e8, 9e8, 8e8]),
    )


def test_daily_table(panel):
    out = daily_table(panel)
    assert len(out) == 6
    assert list(out.columns[:2]) == ["Pair", "Date"]
    lam, low, high = out["Lambda (bps per $1M)"], out["Lambda 95% CI low"], out["Lambda 95% CI high"]
    assert (high - low).tolist() == pytest.approx([2 * Z_95 * 0.5] * 6)
    assert ((low + high) / 2).tolist() == pytest.approx(lam.tolist())
    # each pair's median day has a relative lambda of exactly 1
    assert out["Lambda vs pair median"].tolist() == pytest.approx([2 / 3, 1, 4 / 3, 0.75, 1, 1.25])
    # percentile ranks within the pair: the most volatile day is 1
    assert out["Vol percentile in pair"].tolist() == pytest.approx([1 / 3, 1, 2 / 3, 2 / 3, 1 / 3, 1])
    assert out["High-vol day"].tolist() == ["Other day", "High-vol day", "Other day", "Other day", "Other day", "High-vol day"]
    assert out["Realized vol (%)"].iloc[1] == pytest.approx(3.0)
    assert out["Volume ($M)"].iloc[0] == pytest.approx(500.0)
    assert out["Roll spread (bps)"].isna().sum() == 2


def test_added_variable_slope_matches_multiple_regression():
    """Frisch-Waugh-Lovell: the slope through the partialled-out columns is the multiple-regression slope."""
    rng = np.random.default_rng(0)
    n = 200
    frame = pd.DataFrame({"symbol": np.repeat(["A", "B"], n), "v": rng.normal(size=2 * n)})
    frame["x"] = 0.8 * frame["v"] + rng.normal(size=2 * n)
    frame["y"] = 1.5 * frame["x"] - 1.0 * frame["v"] + rng.normal(size=2 * n) + (frame["symbol"] == "B") * 3
    ry = partial_out(frame, "y", "v", by="symbol")
    rx = partial_out(frame, "x", "v", by="symbol")
    for sym in ["A", "B"]:
        g = frame[frame["symbol"] == sym]
        X = np.column_stack([np.ones(len(g)), g["x"], g["v"]])
        expected = np.linalg.lstsq(X, g["y"], rcond=None)[0][1]
        mask = frame["symbol"] == sym
        assert np.polyfit(rx[mask], ry[mask], 1)[0] == pytest.approx(expected)
        assert ry[mask].mean() == pytest.approx(0, abs=1e-12)


def test_bars_table_cumulative_return_restarts_each_day():
    bars = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 4 + ["ETHUSDT"] * 2,
            # deliberately out of order
            "trade_date": ["2026-08-01", "2026-08-02", "2026-08-01", "2026-08-02", "2026-08-01", "2026-08-01"],
            "bar_start_utc": [
                "2026-08-01 00:01:00",
                "2026-08-02 00:01:00",
                "2026-08-01 13:02:00",
                "2026-08-02 00:02:00",
                "2026-08-01 00:01:00",
                "2026-08-01 00:02:00",
            ],
            "ret_bps": [1.0, 5.0, 2.0, -1.0, 3.0, 3.0],
            "flow_musd": [0.1] * 6,
            "n_trades": [100] * 6,
            "volume_musd": [1.0] * 6,
        }
    )
    out = bars_table(bars)
    assert out["Cumulative return (bps)"].tolist() == [1.0, 3.0, 5.0, 4.0, 3.0, 6.0]
    assert out["Hour (UTC)"].tolist() == [0, 13, 0, 0, 0, 0]
    assert "Net flow ($M)" in out.columns


def test_hourly_table():
    hourly = pd.DataFrame(
        {
            "symbol": ["ETHUSDT", "BTCUSDT"],
            "hour_utc": [0, 0],
            "n_obs": [5400, 5400],
            "lambda": [8.0, 3.0],
            "lambda_se_hc1": [1.0, 0.2],
            "r2": [0.2, 0.2],
        }
    )
    out = hourly_table(hourly)
    assert out["Pair"].tolist() == ["BTCUSDT", "ETHUSDT"]
    assert out["Lambda 95% CI high"].tolist() == pytest.approx([3 + Z_95 * 0.2, 8 + Z_95])
