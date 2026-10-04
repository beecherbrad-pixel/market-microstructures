"""Downloader tests. A fake fetch stands in for the network, so these run offline."""

import gzip
import hashlib
import io
import urllib.error
import zipfile
from datetime import date

import pytest

from microstructure.ingest import binance

CSV = b"1,100.5,0.01,1.005,1785542400000000,True,True\n2,100.6,0.02,2.012,1785542400500000,False,True\n"


def make_zip(csv_bytes: bytes, name: str = "BTCUSDT-trades-2026-08-01.csv") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, csv_bytes)
    return buffer.getvalue()


class FakeServer:
    """Serves a zip and its checksum from memory and counts requests."""

    def __init__(self, zip_bytes: bytes, digest: str | None = None):
        self.zip_bytes = zip_bytes
        self.digest = digest or hashlib.sha256(zip_bytes).hexdigest()
        self.calls = []

    def __call__(self, url, dest):
        self.calls.append(url)
        if url.endswith(".CHECKSUM"):
            dest.write_text(f"{self.digest}  BTCUSDT-trades-2026-08-01.zip\n")
        else:
            dest.write_bytes(self.zip_bytes)


def test_daily_periods_cross_a_month_boundary():
    assert binance.periods(date(2026, 7, 30), date(2026, 8, 2)) == [
        "2026-07-30",
        "2026-07-31",
        "2026-08-01",
        "2026-08-02",
    ]


def test_monthly_periods_cross_a_year_boundary():
    assert binance.periods(date(2025, 11, 15), date(2026, 2, 1), "monthly") == [
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
    ]


def test_periods_rejects_reversed_range_and_unknown_freq():
    with pytest.raises(ValueError):
        binance.periods(date(2026, 8, 2), date(2026, 8, 1))
    with pytest.raises(ValueError):
        binance.periods(date(2026, 8, 1), date(2026, 8, 2), "weekly")


def test_file_url_matches_binance_layout():
    assert (
        binance.file_url("btcusdt", "2026-08-01", "daily")
        == "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2026-08-01.zip"
    )
    assert (
        binance.file_url("ETHUSDT", "2026-08", "monthly")
        == "https://data.binance.vision/data/spot/monthly/trades/ETHUSDT/ETHUSDT-trades-2026-08.zip"
    )


def test_parse_checksum_reads_digest_and_rejects_garbage():
    digest = "a" * 64
    assert binance.parse_checksum(f"{digest}  BTCUSDT-trades-2026-08-01.zip\n") == digest
    with pytest.raises(ValueError):
        binance.parse_checksum("<html>Not Found</html>")
    with pytest.raises(ValueError):
        binance.parse_checksum("")


def test_fetch_period_verifies_unpacks_and_cleans_up(tmp_path):
    server = FakeServer(make_zip(CSV))

    result = binance.fetch_period("BTCUSDT", "2026-08-01", "daily", tmp_path, fetch=server)

    assert result.status == "downloaded"
    assert result.path == tmp_path / "symbol=BTCUSDT" / "BTCUSDT-trades-2026-08-01.csv.gz"
    assert gzip.decompress(result.path.read_bytes()) == CSV
    # Only the finished file remains: no zip, checksum or .part leftovers.
    assert [p.name for p in result.path.parent.iterdir()] == [result.path.name]


def test_fetch_period_skips_files_already_on_disk(tmp_path):
    server = FakeServer(make_zip(CSV))
    binance.fetch_period("BTCUSDT", "2026-08-01", "daily", tmp_path, fetch=server)
    calls_after_first = len(server.calls)

    again = binance.fetch_period("BTCUSDT", "2026-08-01", "daily", tmp_path, fetch=server)

    assert again.status == "skipped"
    assert len(server.calls) == calls_after_first


def test_fetch_period_rejects_a_corrupt_download(tmp_path):
    server = FakeServer(make_zip(CSV), digest="0" * 64)

    with pytest.raises(ValueError, match="checksum mismatch"):
        binance.fetch_period("BTCUSDT", "2026-08-01", "daily", tmp_path, fetch=server)

    assert list((tmp_path / "symbol=BTCUSDT").iterdir()) == []


def test_fetch_period_reports_unpublished_days_as_missing(tmp_path):
    def not_found(url, dest):
        raise urllib.error.HTTPError(url, 404, "Not Found", None, None)

    result = binance.fetch_period("BTCUSDT", "2030-01-01", "daily", tmp_path, fetch=not_found)

    assert result.status == "missing"
    assert result.path is None


def test_main_downloads_a_range(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(binance, "download", FakeServer(make_zip(CSV)))

    code = binance.main(["--start", "2026-08-01", "--end", "2026-08-02", "--out", str(tmp_path)])

    assert code == 0
    assert len(list((tmp_path / "symbol=BTCUSDT").glob("*.csv.gz"))) == 2
    assert capsys.readouterr().out.count("downloaded") == 2
