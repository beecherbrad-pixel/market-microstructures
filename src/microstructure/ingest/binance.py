"""Download Binance spot trade files from data.binance.vision.

Binance publishes one zip per symbol per day (and per month), each holding a
single headerless CSV, with a SHA-256 checksum file beside it. This module
downloads a date range, verifies every file against its checksum, and stores
the result as gzip-compressed CSV, which Spark reads natively (it cannot read
zip).

Files land in ``<out>/symbol=<SYMBOL>/``. The raw CSV has no symbol column, so
the directory name is what tells Spark which pair a row belongs to.

Usage:
    python -m microstructure.ingest.binance --start 2026-08-01 --end 2026-08-31 --out C:\\data\\binance

Standard library only, so it runs anywhere Python does.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import shutil
import sys
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

BASE_URL = "https://data.binance.vision/data/spot"
FREQS = ("daily", "monthly")
CHUNK = 1 << 20  # 1 MiB

Fetch = Callable[[str, Path], None]


@dataclass(frozen=True)
class FetchResult:
    period: str
    status: str  # "downloaded" | "skipped" | "missing"
    path: Path | None


def periods(start: date, end: date, freq: str = "daily") -> list[str]:
    """Period labels Binance uses in file names, inclusive of both ends.

    Daily labels look like ``2026-08-01``; monthly labels like ``2026-08``.
    """
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    if freq == "daily":
        return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
    if freq == "monthly":
        labels = []
        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            labels.append(f"{year:04d}-{month:02d}")
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
        return labels
    raise ValueError(f"freq must be one of {FREQS}, got {freq!r}")


def file_stem(symbol: str, period: str) -> str:
    return f"{symbol.upper()}-trades-{period}"


def file_url(symbol: str, period: str, freq: str = "daily") -> str:
    if freq not in FREQS:
        raise ValueError(f"freq must be one of {FREQS}, got {freq!r}")
    return f"{BASE_URL}/{freq}/trades/{symbol.upper()}/{file_stem(symbol, period)}.zip"


def parse_checksum(text: str) -> str:
    """Extract the digest from a ``<sha256>  <filename>`` checksum file."""
    parts = text.split()
    digest = parts[0].lower() if parts else ""
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"not a SHA-256 checksum file: {text[:80]!r}")
    return digest


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def download(url: str, dest: Path) -> None:
    """Stream ``url`` to ``dest``, writing to a .part file until complete."""
    part = dest.with_name(dest.name + ".part")
    with urllib.request.urlopen(url, timeout=60) as response, open(part, "wb") as f:
        shutil.copyfileobj(response, f, length=CHUNK)
    part.replace(dest)


def unpack_to_gzip(zip_path: Path, dest: Path) -> None:
    """Re-compress the single CSV inside ``zip_path`` as gzip at ``dest``."""
    part = dest.with_name(dest.name + ".part")
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if name.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected one CSV in {zip_path.name}, found {members}")
        with archive.open(members[0]) as source, gzip.open(part, "wb", compresslevel=6) as target:
            shutil.copyfileobj(source, target, length=CHUNK)
    part.replace(dest)


def fetch_period(
    symbol: str,
    period: str,
    freq: str,
    out_dir: Path,
    fetch: Fetch | None = None,
) -> FetchResult:
    """Download, verify and unpack one file. Safe to re-run: finished files are skipped.

    ``fetch`` exists so tests can substitute a fake and never touch the network.
    """
    fetch = fetch or download
    target_dir = Path(out_dir) / f"symbol={symbol.upper()}"
    final = target_dir / f"{file_stem(symbol, period)}.csv.gz"
    if final.exists():
        return FetchResult(period, "skipped", final)

    target_dir.mkdir(parents=True, exist_ok=True)
    url = file_url(symbol, period, freq)
    zip_path = target_dir / f"{file_stem(symbol, period)}.zip"
    checksum_path = target_dir / f"{zip_path.name}.CHECKSUM"
    try:
        try:
            fetch(url, zip_path)
            fetch(f"{url}.CHECKSUM", checksum_path)
        except urllib.error.HTTPError as err:
            if err.code == 404:  # Binance has not published this period
                return FetchResult(period, "missing", None)
            raise
        expected = parse_checksum(checksum_path.read_text())
        actual = sha256_of(zip_path)
        if actual != expected:
            raise ValueError(f"checksum mismatch for {zip_path.name}: expected {expected}, got {actual}")
        unpack_to_gzip(zip_path, final)
    finally:
        for leftover in (zip_path, checksum_path):
            leftover.unlink(missing_ok=True)
    return FetchResult(period, "downloaded", final)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download Binance spot trade files.")
    parser.add_argument("--symbol", nargs="+", default=["BTCUSDT"], help="one or more pairs (default: BTCUSDT)")
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="first day, YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="last day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--freq", choices=FREQS, default="daily", help="daily files (default) or one file per month")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(os.environ.get("MICROSTRUCTURE_DATA_DIR", "data/raw")),
        help="output folder (default: $MICROSTRUCTURE_DATA_DIR, else data/raw)",
    )
    args = parser.parse_args(argv)

    failures = 0
    for symbol in args.symbol:
        for period in periods(args.start, args.end, args.freq):
            try:
                result = fetch_period(symbol, period, args.freq, args.out)
            except Exception as err:  # keep going; one bad file should not lose the rest
                failures += 1
                print(f"{symbol} {period}  FAILED  {err}", flush=True)
                continue
            size = f"{result.path.stat().st_size / 1e6:8.1f} MB" if result.path else ""
            print(f"{symbol} {period}  {result.status:<10}  {size}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
