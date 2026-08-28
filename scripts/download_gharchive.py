"""Download GH Archive hourly files for a date range.

Usage:
    python scripts/download_gharchive.py --start 2026-03-01 --end 2026-08-11
"""
import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://data.gharchive.org"
RAW_DIR = Path.home() / "scratch" / "github-lakehouse" / "raw"
# Cloudflare blocks the default Python-urllib UA; pretend to be a browser.
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}


def download_hour(d: date, hour: int) -> bool:
    """Download one hourly file. Returns True if the file already exists or was fetched."""
    filename = f"{d.isoformat()}-{hour}.json.gz"
    dest = RAW_DIR / filename
    if dest.exists() and dest.stat().st_size > 0:
        return True  # Already downloaded; skip (idempotent resume).
    url = f"{BASE_URL}/{filename}"
    log.info("Downloading %s", url)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=120) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        return True
    except Exception as e:
        log.warning("Failed %s: %s (will retry next run)", filename, e)
        return False


def main(start: date, end: date) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    total, ok = 0, 0
    d = start
    while d <= end:
        for hour in range(24):
            total += 1
            if download_hour(d, hour):
                ok += 1
        log.info("Day %s done (%d/%d ok so far)", d, ok, total)
        d += timedelta(days=1)
    log.info("Finished: %d/%d files ok", ok, total)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=str, required=True, help="YYYY-MM-DD")
    p.add_argument("--end", type=str, required=True, help="YYYY-MM-DD")
    args = p.parse_args()
    main(date.fromisoformat(args.start), date.fromisoformat(args.end))
