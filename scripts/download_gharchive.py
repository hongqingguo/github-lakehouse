"""Download GH Archive hourly files for a date range.

Skips files already recorded in <raw_dir>/.downloaded.txt, so re-runs only
fetch missing files (idempotent download).

Usage:
    python scripts/download_gharchive.py --start 2026-07-15 --end 2026-08-11
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
DONE_FILE = RAW_DIR / ".downloaded.txt"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}


def load_done() -> set:
    """Load the set of already-downloaded filenames."""
    if not DONE_FILE.exists():
        return set()
    return set(DONE_FILE.read_text().splitlines())


def append_done(filenames) -> None:
    """Append filenames to the done-file (one per line)."""
    with DONE_FILE.open("a") as f:
        for name in filenames:
            f.write(name + "\n")


def download_hour(d: date, hour: int, done: set) -> bool:
    """Download one hourly file if not already recorded. Returns True on success."""
    filename = f"{d.isoformat()}-{hour}.json.gz"
    if filename in done:
        return True  # already downloaded in a previous run
    dest = RAW_DIR / filename
    if dest.exists() and dest.stat().st_size > 0:
        append_done([filename])  # file exists but not recorded -> record it
        return True
    url = f"{BASE_URL}/{filename}"
    log.info("Downloading %s", url)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=120) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        append_done([filename])
        return True
    except Exception as e:
        log.warning("Failed %s: %s (will retry next run)", filename, e)
        return False


def main(start: date, end: date) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    done = load_done()
    total, ok = 0, 0
    d = start
    while d <= end:
        for hour in range(24):
            total += 1
            if download_hour(d, hour, done):
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
