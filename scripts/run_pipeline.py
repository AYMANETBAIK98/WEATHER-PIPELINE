"""Run the pipeline without Airflow - handy for local dev and backfills.

    python scripts/run_pipeline.py                      # yesterday
    python scripts/run_pipeline.py --date 2026-09-28
    python scripts/run_pipeline.py --start 2026-09-01 --end 2026-09-07
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from etl import pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Weather batch ETL")
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    if args.start and args.end:
        days = [args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)]
    else:
        days = [args.date or date.today() - timedelta(days=1)]

    for d in days:
        logging.info("=== Running pipeline for %s ===", d)
        pipeline.run_all(d)


if __name__ == "__main__":
    main()
