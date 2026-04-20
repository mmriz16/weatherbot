#!/usr/bin/env python3
"""Generate dashboard JSON from the canonical reporting module."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from report_state import sync_state_summary, write_dashboard_json


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT = Path("/var/www/weatherbot/simulation.json")
LEARNING_LOG = DATA_DIR / "learning_log.json"
LEARNING_OUTPUT = Path("/var/www/weatherbot/learning.json")


def main() -> None:
    report = write_dashboard_json(DATA_DIR, OUTPUT)
    sync_state_summary(DATA_DIR, report)

    if LEARNING_LOG.exists():
        LEARNING_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(LEARNING_LOG, LEARNING_OUTPUT)
        LEARNING_OUTPUT.chmod(0o644)

    print(f"✅ Dashboard: {OUTPUT}")
    print(
        f"   Reported: ${report['reported_balance']:.2f} | Equity: ${report['expected_equity']:.2f} | "
        f"Open: {report['open_positions']} | W:{report['wins']} L:{report['losses']} Trail:{report['trail_count']} | "
        f"Deployed: ${report['total_deployed']:.2f} | Drift: {report['balance_drift']:+.2f}"
    )


if __name__ == "__main__":
    main()
