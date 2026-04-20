#!/usr/bin/env python3
"""Generate formatted daily report from the canonical reporting module."""

from __future__ import annotations

from pathlib import Path

from report_state import build_report, render_daily_report


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


def generate_report() -> str:
    return render_daily_report(build_report(DATA_DIR))


if __name__ == "__main__":
    print(generate_report())
