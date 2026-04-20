#!/usr/bin/env python3
"""Canonical weatherbot reporting state.

All user-facing reports should build from this module so counts, PnL, and trade
status stay consistent across CLI, dashboard, and Telegram outputs.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = Path("/var/www/weatherbot/simulation.json")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _market_key(market: dict) -> tuple[str, str]:
    return market.get("city", ""), market.get("date", "")


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def _market_priority(market: dict, file_path: Path) -> tuple[int, int, float]:
    position = market.get("position") or {}
    has_position = 1 if position.get("status") else 0
    closed_or_resolved = 1 if (market.get("status") == "resolved" or position.get("status") == "closed") else 0
    return has_position, closed_or_resolved, file_path.stat().st_mtime


def _dedupe_markets(markets_dir: Path) -> list[dict]:
    markets_map: dict[tuple[str, str], tuple[dict, tuple[int, int, float]]] = {}
    for fp in sorted(markets_dir.glob("*.json")):
        try:
            market = _read_json(fp, {})
        except Exception:
            continue

        key = _market_key(market)
        current = markets_map.get(key)
        priority = _market_priority(market, fp)

        if current is None or priority > current[1]:
            markets_map[key] = (market, priority)

    return [market for market, _priority in markets_map.values()]


def _get_current_price(market: dict, position: dict) -> float:
    exit_price = position.get("exit_price")
    if exit_price is not None:
        return float(exit_price)

    market_id = position.get("market_id", "")
    for outcome in market.get("all_outcomes", []):
        if outcome.get("market_id") == market_id:
            bid = outcome.get("bid")
            price = outcome.get("price")
            if bid is not None:
                return float(bid)
            if price is not None:
                return float(price)

    return float(position.get("entry_price", 0.0) or 0.0)


def _bucket_label(market: dict, position: dict, question: str) -> str:
    bucket_low = position.get("bucket_low")
    bucket_high = position.get("bucket_high")
    unit = "F" if market.get("unit") == "F" else "C"

    if bucket_low is None or bucket_high is None:
        return question[:40] if question else "?"
    if bucket_low <= -900:
        return f"≤{bucket_high}°{unit}"
    if bucket_high >= 900:
        return f"≥{bucket_low}°{unit}"
    return f"{bucket_low}-{bucket_high}°{unit}"


def build_report(data_dir: str | Path, now: datetime | None = None) -> dict:
    data_dir = Path(data_dir)
    now = now or datetime.now()
    state = _read_json(data_dir / "state.json", {})
    markets = _dedupe_markets(data_dir / "markets")

    trades: list[dict] = []
    wins = 0
    losses = 0
    trail_count = 0
    open_positions = 0
    total_deployed = 0.0
    unrealized_pnl = 0.0

    for market in markets:
        position = market.get("position") or {}
        if not position or position.get("status") is None:
            continue

        city_name = market.get("city_name", market.get("city", "Unknown"))
        date = market.get("date", "")
        question = position.get("question", "")
        current_price = _get_current_price(market, position)
        entry_price = float(position.get("entry_price", 0.0) or 0.0)
        shares = float(position.get("shares", 0.0) or 0.0)
        cost = float(position.get("cost", 0.0) or 0.0)
        pnl = position.get("pnl")
        if pnl is None:
            pnl = round((current_price - entry_price) * shares, 2)
        else:
            pnl = round(float(pnl), 2)

        market_status = market.get("status", "open")
        position_status = position.get("status", "open")
        close_reason = position.get("close_reason")
        resolved_outcome = market.get("resolved_outcome")

        if position_status == "open":
            display_status = "active"
            open_positions += 1
            total_deployed += cost
            unrealized_pnl += pnl
        elif market_status == "resolved":
            if resolved_outcome == "win":
                display_status = "won"
                wins += 1
            else:
                display_status = "lost"
                losses += 1
        else:
            if pnl > 0:
                display_status = "trail" if close_reason == "trailing_stop" else "won"
                wins += 1
                if close_reason == "trailing_stop":
                    trail_count += 1
            else:
                display_status = "lost"
                losses += 1

        trades.append(
            {
                "city": city_name,
                "city_code": market.get("city", ""),
                "date": date,
                "bucket": _bucket_label(market, position, question),
                "question": question,
                "entry_price": round(entry_price, 4),
                "current_price": round(current_price, 4),
                "shares": round(shares, 2),
                "cost": round(cost, 2),
                "pnl": round(pnl, 2),
                "ev": round(float(position.get("ev", 0.0) or 0.0), 4),
                "kelly_pct": round(float(position.get("kelly", 0.0) or 0.0) * 100, 1),
                "source": str(position.get("forecast_src", "ECMWF")).upper(),
                "status": display_status,
                "close_reason": close_reason,
                "forecast_temp": position.get("forecast_temp"),
                "actual_temp": market.get("actual_temp"),
                "resolved_outcome": resolved_outcome,
                "market_status": market_status,
                "position_status": position_status,
                "timestamp": position.get("opened_at", market.get("created_at", "")),
            }
        )

    realized_pnl = round(sum(t["pnl"] for t in trades if t["status"] != "active"), 2)
    closed_trades = wins + losses
    starting_balance = float(state.get("starting_balance", state.get("balance", 100.0)) or 100.0)
    balance = float(state.get("balance", starting_balance) or starting_balance)
    total_deployed = round(total_deployed, 2)
    unrealized_pnl = round(unrealized_pnl, 2)
    cash_balance = round(balance, 2)
    position_value = round(total_deployed + unrealized_pnl, 2)
    portfolio_equity = round(cash_balance + position_value, 2)
    expected_equity = round(starting_balance + realized_pnl + unrealized_pnl, 2)
    balance_drift = round(portfolio_equity - expected_equity, 2)
    balance_valid = abs(balance_drift) <= 0.01

    report = {
        "balance": cash_balance,
        "reported_balance": cash_balance,
        "cash_balance": cash_balance,
        "position_value": position_value,
        "portfolio_equity": portfolio_equity,
        "expected_equity": expected_equity,
        "balance_drift": balance_drift,
        "balance_valid": balance_valid,
        "starting_balance": round(starting_balance, 2),
        "peak_balance": round(float(state.get("peak_balance", starting_balance) or starting_balance), 2),
        "total_trades": len(trades),
        "closed_trades": closed_trades,
        "wins": wins,
        "losses": losses,
        "trail_count": trail_count,
        "total_deployed": total_deployed,
        "win_rate": round((wins / closed_trades) * 100, 1) if closed_trades else 0.0,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "open_positions": open_positions,
        "trades": trades,
        "last_scan": now.isoformat(),
        "status": "SIMULATION",
    }
    return report


def write_dashboard_json(data_dir: str | Path, output_path: str | Path = DEFAULT_OUTPUT) -> dict:
    report = build_report(data_dir)
    output_path = Path(output_path)
    _write_json_atomic(output_path, report)
    output_path.chmod(0o644)
    return report


def sync_state_summary(data_dir: str | Path, report: dict | None = None) -> dict:
    data_dir = Path(data_dir)
    state_path = data_dir / "state.json"
    state = _read_json(state_path, {})
    report = report or build_report(data_dir)

    state.update(
        {
            "balance": report["balance"],
            "reported_balance": report["reported_balance"],
            "cash_balance": report["cash_balance"],
            "position_value": report["position_value"],
            "portfolio_equity": report["portfolio_equity"],
            "expected_equity": report["expected_equity"],
            "balance_drift": report["balance_drift"],
            "balance_valid": report["balance_valid"],
            "starting_balance": report["starting_balance"],
            "peak_balance": report["peak_balance"],
            "total_trades": report["total_trades"],
            "closed_trades": report["closed_trades"],
            "wins": report["wins"],
            "losses": report["losses"],
            "trail_count": report["trail_count"],
            "open_positions": report["open_positions"],
            "total_deployed": report["total_deployed"],
            "realized_pnl": report["realized_pnl"],
            "unrealized_pnl": report["unrealized_pnl"],
            "win_rate": report["win_rate"],
            "last_report_at": report["last_scan"],
        }
    )
    _write_json_atomic(state_path, state)
    return state


def render_status_text(report: dict) -> str:
    reported_balance = report["reported_balance"]
    equity = report["expected_equity"]
    start = report["starting_balance"]
    ret_pct = ((equity - start) / start * 100) if start else 0.0
    lines = [
        "=" * 55,
        "  WEATHERBET — STATUS",
        "=" * 55,
        f"  Reported:    ${reported_balance:,.2f} (raw state balance)",
        f"  Equity:      ${equity:,.2f}  (start ${start:,.2f}, {ret_pct:+.1f}%)",
        f"  Positions:   ${report['position_value']:.2f} value | ${report['total_deployed']:.2f} deployed | {report['unrealized_pnl']:+.2f} U-PnL",
        f"  Trades:      {report['closed_trades']} | W: {report['wins']} | L: {report['losses']} | WR: {report['win_rate']:.1f}%",
        f"  Open:        {report['open_positions']}",
    ]

    if not report.get("balance_valid", True):
        lines.append(
            f"  Warning:     reported portfolio drift {report['balance_drift']:+.2f} vs expected equity ${report['expected_equity']:.2f}"
        )

    open_trades = [trade for trade in report["trades"] if trade["status"] == "active"]
    if open_trades:
        lines.append("")
        lines.append("  Open positions:")
        for trade in open_trades:
            lines.append(
                f"    {trade['city']:<16} {trade['date']} | {trade['bucket']:<14} | "
                f"entry ${trade['entry_price']:.3f} -> ${trade['current_price']:.3f} | "
                f"PnL: {trade['pnl']:+.2f} | {trade['source']}"
            )

    lines.append("=" * 55)
    return "\n".join(lines)


def render_daily_report(report: dict, now: datetime | None = None) -> str:
    now = now or datetime.now()
    reported_balance = report["reported_balance"]
    equity = report["expected_equity"]
    starting = report["starting_balance"]
    pct = ((equity - starting) / starting * 100) if starting else 0.0
    active = [trade for trade in report["trades"] if trade["status"] == "active"]

    lines = []
    lines.append("+-----------------------------------------+")
    lines.append("| Trade Daily Report                      |")
    lines.append("+----------------+------------------------+")
    lines.append(f"| Date           | {now.strftime('%B %d, %Y'):22} |")
    lines.append(f"| Reported Bal   | ${reported_balance:.2f}                 |")
    lines.append(f"| Equity         | ${equity:.2f} / ${starting:.0f} ({pct:+.1f}%) |")
    lines.append(f"| Positions      | {len(active):2} Open                 |")
    lines.append(f"| Position Value | ${report['position_value']:.2f}                 |")
    lines.append(f"| Deployed       | ${report['total_deployed']:.2f}                 |")
    lines.append(f"| Unrealized     | ${report['unrealized_pnl']:+.2f}                 |")
    lines.append(f"| Record         | {report['wins']}W / {report['losses']}L / {report['trail_count']}Trail         |")
    if not report.get("balance_valid", True):
        lines.append(f"| Drift Warn     | {report['balance_drift']:+.2f} vs exp ${report['expected_equity']:.2f} |")
    lines.append("+----------------+------------------------+")
    lines.append("")

    if active:
        lines.append("+----+--------------+--------+---------+--------+--------+---------+")
        lines.append("| #  | City         |  Date  | Bucket  | Entry  | Now    | P&L     |")
        lines.append("+----+--------------+--------+---------+--------+--------+---------+")
        for i, trade in enumerate(active, 1):
            pnl_str = f"{trade['pnl']:+.2f}"
            lines.append(
                f"| {i:2} | {trade['city'][:12]:12} | {trade['date'][-5:]:6} | {trade['bucket'][:7]:7} | "
                f"${trade['entry_price']:.3f} | ${trade['current_price']:.3f} | {pnl_str:>7} |"
            )
        lines.append("+----+--------------+--------+---------+--------+--------+---------+")
    else:
        lines.append("No open positions.")

    lines.append("")
    lines.append(f"Unrealized PnL: ${report['unrealized_pnl']:+.2f}")
    return "\n".join(lines)
