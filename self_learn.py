#!/usr/bin/env python3
"""
WeatherBot Self-Learning Analyzer v3
Analyzes trades + feeds Knowledge Base for Hermes RAG loop.
"""
import json
import os
import glob
from datetime import datetime
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path.home() / "weatherbot" / "data"
MARKETS_DIR = DATA_DIR / "markets"
STATE_FILE = DATA_DIR / "state.json"
CONFIG_FILE = Path.home() / "weatherbot" / "config.json"
MEMORY_FILE = DATA_DIR / "learning_log.json"

def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

def analyze():
    print("=" * 55)
    print("BRAIN WEATHERBOT SELF-LEARNING v2")
    print("=" * 55)

    state = load_json(STATE_FILE, {})
    cfg = load_json(CONFIG_FILE, {})
    log = load_json(MEMORY_FILE, {
        "city_accuracy": {}, "source_accuracy": {},
        "ev_performance": [], "adjustments": [],
        "last_analyzed": None, "total_analyses": 0
    })

    # Load markets
    markets = []
    for fp in sorted(glob.glob(str(MARKETS_DIR / "*.json"))):
        with open(fp) as f:
            markets.append(json.load(f))

    # --- SEPARATE OLD vs NEW LOGIC ---
    old_trades = []  # stop_loss exits (old logic)
    new_trades = []  # trailing, forecast, resolved (new logic)
    active_trades = []

    for m in markets:
        pos = m.get("position") or {}
        if not pos or pos.get("status") is None:
            continue

        city = m.get("city_name", m.get("city", "?"))
        pnl = pos.get("pnl") or 0
        close_reason = pos.get("close_reason", "")
        pos_status = pos.get("status", "")
        source = pos.get("forecast_src", "unknown").upper()
        ev = pos.get("ev", 0)

        trade = {
            "city": city, "pnl": pnl, "source": source,
            "ev": ev, "close_reason": close_reason,
            "market_status": m.get("status", "open")
        }

        if pos_status == "open":
            active_trades.append(trade)
        elif close_reason == "stop_loss":
            old_trades.append(trade)  # OLD LOGIC - ignore for analysis
        else:
            new_trades.append(trade)  # NEW LOGIC - analyze this

    # --- ANALYZE NEW LOGIC ONLY ---
    print(f"\nCHART SYSTEM STATUS:")
    print(f"  Balance:    ${state.get('balance', 0):.2f} / ${state.get('starting_balance', 100):.0f}")
    pnl_pct = (state.get('balance', 0) - state.get('starting_balance', 100)) / state.get('starting_balance', 100) * 100
    emoji = "UP" if pnl_pct >= 0 else "DOWN"
    print(f"  Return:     {pnl_pct:+.1f}% {emoji}")
    print(f"  Active:     {len(active_trades)} positions")
    print(f"  New exits:  {len(new_trades)} (trailing/forecast/resolve)")
    print(f"  Old exits:  {len(old_trades)} (stop_loss - ignored)")

    # City performance (new logic only)
    print(f"\nTARGET CITY PERFORMANCE (new logic):")
    city_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0, "ev_sum": 0})

    for t in new_trades:
        c = t["city"]
        city_stats[c]["trades"] += 1
        city_stats[c]["pnl"] += t["pnl"]
        city_stats[c]["ev_sum"] += t["ev"]
        if t["pnl"] > 0:
            city_stats[c]["wins"] += 1

    sorted_cities = sorted(city_stats.items(), key=lambda x: x[1]["pnl"], reverse=True)
    for city, stats in sorted_cities:
        n = stats["trades"]
        wr = stats["wins"] / n * 100 if n > 0 else 0
        avg_ev = stats["ev_sum"] / n if n > 0 else 0
        emoji = "STAR" if stats["pnl"] > 0 else "X"
        print(f"  {emoji} {city:15} | {n} trades | WR {wr:.0f}% | PnL ${stats['pnl']:+.2f} | EV ${avg_ev:.2f}")

    # Source performance (new logic only)
    print(f"\nSIGNAL SOURCE PERFORMANCE (new logic):")
    source_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})

    for t in new_trades:
        s = t["source"]
        source_stats[s]["trades"] += 1
        source_stats[s]["pnl"] += t["pnl"]
        if t["pnl"] > 0:
            source_stats[s]["wins"] += 1

    for src, stats in sorted(source_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
        n = stats["trades"]
        wr = stats["wins"] / n * 100 if n > 0 else 0
        emoji = "STAR" if stats["pnl"] > 0 else "X"
        print(f"  {emoji} {src:10} | {n} trades | WR {wr:.0f}% | PnL ${stats['pnl']:+.2f}")

    # EV accuracy
    ev_trades = [t for t in new_trades if t["ev"] > 0]
    if ev_trades:
        avg_ev = sum(t["ev"] for t in ev_trades) / len(ev_trades)
        avg_pnl = sum(t["pnl"] for t in ev_trades) / len(ev_trades)
        ev_wr = len([t for t in ev_trades if t["pnl"] > 0]) / len(ev_trades) * 100
        print(f"\nCHART EV ACCURACY:")
        print(f"  Avg EV:     ${avg_ev:.2f} per $1")
        print(f"  Avg PnL:    ${avg_pnl:+.2f}")
        print(f"  EV WR:      {ev_wr:.0f}%")
        if avg_pnl < avg_ev * 0.5:
            print(f"  WARNING: Actual PnL << EV prediction. Model may be overconfident.")

    # --- AUTO-ADJUST CONFIG ---
    print(f"\nWRENCH AUTO-ADJUSTMENTS:")
    adjustments = []

    total_new = len(new_trades)
    wins_new = len([t for t in new_trades if t["pnl"] > 0])
    wr_new = wins_new / total_new if total_new > 0 else 0

    balance = state.get("balance", 100)
    starting = state.get("starting_balance", 100)
    actual_return = (balance - starting) / starting * 100

    # Adjust 1: If new logic WR < 30% after 5+ trades → raise min_ev
    if total_new >= 5 and wr_new < 0.30:
        old_ev = cfg.get("min_ev", 0.1)
        new_ev = min(old_ev + 0.05, 0.30)
        if abs(new_ev - old_ev) > 0.001:
            cfg["min_ev"] = round(new_ev, 2)
            adjustments.append(f"min_ev: {old_ev} -> {new_ev} (WR {wr_new:.0%} too low)")
            print(f"  UP min_ev: {old_ev} -> {new_ev}")

    # Adjust 2: If return < -30% → lower max_bet percentage
    if actual_return < -30:
        old_pct = cfg.get("bet_pct", 5)
        new_pct = max(old_pct - 1, 2)
        cfg["bet_pct"] = new_pct
        adjustments.append(f"bet_pct: {old_pct}% -> {new_pct}% (return {actual_return:.0f}%)")
        print(f"  DOWN bet%: {old_pct}% -> {new_pct}%")

    # Adjust 3: If return > +50% and WR > 40% → raise max_bet
    if actual_return > 50 and wr_new > 0.40 and total_new >= 10:
        old_pct = cfg.get("bet_pct", 5)
        new_pct = min(old_pct + 1, 10)
        cfg["bet_pct"] = new_pct
        adjustments.append(f"bet_pct: {old_pct}% -> {new_pct}% (good performance)")
        print(f"  UP bet%: {old_pct}% -> {new_pct}%")

    # Adjust 4: Kelly fraction based on WR
    if total_new >= 10:
        if wr_new < 0.35:
            old_kelly = cfg.get("kelly_fraction", 0.25)
            new_kelly = max(old_kelly - 0.05, 0.10)
            cfg["kelly_fraction"] = round(new_kelly, 2)
            adjustments.append(f"kelly: {old_kelly} -> {new_kelly}")
            print(f"  DOWN kelly: {old_kelly} -> {new_kelly}")

    if adjustments:
        save_json(CONFIG_FILE, cfg)
        log["adjustments"].append({
            "timestamp": datetime.now().isoformat(),
            "changes": adjustments,
            "balance": balance,
            "return_pct": round(actual_return, 1),
            "new_logic_wr": round(wr_new * 100, 1)
        })
    else:
        print(f"  No adjustments needed")

    # --- FEED KNOWLEDGE BASE (RAG) ---
    print(f"\nBRAIN FEEDING KNOWLEDGE BASE:")
    try:
        import knowledge_base as kb
        kb.init()
        kb_findings = kb.analyze_from_trades()
        print(f"  Generated {len(kb_findings)} knowledge entries")
        for f in kb_findings[:5]:
            print(f"    {f}")
        if len(kb_findings) > 5:
            print(f"    ... and {len(kb_findings)-5} more")
    except Exception as e:
        print(f"  Knowledge base error: {e}")

    # Save learning
    log["last_analyzed"] = datetime.now().isoformat()
    log["total_analyses"] = log.get("total_analyses", 0) + 1
    save_json(MEMORY_FILE, log)

    print(f"\n{'='*55}")
    print(f"Total analyses: {log['total_analyses']}")
    print(f"Config: min_ev={cfg.get('min_ev')} kelly={cfg.get('kelly_fraction')} bet_pct={cfg.get('bet_pct', 5)}%")
    print(f"{'='*55}")

if __name__ == "__main__":
    analyze()
