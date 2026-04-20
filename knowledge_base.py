#!/usr/bin/env python3
"""
knowledge_base.py — RAG Knowledge Base untuk WeatherBot
=======================================================
SQLite-backed knowledge store. Hermes query dulu sebelum research,
kalau udah ada knowledge → langsung pake, ga perlu riset ulang.

Struktur:
  - city_insights:   per-kota (forecast bias, best hours, profitable buckets)
  - trade_lessons:   per-trade (kenapa kalah/menang, pattern)
  - source_accuracy: per kota × source (win rate, avg PnL, EV accuracy)
  - config_history:  perubahan config + hasilnya
  - rules:           aturan yang udah terbukti
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "knowledge.db"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init():
    """Create tables if not exist."""
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS city_insights (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city        TEXT NOT NULL,
            category    TEXT NOT NULL,   -- 'forecast_bias', 'best_hours', 'profitable_bucket', 'avoid'
            insight     TEXT NOT NULL,   -- human-readable
            evidence    TEXT,            -- JSON: supporting data
            confidence  REAL DEFAULT 0.5,
            trades_seen INTEGER DEFAULT 0,
            created_at  TEXT,
            updated_at  TEXT,
            UNIQUE(city, category)
        );

        CREATE TABLE IF NOT EXISTS trade_lessons (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT,
            city        TEXT,
            date        TEXT,
            outcome     TEXT,           -- 'win' | 'loss'
            reason      TEXT,           -- close_reason
            lesson      TEXT NOT NULL,  -- what we learned
            category    TEXT,           -- 'timing', 'forecast', 'entry_price', 'sizing', 'exit'
            evidence    TEXT,           -- JSON
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS config_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            param_name  TEXT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            reason      TEXT,
            result      TEXT,           -- filled after observing effect
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS rules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            rule        TEXT NOT NULL,          -- human-readable rule
            trigger     TEXT,                    -- condition
            action      TEXT,                    -- what to do
            source      TEXT,                    -- 'calibration', 'trade_analysis', 'manual'
            evidence_count INTEGER DEFAULT 1,
            active      INTEGER DEFAULT 1,
            created_at  TEXT,
            updated_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS source_accuracy (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city        TEXT NOT NULL,
            source      TEXT NOT NULL,       -- 'ecmwf', 'gfs', 'metar'
            trades      INTEGER DEFAULT 0,
            wins        INTEGER DEFAULT 0,
            losses      INTEGER DEFAULT 0,
            total_pnl   REAL DEFAULT 0.0,
            total_ev    REAL DEFAULT 0.0,
            total_mae   REAL DEFAULT 0.0,    -- forecast error
            best_bucket TEXT,                 -- which temp bucket this source excels at
            best_hour   TEXT,                 -- time of day this source is best
            updated_at  TEXT,
            UNIQUE(city, source)
        );

        CREATE INDEX IF NOT EXISTS idx_source_accuracy_city ON source_accuracy(city);
        CREATE INDEX IF NOT EXISTS idx_source_accuracy_source ON source_accuracy(source);

        CREATE INDEX IF NOT EXISTS idx_city_insights_city ON city_insights(city);
        CREATE INDEX IF NOT EXISTS idx_trade_lessons_city ON trade_lessons(city);
        CREATE INDEX IF NOT EXISTS idx_trade_lessons_outcome ON trade_lessons(outcome);
        CREATE INDEX IF NOT EXISTS idx_rules_active ON rules(active);
        """)
    return True


# =============================================================================
# QUERY — Cek knowledge sebelum research
# =============================================================================

def get_city_insights(city: str) -> list[dict]:
    """Get all insights for a city."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM city_insights WHERE city=? ORDER BY confidence DESC",
            (city,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_city_summary() -> dict:
    """Get best/worst cities by insight confidence."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT city,
                   COUNT(*) as insight_count,
                   AVG(confidence) as avg_confidence,
                   SUM(trades_seen) as total_evidence
            FROM city_insights
            GROUP BY city
            ORDER BY avg_confidence DESC
        """).fetchall()
    return {r["city"]: dict(r) for r in rows}


def get_source_accuracy(city: str = None) -> list[dict]:
    """Get source accuracy data, optionally filtered by city."""
    with get_conn() as conn:
        if city:
            rows = conn.execute("""
                SELECT *, 
                       ROUND(1.0 * wins / MAX(trades, 1), 3) as win_rate,
                       ROUND(total_pnl / MAX(trades, 1), 2) as avg_pnl,
                       ROUND(total_ev / MAX(trades, 1), 2) as avg_ev,
                       ROUND(total_mae / MAX(trades, 1), 2) as avg_mae
                FROM source_accuracy 
                WHERE city=?
                ORDER BY win_rate DESC, avg_pnl DESC
            """, (city,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT *, 
                       ROUND(1.0 * wins / MAX(trades, 1), 3) as win_rate,
                       ROUND(total_pnl / MAX(trades, 1), 2) as avg_pnl,
                       ROUND(total_ev / MAX(trades, 1), 2) as avg_ev,
                       ROUND(total_mae / MAX(trades, 1), 2) as avg_mae
                FROM source_accuracy 
                ORDER BY city, win_rate DESC
            """).fetchall()
    return [dict(r) for r in rows]


def get_best_source_per_city() -> dict:
    """Get the best performing source for each city."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT city, source, 
                   trades, wins,
                   ROUND(1.0 * wins / MAX(trades, 1), 3) as win_rate,
                   ROUND(total_pnl / MAX(trades, 1), 2) as avg_pnl
            FROM source_accuracy
            WHERE trades >= 3
            ORDER BY city, win_rate DESC, avg_pnl DESC
        """).fetchall()

    best = {}
    seen = set()
    for r in rows:
        if r["city"] not in seen:
            best[r["city"]] = dict(r)
            seen.add(r["city"])
    return best


def get_source_ranking() -> list[dict]:
    """Global source ranking across all cities."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT source,
                   SUM(trades) as total_trades,
                   SUM(wins) as total_wins,
                   ROUND(1.0 * SUM(wins) / MAX(SUM(trades), 1), 3) as win_rate,
                   ROUND(SUM(total_pnl), 2) as total_pnl,
                   ROUND(AVG(total_pnl / MAX(trades, 1)), 2) as avg_pnl_per_city
            FROM source_accuracy
            GROUP BY source
            ORDER BY win_rate DESC, total_pnl DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_active_rules() -> list[dict]:
    """Get all active rules."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM rules WHERE active=1 ORDER BY evidence_count DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def search_lessons(city: str = None, category: str = None,
                   outcome: str = None, limit: int = 20) -> list[dict]:
    """Search trade lessons with filters."""
    query = "SELECT * FROM trade_lessons WHERE 1=1"
    params = []
    if city:
        query += " AND city=?"
        params.append(city)
    if category:
        query += " AND category=?"
        params.append(category)
    if outcome:
        query += " AND outcome=?"
        params.append(outcome)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_knowledge_summary() -> str:
    """Get a text summary of all knowledge — for Hermes context."""
    with get_conn() as conn:
        cities = conn.execute("""
            SELECT city, category, insight, confidence
            FROM city_insights ORDER BY city, confidence DESC
        """).fetchall()

        rules = conn.execute(
            "SELECT rule, evidence_count FROM rules WHERE active=1 ORDER BY evidence_count DESC"
        ).fetchall()

        lessons = conn.execute("""
            SELECT category, COUNT(*) as cnt, 
                   SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END) as losses
            FROM trade_lessons GROUP BY category ORDER BY cnt DESC
        """).fetchall()

        sources = conn.execute("""
            SELECT city, source, trades, wins,
                   ROUND(1.0 * wins / MAX(trades, 1), 3) as win_rate,
                   ROUND(total_pnl / MAX(trades, 1), 2) as avg_pnl
            FROM source_accuracy
            WHERE trades >= 2
            ORDER BY city, win_rate DESC
        """).fetchall()

    lines = ["=== KNOWGE BASE SUMMARY ===\n"]

    if sources:
        lines.append("SOURCE ACCURACY (per city):")
        current_city = None
        for s in sources:
            if s["city"] != current_city:
                current_city = s["city"]
                lines.append(f"\n  [{s['city']}]")
            emoji = "⭐" if s["win_rate"] >= 0.5 else "  "
            lines.append(
                f"    {emoji} {s['source']:8s} | {s['trades']:2d} trades | "
                f"WR {s['win_rate']:.0%} | PnL ${s['avg_pnl']:+.2f}"
            )

    if cities:
        lines.append("CITY INSIGHTS:")
        current_city = None
        for c in cities:
            if c["city"] != current_city:
                current_city = c["city"]
                lines.append(f"\n  [{c['city']}]")
            conf = f"{c['confidence']:.0%}"
            lines.append(f"    • {c['category']}: {c['insight']} ({conf})")

    if rules:
        lines.append("\nACTIVE RULES:")
        for r in rules:
            lines.append(f"  • {r['rule']} (evidence: {r['evidence_count']})")

    if lessons:
        lines.append("\nLESSON STATS:")
        for l in lessons:
            lines.append(f"  • {l['category']}: {l['cnt']} trades ({l['losses']} losses)")

    return "\n".join(lines)


# =============================================================================
# WRITE — Simpan knowledge baru
# =============================================================================

def save_city_insight(city: str, category: str, insight: str,
                      evidence: dict = None, confidence: float = 0.5):
    """Upsert a city insight."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, trades_seen FROM city_insights WHERE city=? AND category=?",
            (city, category)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE city_insights 
                SET insight=?, evidence=?, confidence=?, 
                    trades_seen=trades_seen+1, updated_at=?
                WHERE id=?
            """, (insight, json.dumps(evidence) if evidence else None,
                  confidence, _now(), existing["id"]))
        else:
            conn.execute("""
                INSERT INTO city_insights 
                (city, category, insight, evidence, confidence, trades_seen, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            """, (city, category, insight,
                  json.dumps(evidence) if evidence else None,
                  confidence, _now(), _now()))


def save_trade_lesson(market_id: str, city: str, date: str,
                      outcome: str, reason: str, lesson: str,
                      category: str, evidence: dict = None):
    """Save a trade lesson."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO trade_lessons 
            (market_id, city, date, outcome, reason, lesson, category, evidence, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (market_id, city, date, outcome, reason, lesson, category,
              json.dumps(evidence) if evidence else None, _now()))


def save_config_change(param_name: str, old_value, new_value, reason: str):
    """Record a config parameter change."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO config_history (param_name, old_value, new_value, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (param_name, str(old_value), str(new_value), reason, _now()))


def save_source_accuracy(city: str, source: str, won: bool,
                         pnl: float, ev: float, mae: float = None,
                         bucket: str = None, hour: str = None):
    """Update source accuracy stats for a city × source."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, trades, wins, losses, total_pnl, total_ev, total_mae FROM source_accuracy WHERE city=? AND source=?",
            (city, source)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE source_accuracy SET
                    trades = trades + 1,
                    wins = wins + ?,
                    losses = losses + ?,
                    total_pnl = total_pnl + ?,
                    total_ev = total_ev + ?,
                    total_mae = total_mae + ?,
                    best_bucket = COALESCE(?, best_bucket),
                    best_hour = COALESCE(?, best_hour),
                    updated_at = ?
                WHERE id = ?
            """, (1 if won else 0, 0 if won else 1,
                  pnl, ev, mae or 0, bucket, hour, _now(), existing["id"]))
        else:
            conn.execute("""
                INSERT INTO source_accuracy
                (city, source, trades, wins, losses, total_pnl, total_ev, total_mae, best_bucket, best_hour, updated_at)
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (city, source,
                  1 if won else 0, 0 if won else 1,
                  pnl, ev, mae or 0, bucket, hour, _now()))


def save_rule(rule: str, trigger: str = None, action: str = None,
              source: str = "manual"):
    """Save or update a rule. Increments evidence if exists."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, evidence_count FROM rules WHERE rule=? AND active=1",
            (rule,)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE rules SET evidence_count=evidence_count+1, updated_at=?
                WHERE id=?
            """, (_now(), existing["id"]))
        else:
            conn.execute("""
                INSERT INTO rules (rule, trigger, action, source, evidence_count, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, 1, ?, ?)
            """, (rule, trigger, action, source, _now(), _now()))


def deactivate_rule(rule_id: int):
    """Deactivate a rule that's no longer useful."""
    with get_conn() as conn:
        conn.execute("UPDATE rules SET active=0, updated_at=? WHERE id=?",
                     (_now(), rule_id))


# =============================================================================
# ANALYSIS — Auto-generate insights dari trade data
# =============================================================================

def analyze_from_trades(weatherbot_db: str = "weatherbot.db") -> list[str]:
    """
    Baca data dari weatherbot.db + market JSON files, generate insights.
    Dipanggil sama Hermes cron job.
    """
    import glob as _glob
    findings = []
    wb_path = Path(__file__).parent / weatherbot_db
    has_wb_db = wb_path.exists()

    # === SQLite analysis (if weatherbot.db exists) ===
    if has_wb_db:
        wb_conn = sqlite3.connect(wb_path)
        wb_conn.row_factory = sqlite3.Row

        # 1. Win rate per city
        rows = wb_conn.execute("""
            SELECT m.city,
                   COUNT(*) as total,
                   SUM(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END) as wins,
                   ROUND(AVG(p.pnl), 2) as avg_pnl,
                   ROUND(1.0 * SUM(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as wr
            FROM positions p JOIN markets m ON p.market_id = m.id
            WHERE p.status = 'closed'
            GROUP BY m.city
            HAVING total >= 3
            ORDER BY avg_pnl DESC
        """).fetchall()

        for r in rows:
            if r["wr"] >= 0.6 and r["avg_pnl"] > 0:
                save_city_insight(r["city"], "profitable",
                                  f"Win rate {r['wr']:.0%}, avg PnL ${r['avg_pnl']:.2f}",
                                  {"wr": r["wr"], "avg_pnl": r["avg_pnl"], "trades": r["total"]},
                                  confidence=min(0.9, r["wr"]))
                findings.append(f"✅ {r['city']}: profitable (WR {r['wr']:.0%})")
            elif r["wr"] <= 0.3 or r["avg_pnl"] < -1:
                save_city_insight(r["city"], "avoid",
                                  f"Win rate {r['wr']:.0%}, avg PnL ${r['avg_pnl']:.2f}",
                                  {"wr": r["wr"], "avg_pnl": r["avg_pnl"], "trades": r["total"]},
                                  confidence=min(0.9, 1 - r["wr"]))
                findings.append(f"⚠️ {r['city']}: losing (WR {r['wr']:.0%})")

        # 2. Timing patterns
        timing = wb_conn.execute("""
            SELECT m.city,
                   CASE 
                       WHEN m.hours_left < 6 THEN 'short (<6h)'
                       WHEN m.hours_left < 24 THEN 'medium (6-24h)'
                       ELSE 'long (>24h)'
                   END as bucket,
                   COUNT(*) as total,
                   ROUND(1.0 * SUM(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as wr,
                   ROUND(AVG(p.pnl), 2) as avg_pnl
            FROM positions p JOIN markets m ON p.market_id = m.id
            WHERE p.status = 'closed' AND m.hours_left IS NOT NULL
            GROUP BY bucket
            HAVING total >= 3
        """).fetchall()

        for r in timing:
            if r["wr"] >= 0.6:
                save_rule(
                    f"Entry timing '{r['bucket']}' has {r['wr']:.0%} WR",
                    trigger=f"hours_left in {r['bucket']}",
                    action="prefer_entry",
                    source="trade_analysis"
                )
                findings.append(f"⏰ Timing {r['bucket']}: WR {r['wr']:.0%}")

        # 3. Forecast source accuracy from calibration table
        calibration = wb_conn.execute("""
            SELECT city, source, mae, n
            FROM calibration
            WHERE n >= 5
            ORDER BY city, mae ASC
        """).fetchall()

        city_sources = {}
        for r in calibration:
            if r["city"] not in city_sources:
                city_sources[r["city"]] = []
            city_sources[r["city"]].append(r)

        for city, sources in city_sources.items():
            best = sources[0]
            worst = sources[-1]
            save_city_insight(city, "forecast_bias",
                              f"Best source: {best['source']} (MAE {best['mae']:.1f}), "
                              f"Worst: {worst['source']} (MAE {worst['mae']:.1f})",
                              {"best": best["source"], "worst": worst["source"],
                               "mae_diff": round(worst["mae"] - best["mae"], 2)},
                              confidence=min(0.8, best["n"] / 30))
            findings.append(f"📊 {city}: best={best['source']} worst={worst['source']}")

        # 4. Loss reason patterns
        losses = wb_conn.execute("""
            SELECT close_reason, COUNT(*) as cnt
            FROM positions WHERE status = 'closed' AND pnl < 0
            GROUP BY close_reason ORDER BY cnt DESC
        """).fetchall()

        for r in losses:
            if r["cnt"] >= 2:
                save_rule(
                    f"'{r['close_reason']}' caused {r['cnt']} losses — review entry filter",
                    trigger=f"close_reason == '{r['close_reason']}'",
                    action="tighten_filter",
                    source="trade_analysis"
                )
                findings.append(f"❌ {r['close_reason']}: {r['cnt']} losses")

        wb_conn.close()
    else:
        findings.append("weatherbot.db not found — using JSON market data only")

    # === JSON market analysis (always runs) ===
    for fp in _glob.glob(str(Path(__file__).parent / "data" / "markets" / "*.json")):
        try:
            with open(fp) as f:
                mkt = json.load(f)
        except Exception:
            continue

        pos = mkt.get("position") or {}
        if pos.get("status") != "closed" or not pos.get("pnl"):
            continue

        city = mkt.get("city_name", mkt.get("city", "?"))
        src = pos.get("forecast_src", "unknown")
        pnl = pos.get("pnl", 0)
        ev = pos.get("ev", 0)
        won = pnl > 0
        forecast = pos.get("forecast_temp")
        actual = mkt.get("actual_temp")
        mae = abs(forecast - actual) if forecast and actual else None
        bucket = f"{pos.get('bucket_low', '?')}-{pos.get('bucket_high', '?')}"

        save_source_accuracy(city, src, won, pnl, ev, mae=mae, bucket=bucket)

    # Save best source per city as insight
    best_sources = get_best_source_per_city()
    for city, data in best_sources.items():
        if data["trades"] >= 3:
            save_city_insight(
                city, "best_source",
                f"{data['source']} is best: {data['win_rate']:.0%} WR, "
                f"${data['avg_pnl']:+.2f} avg PnL ({data['trades']} trades)",
                {"source": data["source"], "win_rate": data["win_rate"],
                 "avg_pnl": data["avg_pnl"], "trades": data["trades"]},
                confidence=min(0.9, data["trades"] / 10)
            )
            findings.append(
                f"🏆 {city}: best source = {data['source']} "
                f"(WR {data['win_rate']:.0%}, ${data['avg_pnl']:+.2f})"
            )

    # Save global source ranking as rule
    ranking = get_source_ranking()
    for r in ranking:
        if r["total_trades"] >= 5:
            save_rule(
                f"Source '{r['source']}' overall: {r['win_rate']:.0%} WR, "
                f"${r['total_pnl']:+.2f} total across {r['total_trades']} trades",
                trigger="source_selection",
                action=f"prefer_{r['source']}" if r["win_rate"] >= 0.5 else f"deprioritize_{r['source']}",
                source="trade_analysis"
            )

    return findings


# =============================================================================
# INIT
# =============================================================================

if __name__ == "__main__":
    init()
    print("Knowledge base ready:", DB_PATH)
    print()

    findings = analyze_from_trades()
    print(f"Generated {len(findings)} insights:")
    for f in findings:
        print(f"  {f}")
    print()
    print(get_knowledge_summary())
