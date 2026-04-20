#!/usr/bin/env python3
"""
WeatherBot Telegram Gateway
Control weather trading bot via Telegram.
"""

import os
import sys
import json
import asyncio
import subprocess
from datetime import datetime
from pathlib import Path

# Add parent path so we can import from weatherbot
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, ContextTypes
)
from report_state import build_report, render_daily_report

# === CONFIG ===
TOKEN = os.getenv("WEATHERBOT_TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
BOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = Path(BOT_DIR) / "data"
OWNER_ID = os.getenv("TELEGRAM_OWNER_ID")
ALLOWED_USERS = {
    int(value)
    for value in (os.getenv("WEATHERBOT_ALLOWED_USERS") or os.getenv("TELEGRAM_OWNER_ID") or "").split(",")
    if value.strip().isdigit()
}
DAILY_REPORT_CHAT_ID = int(os.getenv("WEATHERBOT_DAILY_REPORT_CHAT_ID") or OWNER_ID or 0) or None


def get_canonical_report() -> dict:
    return build_report(DATA_DIR)


async def ensure_authorized(update: Update) -> bool:
    if not ALLOWED_USERS:
        if update.message:
            await update.message.reply_text("⛔ Bot not configured: no allowed users")
        return False

    user = update.effective_user
    if user and user.id in ALLOWED_USERS:
        return True

    if update.message:
        await update.message.reply_text("⛔ Unauthorized")
    return False

def run_bot_cmd(args: list[str]) -> str:
    """Run bot_v1.py with given args and capture output."""
    cmd = [sys.executable, os.path.join(BOT_DIR, "bot_v1.py")] + args
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
            cwd=BOT_DIR
        )
        output = result.stdout
        if result.stderr:
            output += f"\n⚠️ stderr:\n{result.stderr[-500:]}"
        return output
    except subprocess.TimeoutExpired:
        return "⏰ Timeout — bot took too long (>120s)"
    except Exception as e:
        return f"❌ Error: {e}"

def run_bot_v2(args: list[str]) -> str:
    """Run bot_v2.py (full bot) with given args."""
    cmd = [sys.executable, os.path.join(BOT_DIR, "bot_v2.py")] + args
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=180,
            cwd=BOT_DIR
        )
        output = result.stdout
        if result.stderr:
            output += f"\n⚠️ stderr:\n{result.stderr[-500:]}"
        return output
    except subprocess.TimeoutExpired:
        return "⏰ Timeout — bot took too long (>180s)"
    except Exception as e:
        return f"❌ Error: {e}"


def build_v2_scan_once_cmd(bot_dir: str) -> list[str]:
    inline = f"""
import atexit
import json
import os

os.chdir({bot_dir!r})

import bot_v2
from runtime_guard import AlreadyRunningError, acquire_lock, release_lock

try:
    lock_handle = acquire_lock(bot_v2.RUN_LOCK_FILE)
except AlreadyRunningError as exc:
    print(f"[LOCK] {{exc}}")
    raise SystemExit(1)

atexit.register(release_lock, lock_handle)
bot_v2._cal = bot_v2.load_cal()
result = bot_v2.scan_and_update()
print(json.dumps({{'new': result[0], 'closed': result[1], 'resolved': result[2]}}))
"""
    return [sys.executable, "-c", inline]


def run_bot_v2_scan_once() -> str:
    cmd = build_v2_scan_once_cmd(BOT_DIR)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=180,
            cwd=BOT_DIR
        )
        output = result.stdout
        if result.stderr:
            output += f"\n⚠️ stderr:\n{result.stderr[-500:]}"
        return output
    except subprocess.TimeoutExpired:
        return "⏰ Timeout — v2 scan took too long (>180s)"
    except Exception as e:
        return f"❌ Error: {e}"

def escape_md(text: str) -> str:
    """Escape text for Telegram MarkdownV2-ish formatting."""
    # Simple: just strip problematic chars and use plain text
    return text

# === COMMANDS ===

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    await update.message.reply_text(
        "🌤 *WeatherBot Telegram Gateway*\n\n"
        "Commands:\n"
        "/scan \\- Run paper scan\n"
        "/live \\- Run live simulation\n"
        "/positions \\- Show open positions\n"
        "/status \\- Show bot status\n"
        "/report \\- Full report\n"
        "/reset \\- Reset simulation balance\n"
        "/v2scan \\- Run bot v2 scan\n",
        parse_mode="Markdown"
    )

async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    msg = await update.message.reply_text("🔍 Scanning markets...")
    output = run_bot_cmd([])
    # Truncate if too long for Telegram
    if len(output) > 3500:
        output = output[:3500] + "\n\n... (truncated)"
    await msg.edit_text(f"```\n{output}\n```", parse_mode="Markdown")

async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    msg = await update.message.reply_text("💰 Running live simulation...")
    output = run_bot_cmd(["--live"])
    if len(output) > 3500:
        output = output[:3500] + "\n\n... (truncated)"
    await msg.edit_text(f"```\n{output}\n```", parse_mode="Markdown")

async def cmd_positions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    output = run_bot_cmd(["--positions"])
    await update.message.reply_text(f"```\n{output}\n```", parse_mode="Markdown")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    try:
        report = get_canonical_report()
        balance = report.get("balance", 100.0)
        started = report.get("starting_balance", 100.0)
        trades = report.get("total_trades", 0)
        wins = report.get("wins", 0)
        losses = report.get("losses", 0)
        pnl = ((balance - started) / started) * 100 if started else 0.0
        positions = [trade for trade in report.get("trades", []) if trade.get("status") == "active"]
        emoji = "📈" if pnl >= 0 else "📉"
        text = (
            f"🌤 *WeatherBot Status*\n\n"
            f"{emoji} PnL: *{pnl:+.1f}%*\n"
            f"💰 Balance: *${balance:.2f}*\n"
            f"📊 Trades: {trades} (W:{wins} / L:{losses})\n"
            f"📋 Open positions: {len(positions)}\n"
        )
        if positions:
            text += "\n*Open Positions:*\n"
            for p in positions[:5]:
                q = p.get("question") or f"{p.get('city', '?')} {p.get('bucket', '?')}"
                q = q[:60]
                entry = p.get("entry_price", "?")
                text += f"• {q}... @ ${entry}\n"
    except FileNotFoundError:
        text = "ℹ️ No simulation data yet. Run /scan or /live first."
    except Exception as e:
        text = f"❌ Error reading state: {e}"
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    msg = await update.message.reply_text("📊 Generating report...")
    output = run_bot_v2(["report"])
    if len(output) > 3500:
        output = output[:3500] + "\n\n... (truncated)"
    await msg.edit_text(f"```\n{output}\n```", parse_mode="Markdown")

async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    output = run_bot_cmd(["--reset"])
    await update.message.reply_text(f"✅ {output.strip()}")

async def cmd_daily(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Generate and send daily report."""
    if not await ensure_authorized(update):
        return
    msg = await update.message.reply_text("📊 Generating daily report...")
    
    try:
        report = render_daily_report(get_canonical_report())
        if len(report) > 3500:
            report = report[:3500] + "\n\n... (truncated)"
        await msg.edit_text(f"```\n{report}\n```", parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")


async def daily_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        report = render_daily_report(get_canonical_report())
        if len(report) > 3500:
            report = report[:3500] + "\n\n... (truncated)"
        if ctx.job and ctx.job.chat_id:
            await ctx.bot.send_message(chat_id=ctx.job.chat_id, text=f"```\n{report}\n```", parse_mode="Markdown")
    except Exception as e:
        if ctx.job and ctx.job.chat_id:
            await ctx.bot.send_message(chat_id=ctx.job.chat_id, text=f"❌ Daily report error: {e}")


async def cmd_v2scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    msg = await update.message.reply_text("🔍 Running v2 one-shot scan...")
    output = run_bot_v2_scan_once()
    if len(output) > 3500:
        output = output[:3500] + "\n\n... (truncated)"
    await msg.edit_text(f"```\n{output}\n```", parse_mode="Markdown")

async def cmd_v2live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update):
        return
    await update.message.reply_text(
        "ℹ️ v2 live mode belum ada command terpisah. Bot live utama jalan via weatherbot.service, cek /status atau /report aja.",
        parse_mode="Markdown"
    )

# === MAIN ===

def main():
    if not TOKEN:
        raise RuntimeError("Set WEATHERBOT_TELEGRAM_TOKEN or TELEGRAM_BOT_TOKEN before starting tg_gateway.py")
    if not ALLOWED_USERS:
        raise RuntimeError("Set TELEGRAM_OWNER_ID or WEATHERBOT_ALLOWED_USERS before starting tg_gateway.py")

    app = Application.builder().token(TOKEN).build()

    # Register commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("live", cmd_live))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("v2scan", cmd_v2scan))
    app.add_handler(CommandHandler("v2live", cmd_v2live))

    print("🌤 WeatherBot Telegram Gateway started!")
    print("Press Ctrl+C to stop.")

    # Schedule daily report at 9 AM WIB (UTC+7) = 2 AM UTC
    if app.job_queue and DAILY_REPORT_CHAT_ID:
        app.job_queue.run_daily(
            daily_report_job,
            time=datetime.strptime("02:00", "%H:%M").time(),  # 9 AM WIB
            days=(0, 1, 2, 3, 4, 5, 6),  # Every day
            name="daily_report",
            chat_id=DAILY_REPORT_CHAT_ID,
        )
        print(f"📅 Daily report scheduled at 9 AM WIB for chat {DAILY_REPORT_CHAT_ID}")

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
