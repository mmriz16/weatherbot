#!/bin/bash
# WeatherBot Boot Sequence
# Dipanggil saat agent baru session atau reset
# Load semua state + RAG + learning context

echo "⚡ WeatherBot Boot Sequence"
echo "=========================="

# 1. Check services
echo ""
echo "📦 Services:"
sudo systemctl is-active weatherbot --quiet && echo "  ✅ weatherbot (bot v2)" || echo "  ❌ weatherbot DOWN"
sudo systemctl is-active weatherbot-tg --quiet && echo "  ✅ weatherbot-tg (Telegram)" || echo "  ❌ weatherbot-tg DOWN"

# 2. Check data
echo ""
echo "📁 Data:"
[ -f ~/weatherbot/data/state.json ] && echo "  ✅ state.json" || echo "  ❌ state.json missing"
[ -d ~/weatherbot/data/markets ] && echo "  ✅ markets/ ($(ls ~/weatherbot/data/markets/*.json 2>/dev/null | wc -l) files)" || echo "  ❌ markets/ missing"
[ -d ~/weatherbot/data/chroma ] && echo "  ✅ chroma/ (vector DB)" || echo "  ⚠️ chroma/ missing (run: python3 rag_engine.py index)"
[ -f ~/weatherbot/data/insights.json ] && echo "  ✅ insights.json" || echo "  ⚠️ insights.json missing"
[ -f ~/weatherbot/data/rag_memory.json ] && echo "  ✅ rag_memory.json" || echo "  ⚠️ rag_memory.json missing"
[ -f ~/weatherbot/data/learning_log.json ] && echo "  ✅ learning_log.json" || echo "  ⚠️ learning_log.json missing"
[ -f ~/weatherbot/.env ] && echo "  ✅ .env (wallet)" || echo "  ❌ .env missing"

# 3. Check config
echo ""
echo "⚙️ Config:"
cd ~/weatherbot && python3 -c "
import json
with open('config.json') as f:
    c = json.load(f)
print(f'  balance: \${c[\"balance\"]}')
print(f'  max_bet: \${c[\"max_bet\"]}')
print(f'  min_ev: {c[\"min_ev\"]}')
print(f'  kelly: {c[\"kelly_fraction\"]}')
print(f'  vc_key: {c[\"vc_key\"][:8]}...')
"

# 4. Current status
echo ""
echo "📊 Status:"
cd ~/weatherbot && source venv/bin/activate && python3 -c "
import json
with open('data/state.json') as f:
    s = json.load(f)
bal = s.get('balance', 0)
start = s.get('starting_balance', 100)
pct = (bal - start) / start * 100
print(f'  Balance: \${bal:.2f} / \${start:.0f} ({pct:+.1f}%)')
print(f'  Trades: {s.get(\"total_trades\", 0)}')
print(f'  W: {s.get(\"wins\", 0)} L: {s.get(\"losses\", 0)}')

import glob
open_count = sum(1 for f in glob.glob('data/markets/*.json') 
                 if json.load(open(f)).get('position', {}) or {}.get('status') == 'open')
print(f'  Open: {open_count}')
" 2>/dev/null

# 5. RAG status
echo ""
echo "🧠 RAG Engine:"
cd ~/weatherbot && source venv/bin/activate && python3 -c "
try:
    import chromadb
    client = chromadb.PersistentClient(path='data/chroma')
    col = client.get_collection('weather_trades')
    print(f'  Vector DB: {col.count()} trades indexed')
except Exception as e:
    print(f'  Vector DB: not available ({e})')
    
import os
if os.path.exists('data/insights.json'):
    with open('data/insights.json') as f:
        ins = json.load(f)
    cities = len(ins.get('city_insights', {}))
    rules = sum(len(v.get('learnings', [])) for v in ins.get('city_insights', {}).values())
    print(f'  Insights: {cities} cities, {rules} learnings')
else:
    print('  Insights: none yet')
" 2>/dev/null

# 6. Dashboard
echo ""
echo "🌐 Dashboard:"
curl -s -o /dev/null -w "  HTTP %{http_code}" "http://weatherbot.termicons.com/" 2>/dev/null || echo "  ⚠️ Not accessible"

# 7. Cron jobs
echo ""
echo ""
echo "⏰ Cron Jobs:"
crontab -l 2>/dev/null | grep -v "^#" | while read line; do echo "  $line"; done

echo ""
echo "=========================="
echo "✅ Boot complete. Agent ready."
