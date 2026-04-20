# Skill: WeatherBot Post-Trade Analyzer

## Purpose
Pick up trade analysis records from the weatherbot SQLite database,
synthesize patterns across multiple losing trades, and generate updated
trading rules as a Hermes skill document.

## Trigger
Run this skill after every 10 resolved trades, or when WR drops below 50%
over a rolling 20-trade window.

## Steps

1. Connect to `weatherbot.db` and query `post_trade_log`:
   ```sql
   SELECT * FROM post_trade_log WHERE outcome='loss' ORDER BY created_at DESC LIMIT 20;
   ```

2. Group analyses by theme:
   - Forecast error (ECMWF vs HRRR disagreement)
   - Bad timing (entered too late, <4h to resolution)
   - Liquidity (volume below threshold)
   - Bucket edge (trade near boundary of t_low/t_high)
   - Market manipulation or unusual price movement

3. For each theme with 3+ occurrences, generate a new rule candidate
   in plain English. Example:
   - "When ECMWF and HRRR differ by >3°, reduce position size by 50%"
   - "Skip markets with <6h to resolution regardless of EV"

4. Compare rule candidates against existing config.json parameters.
   If a candidate improves on existing params, update config.json.

5. Save updated rules to `learned_rules.json`:
   ```json
   {
     "version": "auto-{date}",
     "rules": [
       {
         "theme": "forecast_disagreement",
         "trigger": "ecmwf_hrrr_diff > 3",
         "action": "size_multiplier = 0.5",
         "evidence_count": 4,
         "confidence": "medium"
       }
     ]
   }
   ```

6. Log to Telegram: "📊 Hermes updated {N} trading rules based on {M} loss analyses"

## Notes
- Never increase MAX_BET or reduce MIN_EV below 0.05 automatically.
- Rules require evidence_count >= 3 before being applied to live trading.
- In sim mode, rules are applied immediately. In real mode, require manual confirmation.
