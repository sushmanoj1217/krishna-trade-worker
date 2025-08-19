# Disaster Recovery Runbook — KRISHNA

## Triggers
- Broken deploy / bad params / repo wipe / corrupted cache.
- Symptoms: worker crash-looping, no trades, OC stale, Telegram offline.

## Freeze
1. On Render **turn OFF Auto-Deploy** (Service → Settings).
2. Stop traffic to broken version: **Manual Deploy** previous good commit if needed.
3. Snapshot current ENV: Render → Environment → **Download .env** (or copy).

## Rescue (Render Shell)
1. Open a **Shell** on current instance.
2. Export environment:
   ```bash
   printenv | sort > /opt/render/project/.env.backup
