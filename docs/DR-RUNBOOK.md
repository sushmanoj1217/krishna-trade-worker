# DR RUNBOOK

## 1) Freeze
- Pause Render Background Worker & Cron jobs.
- Export env from Render dashboard.

## 2) Rescue
- `git clone` backup remote to fresh worker.
- Set `.env` / Render env.
- `pip install -r requirements.txt`

## 3) Restore
- Run `python krishna_main.py` locally for smoke.
- Redeploy worker on Render.
- Verify Sheets writes: OC_Live, Signals, Trades.
