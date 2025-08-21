# DR RUNBOOK (KTW)
**Freeze → Rescue → Restore**

1. **Freeze**
   - Render Dashboard: Toggle Auto-Deploy OFF.
   - Scale to 0 (if conflict/loop) → pause background worker.

2. **Rescue**
   - Open Shell.
   - `printf` all critical env: TELEGRAM_*, GSHEET_*, OC_* , DHAN_*.
   - Fix obvious drifts (OC_SYMBOL single; DHAN_UNDERLYING_* consistent).
   - `python -u scripts/sheets_admin.py setup` to re-create tabs.

3. **Restore**
   - Manual Deploy → check logs: “✅ Sheets tabs ensured”, “Telegram bot started”, “Day loop started”.
   - `/run oc_auto status` on Telegram.
   - `/oc_now` to verify snapshot & checks.

> Note: Telegram **409 Conflict** तब आएगा जब **एक से ज़्यादा** polling instances चल रहे हों. Ensure only **one** background worker or one shell run at a time.
