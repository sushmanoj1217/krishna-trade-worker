#!/usr/bin/env python3
"""
Scan repo for python-telegram-bot poller start sites that can cause 409 Conflict.
Looks for:
- start_polling
- run_polling
- Updater(           (old style)
- ApplicationBuilder( (v20 style, may be fine but count instances)
Prints file:line with a bit of context.
"""
from __future__ import annotations
import os, sys, re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PAT = re.compile(r"(start_polling|run_polling|Updater\s*\(|ApplicationBuilder\s*\()", re.I)

def scan():
    hits = []
    for dirpath, _, files in os.walk(ROOT):
        # skip venv/build dirs
        if any(k in dirpath for k in ("/.venv", "/venv", "/site-packages", "/build", "/dist", "/__pycache__")):
            continue
        for f in files:
            if not f.endswith(".py"):
                continue
            p = os.path.join(dirpath, f)
            try:
                with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                    for i, line in enumerate(fh, 1):
                        if PAT.search(line):
                            hits.append((p, i, line.rstrip()))
            except Exception:
                pass
    return hits

def main():
    hits = scan()
    if not hits:
        print("No poller calls found.")
        return
    print("=== Potential polling sites ===")
    for p, i, l in hits:
        print(f"{p}:{i}: {l}")
    print("\nSummary:")
    kinds = {"start_polling":0, "run_polling":0, "Updater(":0, "ApplicationBuilder(":0}
    for _, _, l in hits:
        for k in kinds:
            if k in l.replace(" ", ""):
                kinds[k]+=1
    for k,v in kinds.items():
        print(f"  {k}: {v}")
    print("\nTip: keep exactly ONE polling start in your day worker.")
    print("If both telegram_bot.py and krishna_main.py start polling, remove one.")
if __name__ == "__main__":
    main()
