# ... imports same as before ...

def main():
    print(f"[{_now()}] ✅ Starting worker…", flush=True)
    print(f"[{_now()}] 🔧 Ensuring Sheets tabs…", flush=True)
    ensure_all_headers()
    print(f"[{_now()}] ✅ Sheets tabs ensured", flush=True)

    start_tele_router()
    threading.Thread(target=schedule_time_exit_and_eod, name="time_exit_eod", daemon=True).start()
    threading.Thread(target=day_loop, name="day_loop", daemon=True).start()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)
    print(f"[{_now()}] ✅ Worker started", flush=True)
    send_telegram("✅ Worker started.")
    while not _shutdown:
        time.sleep(1.5)
