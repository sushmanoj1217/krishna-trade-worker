# (same as the last full version I gave you) 
# + these additions:

# imports section additions:
try:
    from ops import params_override
except Exception:
    params_override = None

try:
    from ops import closer
except Exception:
    closer = None

try:
    from agents import circuit
except Exception:
    circuit = None

# ... inside main(), after ensure_all_headers:
    if params_override is not None:
        try:
            applied = params_override.apply_overrides(sheet, cfg)
            if applied:
                print(f"[override] applied: {applied}", flush=True)
                logger.log_status(sheet, {"worker_id":cfg.worker_id,"shift_mode":cfg.shift_mode,"state":"OK","message":f"params_override {len(applied)} applied"})
        except Exception as e:
            print(f"[override] failed: {e}", flush=True)

# ... in oc_tick(), before generating signal:
            # Circuit breaker gate
            if circuit is not None and circuit.should_pause():
                try:
                    rem = circuit.pause_remaining_secs()
                except Exception:
                    rem = 0
                print(f"[signal] paused by circuit (rem {rem}s)", flush=True)
                return

# ... schedulers: add Time-Exit at 15:15
    def time_exit_job():
        if closer is None:
            return
        try:
            closer.time_exit_all(sheet, cfg)
        except Exception as e:
            print(f"❌ Job error [time_exit] {e}", flush=True)

    sched.add_job(
        time_exit_job,
        "cron",
        day_of_week="mon-fri",
        hour=15, minute=15,
        id="time_exit",
    )
