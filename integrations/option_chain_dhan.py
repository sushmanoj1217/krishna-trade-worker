def compute_levels_from_oc_v2(oc_json: dict, used_expiry: str) -> dict:
    """
    Build levels + PCR/MaxPain and also expose per-strike OI map for OC-Pattern.
    Returns:
      {
        spot, s1,s2,r1,r2, pcr, max_pain, expiry,
        oc_oi: { strike(float)-> {"ce": int, "pe": int} },
        strike_step: float | None
      }
    """
    data = oc_json.get("data") or {}
    oc = data.get("oc") or {}
    spot = float(data.get("last_price") or 0.0)

    rows = []
    pe_sum = 0
    ce_sum = 0
    oc_oi: dict[float, dict] = {}

    for k, v in oc.items():
        try:
            strike = float(k)
        except Exception:
            continue
        ce_oi = int((v.get("ce") or {}).get("oi") or 0)
        pe_oi = int((v.get("pe") or {}).get("oi") or 0)
        rows.append((strike, ce_oi, pe_oi))
        oc_oi[strike] = {"ce": ce_oi, "pe": pe_oi}
        ce_sum += ce_oi
        pe_sum += pe_oi

    if not rows:
        raise RuntimeError("Empty option chain data")

    rows_sorted = sorted(rows, key=lambda t: t[0])
    diffs = [round(rows_sorted[i+1][0] - rows_sorted[i][0], 2) for i in range(len(rows_sorted)-1)]
    strike_step = min([d for d in diffs if d > 0], default=None)

    top_pe = sorted(rows, key=lambda t: t[2], reverse=True)
    top_ce = sorted(rows, key=lambda t: t[1], reverse=True)
    s1, s2 = (top_pe[0][0], top_pe[1][0]) if len(top_pe) >= 2 else (None, None)
    r1, r2 = (top_ce[0][0], top_ce[1][0]) if len(top_ce) >= 2 else (None, None)
    pcr = round(pe_sum / ce_sum, 4) if ce_sum > 0 else None
    max_pain = max(rows, key=lambda t: (t[1] + t[2]))[0] if rows else None

    return {
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr,
        "max_pain": max_pain,
        "expiry": used_expiry,
        "oc_oi": oc_oi,
        "strike_step": strike_step,
    }
