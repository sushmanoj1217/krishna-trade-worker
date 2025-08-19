# ops/oc_format.py
from __future__ import annotations
from typing import Dict, Any
from agents.signal_generator import buffer_points, adj_support, adj_resistance, classify_market_view, compute_bias_tag, read_pcr_vix

def format_oc_reply(oc: Dict[str,Any]) -> str:
    sym = (oc.get("symbol") or "NIFTY").upper()
    band = buffer_points(sym)
    s1,s2,r1,r2 = oc.get("s1"), oc.get("s2"), oc.get("r1"), oc.get("r2")
    mv, mv_tag = classify_market_view(oc)
    bias = compute_bias_tag()
    pcr, vix = read_pcr_vix()

    def f(x): 
        try: return f"{float(x):.2f}"
        except: return str(x)

    lines = [ "OC updated ✅",
              f"spot={f(oc.get('spot'))}  S1={f(s1)}  S2={f(s2)}",
              f"R1={f(r1)}  R2={f(r2)}",
              f"expiry={oc.get('expiry','')}" ]

    # Buffered (directional) trigger levels:
    buf = []
    if s1 is not None: buf.append(f"S1*={f(adj_support(s1, band))}")
    if s2 is not None: buf.append(f"S2*={f(adj_support(s2, band))}")
    if r1 is not None: buf.append(f"R1*={f(adj_resistance(r1, band))}")
    if r2 is not None: buf.append(f"R2*={f(adj_resistance(r2, band))}")
    if buf:
        lines.append(f"band={band}  " + "  ".join(buf))

    lines.append(f"view: {mv_tag}; {bias}")
    lines.append(f"PCR={pcr if pcr is not None else 'n/a'}  VIX={vix if vix is not None else 'n/a'}")
    return "\n".join(lines)
