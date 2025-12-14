# ===============================================
# oraculo/rules/engine.py  (R1–R22 + extras ya usados)
# ===============================================
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from loguru import logger

from oraculo.obs import metrics as obs_metrics

@dataclass
class RuleContext:
    instrument_id: str
    profile: str = "EU"
    suppress_window_s: int = 90

def _mk(rule: str, side: str, ev: Dict[str, Any], severity: str = "MEDIUM") -> Dict[str, Any]:
    price = ev.get("price")
    dedup_key = f"{rule}|{side}|{int(price or 0)}"
    return {
        "instrument_id": None,  # lo setea el caller antes de upsert
        "rule": rule,
        "side": side,
        "severity": severity,
        "context": {
            "type": ev.get("type"),
            "price": price,
            "intensity": ev.get("intensity"),
            "fields": ev.get("fields") or {},
        },
        "dedup_key": dedup_key,
    }

def _sev_from_abs(x: Optional[float], t1: float, t2: float) -> str:
    if x is None:
        return "MEDIUM"
    ax = abs(x)
    if ax >= t2:
        return "HIGH"
    if ax >= t1:
        return "MEDIUM"
    return "LOW"

def _sev_from_val(x: Optional[float], t1: float, t2: float) -> str:
    if x is None:
        return "MEDIUM"
    if x >= t2:
        return "HIGH"
    if x >= t1:
        return "MEDIUM"
    return "LOW"

def eval_rules(ev: Dict[str, Any], ctx: RuleContext) -> List[Dict[str, Any]]:
    """Mapea eventos normalizados -> R-codes (catálogo Excel)."""
    out: List[Dict[str, Any]] = []

    def _append(rule: str, side_: str, ev_: Dict[str, Any], severity_: str = "MEDIUM") -> None:
        obs_metrics.rule_eval_total.labels(rule=rule).inc()
        out.append(_mk(rule, side_, ev_, severity_))

    try:
        et = ev.get("type")
        side = (ev.get("side") or "na").lower()
        f = ev.get("fields") or {}
        val = ev.get("intensity")

        # ---------- R1/R2: BreakWall + basis velocity ----------
        if et == "break_wall":
            bv = float(f.get("basis_vel_bps_s", 0.0) or 0.0)
            if side == "buy" and bv > 0:
                _append("R1", side, ev, severity_=_sev_from_abs(bv, 2.0, 3.0))
            elif side == "sell" and bv < 0:
                _append("R2", side, ev, severity_=_sev_from_abs(bv, 2.0, 3.0))
            return out

        # ---------- R3/R4: Absorción ----------
        if et == "absorption":
            if side == "buy":
                _append("R3", side, ev, severity_=_sev_from_val(val, 300, 600))
            elif side == "sell":
                _append("R4", side, ev, severity_=_sev_from_val(val, 300, 600))
            return out

        # ---------- R5/R6: Slicing agresivo ----------
        if et == "slicing_aggr":
            if side == "buy":
                _append("R5", side, ev, severity_=_sev_from_val(val, 5, 10))
            elif side == "sell":
                _append("R6", side, ev, severity_=_sev_from_val(val, 5, 10))
            return out

        # ---------- R7/R8: Slicing pasivo ----------
        if et == "slicing_pass":
            if side == "buy":
                _append("R7", side, ev, severity_=_sev_from_val(val, 10, 25))
            elif side == "sell":
                _append("R8", side, ev, severity_=_sev_from_val(val, 10, 25))
            return out

        # ---------- R9/R10: Dominance ----------
        if et == "dominance":
            if side == "buy":
                _append("R9", side, ev, severity_=_sev_from_val(val, 80, 90))
            elif side == "sell":
                _append("R10", side, ev, severity_=_sev_from_val(val, 80, 90))
            return out

        # ---------- R11/R12: Spoofing ----------
        if et == "spoofing":
            if side == "buy":
                _append("R11", side, ev, severity_=_sev_from_val(val, 50, 100))
            elif side == "sell":
                _append("R12", side, ev, severity_=_sev_from_val(val, 50, 100))
            return out

        # ---------- R13/R14: Depletion masivo ----------
        if et == "depletion":
            if side == "buy":
                _append("R13", side, ev, severity_=_sev_from_val(val, 0.40, 0.60))
            elif side == "sell":
                _append("R14", side, ev, severity_=_sev_from_val(val, 0.40, 0.60))
            return out

        # ---------- R15/R16: Basis extremo (+ / −) ----------
        if et == "metric_trigger" and f.get("metric") == "basis_bps":
            thr = float(f.get("threshold", 0.0) or 0.0)
            if val is None:
                return out
            if val >= thr and thr >= 0:
                _append("R15", "na", ev, severity_=_sev_from_val(val, 50, 100))
            if val <= -abs(thr):
                _append("R16", "na", ev, severity_=_sev_from_val(abs(val), 50, 100))
            return out

        # ---------- R17/R18: Basis mean-revert (SELL/BUY) ----------
        if et == "basis_mean_revert":
            if side == "sell":
                _append("R17", "sell", ev, severity_=_sev_from_abs(val, 2.0, 3.0))
            elif side == "buy":
                _append("R18", "buy", ev, severity_=_sev_from_abs(val, 2.0, 3.0))
            return out

        # ---------- R19–R22: Opciones (stubs; los emitirá ingesta de opciones) ----------
        if et == "iv_spike_up":
            _append("R19", "na", ev, "HIGH");  return out
        if et == "iv_spike_down":
            _append("R20", "na", ev, "HIGH");  return out
        if et == "oi_skew_bull":
            _append("R21", "na", ev, "MEDIUM");  return out
        if et == "oi_skew_bear":
            _append("R22", "na", ev, "MEDIUM");  return out

        # ---------- Extras: Tape pressure, spread squeeze ----------
        if et == "tape_pressure":
            if side == "buy":
                _append("R23", "buy", ev, severity_=_sev_from_val(val, 0.8, 0.9))
            elif side == "sell":
                _append("R24", "sell", ev, severity_=_sev_from_val(val, 0.8, 0.9))
            return out

        if et == "metric_trigger" and f.get("metric") == "spread_usd":
            # Si alguien quiere usarlo como R27 (squeeze): intensidad = spread actual
            _append("R27", "na", ev, severity_=_sev_from_val(abs(val or 0.0), 0.5, 1.0))
            return out

        return out
    except Exception as e:
        obs_metrics.rule_eval_errors_total.labels(
            rule="unknown", kind=type(e).__name__
        ).inc()
        logger.exception("[rules] error evaluating rules")
        return out
