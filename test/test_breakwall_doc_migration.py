import types
import sys

# Stubs para dependencias opcionales en CI
class _DummyLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


sys.modules.setdefault("loguru", types.SimpleNamespace(logger=_DummyLogger()))

from oraculo.detect.detectors import BreakWallCfg, BreakWallDetector, Event


def _slicing(side: str, price: float = 100.0, intensity: float = 1.0) -> Event:
    return Event(kind="slicing_aggr", side=side, ts=1.0, price=price, intensity=intensity)


def test_doc_path_emits_with_depletion():
    cfg = BreakWallCfg(
        n_min=1,
        metric_source="doc",
        basis_vel_abs_bps_s=1.0,
        dv_dep_warn=20.0,
        dv_refill_max=10.0,
        top_levels_gate=0,
    )
    det = BreakWallDetector(cfg)
    snap = {
        "basis_vel_bps_s_doc": 5.0,
        "depletion_ask_doc": -80.0,
    }

    ev, gating = det.on_slicing(1.0, _slicing("buy"), snap)
    assert gating is None
    assert ev is not None
    assert ev.fields["metric_source"] == "doc"
    assert ev.fields["metric_used_basis_vel"] == "basis_vel_bps_s_doc"
    assert ev.fields["metric_used_dv"] == "depletion_ask_doc"
    assert ev.fields["depletion_amount"] == 80.0
    assert ev.fields["refill_amount"] == 0.0


def test_doc_blocks_on_refill_high():
    cfg = BreakWallCfg(
        n_min=1,
        metric_source="doc",
        basis_vel_abs_bps_s=1.0,
        dv_dep_warn=0.0,  # permitir dep_ok aunque dv>=0
        dv_refill_max=10.0,
        require_negative=False,
        top_levels_gate=0,
    )
    det = BreakWallDetector(cfg)
    snap = {
        "basis_vel_bps_s_doc": 5.0,
        "depletion_ask_doc": 30.0,  # refill alto
    }

    ev, gating = det.on_slicing(1.0, _slicing("buy"), snap)
    assert ev is None
    assert gating == "refill_high"


def test_auto_falls_back_to_legacy():
    cfg = BreakWallCfg(
        n_min=1,
        metric_source="auto",
        basis_vel_abs_bps_s=1.0,
        top_levels_gate=0,
    )
    det = BreakWallDetector(cfg)
    snap = {
        "basis_vel_bps_s": 3.0,
        "dep_ask": 0.8,
        "refill_ask_3s": 0.1,
    }

    ev, gating = det.on_slicing(1.0, _slicing("buy"), snap)
    assert gating is None
    assert ev is not None
    assert ev.kind == "break_wall"
    assert ev.fields["basis_vel_bps_s"] == 3.0


def test_doc_event_includes_audit_fields():
    cfg = BreakWallCfg(
        n_min=1,
        metric_source="doc",
        basis_vel_abs_bps_s=1.0,
        dv_dep_warn=10.0,
        dv_refill_max=5.0,
        top_levels_gate=0,
    )
    det = BreakWallDetector(cfg)
    snap = {
        "basis_vel_bps_s_doc": 2.5,
        "depletion_bid_doc": -12.0,
    }

    ev, gating = det.on_slicing(1.0, _slicing("sell"), snap)
    assert gating is None
    assert ev is not None
    assert ev.fields["metric_source"] == "doc"
    assert ev.fields["metric_used_basis_vel"] == "basis_vel_bps_s_doc"
    assert ev.fields["metric_used_dv"] == "depletion_bid_doc"
    assert ev.fields["dv"] == -12.0
    assert ev.fields["window_s"] == cfg.doc_window_s
