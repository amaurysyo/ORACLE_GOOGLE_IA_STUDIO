import sys
import types

import pytest

# Stubs para dependencias opcionales (loguru / prometheus_client)
class _DummyLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


sys.modules.setdefault("loguru", types.SimpleNamespace(logger=_DummyLogger()))


class _DummyMetric:
    def __init__(self, *args, **kwargs):
        pass

    def labels(self, *args, **kwargs):
        return self

    def observe(self, *args, **kwargs):
        return self

    def inc(self, *args, **kwargs):
        return self

    def set(self, *args, **kwargs):
        return self


sys.modules.setdefault(
    "prometheus_client",
    types.SimpleNamespace(
        Counter=_DummyMetric,
        Gauge=_DummyMetric,
        Summary=_DummyMetric,
        Histogram=_DummyMetric,
        start_http_server=lambda *args, **kwargs: None,
    ),
)

from oraculo.detect.detectors import DepletionCfg, DepletionDetector  # noqa: E402


def _double_tick(det: DepletionDetector, ts: float, snap: dict):
    # 1er tick arma; 2do emite si procede
    det.on_snapshot(ts, snap)
    return det.on_snapshot(ts, snap)


def test_metric_auto_prefers_doc_when_present():
    cfg = DepletionCfg(
        side="buy",
        metric_source="auto",
        dv_warn=20.0,
        dv_strong=60.0,
        hold_ms=0,
        retrigger_s=0,
    )
    det = DepletionDetector(cfg)
    ev = _double_tick(det, 1.0, {"depletion_bid_doc": -50.0})
    assert ev is not None
    assert ev.fields["metric_source"] == "doc"
    assert ev.intensity == pytest.approx(0.75)
    assert ev.fields["depletion_amount"] == pytest.approx(50.0)


def test_fallback_to_legacy_when_doc_missing():
    cfg = DepletionCfg(side="sell", metric_source="auto", pct_drop=0.35, hold_ms=0, retrigger_s=0)
    det = DepletionDetector(cfg)
    ev = _double_tick(det, 2.0, {"dep_ask": 0.5})
    assert ev is not None
    assert ev.fields["metric_source"] == "legacy"
    assert ev.intensity == pytest.approx(0.5)


def test_require_negative_blocks_positive_doc():
    cfg = DepletionCfg(metric_source="doc", hold_ms=0, retrigger_s=0, dv_warn=5.0, dv_strong=10.0)
    det = DepletionDetector(cfg)
    ev = _double_tick(det, 3.0, {"depletion_bid_doc": 12.0})
    assert ev is None


def test_clamp_prevents_outliers():
    cfg = DepletionCfg(
        side="buy",
        metric_source="doc",
        hold_ms=0,
        retrigger_s=0,
        dv_warn=50.0,
        dv_strong=100.0,
        clamp_abs_dv=200.0,
    )
    det = DepletionDetector(cfg)
    ev = _double_tick(det, 4.0, {"depletion_bid_doc": -1000.0})
    assert ev is not None
    assert ev.fields["depletion_amount"] == pytest.approx(200.0)
    assert ev.intensity == pytest.approx(1.0)
