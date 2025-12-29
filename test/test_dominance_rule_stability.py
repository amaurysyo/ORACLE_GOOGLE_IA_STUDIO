from oraculo.detect.detectors import DominanceCfg, DominanceDetector
from oraculo.rules.engine import RuleContext, eval_rules


def test_dominance_dedup_key_stable_for_intensity_and_volatiles():
    ctx = RuleContext(instrument_id="TEST-INST")
    base_fields = {"metric_used": "dominance_ask_doc", "ts": 1111, "nonce": "a"}
    ev1 = {"type": "dominance", "side": "sell", "price": 25000.12, "intensity": 85.12, "fields": base_fields}
    ev2 = {
        "type": "dominance",
        "side": "sell",
        "price": 25000.12,
        "intensity": 85.9876,
        "fields": {**base_fields, "ts": 2222, "nonce": "b"},
    }

    r1 = eval_rules(ev1, ctx)
    r2 = eval_rules(ev2, ctx)
    assert len(r1) == 1
    assert len(r2) == 1
    assert r1[0]["dedup_key"] == r2[0]["dedup_key"]


def test_dominance_doc_path_respects_hold_and_retrigger():
    det = DominanceDetector(DominanceCfg(hold_ms=500, retrigger_s=2))
    fields = {"metric_used": "dominance_bid_doc"}

    first_attempt = det.maybe_emit_doc(1000.0, "buy", 100.0, 70.0, fields)
    assert first_attempt is None  # hold_ms gating

    gated_emit = det.maybe_emit_doc(1000.6, "buy", 100.0, 70.0, fields)
    assert gated_emit is not None
    assert gated_emit.fields["hold_ms"] == 500

    within_retrigger = det.maybe_emit_doc(1001.0, "buy", 100.0, 70.0, fields)
    assert within_retrigger is None

    after_retrigger = det.maybe_emit_doc(1003.1, "buy", 100.0, 70.0, fields)
    assert after_retrigger is not None
