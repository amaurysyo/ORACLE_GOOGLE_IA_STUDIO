import pytest
from oraculo.detect.detectors import SlicingPassiveDetector, SlicingPassConfig

def test_slicing_passive_require_equal_behavior():
    # Setup: k_min=3, qty_min=3.0, require_equal=True
    cfg = SlicingPassConfig(
        gap_ms=1000,
        k_min=3,
        qty_min=3.0,
        require_equal=True,
        equal_tol_pct=0.0
    )
    det = SlicingPassiveDetector(cfg)
    ts = 100.0

    # 1. First insert: 1.0 (ref established)
    assert det.on_depth(ts, "buy", "insert", 1000.0, 1.0) is None

    # 2. Second insert: 1.0 (matches ref) -> k=2
    assert det.on_depth(ts + 0.1, "buy", "insert", 1000.0, 1.0) is None

    # 3. Third insert: 1.0 (matches ref) -> k=3, trigger
    ev = det.on_depth(ts + 0.2, "buy", "insert", 1000.0, 1.0)
    assert ev is not None
    assert ev.kind == "slicing_pass"
    assert ev.fields["k"] == 3
    assert ev.intensity == 3.0

def test_slicing_passive_require_equal_reset():
    # Setup: require_equal=True
    cfg = SlicingPassConfig(gap_ms=1000, k_min=3, qty_min=3.0, require_equal=True)
    det = SlicingPassiveDetector(cfg)
    ts = 100.0

    # 1. Insert 1.0
    det.on_depth(ts, "buy", "insert", 1000.0, 1.0)
    # 2. Insert 1.0
    det.on_depth(ts + 0.1, "buy", "insert", 1000.0, 1.0)

    # 3. Insert 5.0 (Mismatch!) -> Should reset k to 1, ref to 5.0, acc_qty to 5.0
    # k was 2. Now becomes 1. No trigger.
    ev = det.on_depth(ts + 0.2, "buy", "insert", 1000.0, 5.0)
    assert ev is None

    # 4. Insert 5.0 -> k=2
    ev = det.on_depth(ts + 0.3, "buy", "insert", 1000.0, 5.0)
    assert ev is None

    # 5. Insert 5.0 -> k=3 -> Trigger (accumulated 15.0)
    ev = det.on_depth(ts + 0.4, "buy", "insert", 1000.0, 5.0)
    assert ev is not None
    assert ev.fields["k"] == 3
    assert ev.intensity == 15.0

def test_slicing_passive_ignores_non_inserts_and_resets_on_delete():
    cfg = SlicingPassConfig(gap_ms=1000, k_min=3, qty_min=3.0, require_equal=False)
    det = SlicingPassiveDetector(cfg)
    ts = 100.0

    # 1. Insert 1.0
    det.on_depth(ts, "buy", "insert", 1000.0, 1.0)
    # 2. Update (ignored)
    det.on_depth(ts + 0.1, "buy", "update", 1000.0, 0.5)
    # 3. Insert 1.0 -> k=2
    det.on_depth(ts + 0.2, "buy", "insert", 1000.0, 1.0)

    # 4. Delete on same bucket -> Reset
    det.on_depth(ts + 0.3, "buy", "delete", 1000.0, 2.0)

    # 5. Insert 1.0 -> Should start new block (k=1)
    ev = det.on_depth(ts + 0.4, "buy", "insert", 1000.0, 1.0)
    assert ev is None # k=1

    # 6. Insert 1.0 -> k=2
    det.on_depth(ts + 0.5, "buy", "insert", 1000.0, 1.0)
    # 7. Insert 1.0 -> k=3 -> Trigger
    ev = det.on_depth(ts + 0.6, "buy", "insert", 1000.0, 1.0)
    assert ev is not None
