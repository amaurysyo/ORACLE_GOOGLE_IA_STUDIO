import os


def test_closure_runbook_exists():
    assert os.path.exists("docs/runbooks/ORACULO_CLOSURE.md")
