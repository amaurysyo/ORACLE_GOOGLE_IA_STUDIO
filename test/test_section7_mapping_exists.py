import pathlib


def test_section7_doc_rule_mapping_exists() -> None:
    path = pathlib.Path("docs/analysis/section7_doc_rule_mapping.md")
    assert path.exists(), "docs/analysis/section7_doc_rule_mapping.md should exist for Section 7 mapping"
