from __future__ import annotations

from pathlib import Path

from crimex import qa


def test_validate_run_facts_missing_facts_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    errors = qa.validate_run_facts(run_dir)

    assert errors, "Expected errors when facts/facts.jsonl is missing"
    joined = "\n".join(errors).lower()
    assert "facts" in joined
    assert "jsonl" in joined


def test_validate_run_facts_invalid_jsonl_line(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    facts_dir = run_dir / "facts"
    facts_dir.mkdir(parents=True)

    (facts_dir / "facts.jsonl").write_text("{not valid json}\n", encoding="utf-8")

    errors = qa.validate_run_facts(run_dir)

    assert errors, "Expected errors for invalid JSONL"
    joined = "\n".join(errors).lower()
    assert "json" in joined or "decode" in joined


def test_validate_run_facts_schema_validation_error(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    facts_dir = run_dir / "facts"
    facts_dir.mkdir(parents=True)

    # Valid JSON, but not a valid Fact shape.
    # Missing required fields should trigger schema validation branch.
    (facts_dir / "facts.jsonl").write_text('{"source":"bjs_ncvs"}\n', encoding="utf-8")

    errors = qa.validate_run_facts(run_dir)

    assert errors, "Expected schema/validation errors for incomplete Fact object"
    joined = "\n".join(errors).lower()
    assert "validation" in joined or "schema" in joined or "field" in joined
