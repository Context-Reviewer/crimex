import json
from pathlib import Path

from crimex.qa import validate_run_facts


def _make_run(tmp_path: Path, facts_lines):
    run_dir = tmp_path / "run"
    (run_dir / "facts").mkdir(parents=True)
    facts_path = run_dir / "facts" / "facts.jsonl"
    facts_path.write_text("\n".join(facts_lines), encoding="utf-8")
    return run_dir


def test_qa_pass(tmp_path):
    fact = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "count",
        "value": 1,
        "query_fingerprint": "abc",
    }
    run = _make_run(tmp_path, [json.dumps(fact)])
    errors = validate_run_facts(run)
    assert errors == []


def test_empty_facts_fail(tmp_path):
    run = _make_run(tmp_path, [])
    errors = validate_run_facts(run)
    assert errors == ["EMPTY_FACTS: no fact rows found"]


def test_duplicate_detection(tmp_path):
    fact = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "count",
        "value": 1,
        "query_fingerprint": "abc",
    }
    run = _make_run(tmp_path, [json.dumps(fact), json.dumps(fact)])
    errors = validate_run_facts(run)
    assert any("DUPLICATE_FACT" in e for e in errors)


def test_negative_value(tmp_path):
    fact = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "count",
        "value": -5,
        "query_fingerprint": "abc",
    }
    run = _make_run(tmp_path, [json.dumps(fact)])
    errors = validate_run_facts(run)
    assert any("NEGATIVE_VALUE" in e for e in errors)


def test_missing_denominator(tmp_path):
    fact = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "rate_per_100k",
        "value": 5,
        "query_fingerprint": "abc",
    }
    run = _make_run(tmp_path, [json.dumps(fact)])
    errors = validate_run_facts(run)
    assert any("MISSING_DENOMINATOR" in e for e in errors)


def test_mixed_units(tmp_path):
    f1 = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "count",
        "value": 1,
        "query_fingerprint": "abc",
    }
    f2 = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2021",
        "unit": "rate_per_100k",
        "value": 1,
        "denominator": 100000,
        "query_fingerprint": "abc",
    }
    run = _make_run(tmp_path, [json.dumps(f1), json.dumps(f2)])
    errors = validate_run_facts(run)
    assert any("MIXED_UNITS" in e for e in errors)


def test_malformed_json(tmp_path):
    run = _make_run(tmp_path, ["{not valid json"])
    errors = validate_run_facts(run)
    assert any("MALFORMED_JSON" in e for e in errors)


def test_deterministic_ordering(tmp_path):
    bad1 = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "count",
        "value": -1,
    }
    bad2 = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": "2020",
        "unit": "rate_per_100k",
        "value": 1,
    }
    run = _make_run(tmp_path, [json.dumps(bad1), json.dumps(bad2)])
    errors = validate_run_facts(run)
    assert errors == sorted(errors)
