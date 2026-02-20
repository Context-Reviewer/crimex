from __future__ import annotations

import json
from pathlib import Path

import pytest

from crimex.bundle import BundleError, bundle_content_fingerprint, create_bundle
from crimex.normalize.common import normalize_raw_dir
from crimex.normalize.fbi_normalize import normalize_fbi_cde
from crimex.validate import validate_facts


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_fbi_normalize_nested_offenses_covers_aggregation_and_sort() -> None:
    meta = {
        "source": "fbi_cde",
        "series_name": "violent_crime",
        "query_fingerprint": "fp",
        "params": {"state": "US"},
        "retrieved_at": "2020-01-01T00:00:00Z",
    }

    raw = {
        "offenses": {
            "counts": {
                "Assault": {
                    "01-2020": 10,
                    "02-2020": 20,
                    "01-2021": 30,
                }
            },
            "rates": {
                "Assault": {
                    "01-2020": 1.0,
                    "02-2020": 3.0,
                }
            },
        }
    }

    facts = normalize_fbi_cde(raw, meta)
    assert len(facts) == 3

    # Deterministic sort is (source, series, geo, period, unit, dimensions)
    got = {(f["period"], f["unit"]): f["value"] for f in facts}

    assert got[(2020, "count")] == 15.0
    assert got[(2021, "count")] == 30.0
    assert got[(2020, "rate_per_100k")] == 2.0



def test_fbi_normalize_missing_bucket_raises_value_error() -> None:
    meta = {
        "source": "fbi_cde",
        "series_name": "violent_crime",
        "query_fingerprint": "fp",
        "params": {"state": "US"},
        "retrieved_at": "2020-01-01T00:00:00Z",
    }

    raw = {"offenses": {"counts": {"Assault": {"01-2020": 10}}}}
    with pytest.raises(ValueError):
        normalize_fbi_cde(raw, meta)


def test_normalize_raw_dir_covers_warning_paths(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # meta is not an object
    _write_json(raw_dir / "bad.meta.json", ["nope"])

    # meta missing source
    _write_json(raw_dir / "nosrc.meta.json", {"x": 1})
    _write_json(raw_dir / "nosrc.json", {"data": []})

    # unknown source
    _write_json(raw_dir / "unknown.meta.json", {"source": "nope"})
    _write_json(raw_dir / "unknown.json", {"data": []})

    # raw missing for meta
    _write_json(raw_dir / "missing_raw.meta.json", {"source": "bjs_ncvs"})

    out = tmp_path / "facts.jsonl"
    normalize_raw_dir(str(raw_dir), str(out))

    assert out.exists()
    assert out.read_text(encoding="utf-8") == ""


def test_validate_facts_failure_modes(tmp_path: Path) -> None:
    # no path
    with pytest.raises(SystemExit) as e1:
        validate_facts("")
    assert e1.value.code == 1

    # file not found
    with pytest.raises(SystemExit) as e2:
        validate_facts(str(tmp_path / "missing.jsonl"))
    assert e2.value.code == 1

    # invalid json line
    bad = tmp_path / "bad.jsonl"
    _write_text(bad, "{bad\n")
    with pytest.raises(SystemExit) as e3:
        validate_facts(str(bad))
    assert e3.value.code == 1

    # schema error (missing required fields)
    bad2 = tmp_path / "bad2.jsonl"
    _write_text(bad2, json.dumps({"source": "x"}) + "\n")
    with pytest.raises(SystemExit) as e4:
        validate_facts(str(bad2))
    assert e4.value.code == 1


def test_bundle_error_paths_and_fingerprint(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    (run_dir / "reports").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "facts").mkdir(parents=True, exist_ok=True)

    (run_dir / "logs" / "run.log").write_text("ok\n", encoding="utf-8")
    (run_dir / "reports" / "report.csv").write_text("h\n", encoding="utf-8")

    # manifest required
    _write_json(run_dir / "run_manifest.json", {"run_id": "RID", "artifacts": {}})

    # ambiguous facts under facts/
    (run_dir / "facts" / "a.jsonl").write_text("{}", encoding="utf-8")
    (run_dir / "facts" / "b.jsonl").write_text("{}", encoding="utf-8")
    with pytest.raises(BundleError):
        create_bundle(run_dir)

    # make facts resolvable via root facts.jsonl fallback
    (run_dir / "facts" / "a.jsonl").unlink()
    (run_dir / "facts" / "b.jsonl").unlink()
    (run_dir / "facts.jsonl").write_text("{}", encoding="utf-8")

    bundle_path = create_bundle(run_dir, force=True)
    assert bundle_path.exists()

    fp = bundle_content_fingerprint(bundle_path)
    assert isinstance(fp, str)
    assert len(fp) == 64

    # second time without force should fail
    with pytest.raises(BundleError):
        create_bundle(run_dir, force=False)