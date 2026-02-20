from __future__ import annotations

import hashlib
import json
import runpy
from pathlib import Path

import pytest

import crimex.cli as cli
from crimex import bundle as bundle_mod
from crimex import hashing as hashing_mod
from crimex import io as io_mod
from crimex import manifest as manifest_mod
from crimex.normalize.common import normalize_all
from crimex.validate import validate_facts
from crimex.verify_run import verify_run


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True) + "\n")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class _FakeResp:
    def __init__(self, status_code: int, content: bytes, text: str | None = None):
        self.status_code = status_code
        self.content = content
        self.text = text if text is not None else content.decode("utf-8", errors="ignore")

    def json(self):
        return json.loads(self.text)


def test_hashing_and_io_utils(tmp_path: Path) -> None:
    assert hashing_mod.hash_string("abc") == _sha256_bytes(b"abc")

    p = tmp_path / "x.bin"
    p.write_bytes(b"hello")
    assert hashing_mod.hash_file(str(p)) == _sha256_bytes(b"hello")

    k1 = hashing_mod.compute_cache_key("ep", {"b": 2, "a": 1}, {"Accept": "application/json"})
    k2 = hashing_mod.compute_cache_key("ep", {"a": 1, "b": 2}, {"Accept": "application/json"})
    assert k1 == k2

    fact = {
        "source": "s",
        "series": "x",
        "geo": "US",
        "period": 2020,
        "value": 1.0,
        "unit": "u",
        "denominator": None,
        "dimensions": {},
        "notes": None,
        "se": None,
        "ci_lower": None,
        "ci_upper": None,
        "retrieved_at": "2026-01-01T00:00:00Z",
        "query_fingerprint": "fp",
    }
    hf1 = hashing_mod.hash_fact_content(dict(fact))
    fact2 = dict(fact)
    fact2["retrieved_at"] = "DIFFERENT"
    fact2["query_fingerprint"] = "DIFFERENT"
    hf2 = hashing_mod.hash_fact_content(fact2)
    assert hf1 == hf2

    jpath = tmp_path / "a" / "b.json"
    io_mod.write_json({"k": 1}, str(jpath))
    assert io_mod.read_json(str(jpath)) == {"k": 1}

    jlpath = tmp_path / "a" / "facts.jsonl"
    io_mod.write_jsonl([{"x": 1}, {"x": 2}], str(jlpath))
    assert io_mod.read_jsonl(str(jlpath)) == [{"x": 1}, {"x": 2}]

    tpath = tmp_path / "t" / "x.txt"
    io_mod.write_text("hi", str(tpath))
    assert io_mod.load_text(str(tpath)) == "hi"


def test_manifest_generate_manifest(tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("A", encoding="utf-8")
    (root / "sub" / "b.txt").write_text("B", encoding="utf-8")

    out = tmp_path / "m.json"
    m = manifest_mod.generate_manifest(str(root), str(out), "cmd --x 1")
    assert "artifacts" in m
    assert "a.txt" in m["artifacts"]
    assert "sub/b.txt" in m["artifacts"]
    assert out.exists()


def test_normalize_all_reads_meta_and_raw_and_writes_facts(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    out_jsonl = tmp_path / "facts.jsonl"
    raw_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        raw_dir / "ncvs.meta.json",
        {
            "source": "bjs_ncvs",
            "series_name": "rate",
            "query_fingerprint": "xyz",
            "expected_unit": "per_1000",
        },
    )
    _write_json(
        raw_dir / "ncvs.json",
        [
            {"year": 2020, "race": "White", "rate": 15.2},
            {"year": 2020, "race": "Black", "rate": 18.5},
        ],
    )

    _write_json(
        raw_dir / "fbi.meta.json",
        {
            "source": "fbi_cde",
            "series_name": "violent_crime",
            "query_fingerprint": "abc",
            "params": {"state": "US"},
        },
    )
    _write_json(
        raw_dir / "fbi.json",
        {"results": [{"data_year": 2020, "violent_crime": 100}, {"data_year": 2021, "violent_crime": 110}]},
    )

    normalize_all(str(raw_dir), str(out_jsonl))
    lines = [ln for ln in out_jsonl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 4


def test_validate_facts_success_and_failures(tmp_path: Path) -> None:
    ok_path = tmp_path / "ok.jsonl"
    bad_json_path = tmp_path / "bad_json.jsonl"
    bad_schema_path = tmp_path / "bad_schema.jsonl"

    _write_jsonl(
        ok_path,
        [
            {
                "source": "bjs_ncvs",
                "series": "rate",
                "geo": "US",
                "period": 2020,
                "value": 15.2,
                "unit": "per_1000",
                "denominator": None,
                "dimensions": {"race": "White"},
                "notes": None,
                "se": None,
                "ci_lower": None,
                "ci_upper": None,
                "retrieved_at": "2026-01-01T00:00:00Z",
                "query_fingerprint": "xyz",
            }
        ],
    )
    validate_facts(str(ok_path))

    bad_json_path.write_text("{not json}\n", encoding="utf-8")
    with pytest.raises(SystemExit) as e1:
        validate_facts(str(bad_json_path))
    assert e1.value.code == 1

    _write_jsonl(bad_schema_path, [{"source": "bjs_ncvs"}])
    with pytest.raises(SystemExit) as e2:
        validate_facts(str(bad_schema_path))
    assert e2.value.code == 1


def test_cli_commands_fetch_normalize_manifest_validate_verify_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    def _fake_get(url: str, timeout: int = 60, headers=None, params=None):
        if "api.ojp.gov" in url:
            return _FakeResp(200, b'{"data":[{"year":2020,"rate":15.2}]}\n')
        if "api.usa.gov" in url:
            return _FakeResp(200, b'{"results":[{"data_year":2020,"violent_crime":100}]}\n')
        return _FakeResp(404, b"not found", text="not found")

    monkeypatch.setattr(requests, "get", _fake_get)

    import crimex.config as config_mod

    monkeypatch.setattr(config_mod, "require_fbi_api_key", lambda: "FAKEKEY")

    ncvs_spec = tmp_path / "ncvs_spec.json"
    _write_json(
        ncvs_spec,
        {
            "source": "bjs_ncvs",
            "dataset": "NCVS_VICT",
            "format": "json",
            "year_min": 2020,
            "limit": 10,
        },
    )

    fbi_spec = tmp_path / "fbi_spec.json"
    _write_json(
        fbi_spec,
        {
            "source": "fbi_cde",
            "endpoint": "/estimates/states/US",
            "params": {"from": "01-2020", "to": "12-2020"},
        },
    )

    raw_out = tmp_path / "raw_out"
    raw_out.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "fetch", "--spec", str(ncvs_spec), "--out", str(raw_out), "--force"],
    )
    cli.main()

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "fetch", "--spec", str(fbi_spec), "--out", str(raw_out), "--force"],
    )
    cli.main()

    norm_raw = tmp_path / "norm_raw"
    norm_raw.mkdir(parents=True, exist_ok=True)
    _write_json(
        norm_raw / "ncvs.meta.json",
        {
            "source": "bjs_ncvs",
            "series_name": "rate",
            "query_fingerprint": "xyz",
            "expected_unit": "per_1000",
        },
    )
    _write_json(norm_raw / "ncvs.json", [{"year": 2020, "race": "White", "rate": 15.2}])

    _write_json(
        norm_raw / "fbi.meta.json",
        {
            "source": "fbi_cde",
            "series_name": "violent_crime",
            "query_fingerprint": "abc",
            "params": {"state": "US"},
        },
    )
    _write_json(norm_raw / "fbi.json", {"results": [{"data_year": 2020, "violent_crime": 100}]})

    facts_out = tmp_path / "facts.jsonl"
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "normalize", "--raw", str(norm_raw), "--out", str(facts_out)],
    )
    cli.main()
    assert facts_out.exists()

    monkeypatch.setattr(cli.sys, "argv", ["crimex", "validate", "--facts", str(facts_out)])
    cli.main()

    man_out = tmp_path / "manifest.json"
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "manifest", "--root", str(norm_raw), "--out", str(man_out)],
    )
    cli.main()
    assert man_out.exists()

    run_dir = tmp_path / "run"
    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    (run_dir / "reports").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "facts").mkdir(parents=True, exist_ok=True)

    (run_dir / "reports" / "report.csv").write_text("h\n", encoding="utf-8")
    (run_dir / "reports" / "report.md").write_text("# r\n", encoding="utf-8")
    (run_dir / "logs" / "run.log").write_text("ok\n", encoding="utf-8")
    (run_dir / "raw" / "x.bin").write_bytes(b"x")
    (run_dir / "facts" / "facts.jsonl").write_text("{}", encoding="utf-8")

    manifest = {
        "run_id": "RID",
        "artifacts": {
            "raw/x.bin": _sha256_file(run_dir / "raw" / "x.bin"),
            "reports/report.csv": _sha256_file(run_dir / "reports" / "report.csv"),
            "reports/report.md": _sha256_file(run_dir / "reports" / "report.md"),
            "logs/run.log": _sha256_file(run_dir / "logs" / "run.log"),
            "facts/facts.jsonl": _sha256_file(run_dir / "facts" / "facts.jsonl"),
        },
    }
    (run_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli.sys, "argv", ["crimex", "verify-run", "--run-dir", str(run_dir)])
    with pytest.raises(SystemExit) as e_verify:
        cli.main()
    assert e_verify.value.code == 0

    monkeypatch.setattr(cli.sys, "argv", ["crimex", "bundle", "--run-dir", str(run_dir)])
    with pytest.raises(SystemExit) as e_bundle:
        cli.main()
    assert e_bundle.value.code == 0

    bp = run_dir / "run_bundle.zip"
    assert bp.exists()
    _ = bundle_mod.bundle_content_fingerprint(bp)

    updated = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert "run_bundle.zip" in updated["artifacts"]


def test_verify_run_failure_modes(tmp_path: Path) -> None:
    run_dir = tmp_path / "r"
    run_dir.mkdir(parents=True, exist_ok=True)

    r1 = verify_run(run_dir)
    assert r1.ok is False
    assert r1.checked == 0

    (run_dir / "run_manifest.json").write_text("{bad\n", encoding="utf-8")
    r2 = verify_run(run_dir)
    assert r2.ok is False
    assert r2.checked == 0

    (run_dir / "run_manifest.json").write_text(json.dumps({"artifacts": ["x"]}), encoding="utf-8")
    r3 = verify_run(run_dir)
    assert r3.ok is False
    assert r3.checked == 0


def test___main___module_executes_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"ok": False}

    def _fake_main() -> None:
        called["ok"] = True

    monkeypatch.setattr(cli, "main", _fake_main)
    runpy.run_module("crimex.__main__", run_name="__main__")
    assert called["ok"] is True
