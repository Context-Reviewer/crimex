import json
import sys
from types import SimpleNamespace

import pytest


def _call_main(argv):
    import crimex.cli as cli

    old_argv = sys.argv
    try:
        sys.argv = argv
        cli.main()
    finally:
        sys.argv = old_argv


def test_fetch_with_invalid_spec(tmp_path):
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps([1, 2]))

    with pytest.raises(SystemExit) as e:
        _call_main(["crimex", "fetch", "--spec", str(spec_file), "--out", str(tmp_path / "out")])
    assert e.value.code == 1


def test_fetch_with_unknown_source(tmp_path):
    spec_file = tmp_path / "spec2.json"
    spec_file.write_text(json.dumps({"source": "no_such_source"}))

    with pytest.raises(SystemExit) as e:
        _call_main(["crimex", "fetch", "--spec", str(spec_file), "--out", str(tmp_path / "out")])
    assert e.value.code == 1


def test_bundle_handles_unexpected_exception(monkeypatch, tmp_path):
    import crimex.cli as cli

    def _raise(*a, **k):
        raise ValueError("boom")

    monkeypatch.setattr(cli, "create_bundle", _raise)

    run_dir = tmp_path / "run"
    run_dir.mkdir()

    with pytest.raises(SystemExit) as e:
        _call_main(["crimex", "bundle", "--run-dir", str(run_dir)])
    assert e.value.code == 2


def test_verify_run_failure_and_qa_fail(monkeypatch, tmp_path):
    import crimex.cli as cli

    # verify-run failure path
    monkeypatch.setattr(cli, "verify_run", lambda rd: SimpleNamespace(ok=False, errors=["x"]))
    with pytest.raises(SystemExit) as e1:
        _call_main(["crimex", "verify-run", "--run-dir", str(tmp_path)] )
    assert e1.value.code == 1

    # qa failure path
    monkeypatch.setattr(cli, "validate_run_facts", lambda rd: ["qa-error"])
    with pytest.raises(SystemExit) as e2:
        _call_main(["crimex", "qa", "--run-dir", str(tmp_path)])
    assert e2.value.code == 1
