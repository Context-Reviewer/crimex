from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import crimex.cli as cli


def test_main_unknown_command_exits_2(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    # main() parses argv and should reject unknown subcommands
    monkeypatch.setattr("sys.argv", ["crimex", "nope"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code != 0
    err = capsys.readouterr().err
    assert err  # argparse writes to stderr


def test_fetch_unknown_source_exits_1(tmp_path: cli.Path) -> None:
    spec = {"source": "definitely_not_a_source", "series_name": "x", "query_fingerprint": "y"}
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(spec, indent=2, sort_keys=True), encoding="utf-8")

    fetch_args = SimpleNamespace(
        spec=str(spec_path),
        out=str(tmp_path / "out.json"),
        force=False,
        overwrite=True,
        explain=False,
        params=None,
    )


    with pytest.raises(SystemExit) as e:
        cli.handle_fetch(fetch_args)
    assert e.value.code == 1



def test_handle_report_missing_facts_exits_1(tmp_path: cli.Path) -> None:
    # Exercise the report branch error path quickly.
    out_dir = tmp_path / "out"
    args = SimpleNamespace(
        facts=str(tmp_path / "nope.jsonl"),
        out=str(out_dir),
        force=False,
        overwrite=True,
        explain=False,
    )
    with pytest.raises(SystemExit) as e:
        cli.handle_report(args)
    assert e.value.code == 1
