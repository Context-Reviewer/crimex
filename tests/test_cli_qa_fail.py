from __future__ import annotations

from pathlib import Path

import pytest

from crimex import cli


def test_cli_qa_fail_on_invalid_jsonl(
        tmp_path: Path, 
        monkeypatch: pytest.MonkeyPatch, 
        capsys: pytest.CaptureFixture[str]
) -> None:
    run_dir = tmp_path / "run"
    facts_dir = run_dir / "facts"
    facts_dir.mkdir(parents=True)

    # Deliberately invalid JSON line -> should produce QA FAIL and exit 1
    (facts_dir / "facts.jsonl").write_text("{not-json}\n", encoding="utf-8")

    monkeypatch.setattr("sys.argv", ["crimex", "qa", "--run-dir", str(run_dir)])

    with pytest.raises(SystemExit) as e:
        cli.main()

    assert e.value.code == 1
    captured = capsys.readouterr()
    text = (captured.out + captured.err).strip()
    assert "QA" in text  # "QA FAIL" or similar
