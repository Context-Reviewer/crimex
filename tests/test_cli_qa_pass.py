from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pytest

from crimex import cli
from crimex.schemas import Fact


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    return str(o)


def _fact_to_json(f: Fact) -> str:
    # Pydantic v2
    if hasattr(f, "model_dump"):
        data = f.model_dump(mode="json")  # converts datetimes to strings
        return json.dumps(data, sort_keys=True)
    # Pydantic v1
    if hasattr(f, "dict"):
        data = f.dict()  # type: ignore[attr-defined]
        return json.dumps(data, sort_keys=True, 
        default=_json_default)
    # Dataclass-ish
    if hasattr(f, "__dict__"):
        return json.dumps(f.__dict__, 
        sort_keys=True, 
        default=_json_default)
    raise TypeError("Unable to serialize Fact")


def test_cli_qa_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = tmp_path / "run"
    facts_dir = run_dir / "facts"
    facts_dir.mkdir(parents=True)

    fact = Fact(
        source="bjs_ncvs",
        series="rate",
        geo="US",
        period=2020,
        value=1.23,
        unit="rate_per_1000",
        denominator=1000.0,
        ci_lower=None,
        ci_upper=None,
        se=None,
        notes=None,
        query_fingerprint="fp",
    )

    (facts_dir / "facts.jsonl").write_text(_fact_to_json(fact) + "\n", encoding="utf-8")

    monkeypatch.setattr("sys.argv", ["crimex", "qa", "--run-dir", str(run_dir)])

    with pytest.raises(SystemExit) as e:
        cli.main()

    assert e.value.code == 0
    captured = capsys.readouterr()
    assert "QA PASS" in (captured.out + captured.err)
