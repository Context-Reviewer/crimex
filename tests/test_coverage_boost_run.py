from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import crimex.cli as cli
from crimex.run import RunContext


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_run_offline_end_to_end_success_via_seeded_runcontext(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out_base = tmp_path / "out"
    spec_path = tmp_path / "spec.json"
    run_id = "TESTRUN_COVERAGE"

    _write_json(
        spec_path,
        {
            "source": "bjs_ncvs",
            "series_name": "rate",
            "query_fingerprint": "xyz",
            "expected_unit": "per_1000",
        },
    )

    class SeededRunContext(RunContext):
        def __post_init__(self) -> None:
            super().__post_init__()

            raw_source_dir = self.raw_dir() / "bjs_ncvs"
            raw_source_dir.mkdir(parents=True, exist_ok=True)

            _write_json(
                raw_source_dir / "ncvs.meta.json",
                {
                    "source": "bjs_ncvs",
                    "series_name": "rate",
                    "query_fingerprint": "xyz",
                    "expected_unit": "per_1000",
                },
            )
            _write_json(
                raw_source_dir / "ncvs.json",
                [
                    {"year": 2020, "race": "White", "rate": 15.2},
                    {"year": 2020, "race": "Black", "rate": 18.5},
                ],
            )

    monkeypatch.setattr(cli, "RunContext", SeededRunContext)

    args = SimpleNamespace(
        spec=str(spec_path),
        out_base=str(out_base),
        run_id=run_id,
        overwrite=True,
        force=False,
        offline=True,
        explain=True,
    )

    with pytest.raises(SystemExit) as e:
        cli.handle_run(args)

    assert e.value.code == 0


def test_cli_run_unknown_source_exits_1(tmp_path: Path) -> None:
    out_base = tmp_path / "out"
    spec_path = tmp_path / "spec.json"

    _write_json(
        spec_path,
        {
            "source": "definitely_not_real",
            "series_name": "rate",
            "query_fingerprint": "xyz",
            "expected_unit": "per_1000",
        },
    )

    args = SimpleNamespace(
        spec=str(spec_path),
        out_base=str(out_base),
        run_id="RID_UNKNOWN",
        overwrite=True,
        force=False,
        offline=False,
        explain=False,
    )

    with pytest.raises(SystemExit) as e:
        cli.handle_run(args)

    assert e.value.code == 1
