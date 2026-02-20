from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import crimex.cli as cli


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_report_command_covers_report_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    facts_path = tmp_path / "facts.jsonl"
    facts_path.write_text(
        "\n".join(
            [
                json.dumps(
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
                    },
                    sort_keys=True,
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "report", "--facts", str(facts_path), "--out", str(out_dir), "--explain"],
    )

    # report path should not sys.exit on success (per your current behavior)
    cli.main()

    assert (out_dir / "report.csv").exists()
    assert (out_dir / "report.md").exists()


def test_cli_run_end_to_end_covers_big_handle_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out_base = tmp_path / "out"
    spec_path = tmp_path / "spec.json"
    run_id = "TESTRUN_COVERAGE"

    _write_json(
        spec_path,
        {
            "source": "bjs_ncvs",
            # these fields are irrelevant to the stub fetch, but keep spec realistic
            "dataset": "NCVS_VICT",
            "format": "json",
            "year_min": 2020,
            "limit": 10,
        },
    )

    def _stub_fetch_ncvs_data(spec: dict, output_dir: str, force: bool = False) -> None:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # normalize_all() discovers *.meta.json and expects a sibling *.json
        (out_dir / "ncvs.meta.json").write_text(
            json.dumps(
                {
                    "source": "bjs_ncvs",
                    "series_name": "rate",
                    "query_fingerprint": "xyz",
                    "expected_unit": "per_1000",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        (out_dir / "ncvs.json").write_text(
            json.dumps(
                [
                    {"year": 2020, "race": "White", "rate": 15.2},
                    {"year": 2020, "race": "Black", "rate": 18.5},
                ],
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(cli, "fetch_ncvs_data", _stub_fetch_ncvs_data)

    args = SimpleNamespace(
        spec=str(spec_path),
        out_base=str(out_base),
        run_id=run_id,
        overwrite=False,  # run dir does not exist yet; no need to overwrite
        force=False,
        offline=False,    # IMPORTANT: let handle_run call our stub fetch
        explain=True,
    )

    with pytest.raises(SystemExit) as e:
        cli.handle_run(args)
    assert e.value.code == 0
def test_cli_fetch_unknown_source_hits_error_branch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    spec_path = tmp_path / "bad_spec.json"
    _write_json(spec_path, {"source": "not_a_real_source"})

    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["crimex", "fetch", "--spec", str(spec_path), "--out", str(out_dir)],
    )

    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1
