from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from crimex.normalize.ncvs_normalize import normalize_ncvs
from crimex.schemas import QuerySpec, generate_json_schemas


def test_generate_json_schemas_writes_expected_files(tmp_path) -> None:
    generate_json_schemas(str(tmp_path))

    expected = {"fact-1.0.json", "query_spec-1.0.json", "run_manifest-1.0.json"}
    found = {p.name for p in tmp_path.iterdir() if p.is_file()}
    assert expected == found

    for name in expected:
        data = json.loads((tmp_path / name).read_text(encoding="utf-8"))
        assert isinstance(data, dict)
        assert "properties" in data


def test_query_spec_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        QuerySpec(
            source="fbi_cde",
            series_name="series",
            extra_field="nope",
        )


def test_ncvs_normalize_csv_fallback_parses_text() -> None:
    raw_csv = "year,value,state\n2020,1.5,NY\n"
    meta = {
        "source": "bjs_ncvs",
        "series_name": "value",
        "query_fingerprint": "fp",
        "expected_unit": "per_1000",
    }

    facts = normalize_ncvs(raw_csv, meta)
    assert len(facts) == 1
    fact = facts[0]
    assert fact["period"] == 2020
    assert fact["geo"] == "NY"
    assert fact["value"] == 1.5
    assert fact["unit"] == "per_1000"
