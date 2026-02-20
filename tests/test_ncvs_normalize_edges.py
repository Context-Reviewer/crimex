from __future__ import annotations

from crimex.normalize.ncvs_normalize import normalize_ncvs


def test_ncvs_normalize_requires_meta_fields_returns_empty() -> None:
    raw = [{"year": 2020, "race": "White", "rate": 1.2}]
    meta = {"source": "bjs_ncvs"}  # intentionally minimal

    facts = normalize_ncvs(raw, meta)
    assert facts == []



def test_ncvs_normalize_handles_empty_rows() -> None:
    raw = []
    meta = {
        "source": "bjs_ncvs",
        "series_name": "rate",
        "query_fingerprint": "fp",
        "expected_unit": "per_1000",
        "retrieved_at": "2020-01-01T00:00:00Z",
    }

    facts = normalize_ncvs(raw, meta)
    assert facts == []
