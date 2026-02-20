from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest

from crimex.connectors.bjs_ncvs import _build_url, _parse_spec_to_request


def _qs(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query, keep_blank_values=True)


def test_ncvs_build_url_uses_where_limit_and_extra_params() -> None:
    spec = {
        "dataset_id": "ncvs-2020",
        "params": {
            "where": "year=2020",
            "limit": 123,
            "select": "year,state",
        },
    }

    req = _parse_spec_to_request(spec)
    url = _build_url(req)
    qs = _qs(url)

    assert qs["where"] == ["year=2020"]
    assert qs["limit"] == ["123"]
    assert qs["select"] == ["year,state"]


def test_ncvs_supports_dollar_params_and_year_filters() -> None:
    spec = {
        "dataset": "ncvs-2020",
        "year_min": 2019,
        "params": {
            "$where": "region='NE'",
            "$limit": 5,
        },
    }

    req = _parse_spec_to_request(spec)
    url = _build_url(req)
    qs = _qs(url)

    assert "$where" in qs
    assert "$limit" in qs
    assert qs["$limit"] == ["5"]
    assert "year >= 2019" in qs["$where"][0]
    assert "AND" in qs["$where"][0]


def test_ncvs_params_must_be_dict() -> None:
    with pytest.raises(ValueError):
        _parse_spec_to_request({"dataset": "ncvs-2020", "params": ["nope"]})
