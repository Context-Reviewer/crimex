"""
Tests for normalization modules.
"""

from crimex.normalize import fbi_normalize, ncvs_normalize


def test_fbi_normalize():
    raw_data = {
        "results": [
            {"data_year": 2020, "violent_crime": 100},
            {"data_year": 2021, "violent_crime": 110},
        ]
    }
    meta = {
        "source": "fbi_cde",
        "series_name": "violent_crime",
        "query_fingerprint": "abc",
        "params": {"state": "US"},
    }

    facts = fbi_normalize.normalize(raw_data, meta)

    assert len(facts) == 2
    assert facts[0].period == 2020
    assert facts[0].value == 100.0
    assert facts[0].series == "violent_crime"
    assert facts[0].geo == "US"
    assert facts[0].query_fingerprint == "abc"


def test_ncvs_normalize_list():
    raw_data = [
        {"year": 2020, "race": "White", "rate": 15.2},
        {"year": 2020, "race": "Black", "rate": 18.5},
    ]
    meta = {
        "source": "bjs_ncvs",
        "series_name": "rate",
        "query_fingerprint": "xyz",
        "expected_unit": "per_1000",
    }

    facts = ncvs_normalize.normalize(raw_data, meta)

    assert len(facts) == 2
    f1 = facts[0]
    assert f1.period == 2020
    assert f1.value == 15.2
    assert f1.unit == "per_1000"
    assert f1.dimensions == {"race": "White"}
    assert f1.geo == "US"  # Default


def test_ncvs_normalize_dict_wrapped():
    raw_data = {"data": [{"year": "2019", "count": "500", "state": "NY"}]}
    meta = {"source": "bjs_ncvs", "series_name": "count", "query_fingerprint": "123"}

    facts = ncvs_normalize.normalize(raw_data, meta)

    assert len(facts) == 1
    assert facts[0].period == 2019
    assert facts[0].value == 500.0
    assert facts[0].geo == "NY"
