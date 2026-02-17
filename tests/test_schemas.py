"""
Tests for schemas module.
"""
import pytest
from crimex.schemas import Fact, QuerySpec
from pydantic import ValidationError

def test_fact_valid():
    """Test valid Fact."""
    fact = Fact(
        source="fbi_cde",
        series="violent_crime",
        geo="US",
        period=2020,
        value=100.5,
        unit="count",
        query_fingerprint="abc"
    )
    assert fact.value == 100.5
    assert fact.retrieved_at is not None

def test_fact_invalid_missing_field():
    """Test Fact validation fails on missing required field."""
    with pytest.raises(ValidationError):
        Fact(
            source="fbi_cde",
            # series missing
            geo="US",
            period=2020,
            value=100.5,
            unit="count",
            query_fingerprint="abc"
        )

def test_query_spec_valid():
    """Test valid QuerySpec."""
    spec = QuerySpec(
        source="fbi_cde",
        endpoint="some/api",
        series_name="test_series"
    )
    assert spec.source == "fbi_cde"
