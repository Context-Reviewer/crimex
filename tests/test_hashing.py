"""
Tests for hashing module.
"""

from crimex.hashing import compute_cache_key, hash_string


def test_hash_determinism():
    """Test that hashing is deterministic."""
    s1 = "hello world"
    s2 = "hello world"
    assert hash_string(s1) == hash_string(s2)


def test_compute_cache_key_order():
    """Test that param order doesn't affect cache key."""
    endpoint = "test/api"
    params1 = {"a": 1, "b": 2}
    params2 = {"b": 2, "a": 1}

    key1 = compute_cache_key(endpoint, params1)
    key2 = compute_cache_key(endpoint, params2)

    assert key1 == key2


def test_compute_cache_key_different():
    """Test that different params produce different keys."""
    endpoint = "test/api"
    params1 = {"a": 1}
    params2 = {"a": 2}

    assert compute_cache_key(endpoint, params1) != compute_cache_key(endpoint, params2)
