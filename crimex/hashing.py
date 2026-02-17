"""
Hashing utilities for crimex.
Ensures stable caching keys across platforms.
"""
import hashlib
import json
from typing import Any, Dict

def hash_string(data: str) -> str:
    """Computes SHA256 hash of a string."""
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

def hash_file(filepath: str) -> str:
    """Computes SHA256 hash of a file's content."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def compute_cache_key(endpoint: str, params: Dict[str, Any], headers: Dict[str, Any] | None = None) -> str:
    """
    Computes a stable cache key for an API request.
    Key = SHA256(endpoint + sorted_params + relevant_headers)
    """
    # Sort params to ensure determinism
    sorted_params = json.dumps(params, sort_keys=True, separators=(',', ':'))
    
    # We include headers that might affect the response content (e.g., Accept header)
    # Most auth headers don't affect content, so we generally ignore them for caching purposes
    # unless specified otherwise.
    relevant_headers = ""
    if headers:
        # Sort headers too
        sorted_headers = json.dumps(headers, sort_keys=True, separators=(',', ':'))
        relevant_headers = sorted_headers
        
    raw_key = f"{endpoint}|{sorted_params}|{relevant_headers}"
    return hash_string(raw_key)

def hash_fact_content(fact_dict: Dict[str, Any]) -> str:
    """
    Computes a hash for a normalized fact to detect duplicates or changes.
    """
    # Exclude volatile fields if any (e.g. retrieved_at might be different but content same)
    # However, for strict determinism, we might want to include everything except maybe `retrieved_at`.
    # Let's exclude `retrieved_at` and `query_fingerprint` for content hashing.
    content = {k: v for k, v in fact_dict.items() if k not in ("retrieved_at", "query_fingerprint")}
    return hash_string(json.dumps(content, sort_keys=True, separators=(',', ':')))
