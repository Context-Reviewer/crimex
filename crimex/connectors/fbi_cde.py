"""
FBI Crime Data Explorer (CDE) connector.
Fetches data from the FBI CDE API.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from crimex.config import require_fbi_api_key
from crimex.hashing import compute_cache_key
from crimex.io import read_json, write_json

BASE_URL = "https://api.usa.gov/crime/fbi/cde"
_MM_YYYY = re.compile(r"^(0[1-9]|1[0-2])-\d{4}$")

# Deterministic retry schedule (seconds). No jitter.
RETRY_DELAYS = (1.0, 2.0, 4.0)
RETRY_STATUS = {429, 500, 502, 503, 504}


class FbiFetchError(RuntimeError):
    pass


def _validate_month_year(value: Any, field: str) -> None:
    if not isinstance(value, str) or not _MM_YYYY.match(value):
        raise FbiFetchError(f"Invalid '{field}' value: expected MM-YYYY string, got {value!r}")


def _resolve_cache_dir(output_dir: str) -> Path:
    """
    Backward-compatible behavior:
      - If output_dir already points at .../raw/fbi_cde (governed runs), use it as-is.
      - Otherwise (plain fetch command), create output_dir/raw/fbi_cde.
    """
    base = Path(output_dir)
    if base.name == "fbi_cde":
        return base
    return base / "raw" / "fbi_cde"


def _try_read_cache(cache_file: Path, meta_file: Path, spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if cache_file.exists():
        # Ensure metadata exists too
        if not meta_file.exists():
            write_json(spec, str(meta_file))
        return read_json(str(cache_file))
    return None


def fetch_fbi_data(spec: Dict[str, Any], output_dir: str, force: bool = False) -> Dict[str, Any]:
    """
    Fetches data from FBI CDE API with caching.

    Determinism rules:
    - Cache key excludes api_key.
    - If cached and not force: reuse.
    - On transient HTTP/network errors: retry deterministically.
    - If retries fail and cache exists (and not force): fallback to cache.
    """
    endpoint = spec.get("endpoint")
    if not endpoint or not isinstance(endpoint, str):
        raise FbiFetchError("Spec missing required string field: 'endpoint'")

    params = spec.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise FbiFetchError("Spec field 'params' must be an object/dict when provided")

    api_key = require_fbi_api_key()
    if not api_key:
        raise FbiFetchError("Missing FBI API Key. Please set FBI_API_KEY or DATA_GOV_API_KEY.")

    endpoint_norm = endpoint[1:] if endpoint.startswith("/") else endpoint
    url = f"{BASE_URL}/{endpoint_norm}"

    if "from" in params:
        _validate_month_year(params["from"], "from")
    if "to" in params:
        _validate_month_year(params["to"], "to")

    request_params = params.copy()
    request_params["api_key"] = api_key
    headers = {"Accept": "application/json"}

    cache_key = compute_cache_key(endpoint, params, headers)

    cache_dir = _resolve_cache_dir(output_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_file = cache_dir / f"{cache_key}.json"
    meta_file = cache_dir / f"{cache_key}.meta.json"

    # Normal cache reuse path
    if not force:
        cached = _try_read_cache(cache_file, meta_file, spec)
        if cached is not None:
            print(f"Using cached response: {cache_file}")
            return cached

    print(f"Fetching from {url} ...")

    last_error: Optional[str] = None

    # Attempt 1 + retries
    attempts = 1 + len(RETRY_DELAYS)
    for i in range(attempts):
        try:
            response = requests.get(url, params=request_params, headers=headers, timeout=30)
        except requests.RequestException as e:
            last_error = f"Network error fetching {url}: {e}"
            # retry
        else:
            if response.status_code == 200:
                data = response.json()
                write_json(data, str(cache_file))
                write_json(spec, str(meta_file))
                print(f"Saved response to {cache_file}")
                return data

            snippet = response.text[:500]
            last_error = f"HTTP {response.status_code} fetching {url}: {snippet}"

            # Only retry transient codes
            if response.status_code not in RETRY_STATUS:
                raise FbiFetchError(last_error)

        # If there is another retry scheduled, sleep deterministically
        if i < len(RETRY_DELAYS):
            time.sleep(RETRY_DELAYS[i])

    # Retries exhausted: if we have cache and not force, fallback
    if not force:
        cached = _try_read_cache(cache_file, meta_file, spec)
        if cached is not None:
            print(f"FALLBACK: using cached response after fetch failures: {cache_file}")
            return cached

    raise FbiFetchError(last_error or "Unknown fetch failure")
