# FILE: crimex/connectors/fbi_cde.py
"""
FBI Crime Data Explorer (CDE) connector.
Fetches data from the FBI CDE API.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
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
    base = Path(output_dir)
    if base.name == "fbi_cde":
        return base
    return base / "raw" / "fbi_cde"


def _try_read_cache(cache_file: Path, meta_file: Path, spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if cache_file.exists():
        if not meta_file.exists():
            write_json(spec, str(meta_file))
        return read_json(str(cache_file))
    return None


def _write_receipt(
    *,
    source: str,
    endpoint: str,
    request_url: str,
    params: Dict[str, Any],
    http_status: int,
    retry_attempts: int,
    fallback_used: bool,
    response_sha256: str,
    raw_path: Path,
) -> None:
    safe_params = {k: v for k, v in params.items() if k.lower() != "api_key"}
    safe_params = dict(sorted(safe_params.items(), key=lambda kv: kv[0]))

    receipt = {
        "source": source,
        "endpoint": endpoint,
        "request_url": request_url,
        "request_params_redacted": safe_params,
        "http_status": http_status,
        "retry_attempts": retry_attempts,
        "fallback_used": fallback_used,
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "response_sha256": response_sha256,
        "artifact_path": f"raw/{source}/{raw_path.name}",
    }

    receipt_path = raw_path.parent / f"{response_sha256}.receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fetch_fbi_data(spec: Dict[str, Any], output_dir: str, force: bool = False) -> Dict[str, Any]:
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

    if not force:
        cached = _try_read_cache(cache_file, meta_file, spec)
        if cached is not None:
            return cached

    last_error: Optional[str] = None
    attempts = 1 + len(RETRY_DELAYS)

    for i in range(attempts):
        try:
            response = requests.get(url, params=request_params, headers=headers, timeout=30)
        except requests.RequestException as e:
            last_error = f"Network error fetching {url}: {e}"
        else:
            if response.status_code == 200:
                data = response.json()
                write_json(data, str(cache_file))
                write_json(spec, str(meta_file))

                response_sha256 = cache_file.stem
                _write_receipt(
                    source="fbi_cde",
                    endpoint=endpoint,
                    request_url=url,
                    params=request_params,
                    http_status=response.status_code,
                    retry_attempts=i,
                    fallback_used=False,
                    response_sha256=response_sha256,
                    raw_path=cache_file,
                )
                return data

            snippet = response.text[:500]
            last_error = f"HTTP {response.status_code} fetching {url}: {snippet}"

            if response.status_code not in RETRY_STATUS:
                raise FbiFetchError(last_error)

        if i < len(RETRY_DELAYS):
            time.sleep(RETRY_DELAYS[i])

    if not force:
        cached = _try_read_cache(cache_file, meta_file, spec)
        if cached is not None:
            return cached

    raise FbiFetchError(last_error or "Unknown fetch failure")


