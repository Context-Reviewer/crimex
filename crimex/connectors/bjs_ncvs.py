from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests


BASE_URL = "https://api.ojp.gov/bjsdataset/v1"
DEFAULT_LIMIT = 500000  # NCVS datasets exceed 1,000 rows; pick a safe high default


@dataclass(frozen=True)
class NcvsRequest:
    dataset: str                 # e.g. "gcuy-rt5g"
    fmt: str = "json"            # "json" or "csv"
    where: Optional[str] = None  # Socrata-style $where
    limit: int = DEFAULT_LIMIT


class NcvsFetchError(RuntimeError):
    pass


def _safe_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".", "@") else "_" for c in s)


def _build_url(req: NcvsRequest) -> str:
    resource = f"{req.dataset}.{req.fmt}"
    params: Dict[str, Any] = {"$limit": int(req.limit)}
    if req.where:
        params["$where"] = req.where

    qs = urlencode(params, doseq=True)
    return f"{BASE_URL}/{resource}?{qs}"


def _get_param(spec: Dict[str, Any], key: str) -> Any:
    """
    Helper for legacy spec shape: params may contain $where, $limit, etc.
    """
    params = spec.get("params")
    if isinstance(params, dict):
        return params.get(key)
    return None


def _parse_spec_to_request(spec: Dict[str, Any]) -> NcvsRequest:
    """
    Accepts both:
      New shape:
        dataset, format, where, limit
      Legacy shape:
        dataset_id, params: {"$where": "...", "$limit": N}
    Ignores unrelated fields (series_name, expected_unit, etc).
    """
    dataset = spec.get("dataset")
    if dataset is None:
        dataset = spec.get("dataset_id")

    if not dataset or not isinstance(dataset, str):
        raise ValueError("NCVS spec missing required string field: 'dataset' (or legacy 'dataset_id')")

    fmt = spec.get("format", None)
    if fmt is None:
        fmt = spec.get("fmt", None)
    if fmt is None:
        # legacy sometimes implies json
        fmt = "json"

    if fmt not in ("json", "csv"):
        raise ValueError("NCVS spec 'format' must be 'json' or 'csv'")

    where = spec.get("where")
    if where is None:
        where = _get_param(spec, "$where")

    if where is not None and not isinstance(where, str):
        raise ValueError("NCVS spec 'where' must be a string when provided (or params.$where)")

    limit = spec.get("limit")
    if limit is None:
        limit = _get_param(spec, "$limit")
    if limit is None:
        limit = DEFAULT_LIMIT

    if not isinstance(limit, int) or limit <= 0:
        raise ValueError("NCVS spec 'limit' must be a positive integer (or params.$limit)")

    return NcvsRequest(dataset=dataset, fmt=fmt, where=where, limit=limit)


def fetch_ncvs_data(spec: Dict[str, Any], output_dir: str, force: bool = False) -> None:
    """
    Fetch NCVS data from the OJP NCVS REST API (api.ojp.gov) and store raw artifact(s).

    Determinism rules:
    - output file name is derived from dataset + format + where + limit (sanitized)
    - if output exists and not force: do not overwrite
    - failures raise exceptions (no sys.exit)
    """
    req = _parse_spec_to_request(spec)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    where_part = _safe_filename(req.where) if req.where else "all"
    fname = f"{req.dataset}.{req.fmt}.where_{where_part}.limit_{req.limit}.raw"
    raw_path = out_dir / fname

    if raw_path.exists() and not force:
        print(f"NCVS raw cache hit: {raw_path}")
        return

    url = _build_url(req)
    print(f"Fetching from {url} ...")

    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        raise NcvsFetchError(f"Network error fetching {url}: {e}") from e

    if resp.status_code != 200:
        snippet = resp.text[:300].replace("\n", " ").strip()
        raise NcvsFetchError(f"HTTP {resp.status_code} fetching {url}: {snippet}")

    raw_path.write_bytes(resp.content)
    print(f"Wrote raw NCVS artifact: {raw_path}")

    if req.fmt == "json":
        try:
            data = resp.json()
            pretty_path = out_dir / (raw_path.name + ".pretty.json")
            pretty_path.write_text(
                json.dumps(data, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            print(f"Wrote pretty JSON: {pretty_path}")
        except Exception:
            pass
