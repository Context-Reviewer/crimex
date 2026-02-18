# FILE: crimex/connectors/bjs_ncvs.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlencode

import requests


BASE_URL = "https://api.ojp.gov/bjsdataset/v1"
DEFAULT_LIMIT = 500000


@dataclass(frozen=True)
class NcvsRequest:
    dataset: str
    fmt: str = "json"
    where: Optional[str] = None
    limit: int = DEFAULT_LIMIT


class NcvsFetchError(RuntimeError):
    pass


def _safe_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".", "@") else "_" for c in s)


def _build_url(req: NcvsRequest) -> str:
    resource = f"{req.dataset}.{req.fmt}"
    params: Dict[str, Any] = {"": int(req.limit)}
    if req.where:
        params[""] = req.where
    qs = urlencode(params, doseq=True)
    return f"{BASE_URL}/{resource}?{qs}"


def _get_param(spec: Dict[str, Any], key: str) -> Any:
    params = spec.get("params")
    if isinstance(params, dict):
        return params.get(key)
    return None


def _years_clause(years: Sequence[int]) -> str:
    vals = ", ".join(str(int(y)) for y in years)
    return f"year in ({vals})"


def _year_min_clause(year_min: int) -> str:
    return f"year >= {int(year_min)}"


def _combine_where(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a and b:
        return f"({a}) AND ({b})"
    return a or b


def _parse_spec_to_request(spec: Dict[str, Any]) -> NcvsRequest:
    dataset = spec.get("dataset") or spec.get("dataset_id")
    if not dataset or not isinstance(dataset, str):
        raise ValueError("NCVS spec missing required string field: 'dataset'")

    fmt = spec.get("format") or spec.get("fmt") or "json"
    if fmt not in ("json", "csv"):
        raise ValueError("NCVS spec 'format' must be 'json' or 'csv'")

    where = spec.get("where") or _get_param(spec, "")
    year_min = spec.get("year_min")
    years = spec.get("years")

    structured_where: Optional[str] = None
    if year_min is not None:
        structured_where = _combine_where(structured_where, _year_min_clause(int(year_min)))
    if years is not None:
        structured_where = _combine_where(structured_where, _years_clause(years))

    where = _combine_where(where, structured_where)

    limit = spec.get("limit") or _get_param(spec, "") or DEFAULT_LIMIT
    return NcvsRequest(dataset=dataset, fmt=fmt, where=where, limit=int(limit))


def fetch_ncvs_data(spec: Dict[str, Any], output_dir: str, force: bool = False) -> None:
    req = _parse_spec_to_request(spec)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    where_part = _safe_filename(req.where) if req.where else "all"
    fname = f"{req.dataset}.{req.fmt}.where_{where_part}.limit_{req.limit}.raw"
    raw_path = out_dir / fname

    if raw_path.exists() and not force:
        return

    url = _build_url(req)
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        raise NcvsFetchError(f"Network error fetching {url}: {e}") from e

    if resp.status_code != 200:
        snippet = resp.text[:300].replace("\n", " ").strip()
        raise NcvsFetchError(f"HTTP {resp.status_code} fetching {url}: {snippet}")

    raw_path.write_bytes(resp.content)

    response_sha256 = raw_path.stem
    receipt = {
        "source": "bjs_ncvs",
        "endpoint": req.dataset,
        "request_url": url,
        "request_params_redacted": dict(sorted({"": req.limit}.items())),
        "http_status": resp.status_code,
        "retry_attempts": 0,
        "fallback_used": False,
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "response_sha256": response_sha256,
        "artifact_path": f"raw/bjs_ncvs/{raw_path.name}",
    }

    receipt_path = raw_path.parent / f"{response_sha256}.receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")


