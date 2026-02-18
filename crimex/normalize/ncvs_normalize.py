from __future__ import annotations

import json
from typing import Any

from crimex.schemas import Fact, utc_now


def normalize_ncvs(raw_data: Any, meta: dict[str, Any]) -> list[dict[str, Any]]:
    source = meta.get("source", "bjs_ncvs")
    series_name = meta.get("series_name") or meta.get("series") or "ncvs"

    query_fingerprint = meta.get("query_fingerprint") or meta.get("sha256") or "unknown"

    records: Any = None
    if isinstance(raw_data, list):
        records = raw_data
    elif isinstance(raw_data, dict):
        records = raw_data.get("data") or raw_data.get("results")
        if records is None:
            raise ValueError("Unknown NCVS response format. Expected list or dict with 'data'/'results'.")
    elif isinstance(raw_data, str):
        try:
            records = json.loads(raw_data)
            if isinstance(records, dict):
                records = records.get("data") or records.get("results")
        except json.JSONDecodeError:
            import csv
            from io import StringIO

            f = StringIO(raw_data)
            reader = csv.DictReader(f)
            records = list(reader)
    else:
        raise ValueError(f"Unknown raw data type: {type(raw_data)}")

    if not records:
        return []

    default_unit = meta.get("expected_unit") or meta.get("unit") or "rate_per_1000"

    base_geo = "US"
    params = meta.get("params") or {}
    if isinstance(params, dict):
        base_geo = str(params.get("geo") or params.get("state") or "US")

    meta_dims = meta.get("dimensions") or {}
    if not isinstance(meta_dims, dict):
        meta_dims = {}

    facts_with_idx: list[tuple[int, dict[str, Any]]] = []

    for idx, item in enumerate(records):
        if not isinstance(item, dict):
            continue

        year = item.get("year") or item.get("data_year") or item.get("period")
        if year is None or year == "":
            continue

        value = item.get("value")
        if value is None:
            value = item.get(series_name)
        if value is None:
            continue

        geo = base_geo
        if item.get("state") is not None:
            geo = str(item.get("state"))

        unit = item.get("unit") or default_unit
        denominator = item.get("denominator") or meta.get("denominator")

        dims: dict[str, Any] = dict(meta_dims)
        excluded = {
            "year",
            "data_year",
            "period",
            "state",
            "geo",
            "unit",
            "denominator",
            "value",
            str(series_name),
        }
        for k, v in item.items():
            if k in excluded:
                continue
            if v is None:
                continue
            dims[k] = v

        fact = Fact(
            source=str(source),
            series=str(series_name),
            geo=str(geo),
            period=int(year),
            value=float(value),
            unit=str(unit),
            denominator=float(denominator) if denominator is not None else None,
            dimensions=dims,
            notes=None,
            se=None,
            ci_lower=None,
            ci_upper=None,
            retrieved_at=utc_now(),
            query_fingerprint=str(query_fingerprint),
        )
        facts_with_idx.append((idx, fact.model_dump(mode="json")))

    facts_with_idx.sort(
        key=lambda pair: (
            pair[1].get("source", ""),
            pair[1].get("series", ""),
            pair[1].get("geo", ""),
            pair[1].get("period", ""),
            pair[1].get("unit", ""),
            pair[0],
        )
    )

    return [f for _idx, f in facts_with_idx]


def normalize(raw_data: Any, meta: dict[str, Any]) -> list[Fact]:
    return [Fact(**f) for f in normalize_ncvs(raw_data, meta)]
