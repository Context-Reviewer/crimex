from __future__ import annotations

import datetime
from typing import Any

from crimex.schemas import Fact


def _parse_mm_yyyy(key: str) -> tuple[int, int]:
    parts = key.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid MM-YYYY key: {key}")
    mm = int(parts[0])
    yyyy = int(parts[1])
    return yyyy, mm


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def normalize(raw_data: Any, meta: dict[str, Any]) -> list[Fact]:
    # Public API: tests expect Fact objects.
    return [Fact(**f) for f in normalize_fbi_cde(raw_data, meta)]


def normalize_fbi_cde(raw_data: Any, meta: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normalize FBI CDE raw responses into canonical facts.

    Supported response patterns:
      - Pattern A (legacy list): {"results": [...]} or {"data": [...]} or list directly
      - Pattern B (nested dict): {"offenses": {"rates": {"Label": {"MM-YYYY": value, ...}}, "counts": {...}}, ...}

    Deterministic aggregation:
      - For nested MM-YYYY keys: aggregate by YEAR as mean of available months.
      - Output sorted by (source, series, geo, period, unit, dimensions)
    """
    source = meta.get("source", "fbi_cde")
    series_name = meta.get("series_name") or meta.get("series") or meta.get("seriesName") or "unknown_series"

    params = meta.get("params") or {}
    if isinstance(params, dict):
        geo = str(params.get("state") or params.get("geo") or params.get("location") or "US")
    else:
        geo = "US"

    query_fingerprint = meta.get("query_fingerprint")
    if not query_fingerprint:
        query_fingerprint = meta.get("sha256") or meta.get("fingerprint") or "unknown"

    retrieved_at = meta.get("retrieved_at")
    if retrieved_at is None:
        retrieved_at = _utc_now().isoformat().replace("+00:00", "Z")

    if isinstance(raw_data, dict) and ("results" in raw_data or "data" in raw_data):
        records = raw_data.get("results") or raw_data.get("data") or []
    elif isinstance(raw_data, list):
        records = raw_data
    else:
        records = None

    facts: list[dict[str, Any]] = []

    if isinstance(records, list):
        for row in records:
            if not isinstance(row, dict):
                continue

            year = row.get("data_year") or row.get("year") or row.get("period")
            if year is None:
                continue

            # Prefer explicit "value", but fall back to the series field name (test contract).
            value = row.get("value")
            if value is None:
                value = row.get(series_name)
            if value is None:
                continue

            unit = meta.get("unit") or row.get("unit") or "count"
            denom = row.get("denominator")

            dims = meta.get("dimensions") or {}
            if not isinstance(dims, dict):
                dims = {}

            fact = Fact(
                source=str(source),
                series=str(series_name),
                geo=str(geo),
                period=int(year),
                value=float(value),
                unit=str(unit),
                denominator=float(denom) if denom is not None else None,
                dimensions=dims,
                notes=None,
                se=None,
                ci_lower=None,
                ci_upper=None,
                retrieved_at=datetime.datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")),
                query_fingerprint=str(query_fingerprint),
            )
            facts.append(fact.model_dump(mode="json"))
    else:
        offenses = None
        if isinstance(raw_data, dict):
            offenses = raw_data.get("offenses") or raw_data.get("offense") or raw_data.get("data")

        if not isinstance(offenses, dict):
            return []

        for bucket_name, unit in (("counts", "count"), ("rates", "rate_per_100k")):
            bucket = offenses.get(bucket_name)
            if not isinstance(bucket, dict):
                available = list(offenses)
                raise ValueError(
                    f"FBI offenses format missing expected bucket '{bucket_name}'. Available buckets: {available}"
                )

            for label, timeseries in bucket.items():
                if not isinstance(timeseries, dict):
                    continue

                by_year: dict[int, list[float]] = {}
                for tkey, val in timeseries.items():
                    if not isinstance(tkey, str):
                        continue
                    try:
                        year, _month = _parse_mm_yyyy(tkey)
                    except Exception:
                        continue
                    try:
                        fval = float(val)
                    except Exception:
                        continue
                    by_year.setdefault(year, []).append(fval)

                for year in sorted(by_year.keys()):
                    vals = by_year[year]
                    if not vals:
                        continue
                    avg = sum(vals) / float(len(vals))
                    dims = {"label": str(label)} if label is not None else {}
                    fact = Fact(
                        source=str(source),
                        series=str(series_name),
                        geo=str(geo),
                        period=int(year),
                        value=float(avg),
                        unit=str(unit),
                        denominator=None,
                        dimensions=dims,
                        notes=None,
                        se=None,
                        ci_lower=None,
                        ci_upper=None,
                        retrieved_at=datetime.datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")),
                        query_fingerprint=str(query_fingerprint),
                    )
                    facts.append(fact.model_dump(mode="json"))

    facts.sort(
        key=lambda f: (
            f.get("source", ""),
            f.get("series", ""),
            f.get("geo", ""),
            f.get("period", ""),
            f.get("unit", ""),
            str(f.get("dimensions", "")),
        )
    )
    return facts
