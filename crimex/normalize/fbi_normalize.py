"""
FBI CDE normalization logic.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from crimex.schemas import Fact


def _pick_year(item: Dict[str, Any]) -> Optional[int]:
    y = item.get("data_year")
    if y is None:
        y = item.get("year")
    if y is None:
        return None
    try:
        return int(y)
    except Exception:
        return None


def _pick_value(item: Dict[str, Any], series_name: str) -> Optional[float]:
    cand = None
    if series_name in item:
        cand = item.get(series_name)
    elif "value" in item:
        cand = item.get("value")
    elif "actual" in item:
        cand = item.get("actual")
    elif "count" in item:
        cand = item.get("count")

    if cand is None:
        return None
    try:
        return float(cand)
    except Exception:
        return None


def _parse_mm_yyyy(key: str) -> Tuple[int, int]:
    # key like "01-2010"
    parts = key.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid time key (expected MM-YYYY): {key!r}")
    mm = int(parts[0])
    yyyy = int(parts[1])
    if mm < 1 or mm > 12:
        raise ValueError(f"Invalid month in time key: {key!r}")
    return yyyy, mm


def _aggregate(values: List[float], mode: str) -> float:
    if not values:
        raise ValueError("Cannot aggregate empty value list")
    if mode == "mean":
        return sum(values) / float(len(values))
    if mode == "sum":
        return float(sum(values))
    raise ValueError(f"Unknown aggregation mode: {mode!r}")


def normalize(raw_data: Dict[str, Any], meta: Dict[str, Any]) -> List[Fact]:
    """
    Normalizes FBI CDE raw data into Facts.

    Supported response patterns:
      - Pattern A (legacy list): {"results": [...]} or {"data": [...]} or list directly
      - Pattern B (nested dict): {"offenses": {"rates": {"Label": {"MM-YYYY": value, ...}}, "counts": {...}}, ...}

    Deterministic aggregation:
      - For monthly series mapped by MM-YYYY:
        * rates -> yearly mean
        * counts -> yearly sum
    """
    facts: List[Fact] = []

    source = meta.get("source", "fbi_cde")
    series_name = meta.get("series_name")
    if not series_name:
        raise ValueError("Metadata missing 'series_name'")

    unit = meta.get("expected_unit", "count")
    denominator = meta.get("expected_denominator")

    # Common heuristic: rates per 100k
    if unit in ("rate_per_100k", "rate_per_100000") and not denominator:
        denominator = 100000.0

    # Geo heuristic
    geo_default = "US"
    params = meta.get("params", {}) or {}
    if isinstance(params, dict):
        state = params.get("stateAbbr") or params.get("state")
        if state:
            geo_default = str(state)

    # -------- Pattern B: nested dict with offenses/rates/counts ----------
    offenses = raw_data.get("offenses")
    if isinstance(offenses, dict):
        # Select metric bucket based on unit
        bucket_name: Optional[str] = None
        agg_mode: Optional[str] = None

        if isinstance(unit, str) and unit.startswith("rate"):
            bucket_name = "rates"
            agg_mode = "mean"
        elif unit == "count":
            bucket_name = "counts"
            agg_mode = "sum"

        if bucket_name is None or agg_mode is None:
            raise ValueError(
                f"Unsupported expected_unit for FBI offenses dict format: {unit!r}. "
                f"Use 'count' or a rate unit like 'rate_per_100k'."
            )

        bucket = offenses.get(bucket_name)
        if not isinstance(bucket, dict):
            available = [k for k in offenses.keys()]
            raise ValueError(
                f"FBI offenses format missing expected bucket '{bucket_name}'. "
                f"Available under offenses: {available}"
            )

        # bucket: {label: {MM-YYYY: value}}
        for label, series_map in sorted(bucket.items(), key=lambda kv: str(kv[0])):
            if not isinstance(series_map, dict):
                continue

            # collect values by year
            by_year: Dict[int, List[float]] = {}
            for tkey, val in series_map.items():
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
                annual_value = _aggregate(by_year[year], agg_mode)

                dims: Dict[str, Any] = {
                    "label": str(label),
                    "aggregation": "mean_monthly" if agg_mode == "mean" else "sum_monthly",
                }

                facts.append(
                    Fact(
                        source=source,
                        series=series_name,
                        geo=geo_default,
                        period=int(year),
                        value=float(annual_value),
                        unit=unit,
                        denominator=denominator,
                        dimensions=dims,
                        notes=meta.get("notes"),
                        query_fingerprint=meta.get("query_fingerprint", "unknown"),
                    )
                )

        return facts

    # -------- Pattern A: list-based responses (legacy) ----------
    results = raw_data.get("results") or raw_data.get("data")
    if results is None:
        if isinstance(raw_data, list):
            results = raw_data
        else:
            raise ValueError(f"Unknown FBI CDE response format. Keys: {list(raw_data.keys())}")

    for item in results:
        if not isinstance(item, dict):
            continue

        year = _pick_year(item)
        if year is None:
            continue

        val_float = _pick_value(item, series_name)
        if val_float is None:
            raise ValueError(
                f"Cannot determine value for FBI item. "
                f"Expected key '{series_name}' or one of ['value','actual','count']. "
                f"Item keys: {list(item.keys())}"
            )

        facts.append(
            Fact(
                source=source,
                series=series_name,
                geo=geo_default,
                period=int(year),
                value=float(val_float),
                unit=unit,
                denominator=denominator,
                dimensions={},
                notes=meta.get("notes"),
                query_fingerprint=meta.get("query_fingerprint", "unknown"),
            )
        )

    return facts
