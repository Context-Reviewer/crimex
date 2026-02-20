import json
from pathlib import Path
from typing import Any


class QAError(Exception):
    pass


REQUIRED_FIELDS = {
    "source",
    "series",
    "geo",
    "period",
    "unit",
    "value",
    "query_fingerprint",
}


def _normalize_dimensions(dimensions: Any) -> tuple[tuple[str, Any], ...]:
    if dimensions is None:
        return tuple()
    if isinstance(dimensions, dict):
        return tuple(sorted((str(k), dimensions[k]) for k in dimensions))
    return tuple()


def validate_run_facts(run_dir: Path) -> list[str]:
    run_dir = Path(run_dir).resolve()
    facts_path = run_dir / "facts" / "facts.jsonl"

    errors: list[str] = []

    if not facts_path.exists():
        errors.append("MISSING_FACTS_FILE: facts/facts.jsonl not found")
        errors.sort()
        return errors

    seen_keys = set()
    units_by_series: dict[tuple[str, str], set] = {}
    valid_rows = 0

    try:
        lines = facts_path.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        errors.append(f"READ_ERROR: {type(e).__name__}: {e}")
        errors.sort()
        return errors

    for idx, line in enumerate(lines, start=1):
        if not line.strip():
            continue

        try:
            obj = json.loads(line)
        except Exception:
            errors.append(f"MALFORMED_JSON: line={idx}")
            continue

        if not isinstance(obj, dict):
            errors.append(f"MALFORMED_JSON: line={idx}")
            continue

        valid_rows += 1

        # Required field validation
        for field in REQUIRED_FIELDS:
            if field not in obj:
                errors.append(f"MISSING_FIELD: field={field} at line={idx}")

        source = obj.get("source")
        series = obj.get("series")
        geo = obj.get("geo")
        period = obj.get("period")
        unit = obj.get("unit")
        value = obj.get("value")
        denominator = obj.get("denominator")
        dimensions = obj.get("dimensions")

        # Duplicate detection
        key = (
            source,
            series,
            geo,
            period,
            _normalize_dimensions(dimensions),
        )
        if key in seen_keys:
            errors.append(f"DUPLICATE_FACT: source={source} series={series} geo={geo} period={period}")
        else:
            seen_keys.add(key)

        # Negative value check
        if isinstance(unit, str) and (unit == "count" or unit.startswith("rate")):
            try:
                if value is None or float(value) < 0:
                    errors.append(
                        f"NEGATIVE_VALUE: source={source} series={series} geo={geo} period={period} value={value}"
                    )
            except Exception:
                errors.append(
                    f"NEGATIVE_VALUE: source={source} series={series} geo={geo} period={period} value={value}"
                )

        # Denominator consistency
        if isinstance(unit, str) and unit.startswith("rate"):
            if denominator is None:
                errors.append(f"MISSING_DENOMINATOR: source={source} series={series} geo={geo} period={period}")

        # Unit consistency tracking
        if source is not None and series is not None and unit is not None:
            group = (source, series)
            if group not in units_by_series:
                units_by_series[group] = set()
            units_by_series[group].add(unit)

    # Mixed unit detection
    for (source, series), unit_set in units_by_series.items():
        if len(unit_set) > 1:
            errors.append(f"MIXED_UNITS: source={source} series={series}")

    if valid_rows == 0:
        errors.append("EMPTY_FACTS: no fact rows found")

    errors.sort()
    return errors
