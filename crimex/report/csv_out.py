import csv
from typing import Any


def write_facts_to_csv(facts: list[dict[str, Any]], output_file: str) -> None:
    """
    Writes facts to a CSV file with deterministic column ordering.
    """
    if not facts:
        # Write just headers if no facts
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["source", "series", "geo", "period", "value", "unit", "denominator", "dimensions"])
        return

    # Determine all possible dimension keys
    dim_keys: set[str] = set()
    for fact in facts:
        dims = fact.get("dimensions") or {}
        if isinstance(dims, dict):
            dim_keys.update(dims.keys())

    sorted_dim_keys = sorted(dim_keys)

    # Define CSV headers
    headers = ["source", "series", "geo", "period", "value", "unit", "denominator"] + [
        f"dim_{k}" for k in sorted_dim_keys
    ]

    # Deterministic sort of rows
    facts_sorted = sorted(
        facts,
        key=lambda f: (
            f.get("source", ""),
            f.get("series", ""),
            f.get("geo", ""),
            f.get("period", ""),
            f.get("unit", ""),
        ),
    )

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for fact in facts_sorted:
            row: dict[str, Any] = {
                k: fact.get(k) for k in ["source", "series", "geo", "period", "value", "unit", "denominator"]
            }
            dims = fact.get("dimensions") or {}
            if isinstance(dims, dict):
                for k in sorted_dim_keys:
                    row[f"dim_{k}"] = dims.get(k)
            writer.writerow(row)
