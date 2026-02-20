from typing import Any


def write_facts_to_markdown(facts: list[dict[str, Any]], output_file: str, explain: bool = False) -> None:
    """
    Writes a list of facts (as dicts) to a Markdown file.
    """
    md = "# CrimEx Report\n\n"
    md += f"Total facts: {len(facts)}\n\n"

    if explain:
        md += "## Sources\n\n"
        md += "### FBI Crime Data Explorer (CDE)\n\n"
        md += "- **Type:** Law enforcement reported crime (UCR/NIBRS aggregates).\n"
        md += "- **Unit:** Counts and rates per 100k population.\n"
        md += (
            "- **Note:** This reflects crimes reported to law enforcement. "
            "It often undercounts total crime compared to victimization surveys.\n\n"
        )

        md += "### Bureau of Justice Statistics (BJS) NCVS\n\n"
        md += "- **Type:** Survey of households about victimization experiences.\n"
        md += "- **Unit:** Rates per 1,000 persons (age 12+) or households.\n"
        md += "- **Note:** NCVS captures both reported and unreported crimes. Confidence intervals apply.\n\n"

        md += "### Unit Conversion\n"
        md += "- A 'rate_per_100k' is per 100,000 population.\n"
        md += "- A 'rate_per_1000' is per 1,000 persons/households.\n\n"

    if not facts:
        md += "_No facts available._\n"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(md)
        return

    # Determine columns deterministically
    columns = ["source", "series", "geo", "period", "value", "unit", "denominator", "dimensions"]

    md += "## Facts\n\n"
    md += "| " + " | ".join(columns) + " |\n"
    md += "| " + " | ".join(["---"] * len(columns)) + " |\n"

    facts_sorted = sorted(
        facts,
        key=lambda x: (
            x.get("source", ""),
            x.get("series", ""),
            x.get("geo", ""),
            x.get("period", ""),
            x.get("unit", ""),
            str(x.get("dimensions", "")),
        ),
    )

    for fact in facts_sorted:
        row = []
        for col in columns:
            val: Any = fact.get(col, "")
            if col == "dimensions":
                # Convert dimensions to string representation
                dims = val
                val = "" if not dims else ", ".join(f"{k}={v}" for k, v in dims.items())

            # Format value
            if col == "value" and isinstance(val, (int, float)):
                val = f"{val:.2f}"

            row.append(str(val))

        md += "| " + " | ".join(row) + " |\n"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(md)
