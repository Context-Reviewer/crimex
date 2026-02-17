"""
Markdown Reporting module.
"""
from typing import List, Dict, Any
from crimex.schemas import Fact

def write_facts_to_markdown(facts: List[Dict[str, Any]], output_file: str, explain: bool = False) -> None:
    """
    Writes a list of facts (as dicts) to a Markdown file.
    Creates a simple table.
    """
    if not facts:
        print("No facts to report.")
        return
        
    md = "# Crime Data Report\n\n"
    
    if explain:
        md += "## Data Sources & Methodology\n\n"
        md += "### FBI UCR (Uniform Crime Reporting)\n"
        md += "- **Source:** FBI Crime Data Explorer (CDE) API.\n"
        md += "- **Type:** Police-reported crime data (offenses and arrests).\n"
        md += "- **Unit:** Counts or rates per 100,000 population.\n"
        md += "- **Note:** UCR data only includes crimes reported to law enforcement. It often undercounts total crime compared to victimization surveys.\n\n"
        
        md += "### BJS NCVS (National Crime Victimization Survey)\n"
        md += "- **Source:** Bureau of Justice Statistics (BJS).\n"
        md += "- **Type:** Survey of households about victimization experiences.\n"
        md += "- **Unit:** Rates per 1,000 persons (age 12+) or households.\n"
        md += "- **Note:** NCVS captures both reported and unreported crimes. Confidence intervals apply.\n\n"
        
        md += "### Unit Conversion\n"
        md += "- FBI data is typically **per 100,000** population.\n"
        md += "- NCVS data is typically **per 1,000** persons.\n"
        md += "- **Be careful when comparing!**\n\n"

    md += "## Facts Table\n\n"

    # Determine columns
    # We select key columns for readability
    columns = ["source", "series", "geo", "period", "value", "unit"]
    
    # Check if dimensions exist
    has_dims = any(f.get("dimensions") for f in facts)
    if has_dims:
        columns.append("dimensions")
        
    # Header
    md += "| " + " | ".join(columns) + " |\n"
    md += "| " + " | ".join(["---"] * len(columns)) + " |\n"
    
    for fact in facts:
        row = []
        for col in columns:
            val = fact.get(col, "")
            if col == "dimensions":
                # Convert dimensions to string representation
                dims = val
                if not dims:
                    val = ""
                else:
                    # Format as key=value
                    val = ", ".join([f"{k}={v}" for k, v in dims.items()])
            
            # Format value
            if col == "value" and isinstance(val, (int, float)):
                val = f"{val:.2f}"
                
            row.append(str(val))
            
        md += "| " + " | ".join(row) + " |\n"
        
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(md)
        
    print(f"Wrote Markdown report to {output_file}")
