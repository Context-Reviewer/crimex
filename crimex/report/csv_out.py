"""
CSV Reporting module.
"""
import csv
import json
import os
from typing import List, Dict, Any, Set
from crimex.schemas import Fact

def write_facts_to_csv(facts: List[Dict[str, Any]], output_file: str) -> None:
    """
    Writes a list of facts (as dicts) to a CSV file.
    Flattens dimensions into columns.
    """
    if not facts:
        print("No facts to report.")
        return

    # Identify all dimension keys
    dim_keys: Set[str] = set()
    for fact in facts:
        dims = fact.get("dimensions", {})
        if dims:
            dim_keys.update(dims.keys())
            
    sorted_dim_keys = sorted(list(dim_keys))
    
    # Define CSV headers
    # Standard fields first
    headers = [
        "source", "series", "geo", "period", "value", "unit", "denominator",
        "ci_lower", "ci_upper", "se", "notes", "query_fingerprint", "retrieved_at"
    ]
    
    # Add dimension columns
    headers.extend([f"dim_{k}" for k in sorted_dim_keys])
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for fact in facts:
            # Flatten fact for CSV
            row = {k: fact.get(k) for k in headers if not k.startswith("dim_")}
            
            # Add dimensions
            dims = fact.get("dimensions", {})
            for k in sorted_dim_keys:
                row[f"dim_{k}"] = dims.get(k, "")
                
            writer.writerow(row)
            
    print(f"Wrote CSV report to {output_file}")
