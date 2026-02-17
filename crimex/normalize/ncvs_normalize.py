"""
NCVS Normalization logic.
"""
from typing import Any, Dict, List
from crimex.schemas import Fact
import json

def normalize(raw_data: Any, meta: Dict[str, Any]) -> List[Fact]:
    """
    Normalizes NCVS raw data into Facts.
    """
    facts = []
    source = meta.get("source", "bjs_ncvs")
    series_name = meta.get("series_name")
    
    if not series_name:
        raise ValueError("Metadata missing 'series_name'")
        
    query_fingerprint = meta.get("query_fingerprint")
    
    # Unit/Denominator handling
    unit = meta.get("expected_unit")
    denominator = meta.get("expected_denominator")
    
    # If unit missing, try to infer from keys
    
    # Check if raw_data is list (SODA usually returns list of dicts)
    if isinstance(raw_data, list):
        records = raw_data
    elif isinstance(raw_data, dict):
        records = raw_data.get("data") or raw_data.get("results")
        if not records:
             raise ValueError("Unknown NCVS response format. Expected list or dict with 'data'/'results'.")
    elif isinstance(raw_data, str):
        try:
            records = json.loads(raw_data)
            if not isinstance(records, list):
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

    # Iterate over records
    for item in records:
        year = item.get("year") or item.get("data_year") or item.get("period")
        if not year:
            continue
            
        try:
            year_int = int(year)
        except (ValueError, TypeError):
            continue
            
        value = None
        if "value" in item:
            value = item["value"]
        elif series_name in item:
            value = item[series_name]
        else:
            candidates = ["rate", "count", "weighted_victimizations", "victimization_rate"]
            for key in candidates:
                if key in item:
                    value = item[key]
                    if not unit:
                        if key == "rate" or key == "victimization_rate":
                            unit = "rate_per_1000" # NCVS standard
                            if not denominator:
                                denominator = 1000.0
                        elif key == "count" or key == "weighted_victimizations":
                            unit = "count"
                    break
        
        if value is None:
             continue 
             
        try:
            val_float = float(value)
        except (ValueError, TypeError):
            continue
            
        dims = {}
        exclude = {"year", "data_year", "period", "value", series_name, "count", "rate", "weighted_victimizations", "victimization_rate"}
        
        for k, v in item.items():
            if k not in exclude and v is not None:
                dims[k] = v
                
        geo = dims.get("geo") or dims.get("state") or "US"
        
        if not unit:
            unit = "unknown"
            
        fact = Fact(
            source=source,
            series=series_name,
            geo=geo,
            period=year_int,
            value=val_float,
            unit=unit,
            denominator=denominator,
            dimensions=dims,
            notes=meta.get("notes"),
            query_fingerprint=query_fingerprint
        )
        facts.append(fact)
        
    return facts
