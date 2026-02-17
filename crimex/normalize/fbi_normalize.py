"""
FBI CDE normalization logic.
"""
from typing import Any, Dict, List
from crimex.schemas import Fact

def normalize(raw_data: Dict[str, Any], meta: Dict[str, Any]) -> List[Fact]:
    """
    Normalizes FBI CDE raw data into Facts.
    """
    facts = []
    
    # Extract metadata
    source = meta.get("source", "fbi_cde")
    series_name = meta.get("series_name")
    if not series_name:
        raise ValueError("Metadata missing 'series_name'")
    
    # Default unit handling for FBI
    # Usually "count" unless specified "rate_per_100000"
    unit = meta.get("expected_unit", "count")
    denominator = meta.get("expected_denominator")
    
    # Heuristic for FBI: if unit is rate_per_100k, denom is 100000
    if unit == "rate_per_100000" and not denominator:
        denominator = 100000.0
    
    # FBI API responses vary. We handle common patterns.
    # Pattern 1: {"results": [...], ...} or {"data": [...]}
    # The list items usually have "data_year" and value keys.
    
    results = raw_data.get("results") or raw_data.get("data")
    if results is None:
        # Some endpoints return list directly?
        if isinstance(raw_data, list):
            results = raw_data
        else:
            # Maybe just a dict with keys as years? unlikely for CDE API.
            # But let's log and skip or error.
            # Fail loud requirement says error.
            raise ValueError(f"Unknown FBI CDE response format. Keys: {list(raw_data.keys())}")
            
    # Iterate over results
    for item in results:
        # Check year
        year = item.get("data_year") or item.get("year")
        if year is None:
            continue
            
        # Value extraction
        value = None
        
        if "value" in item:
            value = item["value"]
        elif series_name in item:
            value = item[series_name]
        else:
            # Heuristic: try to find the numeric field that is not year
            # Or use the 'count' key if present
            if "count" in item:
                value = item["count"]
            elif "actual" in item:
                value = item["actual"]
            else:
                 pass
        
        if value is None:
             raise ValueError(f"Cannot determine value for item: {item}. Expected key '{series_name}' or 'value' or 'count'.")

        # Convert value to float
        try:
            val_float = float(value)
        except (ValueError, TypeError):
             continue

        # Fact construction
        fact = Fact(
            source=source,
            series=series_name,
            geo="US", # Default to US for now, but should be derived from params/metadata
            period=int(year),
            value=val_float,
            unit=unit,
            denominator=denominator,
            dimensions={}, # Extract dims if present
            notes=meta.get("notes"),
            query_fingerprint=meta.get("query_fingerprint", "unknown") # Should compute hash of meta if not present?
        )
        
        # Override geo from params
        params = meta.get("params", {})
        state = params.get("stateAbbr") or params.get("state")
        if state:
            fact.geo = state
            
        facts.append(fact)
        
    return facts
