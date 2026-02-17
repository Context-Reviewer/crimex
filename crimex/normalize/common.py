"""
Common normalization logic.
"""
import os
import sys
import json
from typing import List, Dict, Any
from crimex.schemas import Fact, QuerySpec
from crimex.io import read_json, write_jsonl, load_text
from crimex.hashing import hash_string

# Registry for normalizers
NORMALIZERS = {}

def register_normalizer(source: str, func):
    NORMALIZERS[source] = func

def normalize_all(raw_dir: str, output_file: str) -> None:
    """
    Scans raw directory, normalizes all data, and writes to output file.
    """
    all_facts: List[Dict[str, Any]] = []
    
    # Import specific normalizers to register them
    from crimex.normalize import fbi_normalize, ncvs_normalize
    
    # Register normalizers manually here to ensure they are available
    NORMALIZERS["fbi_cde"] = fbi_normalize.normalize
    NORMALIZERS["bjs_ncvs"] = ncvs_normalize.normalize
    
    # Sources to check
    sources = ["fbi_cde", "bjs_ncvs"]
    
    for source in sources:
        source_dir = os.path.join(raw_dir, source)
        if not os.path.exists(source_dir):
            continue
            
        print(f"Processing source: {source} in {source_dir} ...")
        
        # List all .meta.json files
        for filename in os.listdir(source_dir):
            if not filename.endswith(".meta.json"):
                continue
                
            meta_path = os.path.join(source_dir, filename)
            try:
                meta = read_json(meta_path)
            except Exception as e:
                print(f"Error reading metadata {meta_path}: {e}", file=sys.stderr)
                continue
                
            # Extract query fingerprint from filename
            sha = filename.replace(".meta.json", "")
            meta["query_fingerprint"] = sha
            
            # Find corresponding raw file
            raw_path = None
            for ext in [".json", ".csv", ".dat"]:
                candidate = os.path.join(source_dir, f"{sha}{ext}")
                if os.path.exists(candidate):
                    raw_path = candidate
                    break
            
            if not raw_path:
                print(f"Warning: Raw file not found for metadata {filename}", file=sys.stderr)
                continue
                
            # Read raw data
            try:
                if raw_path.endswith(".json"):
                    raw_data = read_json(raw_path)
                else:
                    raw_data = load_text(raw_path)
            except Exception as e:
                print(f"Error reading raw file {raw_path}: {e}", file=sys.stderr)
                continue
                
            # Normalize
            normalizer = NORMALIZERS.get(source)
            if not normalizer:
                print(f"Warning: No normalizer for source {source}", file=sys.stderr)
                continue
                
            try:
                facts = normalizer(raw_data, meta)
                
                # Validate facts using Pydantic
                for fact in facts:
                    if isinstance(fact, Fact):
                        # Use mode='json' to serialize datetimes
                        fact_dict = fact.model_dump(mode='json')
                    else:
                        # If normalizer returns dicts, validate them
                        if "query_fingerprint" not in fact:
                            fact["query_fingerprint"] = sha
                        fact_obj = Fact(**fact)
                        fact_dict = fact_obj.model_dump(mode='json')
                    
                    # Ensure derived fields are not None where required
                    # Fact schema says query_fingerprint is required.
                    if not fact_dict.get("query_fingerprint"):
                        fact_dict["query_fingerprint"] = sha
                        
                    all_facts.append(fact_dict)
                    
            except Exception as e:
                print(f"Error normalizing {raw_path}: {e}", file=sys.stderr)
                # Fail loud requirement
                raise e

    # Sort facts deterministically
    all_facts.sort(key=lambda x: (
        x["source"],
        x["series"],
        x["geo"],
        x["period"]
    ))
    
    write_jsonl(all_facts, output_file)
    print(f"Wrote {len(all_facts)} facts to {output_file}")
