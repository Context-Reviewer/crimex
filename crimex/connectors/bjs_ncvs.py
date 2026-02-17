"""
BJS NCVS connector.
Fetches data from BJS NCVS (SODA API or direct download).
"""
import requests
import sys
import os
import json
from typing import Any, Dict, Optional
from crimex.hashing import compute_cache_key, hash_string
from crimex.io import read_json, write_json, write_text, load_text

SODA_BASE_URL = "https://data.ojp.usdoj.gov/resource"

def fetch_ncvs_data(spec: Dict[str, Any], output_dir: str, force: bool = False) -> Any:
    """
    Fetches data from BJS NCVS using SODA API or direct download.
    Saves both raw response and metadata.
    """
    dataset_id = spec.get("dataset_id")
    download_url = spec.get("download_url")
    params = spec.get("params", {})
    
    if not dataset_id and not download_url:
        print("Error: NCVS query spec must provide either 'dataset_id' or 'download_url'.", file=sys.stderr)
        sys.exit(1)
        
    headers = {"Accept": "application/json"}
    
    # Determine source and cache key
    if dataset_id:
        endpoint = f"{dataset_id}.json"
        url = f"{SODA_BASE_URL}/{endpoint}"
        # SODA params
        request_params = params
        # Include headers in cache key
        cache_key = compute_cache_key(endpoint, request_params, headers)
        is_json = True
    else:
        # Direct download
        url = download_url
        request_params = {}
        # Direct download cache key includes URL + headers (if any relevant)
        cache_key = hash_string(url) # Headers for direct download might be minimal
        is_json = url.lower().endswith(".json")

    # Define cache paths
    cache_dir = os.path.join(output_dir, "raw", "bjs_ncvs")
    ext = "json" if is_json else "csv" 
    if not is_json:
        if url.lower().endswith(".csv"):
            ext = "csv"
        else:
            ext = "dat"
            
    cache_file = os.path.join(cache_dir, f"{cache_key}.{ext}")
    meta_file = os.path.join(cache_dir, f"{cache_key}.meta.json")
    
    if os.path.exists(cache_file) and not force:
        print(f"Using cached response: {cache_file}")
        if not os.path.exists(meta_file):
            write_json(spec, meta_file)
        if is_json or ext == "json":
            return read_json(cache_file)
        else:
            return load_text(cache_file)
    
    print(f"Fetching from {url} ...")
    try:
        response = requests.get(url, params=request_params, headers=headers, timeout=60)
        
        if response.status_code != 200:
            print(f"Error fetching data from {url}", file=sys.stderr)
            print(f"Status Code: {response.status_code}", file=sys.stderr)
            print(f"Response Body (first 500 chars): {response.text[:500]}", file=sys.stderr)
            sys.exit(1)
            
        # Save to cache
        if is_json:
            data = response.json()
            write_json(data, cache_file)
            write_json(spec, meta_file)
            print(f"Saved response to {cache_file}")
            return data
        else:
            # Text/CSV content
            content = response.text
            write_text(content, cache_file)
            write_json(spec, meta_file)
            print(f"Saved response to {cache_file}")
            return content
            
    except requests.RequestException as e:
        print(f"Network error fetching {url}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
