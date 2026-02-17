"""
Manifest generation module.
"""
import os
import sys
import hashlib
from datetime import datetime
from typing import List
from crimex.schemas import RunManifest, ManifestEntry
from crimex.hashing import hash_file
from crimex.io import write_json

def generate_manifest(root_dir: str, output_file: str, command_str: str = "unknown") -> None:
    """
    Generates a manifest of all files in root_dir.
    """
    artifacts: List[ManifestEntry] = []
    
    # Walk the directory
    for root, dirs, files in os.walk(root_dir):
        # Sort files to ensure deterministic order of traversal (at least within directory)
        # os.walk yields random order. We should sort `files` and `dirs` in place?
        # Actually, we can just sort artifacts list at the end.
        
        for file in files:
            filepath = os.path.join(root, file)
            rel_path = os.path.relpath(filepath, root_dir)
            
            # Compute hash
            try:
                sha256 = hash_file(filepath)
                size = os.path.getsize(filepath)
                created_at = datetime.utcfromtimestamp(os.path.getctime(filepath))
                
                entry = ManifestEntry(
                    filepath=rel_path,
                    sha256=sha256,
                    size_bytes=size,
                    created_at=created_at
                )
                artifacts.append(entry)
            except Exception as e:
                print(f"Error processing {filepath}: {e}", file=sys.stderr)
                continue
                
    # Deterministic sort of artifacts by filepath
    artifacts.sort(key=lambda x: x.filepath)
    
    # Create manifest
    run_id = f"{datetime.utcnow().isoformat()}-{hashlib.sha256(command_str.encode()).hexdigest()[:8]}"
    
    manifest = RunManifest(
        run_id=run_id,
        timestamp=datetime.utcnow(),
        command=command_str,
        artifacts=artifacts
    )
    
    write_json(manifest.dict(), output_file)
    print(f"Wrote manifest with {len(artifacts)} artifacts to {output_file}")
