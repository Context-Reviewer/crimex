"""
Input/Output utilities for crimex.
Handles file system operations, file reading, and writing.
"""
import json
import os
from typing import Any, Dict, List

def write_json(data: Dict[str, Any] | List[Any], filepath: str, indent: int = 4) -> None:
    """Writes a JSON object to a file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)

def read_json(filepath: str) -> Dict[str, Any] | List[Any]:
    """Reads a JSON file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def write_jsonl(data: List[Dict[str, Any]], filepath: str) -> None:
    """Writes a list of JSON objects to a JSONL file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")

def read_jsonl(filepath: str) -> List[Dict[str, Any]]:
    """Reads a JSONL file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    items = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
    return items

def ensure_directory(path: str) -> None:
    """Ensures a directory exists."""
    os.makedirs(path, exist_ok=True)

def load_text(filepath: str) -> str:
    """Reads a text file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()

def write_text(content: str, filepath: str) -> None:
    """Writes content to a text file."""
    ensure_directory(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
