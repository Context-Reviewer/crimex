"""
Validation module.
"""

import json
import sys

from crimex.schemas import Fact


def validate_facts(facts_path: str) -> None:
    """
    Validates a JSONL facts file against the Fact schema.
    """
    if not facts_path:
        print("Error: No facts file specified.", file=sys.stderr)
        sys.exit(1)

    print(f"Validating facts in {facts_path} ...")

    try:
        with open(facts_path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    Fact(**data)  # Validate
                except json.JSONDecodeError as e:
                    print(f"Validation Error at line {i}: Invalid JSON - {e}", file=sys.stderr)
                    sys.exit(1)
                except Exception as e:
                    print(f"Validation Error at line {i}: {e}", file=sys.stderr)
                    sys.exit(1)

        print(f"Success: {facts_path} is valid.")

    except FileNotFoundError:
        print(f"Error: File not found: {facts_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
