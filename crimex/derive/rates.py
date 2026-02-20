"""
Derivations module: Rates and conversions.
"""

from crimex.schemas import Fact


def convert_rate(value: float, from_unit: str, to_unit: str) -> float:
    """
    Converts a rate value from one unit to another.
    """
    if from_unit == to_unit:
        return value

    # Example: per_1000 -> per_100000
    if (from_unit == "rate_per_1000" or from_unit == "per_1000") and (
        to_unit == "rate_per_100000" or to_unit == "per_100000"
    ):
        return value * 100.0

    # Example: per_100000 -> per_1000
    if (from_unit == "rate_per_100000" or from_unit == "per_100000") and (
        to_unit == "rate_per_1000" or to_unit == "per_1000"
    ):
        return value / 100.0

    raise ValueError(f"Unsupported conversion: {from_unit} -> {to_unit}")


def per_1000_to_per_100000(facts: list[Fact]) -> list[Fact]:
    """
    Converts facts with unit 'rate_per_1000' (or 'per_1000') to 'rate_per_100000'.
    Returns a NEW list of facts (does not mutate in place).
    """
    new_facts = []
    for f in facts:
        # Check unit variations
        if f.unit in ("rate_per_1000", "per_1000"):
            new_val = convert_rate(f.value, f.unit, "rate_per_100000")

            # Pydantic v2 uses model_copy
            if hasattr(f, "model_copy"):
                new_fact = f.model_copy(
                    update={
                        "value": new_val,
                        "unit": "rate_per_100000",
                        "notes": (f.notes or "") + " [Converted from rate_per_1000]",
                    }
                )
            else:
                # Pydantic v1 fallback
                new_fact = f.copy(
                    update={
                        "value": new_val,
                        "unit": "rate_per_100000",
                        "notes": (f.notes or "") + " [Converted from rate_per_1000]",
                    }
                )
            new_facts.append(new_fact)
        else:
            new_facts.append(f)
    return new_facts


# Stubs for other derivations
def compute_rate_ratio(facts: list[Fact], group_a: str, group_b: str) -> list[Fact]:
    """
    Computes rate ratio between two groups.
    (Stub implementation)
    """
    return []


def compute_rolling_average(facts: list[Fact], window: int = 3) -> list[Fact]:
    """
    Computes rolling average for facts.
    (Stub implementation)
    """
    return []
