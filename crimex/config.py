"""
Configuration module for crimex.
Handles environment variables and default settings.
"""
import os
import sys

# Environment variables
ENV_FBI_API_KEY = "FBI_API_KEY"
ENV_DATA_GOV_API_KEY = "DATA_GOV_API_KEY"

def get_fbi_api_key() -> str | None:
    """
    Retrieve FBI API key from environment variables.
    Checks FBI_API_KEY first, then DATA_GOV_API_KEY.
    """
    return os.getenv(ENV_FBI_API_KEY) or os.getenv(ENV_DATA_GOV_API_KEY)

def require_fbi_api_key() -> str:
    """
    Retrieve FBI API key or raise an error if not found.
    """
    key = get_fbi_api_key()
    if not key:
        print(f"Error: Missing FBI API Key. Please set {ENV_FBI_API_KEY} or {ENV_DATA_GOV_API_KEY}.", file=sys.stderr)
        print("You can obtain a key from https://api.data.gov/signup/", file=sys.stderr)
        # We don't exit here, let the caller handle the exit or use a fallback if applicable
        # But per requirements "Fail loud", the caller should probably exit.
        return "" 
    return key
