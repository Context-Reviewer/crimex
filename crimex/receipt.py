# FILE: crimex/receipt.py
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


_SECRET_KEYS = {"api_key", "apikey", "token", "authorization"}


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class Receipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    endpoint: str
    request_url: str
    request_params_redacted: dict[str, Any] = Field(default_factory=dict)
    http_status: int
    retry_attempts: int
    fallback_used: bool
    fetched_at: str
    response_sha256: str
    artifact_path: str

    @field_validator("fetched_at")
    @classmethod
    def validate_fetched_at(cls, v: str) -> str:
        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", v):
            raise ValueError("fetched_at must be UTC ISO format without microseconds, ending with Z")
        return v

    @field_validator("request_params_redacted")
    @classmethod
    def validate_no_secrets(cls, v: dict[str, Any]) -> dict[str, Any]:
        for key in v.keys():
            if key.lower() in _SECRET_KEYS:
                raise ValueError(f"request_params_redacted must not contain secret key: {key}")
        return v

    @field_validator("response_sha256")
    @classmethod
    def validate_sha(cls, v: str) -> str:
        if not re.match(r"^[0-9a-f]{64}$", v):
            raise ValueError("response_sha256 must be 64 lowercase hex characters")
        return v


def _sorted_params_without_secrets(params: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in params.items():
        if key.lower() in _SECRET_KEYS:
            continue
        safe[key] = value
    return dict(sorted(safe.items(), key=lambda item: item[0]))


def build_receipt(
    *,
    source: str,
    endpoint: str,
    request_url: str,
    request_params: dict[str, Any],
    http_status: int,
    retry_attempts: int,
    fallback_used: bool,
    response_sha256: str,
    artifact_path: str,
    fetched_at: str | None = None,
) -> Receipt:
    return Receipt(
        source=source,
        endpoint=endpoint,
        request_url=request_url,
        request_params_redacted=_sorted_params_without_secrets(request_params),
        http_status=http_status,
        retry_attempts=retry_attempts,
        fallback_used=fallback_used,
        fetched_at=fetched_at or utc_now_iso(),
        response_sha256=response_sha256,
        artifact_path=artifact_path,
    )


def write_receipt(*, receipt: Receipt, run_root: Path) -> Path:
    relative_path = Path("raw") / receipt.source / f"{receipt.response_sha256}.receipt.json"
    out_path = run_root / relative_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = receipt.model_dump()
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(out_path)

    return out_path
