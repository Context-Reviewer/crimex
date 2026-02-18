# FILE: tests/test_receipts.py
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

import crimex.receipt as receipt_mod


class _FrozenDatetime:
    @classmethod
    def utcnow(cls):
        from datetime import datetime
        return datetime(2026, 2, 17, 15, 4, 12)


def test_receipt_build_is_deterministic_and_secret_free(monkeypatch):
    monkeypatch.setattr(receipt_mod, "datetime", _FrozenDatetime)

    r1 = receipt_mod.build_receipt(
        source="fbi_cde",
        endpoint="/api/test",
        request_url="https://example.test/api/test",
        request_params={"state": "NC", "api_key": "SECRET", "year": "2023"},
        http_status=200,
        retry_attempts=2,
        fallback_used=False,
        response_sha256="a" * 64,
        artifact_path="raw/fbi_cde/" + ("a" * 64) + ".json",
    )

    r2 = receipt_mod.build_receipt(
        source="fbi_cde",
        endpoint="/api/test",
        request_url="https://example.test/api/test",
        request_params={"year": "2023", "state": "NC", "api_key": "DIFFERENT"},
        http_status=200,
        retry_attempts=2,
        fallback_used=False,
        response_sha256="a" * 64,
        artifact_path="raw/fbi_cde/" + ("a" * 64) + ".json",
    )

    assert r1.fetched_at == "2026-02-17T15:04:12Z"
    assert r2.fetched_at == "2026-02-17T15:04:12Z"
    assert "api_key" not in r1.request_params_redacted
    assert list(r1.request_params_redacted.keys()) == sorted(r1.request_params_redacted.keys())
    assert r1.model_dump() == r2.model_dump()

    j1 = json.dumps(r1.model_dump(), sort_keys=True, separators=(",", ":"))
    j2 = json.dumps(r2.model_dump(), sort_keys=True, separators=(",", ":"))
    assert j1 == j2


def test_receipt_rejects_secret_key_in_redacted_params():
    with pytest.raises(ValidationError):
        receipt_mod.Receipt(
            source="fbi_cde",
            endpoint="/api/test",
            request_url="https://example.test/api/test",
            request_params_redacted={"api_key": "BAD"},
            http_status=200,
            retry_attempts=0,
            fallback_used=False,
            fetched_at="2026-02-17T15:04:12Z",
            response_sha256="b" * 64,
            artifact_path="raw/fbi_cde/" + ("b" * 64) + ".json",
        )


def test_write_receipt_creates_source_scoped_sidecar(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(receipt_mod, "datetime", _FrozenDatetime)

    receipt = receipt_mod.build_receipt(
        source="ncvs",
        endpoint="/v1/test",
        request_url="https://api.ojp.gov/bjsdataset/v1/test",
        request_params={"dataset_id": "123", "token": "SECRET"},
        http_status=200,
        retry_attempts=0,
        fallback_used=False,
        response_sha256="c" * 64,
        artifact_path="raw/ncvs/" + ("c" * 64) + ".json",
    )

    out_path = receipt_mod.write_receipt(receipt=receipt, run_root=tmp_path)

    expected = tmp_path / "raw" / "ncvs" / (("c" * 64) + ".receipt.json")

    assert out_path == expected
    assert out_path.exists()

    contents = out_path.read_text(encoding="utf-8")
    assert "token" not in contents
    assert "api_key" not in contents
