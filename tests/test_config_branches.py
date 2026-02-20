from __future__ import annotations

import pytest

from crimex.config import require_fbi_api_key


def test_require_fbi_api_key_prefers_fbi_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FBI_API_KEY", "A")
    monkeypatch.setenv("DATA_GOV_API_KEY", "B")
    assert require_fbi_api_key() == "A"


def test_require_fbi_api_key_falls_back_to_data_gov(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FBI_API_KEY", raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "B")
    assert require_fbi_api_key() == "B"


def test_require_fbi_api_key_returns_empty_and_prints_when_missing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("FBI_API_KEY", raising=False)
    monkeypatch.delenv("DATA_GOV_API_KEY", raising=False)

    key = require_fbi_api_key()
    assert not key  # None or ""

    captured = capsys.readouterr()
    assert "Missing FBI API Key" in captured.err

