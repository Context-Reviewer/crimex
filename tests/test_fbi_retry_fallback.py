import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from crimex.connectors.fbi_cde import fetch_fbi_data


def test_fbi_retries_then_succeeds(tmp_path: Path) -> None:
    spec = {"endpoint": "test-endpoint", "params": {}, "series_name": "x", "source": "fbi_cde"}
    out_dir = tmp_path / "out"

    r1 = MagicMock(status_code=503, text="upstream fail")
    r2 = MagicMock(status_code=503, text="upstream fail")
    r3 = MagicMock(status_code=200)
    r3.json.return_value = {"results": []}

    with patch.dict(os.environ, {"FBI_API_KEY": "dummy"}):
        with patch("requests.get", side_effect=[r1, r2, r3]):
            data = fetch_fbi_data(spec, str(out_dir), force=True)

    assert data == {"results": []}
    assert (out_dir / "raw" / "fbi_cde").exists()


def test_fbi_falls_back_to_cache_on_persistent_503(tmp_path: Path) -> None:
    spec = {"endpoint": "test-endpoint", "params": {}, "series_name": "x", "source": "fbi_cde"}
    out_dir = tmp_path / "out"

    ok = MagicMock(status_code=200)
    ok.json.return_value = {"results": [{"data_year": 2020, "value": 1}]}

    with patch.dict(os.environ, {"FBI_API_KEY": "dummy"}):
        with patch("requests.get", return_value=ok):
            _ = fetch_fbi_data(spec, str(out_dir), force=True)

    rfail = MagicMock(status_code=503, text="upstream fail")

    with patch.dict(os.environ, {"FBI_API_KEY": "dummy"}):
        with patch("requests.get", return_value=rfail):
            data2 = fetch_fbi_data(spec, str(out_dir), force=False)

    assert "results" in data2
