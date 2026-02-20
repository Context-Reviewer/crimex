from __future__ import annotations

import json
import socket
import threading
import time
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from crimex.ui.server import UiConfig, UiHandler


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310
        assert resp.status == 200
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def _make_minimal_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "raw" / "fbi_cde").mkdir(parents=True)
    (run_dir / "facts").mkdir()
    (run_dir / "reports").mkdir()
    (run_dir / "logs").mkdir()

    # Minimal files expected by UI
    (run_dir / "logs" / "run.log").write_text("smoke run", encoding="utf-8")
    (run_dir / "facts" / "facts.jsonl").write_text(
        '{"fact_type":"demo","value":1}\n{"fact_type":"demo","value":2}\n',
        encoding="utf-8",
    )
    (run_dir / "reports" / "report.md").write_text("# Report\n\nOK\n", encoding="utf-8")
    (run_dir / "reports" / "report.csv").write_text("k,v\nx,1\n", encoding="utf-8")

    manifest = {
        "app": {"name": "crimex", "version": "test"},
        "manifest_version": "1.0",
        "inputs": {"paths": [], "files": [], "inputs_sha256": "0" * 64},
        "artifacts": [],
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")

    # Fake bundle present
    (run_dir / "run_bundle.zip").write_bytes(b"PK\x03\x04FAKEZIP")
    return run_dir


@pytest.mark.timeout(10)
def test_ui_phase0_health_and_summary(tmp_path: Path) -> None:
    run_dir = _make_minimal_run_dir(tmp_path)
    port = _pick_free_port()

    cfg = UiConfig(run_dir=run_dir, host="127.0.0.1", port=port, max_file_bytes=100_000)
    httpd = ThreadingHTTPServer((cfg.host, cfg.port), UiHandler)
    httpd._cfg = cfg
    httpd._verbose = False

    th = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    th.start()

    try:
        # Wait briefly for server
        time.sleep(0.2)

        health = _http_get_json(f"http://127.0.0.1:{port}/health")
        assert health == {"ok": True}

        summary = _http_get_json(f"http://127.0.0.1:{port}/api/run/summary")
        summary_run_dir = summary["run_dir"]
        assert summary_run_dir.endswith(str(run_dir).replace("\\", "/")) or summary_run_dir.endswith(str(run_dir))
        assert summary["manifest"]["exists"] is True
        assert summary["facts"]["exists"] is True
        assert summary["facts"]["records"] == 2
        assert summary["bundle_exists"] is True

        tree = _http_get_json(f"http://127.0.0.1:{port}/api/tree")
        assert tree["type"] == "dir"

        # file read (text)
        file_path = urllib.parse.quote("reports/report.md")
        file_url = f"http://127.0.0.1:{port}/api/file?path={file_path}"
        file_data = _http_get_json(file_url)
        assert "Report" in file_data["content"]

    finally:
        httpd.shutdown()
        httpd.server_close()
