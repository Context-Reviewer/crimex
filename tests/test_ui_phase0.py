from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

import crimex.cli as cli
import crimex.ui.server as ui_server
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


def _http_get_json_status(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310
            data = resp.read()
            return resp.status, json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as e:
        data = e.read()
        try:
            return int(e.code), json.loads(data.decode("utf-8"))
        except Exception:
            return int(e.code), {}


def _wait_for_health(port: int, timeout_s: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_s
    url = f"http://127.0.0.1:{port}/health"
    while time.monotonic() < deadline:
        try:
            _ = _http_get_json(url)
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not become healthy in time")


def _start_server(
    run_dir: Path,
    *,
    base_dir: Path | None = None,
    max_file_bytes: int = 100_000,
    cmd_timeout_s: float = 0.5,
) -> tuple[ThreadingHTTPServer, int]:
    port = _pick_free_port()
    cfg = UiConfig(
        run_dir=run_dir,
        base_dir=(base_dir or run_dir.parent),
        host="127.0.0.1",
        port=port,
        max_file_bytes=max_file_bytes,
        cmd_timeout_s=cmd_timeout_s,
    )
    httpd = ThreadingHTTPServer((cfg.host, cfg.port), UiHandler)
    httpd._cfg = cfg
    httpd._verbose = False

    th = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    th.start()
    _wait_for_health(port)
    return httpd, port


def _make_minimal_run_dir(base: Path, name: str) -> Path:
    run_dir = base / name
    (run_dir / "raw" / "fbi_cde").mkdir(parents=True)
    (run_dir / "facts").mkdir()
    (run_dir / "reports").mkdir()
    (run_dir / "logs").mkdir()

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

    (run_dir / "run_bundle.zip").write_bytes(b"PK\x03\x04FAKEZIP")
    return run_dir


def _list_files(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)).replace("\\", "/") for p in root.rglob("*") if p.is_file())


def _fake_completed_process(rc: int, out: str = "", err: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["x"], returncode=rc, stdout=out, stderr=err)


@pytest.mark.timeout(10)
def test_ui_phase1b_helper_branches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bad = tmp_path / "bad.txt"
    bad.write_bytes(b"\xff\xfe\xfa")
    txt = ui_server._read_text(bad, max_bytes=10)
    assert txt == bad.read_bytes().decode("latin-1", errors="replace")

    assert ui_server._safe_relpath(".") == ""
    assert ui_server._summarize_text("x" * 300).endswith("...")

    def _raise_timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="x", timeout=0.01)

    monkeypatch.setattr(ui_server.subprocess, "run", _raise_timeout)
    res = ui_server._run_cli(["x"], timeout_s=0.01)
    assert res["stderr_1"] == "TIMEOUT"

    def _raise_oserror(*_args, **_kwargs):
        raise OSError("boom")

    monkeypatch.setattr(ui_server.subprocess, "run", _raise_oserror)
    res2 = ui_server._run_cli(["x"], timeout_s=0.01)
    assert res2["stderr_1"].startswith("OSError:")


@pytest.mark.timeout(10)
def test_ui_phase0_health_and_summary(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")
    httpd, port = _start_server(run_dir, base_dir=base)

    try:
        health = _http_get_json(f"http://127.0.0.1:{port}/health")
        assert health == {"ok": True}

        summary = _http_get_json(f"http://127.0.0.1:{port}/api/run/summary")
        assert "runA" in summary["run_dir"]
        assert summary["manifest"]["exists"] is True
        assert summary["facts"]["exists"] is True
        assert summary["facts"]["records"] == 2
        assert summary["bundle_exists"] is True

        tree = _http_get_json(f"http://127.0.0.1:{port}/api/tree")
        assert tree["type"] == "dir"

        file_path = urllib.parse.quote("reports/report.md")
        file_url = f"http://127.0.0.1:{port}/api/file?path={file_path}"
        file_data = _http_get_json(file_url)
        assert "Report" in file_data["content"]

    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase0_api_file_errors_and_truncation(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")

    long_path = run_dir / "reports" / "long.txt"
    long_path.write_text("X" * 40, encoding="utf-8")

    httpd, port = _start_server(run_dir, base_dir=base, max_file_bytes=10)
    try:
        base_url = f"http://127.0.0.1:{port}"

        traversal = urllib.parse.quote("../secrets.txt")
        status, payload = _http_get_json_status(f"{base_url}/api/file?path={traversal}")
        assert status == 400
        assert "error" in payload

        missing = urllib.parse.quote("does/not/exist.txt")
        status, payload = _http_get_json_status(f"{base_url}/api/file?path={missing}")
        assert status == 404
        assert payload.get("error") == "file not found"

        status, payload = _http_get_json_status(f"{base_url}/api/file?path=run_bundle.zip")
        assert status == 415
        assert "unsupported" in payload.get("error", "")

        status, payload = _http_get_json_status(f"{base_url}/api/file?path=reports/long.txt")
        assert status == 200
        assert payload.get("truncated") is True
        assert len(payload.get("content", "")) <= 10

    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase0_summary_invalid_manifest_and_missing_facts(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")

    (run_dir / "run_manifest.json").write_text("{", encoding="utf-8")
    (run_dir / "facts" / "facts.jsonl").unlink()

    httpd, port = _start_server(run_dir, base_dir=base)
    try:
        summary = _http_get_json(f"http://127.0.0.1:{port}/api/run/summary")
        assert summary["manifest"]["exists"] is True
        assert "ERROR parsing manifest" in summary["manifest"]["pretty"]
        assert summary["facts"]["exists"] is False
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase0_tree_includes_nested_files(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")
    (run_dir / "raw" / "fbi_cde" / "nested" / "x.txt").parent.mkdir(parents=True, exist_ok=True)
    (run_dir / "raw" / "fbi_cde" / "nested" / "x.txt").write_text("x", encoding="utf-8")

    httpd, port = _start_server(run_dir, base_dir=base)
    try:
        tree = _http_get_json(f"http://127.0.0.1:{port}/api/tree")

        def _has_path(node: dict, parts: list[str]) -> bool:
            if not parts:
                return True
            if node.get("type") != "dir":
                return False
            name = parts[0]
            for ch in node.get("children", []):
                if ch.get("name") == name:
                    return _has_path(ch, parts[1:])
            return False

        assert _has_path(tree, ["raw", "fbi_cde", "nested", "x.txt"]) is True
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1a_status_endpoint_pass_fail_skip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")

    calls: list[list[str]] = []

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        calls.append(list(argv))
        cmd = argv[3] if len(argv) > 3 else ""
        if cmd == "verify-run":
            return _fake_completed_process(0, out="OK: verified 7 artifact(s)")
        if cmd == "qa":
            return _fake_completed_process(1, out="QA FAIL", err="some rule violated")
        if cmd == "validate":
            return _fake_completed_process(0, out="VALIDATE PASS")
        return _fake_completed_process(2, err="unknown")

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)

    httpd, port = _start_server(run_dir, base_dir=base, cmd_timeout_s=0.25)
    try:
        st = _http_get_json(f"http://127.0.0.1:{port}/api/status")
        checks = st["checks"]

        assert checks["verify_run"]["status"] == "PASS"
        assert "OK:" in (checks["verify_run"]["summary"] or "")

        assert checks["qa"]["status"] == "FAIL"
        assert "some rule violated" in (checks["qa"]["summary"] or "")

        assert checks["validate"]["status"] == "PASS"

        invoked = [c[3] for c in calls if len(c) > 3]
        assert invoked == ["verify-run", "qa", "validate"]
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1b_runs_endpoint_and_run_selection(tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")

    httpd, port = _start_server(run_a, base_dir=base)
    try:
        runs = _http_get_json(f"http://127.0.0.1:{port}/api/runs")
        assert runs["base_dir"].endswith(str(base))
        listed = [r["run"] for r in runs["runs"]]
        assert listed == ["runA", "runB"]

        summary_b = _http_get_json(f"http://127.0.0.1:{port}/api/run/summary?run=runB")
        assert "runB" in summary_b["run_dir"]

        tree_b = _http_get_json(f"http://127.0.0.1:{port}/api/tree?run=runB")
        assert tree_b["type"] == "dir"

        status, payload = _http_get_json_status(f"http://127.0.0.1:{port}/api/run/summary?run=../evil")
        assert status == 400
        assert "error" in payload

        status2, payload2 = _http_get_json_status(f"http://127.0.0.1:{port}/api/run/summary?run=missing")
        assert status2 == 400
        assert "error" in payload2
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1b_status_uses_selected_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    run_b = _make_minimal_run_dir(base, "runB")

    calls: list[list[str]] = []

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        calls.append(list(argv))
        return _fake_completed_process(0, out="OK")

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25)
    try:
        _ = _http_get_json(f"http://127.0.0.1:{port}/api/status?run=runB")
        assert calls, "expected subprocess calls"
        assert str(run_b) in calls[0]
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_cli_ui_dispatches_to_server_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")
    captured: dict[str, list[str]] = {}

    def _fake_main(argv: list[str] | None = None) -> int:
        captured["argv"] = list(argv or [])
        return 0

    monkeypatch.setattr("crimex.ui.server.main", _fake_main)
    monkeypatch.setattr(cli.sys, "argv", ["crimex", "ui", "--run-dir", str(run_dir), "--port", "0"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    assert "--run-dir" in captured["argv"]
    assert str(run_dir) in captured["argv"]
    assert "--port" in captured["argv"]


@pytest.mark.timeout(10)
def test_cli_ui_serves_health_without_writes(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")
    before = _list_files(run_dir)
    port = _pick_free_port()

    cmd = [
        sys.executable,
        "-m",
        "crimex.cli",
        "ui",
        "--run-dir",
        str(run_dir),
        "--port",
        str(port),
    ]

    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        _wait_for_health(port, timeout_s=2.0)
        health = _http_get_json(f"http://127.0.0.1:{port}/health")
        assert health == {"ok": True}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)

    after = _list_files(run_dir)
    assert before == after
