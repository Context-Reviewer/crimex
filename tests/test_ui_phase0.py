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
from pathlib import Path

import pytest

import crimex.cli as cli
import crimex.ui.server as ui_server
from crimex.ui.server import UiConfig, UiHandler, UiHTTPServer


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
    runs_status_budget_ms: int = 2000,
) -> tuple[UiHTTPServer, int]:
    port = _pick_free_port()
    cfg = UiConfig(
        run_dir=run_dir,
        base_dir=(base_dir or run_dir.parent),
        host="127.0.0.1",
        port=port,
        max_file_bytes=max_file_bytes,
        cmd_timeout_s=cmd_timeout_s,
        runs_status_budget_ms=runs_status_budget_ms,
    )
    httpd = UiHTTPServer((cfg.host, cfg.port), UiHandler)
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


@pytest.mark.timeout(10)
def test_ui_phase1c_runs_status_deterministic_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")

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
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/status")
        listed = [r["run"] for r in data["runs"]]
        assert listed == ["runA", "runB"]

        seq: list[tuple[str, str]] = []
        for argv in calls:
            cmd = argv[3] if len(argv) > 3 else ""
            run_name = Path(argv[5]).parents[1].name if cmd == "validate" else Path(argv[5]).name
            seq.append((cmd, run_name))

        assert seq == [
            ("verify-run", "runA"),
            ("qa", "runA"),
            ("validate", "runA"),
            ("verify-run", "runB"),
            ("qa", "runB"),
            ("validate", "runB"),
        ]
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1c_budget_skip_remaining(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")
    _ = _make_minimal_run_dir(base, "runC")

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

    tick = {"ms": 0}

    def _fake_now() -> int:
        tick["ms"] += 100
        return tick["ms"]

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", _fake_now)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=500)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/status")
        listed = [r["run"] for r in data["runs"]]
        assert listed == ["runA", "runB", "runC"]

        invoked = [c[3] for c in calls if len(c) > 3]
        assert invoked == ["verify-run", "qa", "validate"]

        run_b = data["runs"][1]
        run_c = data["runs"][2]
        for item in (run_b, run_c):
            checks = item["checks"]
            assert checks["verify_run"]["status"] == "SKIP"
            assert checks["qa"]["status"] == "SKIP"
            assert checks["validate"]["status"] == "SKIP"
            assert checks["verify_run"]["summary"] == "TIME BUDGET EXCEEDED"
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1c_facts_missing_per_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    run_b = _make_minimal_run_dir(base, "runB")
    (run_b / "facts" / "facts.jsonl").unlink()

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

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/status")
        listed = [r["run"] for r in data["runs"]]
        assert listed == ["runA", "runB"]

        invoked = [c[3] for c in calls if len(c) > 3]
        assert invoked == ["verify-run", "qa", "validate", "verify-run"]

        run_b_checks = data["runs"][1]["checks"]
        assert run_b_checks["verify_run"]["status"] == "PASS"
        assert run_b_checks["qa"]["status"] == "SKIP"
        assert run_b_checks["validate"]["status"] == "SKIP"
        assert run_b_checks["qa"]["summary"] == "facts missing"
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1d_runs_overview_deterministic_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")

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

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        listed = [r["run"] for r in data["runs"]]
        assert listed == ["runA", "runB"]
        assert data["budget_ms"] == 10_000

        for item in data["runs"]:
            assert item["has_manifest"] is True
            assert item["has_facts"] is True
            assert item["has_bundle"] is True
            checks = item["checks"]
            assert "verify_run" in checks
            assert "qa" in checks
            assert "validate" in checks

        seq: list[tuple[str, str]] = []
        for argv in calls:
            cmd = argv[3] if len(argv) > 3 else ""
            run_name = Path(argv[5]).parents[1].name if cmd == "validate" else Path(argv[5]).name
            seq.append((cmd, run_name))

        assert seq == [
            ("verify-run", "runA"),
            ("qa", "runA"),
            ("validate", "runA"),
            ("verify-run", "runB"),
            ("qa", "runB"),
            ("validate", "runB"),
        ]
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1d_overview_budget_skip_includes_meta(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")
    _ = _make_minimal_run_dir(base, "runC")

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

    tick = {"ms": 0}

    def _fake_now() -> int:
        tick["ms"] += 100
        return tick["ms"]

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", _fake_now)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=500)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        listed = [r["run"] for r in data["runs"]]
        assert listed == ["runA", "runB", "runC"]
        assert data["budget_ms"] == 500

        invoked = [c[3] for c in calls if len(c) > 3]
        assert invoked == ["verify-run", "qa", "validate"]

        run_b = data["runs"][1]
        run_c = data["runs"][2]
        for item in (run_b, run_c):
            assert item["has_manifest"] is True
            assert item["has_facts"] is True
            assert item["has_bundle"] is True
            checks = item["checks"]
            assert checks["verify_run"]["status"] == "SKIP"
            assert checks["qa"]["status"] == "SKIP"
            assert checks["validate"]["status"] == "SKIP"
            assert checks["verify_run"]["summary"] == "TIME BUDGET EXCEEDED"
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1i_overview_includes_abs_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _ = _make_minimal_run_dir(base, "runB")

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        return _fake_completed_process(0, out="OK")

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        for item in data["runs"]:
            assert item["abs_run_dir"] is not None
            assert Path(item["abs_run_dir"]).is_absolute()
            assert item["abs_manifest_path"].endswith("run_manifest.json")
            assert item["abs_facts_path"].endswith("facts.jsonl")
            assert item["abs_bundle_path"].endswith("run_bundle.zip")
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1i_overview_abs_paths_missing_facts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    run_b = _make_minimal_run_dir(base, "runB")
    (run_b / "facts" / "facts.jsonl").unlink()

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        return _fake_completed_process(0, out="OK")

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        run_b_item = next(item for item in data["runs"] if item["run"] == "runB")
        assert run_b_item["has_facts"] is False
        assert run_b_item["abs_facts_path"] is None
        assert run_b_item["abs_manifest_path"].endswith("run_manifest.json")
        assert run_b_item["abs_bundle_path"].endswith("run_bundle.zip")
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1h_overview_copy_bundle_pass_and_collapse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        cmd = argv[3] if len(argv) > 3 else ""
        if cmd == "verify-run":
            return _fake_completed_process(0, out="OK\t\tPASS  \nextra")
        if cmd == "qa":
            return _fake_completed_process(0, out="QA\tPASS")
        if cmd == "validate":
            return _fake_completed_process(0, out="VAL   OK")
        return _fake_completed_process(2, err="unknown")

    def _fake_now() -> int:
        return 1000

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", _fake_now)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        item = data["runs"][0]
        assert "copy_bundle" in item
        expected = "\n".join(
            [
                "crimex run: runA",
                "overall: PASS",
                "verify_run: PASS exit=0 ms=0 summary=OK PASS",
                "qa: PASS exit=0 ms=0 summary=QA PASS",
                "validate: PASS exit=0 ms=0 summary=VAL OK",
            ]
        )
        assert item["copy_bundle"] == expected
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1h_overview_copy_bundle_fail_overall(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")

    def _fake_run(
        argv: list[str],
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        cmd = argv[3] if len(argv) > 3 else ""
        if cmd == "verify-run":
            return _fake_completed_process(0, out="OK")
        if cmd == "qa":
            return _fake_completed_process(1, err="QA\tFAIL")
        if cmd == "validate":
            return _fake_completed_process(0, out="VAL OK")
        return _fake_completed_process(2, err="unknown")

    def _fake_now() -> int:
        return 1000

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", _fake_now)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.25, runs_status_budget_ms=10_000)
    try:
        data = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview")
        bundle = data["runs"][0]["copy_bundle"]
        lines = bundle.splitlines()
        assert lines[0] == "crimex run: runA"
        assert lines[1] == "overall: FAIL"
        assert lines[3] == "qa: FAIL exit=1 ms=0 summary=QA FAIL"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_ui_phase1j_diag_payload_pass() -> None:
    run_row = {
        "run": "runA",
        "has_manifest": True,
        "has_facts": True,
        "has_bundle": True,
        "checks": {
            "verify_run": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 5,
            },
            "qa": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "QA ok",
                "elapsed_ms": 7,
            },
            "validate": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "VAL ok",
                "elapsed_ms": 9,
            },
        },
    }
    payload = ui_server._build_diag_payload(run_row)
    expected = (
        '{"run":"runA","overall":"PASS","facts":{"has_manifest":true,"has_facts":true,'
        '"has_bundle":true},"checks":{"verify_run":{"status":"PASS","exit_code":0,'
        '"summary":"OK","elapsed_ms":5},"qa":{"status":"PASS","exit_code":0,'
        '"summary":"QA ok","elapsed_ms":7},"validate":{"status":"PASS","exit_code":0,'
        '"summary":"VAL ok","elapsed_ms":9}},"paths":{"run_rel":"runA",'
        '"manifest":"runA/run_manifest.json","facts":"runA/facts/facts.jsonl",'
        '"bundle":"runA/run_bundle.zip"}}'
    )
    assert payload == expected


def test_ui_phase1j_diag_payload_fail_overall() -> None:
    run_row = {
        "run": "runB",
        "has_manifest": True,
        "has_facts": True,
        "has_bundle": True,
        "checks": {
            "verify_run": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 1,
            },
            "qa": {
                "status": "FAIL",
                "exit_code": 2,
                "summary": "bad",
                "elapsed_ms": 3,
            },
            "validate": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 2,
            },
        },
    }
    payload = ui_server._build_diag_payload(run_row)
    assert '"overall":"FAIL"' in payload
    assert '"qa":{"status":"FAIL","exit_code":2,"summary":"bad","elapsed_ms":3}' in payload


def test_ui_phase1j_diag_payload_skip_defaults() -> None:
    run_row = {
        "run": "runC",
        "has_manifest": False,
        "has_facts": False,
        "has_bundle": False,
        "checks": {},
    }
    payload = ui_server._build_diag_payload(run_row)
    expected = (
        '{"run":"runC","overall":"SKIP","facts":{"has_manifest":false,"has_facts":false,'
        '"has_bundle":false},"checks":{"verify_run":{"status":"SKIP","exit_code":null,'
        '"summary":"","elapsed_ms":0},"qa":{"status":"SKIP","exit_code":null,"summary":"",'
        '"elapsed_ms":0},"validate":{"status":"SKIP","exit_code":null,"summary":"",'
        '"elapsed_ms":0}},"paths":{"run_rel":"runC","manifest":"runC/run_manifest.json",'
        '"facts":"runC/facts/facts.jsonl","bundle":"runC/run_bundle.zip"}}'
    )
    assert payload == expected


def test_ui_phase1k_fail_jsonl_filters_and_orders() -> None:
    row_b = {
        "run": "runB",
        "has_manifest": True,
        "has_facts": True,
        "has_bundle": True,
        "checks": {
            "verify_run": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 3,
            },
            "qa": {
                "status": "FAIL",
                "exit_code": 5,
                "summary": "nope",
                "elapsed_ms": 4,
            },
            "validate": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 5,
            },
        },
    }
    row_c = {
        "run": "runC",
        "has_manifest": False,
        "has_facts": True,
        "has_bundle": False,
        "checks": {
            "verify_run": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 1,
            },
            "qa": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 2,
            },
            "validate": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "OK",
                "elapsed_ms": 3,
            },
        },
    }
    row_a = {
        "run": "runA",
        "has_manifest": True,
        "has_facts": False,
        "has_bundle": True,
        "checks": {
            "verify_run": {
                "status": "FAIL",
                "exit_code": 2,
                "summary": "boom",
                "elapsed_ms": 1,
            },
            "qa": {
                "status": "PASS",
                "exit_code": 0,
                "summary": "ok",
                "elapsed_ms": 2,
            },
            "validate": {
                "status": "SKIP",
                "exit_code": None,
                "summary": "",
                "elapsed_ms": 0,
            },
        },
    }
    jsonl = ui_server._build_fail_jsonl([row_b, row_c, row_a])
    lines = jsonl.splitlines()
    assert lines == [
        ui_server._build_diag_payload(row_a),
        ui_server._build_diag_payload(row_b),
    ]


def test_ui_phase1k_fail_jsonl_no_fails() -> None:
    rows = [
        {
            "run": "runA",
            "has_manifest": True,
            "has_facts": True,
            "has_bundle": True,
            "checks": {
                "verify_run": {"status": "PASS", "exit_code": 0, "summary": "OK", "elapsed_ms": 1},
                "qa": {"status": "PASS", "exit_code": 0, "summary": "OK", "elapsed_ms": 2},
                "validate": {"status": "PASS", "exit_code": 0, "summary": "OK", "elapsed_ms": 3},
            },
        },
        {
            "run": "runB",
            "has_manifest": False,
            "has_facts": False,
            "has_bundle": False,
            "checks": {},
        },
    ]
    assert ui_server._build_fail_jsonl(rows) == ""


def test_ui_phase1k_fail_jsonl_missing_checks_defaults() -> None:
    rows = [
        {
            "run": "runX",
            "checks": {
                "verify_run": {
                    "status": "FAIL",
                    "exit_code": 1,
                    "summary": "bad",
                    "elapsed_ms": 3,
                }
            },
        }
    ]
    jsonl = ui_server._build_fail_jsonl(rows)
    expected = (
        '{"run":"runX","overall":"FAIL","facts":{"has_manifest":false,"has_facts":false,'
        '"has_bundle":false},"checks":{"verify_run":{"status":"FAIL","exit_code":1,'
        '"summary":"bad","elapsed_ms":3},"qa":{"status":"SKIP","exit_code":null,'
        '"summary":"","elapsed_ms":0},"validate":{"status":"SKIP","exit_code":null,'
        '"summary":"","elapsed_ms":0}},"paths":{"run_rel":"runX","manifest":"runX/run_manifest.json",'
        '"facts":"runX/facts/facts.jsonl","bundle":"runX/run_bundle.zip"}}'
    )
    assert jsonl == expected


@pytest.mark.timeout(10)
def test_ui_phase1l_overview_snapshot_skips_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _make_minimal_run_dir(base, "runB")

    calls: list[list[str]] = []

    def _fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        return _fake_completed_process(0, out="OK")

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", lambda: 1000)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.1, runs_status_budget_ms=10_000)
    try:
        _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview?mode=refresh")
        cmds = [c[3] for c in calls]
        assert cmds == ["verify-run", "qa", "validate", "verify-run", "qa", "validate"]
        assert "runA" in calls[0][5]
        assert "runB" in calls[3][5]

        before = len(calls)
        _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview?mode=snapshot")
        assert len(calls) == before
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1l_overview_compute_does_not_overwrite_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_a = _make_minimal_run_dir(base, "runA")
    _make_minimal_run_dir(base, "runB")

    calls: list[list[str]] = []

    def _fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        return _fake_completed_process(0, out="OK")

    now = {"ms": 1000}

    def _fake_now() -> int:
        return int(now["ms"])

    monkeypatch.setattr(ui_server.subprocess, "run", _fake_run)
    monkeypatch.setattr(ui_server, "_now_monotonic_ms", _fake_now)

    httpd, port = _start_server(run_a, base_dir=base, cmd_timeout_s=0.1, runs_status_budget_ms=10_000)
    try:
        data_refresh = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview?mode=refresh")
        assert data_refresh["snapshot"]["exists"] is True
        created_1 = int(data_refresh["snapshot"]["created_ms"])

        before_calls = len(calls)
        now["ms"] = 2000
        data_compute = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview?mode=compute")
        assert data_compute["snapshot"]["exists"] is False
        assert len(calls) > before_calls

        now["ms"] = 3000
        data_snapshot = _http_get_json(f"http://127.0.0.1:{port}/api/runs/overview?mode=snapshot")
        assert data_snapshot["snapshot"]["exists"] is True
        created_2 = int(data_snapshot["snapshot"]["created_ms"])
        assert created_2 == created_1
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.timeout(10)
def test_ui_phase1l_overview_snapshot_missing_returns_error(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()
    run_dir = _make_minimal_run_dir(base, "runA")
    httpd, port = _start_server(run_dir, base_dir=base)

    try:
        status, data = _http_get_json_status(f"http://127.0.0.1:{port}/api/runs/overview?mode=snapshot")
        assert status == 409
        assert "snapshot" in (data.get("error") or "")
    finally:
        httpd.shutdown()
        httpd.server_close()
