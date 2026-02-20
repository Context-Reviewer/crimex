from __future__ import annotations

import argparse
import json
import mimetypes
import os
import posixpath
import socket
import sys
import time
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

# ----------------------------
# Core helpers (deterministic)
# ----------------------------


def _now_monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def _read_text(path: Path, *, max_bytes: int) -> str:
    data = path.read_bytes()
    if len(data) > max_bytes:
        data = data[:max_bytes]
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        # Fall back to latin-1 to keep it viewable; still read-only.
        return data.decode("latin-1", errors="replace")


def _safe_relpath(p: str) -> str:
    # Normalize to forward slashes, strip leading slashes, prevent traversal.
    p = p.replace("\\", "/")
    p = posixpath.normpath(p)
    p = p.lstrip("/")
    if p == ".":
        return ""
    # Block traversal
    if p.startswith("..") or "/.." in p or "\\.." in p:
        raise ValueError("path traversal not allowed")
    return p


def _is_under(base: Path, target: Path) -> bool:
    try:
        base_res = base.resolve(strict=False)
        target_res = target.resolve(strict=False)
        return base_res == target_res or str(target_res).startswith(str(base_res) + os.sep)
    except Exception:
        return False


def _json_dumps(obj: Any) -> str:
    # Deterministic JSON output (sorted keys, stable formatting)
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def _iter_files_sorted(root: Path) -> Iterable[Path]:
    # Deterministic traversal: sort directories and files lexicographically.
    # Exclude common noise (but keep conservative; run_dir should be clean anyway).
    if not root.exists():
        return []
    all_paths: list[Path] = []
    for p in root.rglob("*"):
        if p.is_file():
            all_paths.append(p)
    all_paths.sort(key=lambda x: str(x.relative_to(root)).replace("\\", "/"))
    return all_paths


def _build_tree(root: Path) -> dict[str, Any]:
    """
    Build a deterministic directory tree structure.
    Format:
      {"type":"dir","name":"","children":[...]}
    """

    def add_node(tree: dict[str, Any], parts: list[str], is_file: bool) -> None:
        cur = tree
        for i, part in enumerate(parts):
            children = cur.setdefault("children", [])
            # find existing
            found = None
            for ch in children:
                if ch["name"] == part:
                    found = ch
                    break
            if found is None:
                node_type = "file" if (i == len(parts) - 1 and is_file) else "dir"
                found = {"type": node_type, "name": part}
                if node_type == "dir":
                    found["children"] = []
                children.append(found)
                # keep children sorted deterministically
                children.sort(key=lambda n: (n["type"] != "dir", n["name"]))
            cur = found

    tree: dict[str, Any] = {"type": "dir", "name": "", "children": []}
    for f in _iter_files_sorted(root):
        rel = f.relative_to(root).as_posix()
        parts = [p for p in rel.split("/") if p]
        if not parts:
            continue
        add_node(tree, parts, is_file=True)
    return tree


def _count_jsonl_lines(path: Path, *, max_bytes: int = 5_000_000) -> dict[str, Any]:
    """
    Count JSONL records (line-based) with a safety cap to keep UI snappy.
    """
    if not path.exists():
        return {"exists": False, "records": 0, "truncated": False}
    data = path.read_bytes()
    truncated = False
    if len(data) > max_bytes:
        data = data[:max_bytes]
        truncated = True
    # Count non-empty lines
    text = data.decode("utf-8", errors="replace")
    records = sum(1 for line in text.splitlines() if line.strip())
    return {"exists": True, "records": records, "truncated": truncated}


def _load_json_if_exists(path: Path, *, max_bytes: int) -> tuple[bool, Any | None, str | None]:
    if not path.exists():
        return False, None, None
    try:
        txt = _read_text(path, max_bytes=max_bytes)
        return True, json.loads(txt), None
    except Exception as e:
        return True, None, f"{type(e).__name__}: {e}"


# ----------------------------
# HTTP Server
# ----------------------------


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>crimex UI (Phase 0) - Run Viewer</title>
  <style>
    :root { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; }
    body { margin: 0; padding: 0; background: #0b0d10; color: #e9eef5; }
    header { padding: 16px 18px; border-bottom: 1px solid #1c2430; background: #0f1319; }
    header h1 { margin: 0; font-size: 16px; letter-spacing: 0.2px; }
    header .sub { margin-top: 6px; font-size: 12px; color: #a9b6c6; }
    main { display: grid; grid-template-columns: 360px 1fr; gap: 14px; padding: 14px; }
    .card { background: #0f1319; border: 1px solid #1c2430; border-radius: 10px; overflow: hidden; }
    .card h2 { margin: 0; padding: 10px 12px; font-size: 13px; border-bottom: 1px solid #1c2430; }
    .card h2 { color: #cfe0f5; }
    .card .body { padding: 10px 12px; font-size: 12px; color: #d7e0eb; }
    .kv { display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; }
    .k { color: #9fb0c4; }
    .v { color: #e9eef5; word-break: break-word; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .tree { max-height: 52vh; overflow: auto; padding-right: 6px; }
    details { margin: 2px 0; }
    summary { cursor: pointer; }
    .file { cursor: pointer; text-decoration: underline; text-decoration-color: #2b7cff55; }
    pre { margin: 0; white-space: pre-wrap; word-break: break-word; }
    .viewer { max-height: 74vh; overflow: auto; }
    .muted { color: #a9b6c6; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; }
    .pill { border: 1px solid #1c2430; background: #0b0d10; font-size: 11px; }
    .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    button { background:#132033; border:1px solid #1c2430; color:#e9eef5; }
    button { border-radius:8px; padding:6px 10px; font-size:12px; cursor:pointer; }
    button:hover { border-color:#2b7cff; }
    a { color: #9cc4ff; }
  </style>
</head>
<body>
<header>
  <h1>crimex UI (Phase 0) - Run Viewer</h1>
  <div class="sub">
    Read-only. Deterministic listings. No writes to run directory.
    <span class="pill mono" id="runDirPill"></span>
  </div>
</header>

<main>
  <section class="card">
    <h2>Run Summary</h2>
    <div class="body">
      <div class="kv" id="summaryKv"></div>
      <div style="height:10px"></div>
      <div class="row">
        <button id="refreshBtn">Refresh</button>
        <span class="muted" id="statusText"></span>
      </div>
    </div>
  </section>

  <section class="card">
    <h2>Manifest</h2>
    <div class="body viewer">
      <pre class="mono" id="manifestPre">(loading...)</pre>
    </div>
  </section>

  <section class="card">
    <h2>Artifact Tree</h2>
    <div class="body tree" id="treeHost">(loading...)</div>
  </section>

  <section class="card">
    <h2>File Viewer</h2>
    <div class="body viewer">
      <div class="muted mono" id="filePath">(no file selected)</div>
      <div style="height:10px"></div>
      <pre class="mono" id="filePre"></pre>
    </div>
  </section>
</main>

<script>
  const $ = (id) => document.getElementById(id);

  function escapeHtml(s) {
    return s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
  }

  async function api(path) {
    const r = await fetch(path, {cache:"no-store"});
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  }

  function renderKv(obj) {
    const host = $("summaryKv");
    host.innerHTML = "";
    const entries = Object.entries(obj);
    for (const [k,v] of entries) {
      const kdiv = document.createElement("div");
      kdiv.className = "k mono";
      kdiv.textContent = k;

      const vdiv = document.createElement("div");
      vdiv.className = "v mono";
      vdiv.textContent = (v === null || v === undefined) ? "" : String(v);

      host.appendChild(kdiv);
      host.appendChild(vdiv);
    }
  }

  function renderTreeNode(node, prefixPath) {
    const name = node.name || "";
    const curPath = prefixPath ? (prefixPath + "/" + name) : name;

    if (node.type === "dir") {
      const d = document.createElement("details");
      d.open = (prefixPath === "" && name === "");
      const s = document.createElement("summary");
      s.className = "mono";
      s.textContent = name === "" ? "(root)" : name;
      d.appendChild(s);

      const children = node.children || [];
      for (const ch of children) {
        d.appendChild(renderTreeNode(ch, curPath));
      }
      return d;
    } else {
      const div = document.createElement("div");
      div.className = "mono file";
      div.style.marginLeft = "18px";
      div.textContent = name;
      div.addEventListener("click", async () => {
        $("statusText").textContent = "Loading file...";
        try {
          const q = encodeURIComponent(curPath);
          const data = await api(`/api/file?path=${q}`);
          $("filePath").textContent = data.path;
          $("filePre").textContent = data.content;
        } catch (e) {
          $("filePath").textContent = curPath;
          $("filePre").textContent = `ERROR: ${e}`;
        } finally {
          $("statusText").textContent = "";
        }
      });
      return div;
    }
  }

  async function refreshAll() {
    $("statusText").textContent = "Refreshing...";
    try {
      const summary = await api("/api/run/summary");
      $("runDirPill").textContent = summary.run_dir;
      renderKv({
        "run_dir": summary.run_dir,
        "manifest_exists": summary.manifest.exists,
        "facts_exists": summary.facts.exists,
        "facts_records": summary.facts.records,
        "facts_truncated": summary.facts.truncated,
        "reports_dir_exists": summary.reports_dir_exists,
        "bundle_exists": summary.bundle_exists,
        "tree_files": summary.tree_files,
      });

      $("manifestPre").textContent = summary.manifest.pretty || "(no manifest)";

      const tree = await api("/api/tree");
      const host = $("treeHost");
      host.innerHTML = "";
      host.appendChild(renderTreeNode(tree, ""));
    } catch (e) {
      $("statusText").textContent = `ERROR: ${e}`;
    } finally {
      setTimeout(() => { $("statusText").textContent = ""; }, 800);
    }
  }

  $("refreshBtn").addEventListener("click", refreshAll);
  refreshAll();
</script>
</body>
</html>
"""


@dataclass(frozen=True)
class UiConfig:
    run_dir: Path
    host: str
    port: int
    max_file_bytes: int


class UiHandler(BaseHTTPRequestHandler):
    server_version = "crimex-ui/phase0"

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, obj: Any) -> None:
        body = _json_dumps(obj).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _send_text(self, status: int, text: str, *, content_type: str = "text/plain; charset=utf-8") -> None:
        self._send(status, text.encode("utf-8"), content_type)

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep deterministic-ish logs (no timestamps). Also quiet by default.
        # If you want logs, run with --verbose.
        if hasattr(self.server, "_verbose") and self.server._verbose:
            super().log_message(fmt, *args)

    @property
    def cfg(self) -> UiConfig:
        return self.server._cfg

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._send(HTTPStatus.OK, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return

        if path == "/health":
            self._send_json(HTTPStatus.OK, {"ok": True})
            return

        if path == "/api/run/summary":
            self._handle_summary()
            return

        if path == "/api/tree":
            self._handle_tree()
            return

        if path == "/api/file":
            self._handle_file(parsed.query)
            return

        self._send_text(HTTPStatus.NOT_FOUND, "not found")

    def _handle_summary(self) -> None:
        run_dir = self.cfg.run_dir

        manifest_path = run_dir / "run_manifest.json"
        facts_path = run_dir / "facts" / "facts.jsonl"
        reports_dir = run_dir / "reports"
        bundle_path = run_dir / "run_bundle.zip"

        manifest_exists, manifest_obj, manifest_err = _load_json_if_exists(
            manifest_path,
            max_bytes=min(self.cfg.max_file_bytes, 5_000_000),
        )

        pretty_manifest = None
        if manifest_exists and manifest_obj is not None:
            pretty_manifest = _json_dumps(manifest_obj)
        elif manifest_exists and manifest_err is not None:
            pretty_manifest = f"ERROR parsing manifest: {manifest_err}"

        facts_info = _count_jsonl_lines(facts_path)
        tree = _build_tree(run_dir)
        tree_files = _count_files(tree)

        out = {
            "run_dir": str(run_dir),
            "manifest": {
                "exists": manifest_exists,
                "path": str(manifest_path),
                "pretty": pretty_manifest,
                "error": manifest_err,
            },
            "facts": {
                "exists": facts_info["exists"],
                "path": str(facts_path),
                "records": facts_info["records"],
                "truncated": facts_info["truncated"],
            },
            "reports_dir_exists": reports_dir.exists(),
            "bundle_exists": bundle_path.exists(),
            "tree_files": tree_files,
        }
        self._send_json(HTTPStatus.OK, out)

    def _handle_tree(self) -> None:
        tree = _build_tree(self.cfg.run_dir)
        self._send_json(HTTPStatus.OK, tree)

    def _handle_file(self, query: str) -> None:
        params = urllib.parse.parse_qs(query, keep_blank_values=True)
        raw_path = (params.get("path") or [""])[0]

        try:
            rel = _safe_relpath(raw_path)
        except ValueError as e:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        target = (self.cfg.run_dir / rel).resolve(strict=False)
        if not _is_under(self.cfg.run_dir, target):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "path traversal not allowed"})
            return

        if not target.exists() or not target.is_file():
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "file not found", "path": rel})
            return

        # Serve as JSON with content (text) for now. Binary is not supported in Phase 0.
        # If it's likely binary, we return an informative error.
        mime, _ = mimetypes.guess_type(str(target))
        if mime and not mime.startswith(("text/", "application/json", "application/xml")):
            self._send_json(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {
                    "error": "binary or unsupported file type in Phase 0",
                    "path": rel,
                    "mime": mime,
                },
            )
            return

        content = _read_text(target, max_bytes=self.cfg.max_file_bytes)
        self._send_json(
            HTTPStatus.OK,
            {
                "path": rel,
                "content": content,
                "truncated": target.stat().st_size > self.cfg.max_file_bytes,
            },
        )


def _count_files(tree: dict[str, Any]) -> int:
    if tree.get("type") == "file":
        return 1
    total = 0
    for ch in tree.get("children", []):
        total += _count_files(ch)
    return total


def _pick_free_port(host: str) -> int:
    # Bind to port 0 to find an available port, then close.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m crimex.ui.server",
        description="crimex UI (Phase 0): deterministic read-only run viewer (stdlib-only).",
    )
    p.add_argument("--run-dir", required=True, help="Path to an existing run directory.")
    p.add_argument("--host", default="127.0.0.1", help="Bind host (default 127.0.0.1).")
    p.add_argument("--port", type=int, default=0, help="Bind port (0 picks a free port).")
    p.add_argument("--max-file-bytes", type=int, default=500_000, help="Max bytes served per file (default 500k).")
    p.add_argument("--verbose", action="store_true", help="Enable request logging.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)

    run_dir = Path(args.run_dir).resolve(strict=False)
    if not run_dir.exists() or not run_dir.is_dir():
        print(f"ERROR: run_dir does not exist or is not a directory: {run_dir}", file=sys.stderr)
        return 2

    host = str(args.host)
    port = int(args.port)
    if port == 0:
        port = _pick_free_port(host)

    cfg = UiConfig(
        run_dir=run_dir,
        host=host,
        port=port,
        max_file_bytes=int(args.max_file_bytes),
    )

    httpd = ThreadingHTTPServer((cfg.host, cfg.port), UiHandler)
    httpd._cfg = cfg
    httpd._verbose = bool(args.verbose)

    print("crimex UI (Phase 0) running (read-only)")
    print(f"run_dir: {cfg.run_dir}")
    print(f"url: http://{cfg.host}:{cfg.port}/")
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
