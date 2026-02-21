from __future__ import annotations

import argparse
import json
import mimetypes
import os
import posixpath
import socket
import subprocess
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
                # keep children sorted deterministically: dirs first, then files; name lexicographic
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


def _summarize_text(s: str, *, max_chars: int = 240) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    line = s.splitlines()[0].strip()
    if len(line) > max_chars:
        return line[:max_chars] + "..."
    return line


def _run_cli(argv: list[str], *, timeout_s: float) -> dict[str, Any]:
    """
    Run `python -m crimex.cli ...` deterministically and return a small summary.
    """
    t0 = _now_monotonic_ms()
    try:
        cp = subprocess.run(  # noqa: S603
            argv,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        elapsed = _now_monotonic_ms() - t0
        out = (cp.stdout or "").strip()
        err = (cp.stderr or "").strip()
        return {
            "ok": cp.returncode == 0,
            "exit_code": int(cp.returncode),
            "elapsed_ms": int(elapsed),
            "stdout_1": _summarize_text(out),
            "stderr_1": _summarize_text(err),
        }
    except subprocess.TimeoutExpired:
        elapsed = _now_monotonic_ms() - t0
        return {
            "ok": False,
            "exit_code": None,
            "elapsed_ms": int(elapsed),
            "stdout_1": "",
            "stderr_1": "TIMEOUT",
        }
    except Exception as e:
        elapsed = _now_monotonic_ms() - t0
        return {
            "ok": False,
            "exit_code": None,
            "elapsed_ms": int(elapsed),
            "stdout_1": "",
            "stderr_1": f"{type(e).__name__}: {e}",
        }


def _status_from_run_result(res: dict[str, Any]) -> tuple[str, str]:
    if res.get("ok") is True:
        summary = res.get("stdout_1") or res.get("stderr_1") or ""
        return "PASS", summary
    summary = res.get("stderr_1") or res.get("stdout_1") or ""
    return "FAIL", summary


def _count_files(tree: dict[str, Any]) -> int:
    if tree.get("type") == "file":
        return 1
    total = 0
    for ch in tree.get("children", []):
        total += _count_files(ch)
    return total


def _pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _as_posix_rel(base: Path, p: Path) -> str:
    return p.relative_to(base).as_posix()


def _iter_run_dirs(base: Path) -> list[Path]:
    if not base.exists() or not base.is_dir():
        return []
    children = [p for p in base.iterdir() if p.is_dir()]
    children.sort(key=lambda p: p.name)
    return children


def _base_py_cli() -> list[str]:
    return [sys.executable, "-m", "crimex.cli"]


def _run_meta(base: Path, d: Path) -> dict[str, Any]:
    return {
        "run": _as_posix_rel(base, d),
        "has_manifest": (d / "run_manifest.json").exists(),
        "has_facts": (d / "facts" / "facts.jsonl").exists(),
        "has_bundle": (d / "run_bundle.zip").exists(),
    }


# ----------------------------
# HTTP Server
# ----------------------------


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>crimex UI (Phase 1D) - Run Viewer</title>
  <style>
    :root { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; }
    body { margin: 0; padding: 0; background: #0b0d10; color: #e9eef5; }
    header { padding: 16px 18px; border-bottom: 1px solid #1c2430; background: #0f1319; }
    header h1 { margin: 0; font-size: 16px; letter-spacing: 0.2px; }
    header .sub { margin-top: 6px; font-size: 12px; color: #a9b6c6; }
    main { display: grid; grid-template-columns: 360px 1fr; gap: 14px; padding: 14px; }
    .card { background: #0f1319; border: 1px solid #1c2430; border-radius: 10px; overflow: hidden; }
    .card h2 { margin: 0; padding: 10px 12px; font-size: 13px; border-bottom: 1px solid #1c2430; color: #cfe0f5; }
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
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid #1c2430;
      background: #0b0d10;
      font-size: 11px;
    }
    .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    button {
      background:#132033;
      border:1px solid #1c2430;
      color:#e9eef5;
      border-radius:8px;
      padding:6px 10px;
      font-size:12px;
      cursor:pointer;
    }
    button:hover { border-color:#2b7cff; }
    select {
      background:#0b0d10;
      border:1px solid #1c2430;
      color:#e9eef5;
      border-radius:8px;
      padding:6px 10px;
      font-size:12px;
    }
    a { color: #9cc4ff; }

    .statusgrid { display:grid; grid-template-columns: 1fr; gap: 8px; }
    .checkrow { display:flex; justify-content:space-between; gap:10px; align-items:center; }
    .badge { font-size: 11px; padding:2px 8px; border-radius:999px; border:1px solid #1c2430; }
    .badge.pass { background:#0b1a12; border-color:#144a2f; }
    .badge.fail { background:#221012; border-color:#5b1e26; }
    .badge.skip { background:#111827; border-color:#24314a; }
    .small { font-size: 11px; color:#a9b6c6; }
    .runs-table { width:100%; border-collapse: collapse; font-size:12px; }
    .runs-table th { text-align:left; font-size:11px; color:#9fb0c4; padding:4px 0; }
    .runs-table td { padding:4px 0; border-top:1px solid #1c2430; vertical-align:middle; }
    .runs-table .run-name { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .filter-row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
    .filter-row label { display:flex; gap:6px; align-items:center; font-size:11px; color:#cfe0f5; }
    .filter-row input { accent-color:#2b7cff; }
    .fact-pill {
      display:inline-block;
      min-width:18px;
      text-align:center;
      padding:2px 6px;
      border-radius:999px;
      border:1px solid #1c2430;
      font-size:11px;
    }
    .fact-pill.y { background:#0b1a12; border-color:#144a2f; }
    .fact-pill.n { background:#111827; border-color:#24314a; color:#a9b6c6; }
  </style>
</head>
<body>
<header>
  <h1>crimex UI (Phase 1D) - Run Viewer</h1>
  <div class="sub">
    Read-only. Deterministic listings. No writes to run directory.
    <span class="pill mono" id="baseDirPill"></span>
    <span class="pill mono" id="runDirPill"></span>
  </div>
</header>

<main>
  <section class="card">
    <h2>Run Selector</h2>
    <div class="body">
      <div class="row">
        <select id="runSelect"></select>
        <button id="reloadRunsBtn">Reload</button>
      </div>
      <div style="height:10px"></div>
      <div class="row">
        <button id="refreshBtn">Refresh view</button>
        <span class="muted" id="statusText"></span>
      </div>
      <div style="height:10px"></div>
      <div class="kv" id="summaryKv"></div>
    </div>
  </section>

  <section class="card">
    <h2>Manifest</h2>
    <div class="body viewer">
      <pre class="mono" id="manifestPre">(loading...)</pre>
    </div>
  </section>

  <section class="card">
    <h2>Governance Status</h2>
    <div class="body">
      <div class="statusgrid" id="govHost">
        <div class="muted">(loading...)</div>
      </div>
      <div style="height:10px"></div>
      <div class="row">
        <button id="statusBtn">Refresh status</button>
        <span class="muted small" id="govMeta"></span>
      </div>
    </div>
  </section>

  <section class="card">
    <h2>Runs Overview</h2>
    <div class="body">
      <div class="row">
        <button id="runsOverviewBtn">Refresh overview</button>
        <span class="muted small" id="runsOverviewMeta"></span>
      </div>
      <div style="height:10px"></div>
      <div class="filter-row">
        <label><input type="checkbox" id="filterFailOnly"> Show FAIL only</label>
        <label><input type="checkbox" id="filterHidePass"> Hide PASS</label>
        <label><input type="checkbox" id="filterHideSkip"> Hide SKIP</label>
        <span class="muted small">Fail-first sort (FAIL -> SKIP -> PASS)</span>
      </div>
      <div style="height:10px"></div>
      <table class="runs-table">
        <thead>
          <tr>
            <th>run</th>
            <th>manifest</th>
            <th>facts</th>
            <th>bundle</th>
            <th>verify</th>
            <th>qa</th>
            <th>validate</th>
            <th></th>
          </tr>
        </thead>
        <tbody id="runsOverviewBody">
          <tr><td class="muted" colspan="8">(loading...)</td></tr>
        </tbody>
      </table>
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
  let runsOverviewData = null;

  function getSelectedRun() {
    const el = $("runSelect");
    return el && el.value ? el.value : "";
  }

  function withRun(path) {
    const run = getSelectedRun();
    if (!run) return path;
    const sep = path.includes("?") ? "&" : "?";
    return `${path}${sep}run=${encodeURIComponent(run)}`;
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

  function badgeClass(status) {
    if (status === "PASS") return "badge pass";
    if (status === "FAIL") return "badge fail";
    return "badge skip";
  }

  const STATUS_RANK = { "FAIL": 0, "SKIP": 1, "PASS": 2 };

  function overallStatus(checks) {
    const names = ["verify_run", "qa", "validate"];
    let has_skip = false;
    for (const name of names) {
      const ch = checks && checks[name] ? checks[name] : {};
      const st = ch.status || "SKIP";
      if (st === "FAIL") return "FAIL";
      if (st !== "PASS") has_skip = true;
    }
    return has_skip ? "SKIP" : "PASS";
  }

  function compareRuns(a, b) {
    const sa = overallStatus(a.checks || {});
    const sb = overallStatus(b.checks || {});
    const ra = STATUS_RANK[sa] ?? 2;
    const rb = STATUS_RANK[sb] ?? 2;
    if (ra !== rb) return ra - rb;
    const an = a.run || "";
    const bn = b.run || "";
    if (an < bn) return -1;
    if (an > bn) return 1;
    return 0;
  }

  function applyRunsOverviewFilters(runs) {
    const failOnlyEl = $("filterFailOnly");
    const hidePassEl = $("filterHidePass");
    const hideSkipEl = $("filterHideSkip");
    const failOnly = !!(failOnlyEl && failOnlyEl.checked);
    const hidePass = !!(hidePassEl && hidePassEl.checked);
    const hideSkip = !!(hideSkipEl && hideSkipEl.checked);

    return runs.filter((r) => {
      const st = overallStatus(r.checks || {});
      if (failOnly) return st === "FAIL";
      if (hidePass && st === "PASS") return false;
      if (hideSkip && st === "SKIP") return false;
      return true;
    });
  }

  function renderRunsOverviewTable(runs, totalCount) {
    const body = $("runsOverviewBody");
    body.innerHTML = "";
    if (!runs.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 8;
      td.className = "muted";
      td.textContent = totalCount ? "(no matching runs)" : "(no runs found)";
      tr.appendChild(td);
      body.appendChild(tr);
      return;
    }

    function factCell(value) {
      const td = document.createElement("td");
      const span = document.createElement("span");
      const ok = !!value;
      span.className = ok ? "fact-pill y" : "fact-pill n";
      span.textContent = ok ? "Y" : "N";
      td.appendChild(span);
      return td;
    }

    for (const r of runs) {
      const tr = document.createElement("tr");

      const tdRun = document.createElement("td");
      tdRun.className = "run-name";
      tdRun.textContent = r.run || "";
      tr.appendChild(tdRun);

      tr.appendChild(factCell(r.has_manifest));
      tr.appendChild(factCell(r.has_facts));
      tr.appendChild(factCell(r.has_bundle));

      const checks = r.checks || {};
      const names = ["verify_run", "qa", "validate"];
      for (const name of names) {
        const td = document.createElement("td");
        const ch = checks[name] || {};
        const b = document.createElement("span");
        b.className = badgeClass(ch.status || "SKIP");
        b.textContent = ch.status || "SKIP";
        td.appendChild(b);
        tr.appendChild(td);
      }

      const tdOpen = document.createElement("td");
      const btn = document.createElement("button");
      btn.textContent = "Open";
      btn.addEventListener("click", async () => {
        const sel = $("runSelect");
        if (sel) {
          const values = new Set(Array.from(sel.options).map(o => o.value));
          if (!values.has(r.run || "")) {
            await loadRuns();
          }
          sel.value = r.run || "";
        }
        await refreshSummaryAndTree();
        await refreshStatus();
      });
      tdOpen.appendChild(btn);
      tr.appendChild(tdOpen);

      body.appendChild(tr);
    }
  }

  function applyRunsOverviewView() {
    const data = runsOverviewData || {};
    const totalCount = Array.isArray(data.runs) ? data.runs.length : 0;
    let runs = Array.isArray(data.runs) ? data.runs.slice() : [];
    runs.sort(compareRuns);
    runs = applyRunsOverviewFilters(runs);
    renderRunsOverviewTable(runs, totalCount);
  }

  function renderGov(checks) {
    const host = $("govHost");
    host.innerHTML = "";
    const names = ["verify_run", "qa", "validate"];
    for (const name of names) {
      const ch = checks[name] || {};
      const row = document.createElement("div");
      row.className = "checkrow";

      const left = document.createElement("div");
      left.className = "mono";
      left.textContent = name;

      const right = document.createElement("div");
      right.className = "row";

      const b = document.createElement("span");
      b.className = badgeClass(ch.status || "SKIP");
      b.textContent = ch.status || "SKIP";

      const s = document.createElement("span");
      s.className = "mono small";
      s.textContent = ch.summary || "";

      right.appendChild(b);
      right.appendChild(s);

      row.appendChild(left);
      row.appendChild(right);
      host.appendChild(row);
    }
  }

  function renderRunsOverview(data) {
    runsOverviewData = data || { runs: [] };
    applyRunsOverviewView();
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
          const data = await api(withRun(`/api/file?path=${q}`));
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

  async function loadRuns() {
    $("statusText").textContent = "Loading runs...";
    try {
      const data = await api("/api/runs");
      $("baseDirPill").textContent = data.base_dir || "";
      const runs = data.runs || [];
      const sel = $("runSelect");
      const prev = sel.value || "";
      sel.innerHTML = "";

      // Always include default
      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = "(default run_dir)";
      sel.appendChild(opt0);

      for (const r of runs) {
        const opt = document.createElement("option");
        opt.value = r.run || "";
        opt.textContent = r.run || "";
        sel.appendChild(opt);
      }

      // restore selection if still present
      const values = new Set(Array.from(sel.options).map(o => o.value));
      if (values.has(prev)) sel.value = prev;
      else sel.value = "";

      $("statusText").textContent = "";
    } catch (e) {
      $("statusText").textContent = `ERROR: ${e}`;
    } finally {
      setTimeout(() => { $("statusText").textContent = ""; }, 800);
    }
  }

  async function refreshSummaryAndTree() {
    $("statusText").textContent = "Refreshing...";
    try {
      const summary = await api(withRun("/api/run/summary"));
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

      const tree = await api(withRun("/api/tree"));
      const host = $("treeHost");
      host.innerHTML = "";
      host.appendChild(renderTreeNode(tree, ""));
    } catch (e) {
      $("statusText").textContent = `ERROR: ${e}`;
    } finally {
      setTimeout(() => { $("statusText").textContent = ""; }, 800);
    }
  }

  async function refreshStatus() {
    $("govMeta").textContent = "Running...";
    try {
      const st = await api(withRun("/api/status"));
      renderGov(st.checks || {});
      $("govMeta").textContent = `elapsed_ms=${st.elapsed_ms || 0}`;
    } catch (e) {
      $("govMeta").textContent = `ERROR: ${e}`;
    } finally {
      setTimeout(() => { $("govMeta").textContent = ""; }, 1500);
    }
  }

  async function refreshRunsOverview() {
    $("runsOverviewMeta").textContent = "Loading...";
    try {
      const data = await api("/api/runs/overview");
      renderRunsOverview(data);
      $("runsOverviewMeta").textContent = `elapsed_ms=${data.elapsed_ms || 0} budget_ms=${data.budget_ms || 0}`;
    } catch (e) {
      $("runsOverviewMeta").textContent = `ERROR: ${e}`;
    } finally {
      setTimeout(() => { $("runsOverviewMeta").textContent = ""; }, 1500);
    }
  }

  $("reloadRunsBtn").addEventListener("click", async () => {
    await loadRuns();
  });

  $("runSelect").addEventListener("change", async () => {
    await refreshSummaryAndTree();
    await refreshStatus();
  });

  $("refreshBtn").addEventListener("click", async () => {
    await refreshSummaryAndTree();
  });

  $("statusBtn").addEventListener("click", async () => {
    await refreshStatus();
  });

  $("runsOverviewBtn").addEventListener("click", async () => {
    await refreshRunsOverview();
  });

  const filterIds = ["filterFailOnly", "filterHidePass", "filterHideSkip"];
  for (const id of filterIds) {
    const el = $(id);
    if (el) {
      el.addEventListener("change", () => {
        applyRunsOverviewView();
      });
    }
  }

  // boot
  (async () => {
    await loadRuns();
    await refreshSummaryAndTree();
    await refreshStatus();
    await refreshRunsOverview();
  })();
</script>
</body>
</html>
"""


@dataclass(frozen=True)
class UiConfig:
    run_dir: Path
    base_dir: Path
    host: str
    port: int
    max_file_bytes: int
    cmd_timeout_s: float
    runs_status_budget_ms: int


def _compute_governance_checks(run_dir: Path, cfg: UiConfig) -> dict[str, Any]:
    facts_path = run_dir / "facts" / "facts.jsonl"

    vr_res = _run_cli(
        _base_py_cli() + ["verify-run", "--run-dir", str(run_dir)],
        timeout_s=cfg.cmd_timeout_s,
    )
    vr_status, vr_summary = _status_from_run_result(vr_res)

    checks: dict[str, Any] = {
        "verify_run": {
            "status": vr_status,
            "exit_code": vr_res.get("exit_code"),
            "summary": vr_summary,
            "elapsed_ms": vr_res.get("elapsed_ms"),
        }
    }

    if not facts_path.exists():
        checks["qa"] = {"status": "SKIP", "exit_code": None, "summary": "facts missing", "elapsed_ms": 0}
        checks["validate"] = {"status": "SKIP", "exit_code": None, "summary": "facts missing", "elapsed_ms": 0}
        return checks

    qa_res = _run_cli(
        _base_py_cli() + ["qa", "--run-dir", str(run_dir)],
        timeout_s=cfg.cmd_timeout_s,
    )
    qa_status, qa_summary = _status_from_run_result(qa_res)
    checks["qa"] = {
        "status": qa_status,
        "exit_code": qa_res.get("exit_code"),
        "summary": qa_summary,
        "elapsed_ms": qa_res.get("elapsed_ms"),
    }

    val_res = _run_cli(
        _base_py_cli() + ["validate", "--facts", str(facts_path)],
        timeout_s=cfg.cmd_timeout_s,
    )
    val_status, val_summary = _status_from_run_result(val_res)
    checks["validate"] = {
        "status": val_status,
        "exit_code": val_res.get("exit_code"),
        "summary": val_summary,
        "elapsed_ms": val_res.get("elapsed_ms"),
    }
    return checks


def _skip_checks(summary: str) -> dict[str, Any]:
    return {
        "verify_run": {"status": "SKIP", "exit_code": None, "summary": summary, "elapsed_ms": 0},
        "qa": {"status": "SKIP", "exit_code": None, "summary": summary, "elapsed_ms": 0},
        "validate": {"status": "SKIP", "exit_code": None, "summary": summary, "elapsed_ms": 0},
    }


class UiHandler(BaseHTTPRequestHandler):
    server_version = "crimex-ui/phase1d"

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
        if hasattr(self.server, "_verbose") and self.server._verbose:
            super().log_message(fmt, *args)

    @property
    def cfg(self) -> UiConfig:
        return self.server._cfg

    def _selected_run_dir_from_query(self, query: str) -> tuple[Path, str | None]:
        """
        Return (selected_run_dir, error_message). Defaults to cfg.run_dir if no run param.
        """
        params = urllib.parse.parse_qs(query, keep_blank_values=True)
        run_raw = (params.get("run") or [""])[0].strip()
        if not run_raw:
            return self.cfg.run_dir, None

        try:
            run_rel = _safe_relpath(run_raw)
        except ValueError as e:
            return self.cfg.run_dir, str(e)

        base = self.cfg.base_dir
        target = (base / run_rel).resolve(strict=False)
        if not _is_under(base, target):
            return self.cfg.run_dir, "path traversal not allowed"

        if not target.exists() or not target.is_dir():
            return self.cfg.run_dir, "run directory not found"

        return target, None

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._send(HTTPStatus.OK, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return

        if path == "/health":
            self._send_json(HTTPStatus.OK, {"ok": True})
            return

        if path == "/api/runs":
            self._handle_runs()
            return

        if path == "/api/runs/status":
            self._handle_runs_status()
            return

        if path == "/api/runs/overview":
            self._handle_runs_overview()
            return

        if path == "/api/run/summary":
            self._handle_summary(parsed.query)
            return

        if path == "/api/status":
            self._handle_status(parsed.query)
            return

        if path == "/api/tree":
            self._handle_tree(parsed.query)
            return

        if path == "/api/file":
            self._handle_file(parsed.query)
            return

        self._send_text(HTTPStatus.NOT_FOUND, "not found")

    def _handle_runs(self) -> None:
        """
        Deterministically list immediate child directories under base_dir.
        We don't use mtime or OS-dependent ordering. Only lexicographic.
        """
        base = self.cfg.base_dir
        runs: list[dict[str, Any]] = []
        for d in _iter_run_dirs(base):
            runs.append(_run_meta(base, d))

        self._send_json(
            HTTPStatus.OK,
            {
                "base_dir": str(base),
                "runs": runs,
            },
        )

    def _handle_runs_status(self) -> None:
        base = self.cfg.base_dir
        t0 = _now_monotonic_ms()
        runs_out: list[dict[str, Any]] = []
        run_dirs = _iter_run_dirs(base)
        budget_ms = max(0, int(self.cfg.runs_status_budget_ms))

        for idx, d in enumerate(run_dirs):
            elapsed = _now_monotonic_ms() - t0
            if elapsed >= budget_ms:
                for r in run_dirs[idx:]:
                    runs_out.append(
                        {
                            "run": _as_posix_rel(base, r),
                            "checks": _skip_checks("TIME BUDGET EXCEEDED"),
                        }
                    )
                break

            checks = _compute_governance_checks(d, self.cfg)
            runs_out.append({"run": _as_posix_rel(base, d), "checks": checks})

        elapsed_total = _now_monotonic_ms() - t0
        self._send_json(
            HTTPStatus.OK,
            {
                "base_dir": str(base),
                "runs": runs_out,
                "elapsed_ms": int(elapsed_total),
            },
        )

    def _handle_runs_overview(self) -> None:
        base = self.cfg.base_dir
        t0 = _now_monotonic_ms()
        runs_out: list[dict[str, Any]] = []
        run_dirs = _iter_run_dirs(base)
        budget_ms = max(0, int(self.cfg.runs_status_budget_ms))

        for idx, d in enumerate(run_dirs):
            elapsed = _now_monotonic_ms() - t0
            if elapsed >= budget_ms:
                for r in run_dirs[idx:]:
                    meta = _run_meta(base, r)
                    meta["checks"] = _skip_checks("TIME BUDGET EXCEEDED")
                    runs_out.append(meta)
                break

            meta = _run_meta(base, d)
            meta["checks"] = _compute_governance_checks(d, self.cfg)
            runs_out.append(meta)

        elapsed_total = _now_monotonic_ms() - t0
        self._send_json(
            HTTPStatus.OK,
            {
                "base_dir": str(base),
                "runs": runs_out,
                "elapsed_ms": int(elapsed_total),
                "budget_ms": int(budget_ms),
            },
        )

    def _handle_summary(self, query: str) -> None:
        run_dir, err = self._selected_run_dir_from_query(query)
        if err is not None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return

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

    def _handle_status(self, query: str) -> None:
        run_dir, err = self._selected_run_dir_from_query(query)
        if err is not None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return

        t0 = _now_monotonic_ms()
        checks = _compute_governance_checks(run_dir, self.cfg)
        elapsed = _now_monotonic_ms() - t0
        out = {
            "run_dir": str(run_dir),
            "checks": checks,
            "elapsed_ms": int(elapsed),
        }
        self._send_json(HTTPStatus.OK, out)

    def _handle_tree(self, query: str) -> None:
        run_dir, err = self._selected_run_dir_from_query(query)
        if err is not None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return
        tree = _build_tree(run_dir)
        self._send_json(HTTPStatus.OK, tree)

    def _handle_file(self, query: str) -> None:
        params = urllib.parse.parse_qs(query, keep_blank_values=True)

        # Resolve run dir first (Phase 1C)
        run_dir, err = self._selected_run_dir_from_query(query)
        if err is not None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return

        raw_path = (params.get("path") or [""])[0]

        try:
            rel = _safe_relpath(raw_path)
        except ValueError as e:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        target = (run_dir / rel).resolve(strict=False)
        if not _is_under(run_dir, target):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "path traversal not allowed"})
            return

        if not target.exists() or not target.is_file():
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "file not found", "path": rel})
            return

        mime, _ = mimetypes.guess_type(str(target))
        if mime and not mime.startswith(("text/", "application/json", "application/xml")):
            self._send_json(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {
                    "error": "binary or unsupported file type in Phase 0/1A/1B",
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


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m crimex.ui.server",
        description="crimex UI (Phase 1D): run picker + governance status (stdlib-only).",
    )
    p.add_argument("--run-dir", required=True, help="Path to an existing run directory.")
    p.add_argument(
        "--base-dir",
        default=None,
        help="Base directory containing multiple run directories (default: parent of --run-dir).",
    )
    p.add_argument("--host", default="127.0.0.1", help="Bind host (default 127.0.0.1).")
    p.add_argument("--port", type=int, default=0, help="Bind port (0 picks a free port).")
    p.add_argument("--max-file-bytes", type=int, default=500_000, help="Max bytes served per file (default 500k).")
    p.add_argument(
        "--cmd-timeout-s",
        type=float,
        default=2.5,
        help="Timeout seconds per governance check command (default 2.5s).",
    )
    p.add_argument(
        "--runs-status-budget-ms",
        type=int,
        default=2000,
        help="Total time budget for /api/runs/status and /api/runs/overview in ms (default 2000).",
    )
    p.add_argument("--verbose", action="store_true", help="Enable request logging.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)

    run_dir = Path(args.run_dir).resolve(strict=False)
    if not run_dir.exists() or not run_dir.is_dir():
        print(f"ERROR: run_dir does not exist or is not a directory: {run_dir}", file=sys.stderr)
        return 2

    base_dir = Path(args.base_dir).resolve(strict=False) if args.base_dir else run_dir.parent.resolve(strict=False)

    if not base_dir.exists() or not base_dir.is_dir():
        print(f"ERROR: base_dir does not exist or is not a directory: {base_dir}", file=sys.stderr)
        return 2

    host = str(args.host)
    port = int(args.port)
    if port == 0:
        port = _pick_free_port(host)

    cfg = UiConfig(
        run_dir=run_dir,
        base_dir=base_dir,
        host=host,
        port=port,
        max_file_bytes=int(args.max_file_bytes),
        cmd_timeout_s=float(args.cmd_timeout_s),
        runs_status_budget_ms=int(args.runs_status_budget_ms),
    )

    httpd = ThreadingHTTPServer((cfg.host, cfg.port), UiHandler)
    httpd._cfg = cfg
    httpd._verbose = bool(args.verbose)

    print("crimex UI (Phase 1D) running (read-only)")
    print(f"base_dir: {cfg.base_dir}")
    print(f"default run_dir: {cfg.run_dir}")
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
