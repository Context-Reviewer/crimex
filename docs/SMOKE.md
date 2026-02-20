# SMOKE: Canonical Offline Governed Run

**Purpose**

Define the "smallest successful governed run" as an end-to-end, offline, deterministic smoke that exercises the full governance chain. It must:

- Be offline (no network I/O)
- Be deterministic (fixtures and outputs are stable)
- Require `verify-run`
- Require `bundle`

**Prerequisites**

- Windows + PowerShell
- `.venv` active
- `crimex` CLI available
- Fixtures present in repo under `fixtures/smoke/...`
- `FBI_API_KEY` must be set (dummy value is fine; no network occurs)

**Canonical Smoke Procedure**

Clean and stage fixtures (note: `output/` is a runtime-only directory and is ignored by git):

```powershell
$RunDir = Join-Path $PWD "output\smoke_run"
$BundleDir = Join-Path $PWD "output\smoke_bundle"
$FixtureDir = Join-Path $PWD "fixtures\smoke\fbi_cde"

Remove-Item -Recurse -Force $RunDir, $BundleDir -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $RunDir, $BundleDir | Out-Null

$RawDir = Join-Path $RunDir "raw\fbi_cde"
New-Item -ItemType Directory -Force -Path $RawDir | Out-Null
Copy-Item -Force (Join-Path $FixtureDir "*") $RawDir

New-Item -ItemType Directory -Force -Path (Join-Path $RunDir "logs"), (Join-Path $RunDir "reports"), (Join-Path $RunDir "facts") | Out-Null
Set-Content -Path (Join-Path $RunDir "logs\run.log") -Value "smoke run" -Encoding utf8

$env:FBI_API_KEY = "SMOKE"
```

Run the canonical chain (exact CLI flags):

```powershell
crimex fetch     --spec specs\smoke.json --out $RunDir
crimex normalize --raw (Join-Path $RunDir "raw") --out (Join-Path $RunDir "facts\facts.jsonl")
crimex report    --facts (Join-Path $RunDir "facts\facts.jsonl") --out (Join-Path $RunDir "reports")
crimex manifest  --root $RunDir --out (Join-Path $RunDir "run_manifest.json")
crimex validate  --facts (Join-Path $RunDir "facts\facts.jsonl")
crimex verify-run --run-dir $RunDir
crimex bundle    --run-dir $RunDir
Copy-Item -Force (Join-Path $RunDir "run_bundle.zip") (Join-Path $BundleDir "run_bundle.zip")
```

**Expected Run Directory Structure**

- `raw/fbi_cde/*` (raw + meta + receipt)
- `facts/facts.jsonl`
- `reports/report.csv`
- `reports/report.md`
- `logs/run.log`
- `run_manifest.json`
- `run_bundle.zip`

**Deterministic Postconditions**

- `verify-run` exits 0 and verifies all manifest artifacts
- `run_bundle.zip` exists and is non-empty
- No network calls are required; smoke should succeed with no internet connectivity as long as fixtures are present

**CI Guidance**

- Use a dedicated job for smoke
- Set `FBI_API_KEY=SMOKE`
- Run the same chain as above
- Never cache/commit `output/`
- Keep `fixtures/smoke/` in repo

**Troubleshooting**

- Missing `FBI_API_KEY`: the connector guard will fail early; set a dummy value
- Cache miss (fetch tries network): confirm fixture filenames match the cache key/fingerprint used by `specs/smoke.json`
- `verify-run` failure: manifest and raw tree mismatch; re-stage fixtures and rerun the chain
