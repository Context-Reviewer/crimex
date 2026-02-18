from pathlib import Path

from crimex.run import RunContext
from crimex.verify_run import verify_run


def test_verify_run_ok(tmp_path: Path) -> None:
    run = RunContext(base_out=tmp_path, run_id="vr_ok")
    log_path = run.logs_dir() / "run.log"
    log_path.write_text("hello\n", encoding="utf-8")

    run.register_artifact(log_path)
    run.write_manifest()

    result = verify_run(run.path)
    assert result.ok
    assert result.checked == 1
    assert result.errors == []


def test_verify_run_detects_mismatch(tmp_path: Path) -> None:
    run = RunContext(base_out=tmp_path, run_id="vr_bad")
    log_path = run.logs_dir() / "run.log"
    log_path.write_text("hello\n", encoding="utf-8")

    run.register_artifact(log_path)
    run.write_manifest()

    # Mutate after manifest to force mismatch
    log_path.write_text("changed\n", encoding="utf-8")

    result = verify_run(run.path)
    assert not result.ok
    assert result.checked == 1
    assert any("Hash mismatch" in e for e in result.errors)
