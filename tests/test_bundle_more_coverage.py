from __future__ import annotations

import zipfile
from pathlib import Path

from crimex.bundle import bundle_content_fingerprint


def _make_zip(zip_path: Path, files: dict[str, str]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Ensure deterministic entry order
        for name in sorted(files.keys()):
            zf.writestr(name, files[name])


def test_bundle_content_fingerprint_is_stable_for_same_zip(tmp_path: Path) -> None:
    zip_path = tmp_path / "bundle.zip"

    _make_zip(zip_path, {"a.txt": "hello\n", "b.txt": "world\n"})

    fp1 = bundle_content_fingerprint(zip_path)
    fp2 = bundle_content_fingerprint(zip_path)

    assert fp1 == fp2


def test_bundle_content_fingerprint_changes_on_content_change(tmp_path: Path) -> None:
    zip_path = tmp_path / "bundle.zip"

    _make_zip(zip_path, {"a.txt": "hello\n", "b.txt": "world\n"})
    fp1 = bundle_content_fingerprint(zip_path)

    _make_zip(zip_path, {"a.txt": "HELLO\n", "b.txt": "world\n"})
    fp2 = bundle_content_fingerprint(zip_path)

    assert fp1 != fp2


def test_bundle_content_fingerprint_changes_on_file_add_remove(tmp_path: Path) -> None:
    zip_path = tmp_path / "bundle.zip"

    _make_zip(zip_path, {"a.txt": "hello\n"})
    fp1 = bundle_content_fingerprint(zip_path)

    _make_zip(zip_path, {"a.txt": "hello\n", "b.txt": "world\n"})
    fp2 = bundle_content_fingerprint(zip_path)

    assert fp1 != fp2
