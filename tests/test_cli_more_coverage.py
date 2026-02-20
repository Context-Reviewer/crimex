from __future__ import annotations

import pytest

import crimex.cli as cli


def test_main_with_no_args_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["crimex"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 1


