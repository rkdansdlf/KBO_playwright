"""Regression tests for the alert-transport-bypass lint gate."""

from __future__ import annotations

import contextlib
import io
from pathlib import Path

from scripts.lint_alert_transport_bypass import (
    ALLOWED_FILES,
    CLASSIFICATION,
    GRANDFATHERED,
    main as lint_main,
)
from scripts.lint_alert_transport_bypass import scan as scan_file

ROOT = Path(__file__).resolve().parents[2]


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "sample.py"
    path.write_text(body, encoding="utf-8")
    return path


def test_direct_client_import_is_detected(tmp_path: Path) -> None:
    path = _write(tmp_path, "from src.utils.alerting import SlackWebhookClient\n")
    assert scan_file(path) == [1]


def test_module_import_is_detected(tmp_path: Path) -> None:
    path = _write(tmp_path, "import src.utils.alerting\n")
    assert scan_file(path) == [1]


def test_generic_webhook_import_is_detected(tmp_path: Path) -> None:
    path = _write(tmp_path, "from src.utils.alerting import GenericWebhookClient\n")
    assert scan_file(path) == [1]


def test_other_alerting_symbols_are_allowed(tmp_path: Path) -> None:
    path = _write(tmp_path, "from src.utils.alerting import GAP_EMOJI_MAP\n")
    assert scan_file(path) == []


def test_bypass_marker_suppresses(tmp_path: Path) -> None:
    path = _write(tmp_path, "# alert-transport-bypass: reason\nfrom src.utils.alerting import SlackWebhookClient\n")
    assert scan_file(path) == []


def test_allowed_files_are_not_reported(tmp_path: Path) -> None:
    for allowed in ALLOWED_FILES:
        assert (ROOT / allowed).exists(), f"allowlisted file missing: {allowed}"


def test_only_the_two_sanctioned_callers_are_allowlisted() -> None:
    """Pin ALLOWED_FILES, because widening it is a bypass no assertion catches.

    `lint_main` skips an allowlisted path before it is ever scanned
    (`if relative in ALLOWED_FILES: continue`), so a third entry would silence
    any violation in that file: exit 0, no ERROR line, no `[grandfathered]`
    line. `test_migration_is_complete` cannot see it, because it only rules out
    the exemption list. Existence is therefore not enough — the set itself has
    to be asserted.
    """
    actual = ALLOWED_FILES  # lowercase alias; SIM300 reads UPPER_CASE as the constant side
    assert actual == frozenset(
        {
            "src/utils/alerting.py",
            "src/notifications/dispatcher.py",
        },
    ), f"ALLOWED_FILES widened; every listed file is unlinted by design: {sorted(actual)}"


def test_grandfathered_files_exist() -> None:
    for grandfathered in GRANDFATHERED:
        assert (ROOT / grandfathered).exists(), f"grandfathered file missing: {grandfathered}"


def test_repository_is_clean() -> None:
    assert lint_main([]) == 0


def test_violation_returns_one(tmp_path: Path) -> None:
    path = _write(tmp_path, "from src.utils.alerting import TelegramBotClient\n")
    assert lint_main([str(path)]) == 1


def test_migration_is_complete() -> None:
    """Prove the empty exemption lists mean "no bypass exists", not "unlisted".

    The exit code alone cannot carry that claim: a file with a brand new
    violation that also happens to sit in ``GRANDFATHERED`` still exits 0. Only
    stdout separates "the tree is clean" from "the tree is hiding behind an
    exemption", which is why the per-file assertions on GRANDFATHERED and
    CLASSIFICATION below are vacuous on their own once both are empty.
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = lint_main([])
    output = buffer.getvalue()

    assert exit_code == 0, f"transport bypass lint failed:\n{output}"
    assert not GRANDFATHERED, f"exemption list is not empty: {sorted(GRANDFATHERED)}"
    assert not CLASSIFICATION, f"classification list is not empty: {CLASSIFICATION}"
    assert "[grandfathered]" not in output, f"a file is still exempt:\n{output}"
    assert "ERROR:" not in output, f"lint reported a violation:\n{output}"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-q"])
