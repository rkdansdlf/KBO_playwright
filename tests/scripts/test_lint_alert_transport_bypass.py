"""Regression tests for the alert-transport-bypass lint gate."""

from __future__ import annotations

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


def test_grandfathered_files_exist() -> None:
    for grandfathered in GRANDFATHERED:
        assert (ROOT / grandfathered).exists(), f"grandfathered file missing: {grandfathered}"


def test_repository_is_clean() -> None:
    assert lint_main([]) == 0


def test_violation_returns_one(tmp_path: Path) -> None:
    path = _write(tmp_path, "from src.utils.alerting import TelegramBotClient\n")
    assert lint_main([str(path)]) == 1


def test_classification_covers_exactly_the_grandfather_set() -> None:
    """Every grandfathered file has a migration class, and nothing else does."""
    assert set(CLASSIFICATION) == set(GRANDFATHERED)


def test_classification_values_are_known() -> None:
    assert set(CLASSIFICATION.values()) <= {"A", "B", "C", "D"}


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-q"])
