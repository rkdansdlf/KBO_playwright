from pathlib import Path

import pytest

from src.parsers.game_detail_parser import parse_game_detail_html
from src.validators.game_data_validator import validate_game_data

FIXTURE_DIR = Path("tests/fixtures/game_details")
FIXTURE_FILE = FIXTURE_DIR / "20251001NCLG0.html"


@pytest.mark.skipif(not FIXTURE_FILE.exists(), reason="Mock HTML fixture not available")
def test_validate_game_data_ok():
    html = FIXTURE_FILE.read_text(encoding="utf-8")
    payload = parse_game_detail_html(html, "20251001NCLG0", "20251001")
    valid, errors = validate_game_data(payload)
    assert valid, f"Expected payload to be valid, got errors: {errors}"


@pytest.mark.skipif(not FIXTURE_FILE.exists(), reason="Mock HTML fixture not available")
def test_validate_game_data_detects_score_mismatch():
    html = FIXTURE_FILE.read_text(encoding="utf-8")
    payload = parse_game_detail_html(html, "20251001NCLG0", "20251001")
    payload["teams"]["home"]["score"] = (payload["teams"]["home"]["score"] or 0) + 1
    valid, errors = validate_game_data(payload)
    assert not valid
    assert any("home" in err and "team score" in err for err in errors)

