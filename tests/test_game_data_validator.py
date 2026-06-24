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
    valid, errors, warnings = validate_game_data(payload)
    assert valid, f"Expected payload to be valid, got errors: {errors}"


@pytest.mark.skipif(not FIXTURE_FILE.exists(), reason="Mock HTML fixture not available")
def test_validate_game_data_detects_score_mismatch():
    html = FIXTURE_FILE.read_text(encoding="utf-8")
    payload = parse_game_detail_html(html, "20251001NCLG0", "20251001")
    payload["teams"]["home"]["score"] = (payload["teams"]["home"]["score"] or 0) + 1
    valid, errors, warnings = validate_game_data(payload)
    assert valid
    assert any("home" in warn and "hitter runs" in warn for warn in warnings)


@pytest.mark.skipif(not FIXTURE_FILE.exists(), reason="Mock HTML fixture not available")
def test_validate_game_data_detects_invalid_team_code():
    html = FIXTURE_FILE.read_text(encoding="utf-8")
    payload = parse_game_detail_html(html, "20251001NCLG0", "20251001")
    payload["teams"]["home"]["code"] = "KI"
    valid, errors, warnings = validate_game_data(payload)
    assert not valid
    assert any("Invalid home team code: 'KI'" in err for err in errors)


class TestValidateGameDataPure:
    def test_valid_game(self) -> None:
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "2025-04-15",
            "teams": {
                "home": {"code": "SS", "score": 3, "line_score": [1, 1, 1]},
                "away": {"code": "LG", "score": 2, "line_score": [0, 1, 1]},
            },
            "hitters": {
                "home": [{"stats": {"runs": 3}}],
                "away": [{"stats": {"runs": 2}}],
            },
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, warnings = validate_game_data(game)
        assert is_valid
        assert errors == []

    def test_missing_game_id(self) -> None:
        game = {
            "game_date": "2025-04-15",
            "teams": {"home": {"code": "SS"}, "away": {"code": "LG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game)
        assert not is_valid
        assert "Missing game_id" in errors

    def test_missing_game_date(self) -> None:
        game = {
            "game_id": "20250415LGSS0",
            "teams": {"home": {"code": "SS"}, "away": {"code": "LG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game)
        assert not is_valid
        assert "Missing game_date" in errors

    def test_invalid_home_team_code(self) -> None:
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "2025-04-15",
            "teams": {"home": {"code": "XX"}, "away": {"code": "LG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game)
        assert not is_valid
        assert any("Invalid home team code" in e for e in errors)

    def test_no_hitter_rows(self) -> None:
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "2025-04-15",
            "teams": {"home": {"code": "SS"}, "away": {"code": "LG"}},
            "hitters": {"home": [], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game)
        assert not is_valid
        assert "No hitter rows for home" in errors

    def test_runs_mismatch_warning(self) -> None:
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "2025-04-15",
            "teams": {
                "home": {"code": "SS", "score": 5, "line_score": [2, 2, 1]},
                "away": {"code": "LG", "score": 2, "line_score": [1, 0, 1]},
            },
            "hitters": {
                "home": [{"stats": {"runs": 3}}],
                "away": [{"stats": {"runs": 2}}],
            },
            "pitchers": {"home": [{}], "away": [{}]},
        }
        _, _, warnings = validate_game_data(game)
        assert any("hitter runs" in w for w in warnings)

    def test_multiple_errors(self) -> None:
        game = {
            "teams": {},
            "hitters": {"home": [], "away": []},
            "pitchers": {"home": [], "away": []},
        }
        is_valid, errors, _ = validate_game_data(game)
        assert not is_valid
        assert len(errors) >= 3
