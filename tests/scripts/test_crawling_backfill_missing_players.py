from unittest.mock import MagicMock, patch

from scripts.crawling.backfill_missing_players import (
    Candidate,
    _build_payload,
    _clean_optional_photo_url,
    _normalize_status,
    find_candidates,
    parse_args,
    parse_player_ids,
)


class TestParsePlayerIds:
    def test_empty(self):
        assert parse_player_ids(None) == []
        assert parse_player_ids("") == []

    def test_single(self):
        assert parse_player_ids("123") == [123]

    def test_multiple(self):
        assert parse_player_ids("1, 2, 3") == [1, 2, 3]


class TestCleanOptionalPhotoUrl:
    def test_none_or_no_image(self):
        assert _clean_optional_photo_url(None) is None
        assert _clean_optional_photo_url("http://example.com/no-Image.png") is None

    def test_valid(self):
        assert _clean_optional_photo_url("http://example.com/photo.jpg") == "http://example.com/photo.jpg"


class TestNormalizeStatus:
    def test_none(self):
        assert _normalize_status(None) == "backfilled"

    def test_stripped(self):
        assert _normalize_status("  ACTIVE  ") == "active"


class TestBuildPayload:
    def test_basic(self):
        candidate = Candidate(player_id=1, team_code="LG", existing_name="Foo", position="P", source="batting")
        profile = {"name": "Foo", "team": "LG", "position": "P", "status": "active", "status_source": "profile"}
        payload = _build_payload(candidate, profile)
        assert payload["player_id"] == 1
        assert payload["name"] == "Foo"
        assert payload["team"] == "LG"

    def test_fallback_team(self):
        candidate = Candidate(player_id=2, team_code="SSG", existing_name=None, position=None, source="batting")
        profile = {"status": "backfilled", "status_source": "profile_backfill"}
        payload = _build_payload(candidate, profile)
        assert payload["team"] == "SSG"


class TestFindCandidates:
    @patch("scripts.crawling.backfill_missing_players.SessionLocal")
    def test_no_candidates(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        result = find_candidates()
        assert result == []


class TestParseArgs:
    @patch("scripts.crawling.backfill_missing_players.argparse.ArgumentParser.parse_args")
    def test_defaults(self, mock_parse):
        mock_parse.return_value = MagicMock(
            include_pitching=False,
            include_unknown_stubs=False,
            apply=False,
            limit=None,
            ids=None,
            delay=1.0,
            report_dir="data/player_profile_backfill",
        )
        args = parse_args()
        assert args.limit is None
        assert args.apply is False
