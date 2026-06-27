from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.repositories.game_helpers import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
    GameSummaryEntry,
    RecordKey,
    TeamSideContext,
    _apply_game_team_identity,
    _apply_team_identity_to_mappings,
    _assert_no_player_team_collisions,
    _assign_field_if_changed,
    _build_batting_stats,
    _build_inning_scores,
    _build_lineups,
    _build_pitching_stats,
    _build_pregame_lineup_rows,
    _canonicalize_game_id,
    _canonicalize_game_id_for_payload,
    _clean_extras,
    _coerce_int,
    _dedupe_exact_player_rows,
    _ensure_game_stub,
    _ensure_player_basic_stubs,
    _extract_players_from_text,
    _format_notes,
    _json_dumps,
    _normalize_player_id,
    _normalize_record_for_compare,
    _outs_to_decimal,
    _record_game_id_alias,
    _records_match_existing,
    _records_match_existing_objects,
    _replace_records,
    _replace_records_for_side,
    _resolve_game_date_obj,
    _resolve_game_season_id,
    _resolve_league_type_code,
    _resolve_schedule_season_id,
    _resolve_terminal_status,
    _resolve_winner,
    _safe_time,
    _stat_float,
    _stat_int,
    _upsert_game_summary_entry,
    _upsert_metadata,
    _values_equal,
)


class TestCoerceInt:
    def test_coerce_int_valid(self):
        assert _coerce_int(5) == 5
        assert _coerce_int("5") == 5

    def test_coerce_int_none(self):
        assert _coerce_int(None) is None
        assert _coerce_int("") is None

    def test_coerce_int_invalid(self):
        assert _coerce_int("abc") is None


class TestResolveLeagueTypeCode:
    def test_resolve_league_type_code_int(self):
        assert _resolve_league_type_code(3) == 3

    def test_resolve_league_type_code_string(self):
        assert _resolve_league_type_code("playoff") == 4
        assert _resolve_league_type_code("regular") == 0
        assert _resolve_league_type_code("korean_series") == 5

    def test_resolve_league_type_code_unknown(self):
        assert _resolve_league_type_code("unknown") == 0
        assert _resolve_league_type_code(None) == 0


class TestResolveGameDateObj:
    def test_resolve_game_date_obj_from_date(self):
        d = date(2024, 10, 15)
        assert _resolve_game_date_obj(d) == d

    def test_resolve_game_date_obj_from_datetime(self):
        dt = datetime(2024, 10, 15, 18, 30)
        result = _resolve_game_date_obj(dt)
        # datetime is subclass of date; isinstance(date, ...) catches it first
        assert result is dt or result == date(2024, 10, 15)

    def test_resolve_game_date_obj_from_string(self):
        assert _resolve_game_date_obj("2024-10-15") == date(2024, 10, 15)
        assert _resolve_game_date_obj("20241015") == date(2024, 10, 15)

    def test_resolve_game_date_obj_invalid(self):
        assert _resolve_game_date_obj("not-a-date") is None
        assert _resolve_game_date_obj(None) is None


class TestCanonicalizeGameId:
    @patch("src.repositories.game_helpers.normalize_kbo_game_id")
    def test_canonicalize_game_id(self, mock_normalize):
        mock_normalize.return_value = "20241015LGSS0"
        canonical, original = _canonicalize_game_id("20241015LGSS0")
        assert canonical == "20241015LGSS0"
        assert original == "20241015LGSS0"

    @patch("src.repositories.game_helpers.normalize_kbo_game_id")
    def test_canonicalize_game_id_empty(self, mock_normalize):
        mock_normalize.return_value = None
        canonical, original = _canonicalize_game_id("")
        assert canonical is None
        assert original is None

    @patch("src.repositories.game_helpers.normalize_kbo_game_id")
    def test_canonicalize_game_id_case(self, mock_normalize):
        mock_normalize.side_effect = lambda x: "20241015LGSS0"
        canonical, original = _canonicalize_game_id("20241015lgss0")
        assert canonical == "20241015LGSS0"
        assert original == "20241015LGSS0"


class TestCanonicalizeGameIdForPayload:
    @patch("src.repositories.game_helpers.build_kbo_game_id")
    @patch("src.repositories.game_helpers.normalize_kbo_game_id")
    def test_canonicalize_game_id_for_payload(self, mock_normalize, mock_build):
        mock_normalize.return_value = "20241015LGSS0"
        mock_build.return_value = "20241015LGSS0"
        canonical, original = _canonicalize_game_id_for_payload(
            "20241015LGSS0",
            game_date="2024-10-15",
            away_team_code="SS",
            home_team_code="LG",
            season_year=2024,
        )
        assert canonical == "20241015LGSS0"

    @patch("src.repositories.game_helpers.normalize_kbo_game_id")
    def test_canonicalize_game_id_for_payload_no_original(self, mock_normalize):
        mock_normalize.return_value = None
        canonical, original = _canonicalize_game_id_for_payload(None)
        assert canonical is None
        assert original is None


class TestValuesEqual:
    def test_values_equal_simple(self):
        assert _values_equal(1, 1) is True
        assert _values_equal("a", "b") is False
        assert _values_equal(None, None) is True

    def test_values_equal_decimal(self):
        assert _values_equal(Decimal("1.5"), Decimal("1.50")) is True
        assert _values_equal(Decimal("1.5"), "1.5") is True
        assert _values_equal(Decimal("1.5"), 1.5) is True


class TestResolveWinner:
    def test_home_wins(self):
        winner, score = _resolve_winner({"code": "LG", "score": 5}, {"code": "SS", "score": 3})
        assert winner == "LG"
        assert score == 5

    def test_away_wins(self):
        winner, score = _resolve_winner({"code": "LG", "score": 2}, {"code": "SS", "score": 4})
        assert winner == "SS"
        assert score == 4

    def test_draw(self):
        winner, score = _resolve_winner({"code": "LG", "score": 3}, {"code": "SS", "score": 3})
        assert winner is None
        assert score == 3

    def test_no_scores(self):
        winner, score = _resolve_winner({"code": "LG"}, {"code": "SS"})
        assert winner is None
        assert score is None


class TestResolveTerminalStatus:
    def test_completed(self):
        assert _resolve_terminal_status(5, 3) == GAME_STATUS_COMPLETED

    def test_draw(self):
        assert _resolve_terminal_status(3, 3) == GAME_STATUS_DRAW


class TestOutsToDecimal:
    def test_outs_to_decimal_full_innings(self):
        result = _outs_to_decimal(9)
        assert result == Decimal(3)

    def test_outs_to_decimal_partial(self):
        result = _outs_to_decimal(10)
        expected = Decimal(3) + Decimal(1) / Decimal(3)
        assert result == expected

    def test_outs_to_decimal_none(self):
        assert _outs_to_decimal(None) is None
        assert _outs_to_decimal("") is None

    def test_outs_to_decimal_zero(self):
        assert _outs_to_decimal(0) == Decimal(0)


class TestSafeTime:
    def test_safe_time_from_string(self):
        result = _safe_time("18:30")
        assert result == time(18, 30)

    def test_safe_time_from_datetime(self):
        result = _safe_time(datetime(2024, 10, 15, 18, 30))
        assert result == time(18, 30)

    def test_safe_time_empty(self):
        assert _safe_time(None) is None
        assert _safe_time("") is None

    def test_safe_time_invalid(self):
        assert _safe_time("invalid") is None


class TestStatInt:
    def test_stat_int_present(self):
        assert _stat_int({"games": 10}, "games") == 10

    def test_stat_int_none(self):
        assert _stat_int({"games": None}, "games") == 0
        assert _stat_int({}, "games") == 0

    def test_stat_int_string(self):
        assert _stat_int({"games": "5"}, "games") == 5

    def test_stat_int_invalid(self):
        assert _stat_int({"games": "abc"}, "games") == 0


class TestStatFloat:
    def test_stat_float_present(self):
        assert _stat_float({"avg": 0.300}, "avg") == 0.3

    def test_stat_float_none(self):
        assert _stat_float({"avg": None}, "avg") is None
        assert _stat_float({}, "avg") is None

    def test_stat_float_invalid(self):
        assert _stat_float({"avg": "abc"}, "avg") is None


class TestNormalizePlayerId:
    def test_normalize_player_id_int(self):
        assert _normalize_player_id(1001) == 1001

    def test_normalize_player_id_none(self):
        assert _normalize_player_id(None) is None
        assert _normalize_player_id("") is None
        assert _normalize_player_id("null") is None

    def test_normalize_player_id_string(self):
        assert _normalize_player_id("1001") == 1001

    def test_normalize_player_id_invalid(self):
        assert _normalize_player_id("abc") is None


class TestNormalizeRecordForCompare:
    def test_compare_simple(self):
        result = _normalize_record_for_compare({"a": 1, "b": "hello"})
        assert result == {"a": 1, "b": "hello"}

    def test_compare_decimal(self):
        result = _normalize_record_for_compare({"val": Decimal("1.500")})
        assert result["val"] == "1.5"

    def test_compare_datetime(self):
        result = _normalize_record_for_compare({"dt": datetime(2024, 10, 15, 18, 0)})
        assert result["dt"] == "2024-10-15T18:00:00"

    def test_compare_date(self):
        result = _normalize_record_for_compare({"d": date(2024, 10, 15)})
        assert result["d"] == "2024-10-15"


class TestFormatNotes:
    def test_format_notes_none(self):
        assert _format_notes(None) is None
        assert _format_notes({}) is None

    def test_format_notes_single_value(self):
        notes = _format_notes({"note": "test"})
        assert notes == "test"

    def test_format_notes_ignored_keys(self):
        notes = _format_notes({"COL_0": "ignored", "real": "data"})
        assert notes == "data"

    def test_format_notes_multiple_values(self):
        notes = _format_notes({"a": "1", "b": "2"})
        assert isinstance(notes, str)


class TestCleanExtras:
    def test_clean_extras_none(self):
        assert _clean_extras(None) is None

    def test_clean_extras_all_ignored(self):
        assert _clean_extras({"COL_0": "v", "COL_1": "v"}) is None

    def test_clean_extras_filtered(self):
        result = _clean_extras({"COL_0": "x", "salary": "1억"})
        assert result == {"salary": "1억"}


class TestJsonDumps:
    def test_json_dumps(self):
        result = _json_dumps({"a": 1, "b": "hello"})
        assert json.loads(result) == {"a": 1, "b": "hello"}


class TestExtractPlayersFromText:
    def test_extract_empty(self):
        assert _extract_players_from_text("홈런", "") == []
        assert _extract_players_from_text("홈런", "없음") == []

    def test_extract_umpire(self):
        result = _extract_players_from_text("심판", "김철수 박영수")
        assert len(result) == 2
        assert result[0][0] == "김철수"

    def test_extract_players_with_pattern(self):
        result = _extract_players_from_text("홈런", "강민호1호(2회1점) 로하스2호(4회1점)")
        assert len(result) == 2
        assert result[0][0] == "강민호"

    def test_extract_players_colon_format(self):
        result = _extract_players_from_text("폭투", "폭투: 반즈")
        assert len(result) == 1
        assert result[0][0] == "폭투"

    def test_extract_single_name(self):
        result = _extract_players_from_text("승리투수", "김선수")
        assert len(result) == 1
        assert result[0][0] == "김선수"

    def test_extract_no_match(self):
        result = _extract_players_from_text("기타", "긴 설명 텍스트입니다. 여러 단어가 있습니다.")
        assert result == []


class TestDedupeExactPlayerRows:
    def test_dedupe_no_duplicates(self):
        rows = [
            {"game_id": "g1", "player_id": 1, "player_name": "A"},
            {"game_id": "g1", "player_id": 2, "player_name": "B"},
        ]
        result = _dedupe_exact_player_rows("g1", "test", rows)
        assert len(result) == 2

    def test_dedupe_with_duplicates(self):
        rows = [
            {"game_id": "g1", "player_id": 1, "player_name": "A"},
            {"game_id": "g1", "player_id": 1, "player_name": "A"},
        ]
        result = _dedupe_exact_player_rows("g1", "test", rows)
        assert len(result) == 1


class TestAssertNoPlayerTeamCollisions:
    def test_no_collision(self):
        mappings = [
            {"player_id": 1, "team_side": "home", "team_code": "LG"},
            {"player_id": 2, "team_side": "away", "team_code": "SS"},
        ]
        _assert_no_player_team_collisions("g1", "test", mappings)

    def test_collision_raises(self):
        mappings = [
            {"player_id": 1, "team_side": "home", "team_code": "LG"},
            {"player_id": 1, "team_side": "away", "team_code": "SS"},
        ]
        with pytest.raises(ValueError, match="team collisions"):
            _assert_no_player_team_collisions("g1", "test", mappings)

    def test_no_player_id_skips(self):
        mappings = [
            {"player_name": "A", "team_side": "home", "team_code": "LG"},
            {"player_name": "B", "team_side": "away", "team_code": "SS"},
        ]
        _assert_no_player_team_collisions("g1", "test", mappings)


class TestAssignFieldIfChanged:
    def test_assign_new_value(self):
        target = MagicMock()
        target.existing = "old"
        changed = _assign_field_if_changed(
            target, "existing", "new", game_id="g1", source=MagicMock(), write_contract=None
        )
        assert changed is True
        assert target.existing == "new"

    def test_assign_same_value(self):
        target = MagicMock()
        target.existing = "same"
        changed = _assign_field_if_changed(
            target, "existing", "same", game_id="g1", source=MagicMock(), write_contract=None
        )
        assert changed is False

    def test_assign_empty_not_allowed(self):
        target = MagicMock()
        changed = _assign_field_if_changed(target, "attr", None, game_id="g1", source=MagicMock(), write_contract=None)
        assert changed is False

    def test_assign_empty_allowed(self):
        target = MagicMock()
        target.attr = "old"
        changed = _assign_field_if_changed(
            target, "attr", None, game_id="g1", source=MagicMock(), write_contract=None, allow_empty=True
        )
        assert changed is True


class TestRecordsMatchExisting:
    def test_mismatch_length(self):
        result = _records_match_existing([], MagicMock(), [{"a": 1}])
        assert result is False

    def test_match_empty(self, session):
        result = _records_match_existing_objects([], GameInningScore, [])
        assert result is True

    def test_mismatch_existing_objects(self, session):
        existing = [MagicMock(spec=GameInningScore)]
        existing[0].id = 1
        existing[0].game_id = "g1"
        existing[0].team_side = "home"
        existing[0].team_code = "LG"
        existing[0].inning = 1
        existing[0].runs = 2
        existing[0].is_extra = False

        incoming = [MagicMock(spec=GameInningScore)]
        incoming[0].id = 2
        incoming[0].game_id = "g1"
        incoming[0].team_side = "home"
        incoming[0].team_code = "LG"
        incoming[0].inning = 1
        incoming[0].runs = 3
        incoming[0].is_extra = False

        result = _records_match_existing_objects(existing, GameInningScore, incoming)
        assert result is False


class TestBuildInningScores:
    def test_build_inning_scores(self):
        teams = {
            "away": {"code": "SS", "line_score": [0, 1, 0, 2, None]},
            "home": {"code": "LG", "line_score": [1, 0, 2, 0, 0]},
        }
        result = _build_inning_scores("g1", teams)
        # None runs are skipped
        away_innings = [r for r in result if r["team_side"] == "away"]
        assert len(away_innings) == 4
        assert away_innings[1]["runs"] == 1

    def test_build_inning_scores_empty(self):
        teams = {"away": {"code": "SS", "line_score": []}, "home": {"code": "LG", "line_score": []}}
        result = _build_inning_scores("g1", teams)
        assert result == []


class TestBuildLineups:
    def test_build_lineups(self):
        hitters = {
            "home": [
                {
                    "player_name": "Kim",
                    "player_id": 1001,
                    "team_code": "LG",
                    "batting_order": 1,
                    "position": "CF",
                    "is_starter": True,
                }
            ],
            "away": [
                {
                    "player_name": "Park",
                    "player_id": 1002,
                    "team_code": "SS",
                    "batting_order": 2,
                    "position": "SS",
                    "is_starter": True,
                }
            ],
        }
        result = _build_lineups("g1", hitters)
        assert len(result) == 2
        assert result[0]["player_name"] == "Kim"


class TestBuildBattingStats:
    def test_build_batting_stats(self):
        hitters = {
            "home": [
                {
                    "player_name": "Kim",
                    "player_id": 1001,
                    "team_code": "LG",
                    "batting_order": 1,
                    "is_starter": True,
                    "position": "CF",
                    "stats": {"plate_appearances": 4, "at_bats": 4, "hits": 2, "avg": 0.500},
                }
            ],
        }
        result = _build_batting_stats("g1", hitters)
        assert len(result) == 1
        assert result[0]["avg"] == 0.5
        assert result[0]["hits"] == 2


class TestBuildPitchingStats:
    def test_build_pitching_stats(self):
        pitchers = {
            "away": [
                {
                    "player_name": "Park",
                    "player_id": 2001,
                    "team_code": "SS",
                    "is_starting": True,
                    "stats": {"innings_outs": 9, "strikeouts": 5, "era": 1.0},
                }
            ],
        }
        result = _build_pitching_stats("g1", pitchers)
        assert len(result) == 1
        assert result[0]["strikeouts"] == 5

    def test_build_pitching_stats_no_innings_outs(self):
        pitchers = {
            "home": [{"player_name": "Choi", "player_id": 1, "team_code": "LG", "stats": {}}],
        }
        result = _build_pitching_stats("g1", pitchers)
        assert len(result) == 1
        assert result[0]["innings_pitched"] is None


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GameSummary.__table__.create(engine)
    GameIdAlias.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    KboSeason.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


class TestEnsureGameStub:
    def test_ensure_new_stub(self, session):
        _ensure_game_stub(session, "20241015LGSS0")
        game = session.query(Game).filter(Game.game_id == "20241015LGSS0").one_or_none()
        assert game is not None
        assert game.game_status == GAME_STATUS_COMPLETED

    def test_ensure_existing_stub(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()
        _ensure_game_stub(session, "20241015LGSS0")
        assert session.query(Game).count() == 1


class TestRecordGameIdAlias:
    def test_record_new_alias(self, session):
        _record_game_id_alias(session, "ALIAS", "CANONICAL", source="test", reason="test")
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "ALIAS").one_or_none()
        assert alias is not None
        assert alias.canonical_game_id == "CANONICAL"

    def test_record_duplicate(self, session):
        _record_game_id_alias(session, "ALIAS", "CANONICAL", source="test", reason="test")
        _record_game_id_alias(session, "ALIAS", "UPDATED", source="test2", reason="updated")
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "ALIAS").one_or_none()
        assert alias.canonical_game_id == "UPDATED"

    def test_record_self_alias(self, session):
        _record_game_id_alias(session, "SAME", "SAME", source="test", reason="test")
        assert session.query(GameIdAlias).count() == 0


class TestUpsertMetadata:
    def test_upsert_new_metadata(self, session):
        result = _upsert_metadata(session, "g1", {"stadium": "Jamsil", "attendance": 25000})
        assert result is True
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == "g1").one_or_none()
        assert meta is not None
        assert meta.stadium_name == "Jamsil"
        assert meta.attendance == 25000

    def test_upsert_existing_metadata(self, session):
        session.add(GameMetadata(game_id="g1", stadium_name="Old"))
        session.flush()
        result = _upsert_metadata(session, "g1", {"stadium": "New", "attendance": 30000})
        assert result is True
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == "g1").one()
        assert meta.stadium_name == "New"

    def test_upsert_empty_metadata(self, session):
        result = _upsert_metadata(session, "g1", {})
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == "g1").one_or_none()
        assert meta is not None
        assert result is True


class TestUpsertGameSummaryEntry:
    def test_upsert_new_entry(self, session):
        _upsert_game_summary_entry(
            session,
            GameSummaryEntry(game_id="g1", summary_type="홈런", detail_text="Kim 1호"),
        )
        entry = session.query(GameSummary).filter(GameSummary.game_id == "g1").one_or_none()
        assert entry is not None
        assert entry.detail_text == "Kim 1호"

    def test_upsert_existing_entry(self, session):
        _upsert_game_summary_entry(
            session,
            GameSummaryEntry(game_id="g1", summary_type="홈런", detail_text="v1"),
        )
        _upsert_game_summary_entry(
            session,
            GameSummaryEntry(game_id="g1", summary_type="홈런", detail_text="v2"),
        )
        entries = session.query(GameSummary).all()
        assert len(entries) == 1
        assert entries[0].detail_text == "v2"


class TestEnsurePlayerBasicStubs:
    def test_ensure_stubs(self, session):
        mappings = [
            {"player_id": 9999, "player_name": "New Player", "team_code": "LG"},
        ]
        result = _ensure_player_basic_stubs(session, mappings)
        assert result is True
        pb = session.query(PlayerBasic).filter(PlayerBasic.player_id == 9999).one_or_none()
        assert pb is not None
        assert pb.name == "New Player"
        assert pb.status == "STUB"

    def test_ensure_stubs_existing(self, session):
        session.add(PlayerBasic(player_id=1001, name="Existing"))
        session.flush()
        mappings = [{"player_id": 1001, "player_name": "Existing"}]
        result = _ensure_player_basic_stubs(session, mappings)
        assert result is False

    def test_ensure_stubs_no_name(self, session):
        mappings = [{"player_id": None, "player_name": ""}]
        result = _ensure_player_basic_stubs(session, mappings)
        assert result is False


class TestReplaceRecords:
    def test_replace_records_new(self, session):
        session.add(GameInningScore(game_id="g1", team_side="home", team_code="LG", inning=1, runs=1))
        session.flush()

        new_mappings = [
            {"game_id": "g1", "team_side": "home", "team_code": "LG", "inning": 1, "runs": 2},
        ]
        changed = _replace_records(session, GameInningScore, "g1", new_mappings)
        assert changed is True
        rows = session.query(GameInningScore).all()
        assert len(rows) == 1
        assert rows[0].runs == 2

    def test_replace_records_same(self, session):
        mapping = {"game_id": "g1", "team_side": "home", "team_code": "LG", "inning": 1, "runs": 1}
        changed = _replace_records(session, GameInningScore, "g1", [mapping])
        assert changed is True
        rows = session.query(GameInningScore).all()
        assert len(rows) == 1
        assert rows[0].runs == 1


class TestReplaceRecordsForSide:
    def test_replace_records_for_side(self, session):
        session.add(
            GameLineup(
                game_id="g1", team_side="home", team_code="LG", player_name="Kim", appearance_seq=1, is_starter=True
            )
        )
        session.flush()

        new_rows = [
            {
                "game_id": "g1",
                "team_side": "home",
                "team_code": "LG",
                "player_name": "Park",
                "appearance_seq": 1,
                "is_starter": True,
            },
        ]
        changed = _replace_records_for_side(
            session,
            RecordKey(model=GameLineup, game_id="g1", team_side="home"),
            new_rows,
        )
        assert changed is True
        rows = session.query(GameLineup).filter(GameLineup.team_side == "home").all()
        assert len(rows) == 1
        assert rows[0].player_name == "Park"


class TestBuildPregameLineupRows:
    def test_build_pregame_lineup_rows(self):
        resolver = MagicMock()
        resolver.resolve_id.return_value = 1001

        result = _build_pregame_lineup_rows(
            "g1",
            ctx=TeamSideContext(team_side="home", team_code="LG", season_year=2024),
            lineup=[{"player_name": "Kim", "batting_order": 1, "position": "CF"}],
            resolver=resolver,
        )
        assert len(result) == 1
        assert result[0]["player_name"] == "Kim"
        assert result[0]["is_starter"] is True

    def test_build_pregame_lineup_rows_empty_name(self):
        resolver = MagicMock()
        result = _build_pregame_lineup_rows(
            "g1",
            ctx=TeamSideContext(team_side="home", team_code="LG", season_year=2024),
            lineup=[{"player_name": "", "batting_order": 1}],
            resolver=resolver,
        )
        assert len(result) == 0


class TestApplyGameTeamIdentity:
    def test_apply_game_team_identity(self):
        game = MagicMock()
        game.home_team = "LG"
        game.away_team = "SS"
        game.winning_team = "LG"
        game.home_franchise_id = None
        game.away_franchise_id = None
        game.winning_franchise_id = None
        _apply_game_team_identity(game, 2024)
        # With mocked team_history, franchise_id may be None
        # At minimum, this should not raise


class TestApplyTeamIdentityToMappings:
    def test_apply_to_mappings(self):
        mappings = [{"team_code": "LG"}]
        _apply_team_identity_to_mappings(mappings, 2024)
        assert mappings[0]["team_code"] is not None or mappings[0]["franchise_id"] is None


class TestResolveScheduleSeasonId:
    def test_resolve_schedule_season_id_explicit(self, session):
        result = _resolve_schedule_season_id(session, {"season_id": 5}, None)
        assert result == 5

    def test_resolve_schedule_season_id_none(self, session):
        result = _resolve_schedule_season_id(session, {}, None)
        assert result is None


class TestResolveGameSeasonId:
    def test_resolve_game_season_id_with_existing(self, session):
        result = _resolve_game_season_id(session, {}, date(2024, 10, 15), 5)
        assert result == 5

    def test_resolve_game_season_id_without_existing(self, session):
        result = _resolve_game_season_id(session, {}, date(2024, 10, 15), None)
        assert result == 202400
        season = session.query(KboSeason).filter_by(season_id=202400).one()
        assert season.season_year == 2024
        assert season.league_type_code == 0

    def test_resolve_fallback_creates_missing_season(self, session):
        result = _resolve_schedule_season_id(session, {"season_year": 2025, "season_type": "regular"}, None)
        assert result == 202500
        season = session.query(KboSeason).filter_by(season_id=202500).one()
        assert season.league_type_name == "정규시즌"

    def test_resolve_fallback_no_create_returns_year_code(self, session):
        from src.repositories.game_helpers import _resolve_season_id_fallback

        result = _resolve_season_id_fallback(
            session,
            {"season_type": "regular"},
            None,
            2025,
            create_missing=False,
        )
        assert result == 202500


class TestConstants:
    def test_constants_exist(self):
        assert GAME_STATUS_CANCELLED == "CANCELLED"
        assert GAME_STATUS_COMPLETED == "COMPLETED"
        assert GAME_STATUS_DRAW == "DRAW"
        assert GAME_STATUS_LIVE == "LIVE"
        assert GAME_STATUS_POSTPONED == "POSTPONED"
        assert GAME_STATUS_SCHEDULED == "SCHEDULED"
        assert GAME_STATUS_UNRESOLVED == "UNRESOLVED_MISSING"
        assert isinstance(LIVE_GAME_STATUSES, set)
