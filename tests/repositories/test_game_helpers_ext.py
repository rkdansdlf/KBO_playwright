from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.constants import MAX_INNINGS
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
    DerivedGameStatusInput,
    FieldChangeContext,
    GameSummaryEntry,
    RecordKey,
    TeamSideContext,
    _apply_game_team_identity_with_contract,
    _assign_field_if_changed,
    _auto_sync_to_oci,
    _build_inning_scores,
    _canonicalize_game_id_for_payload,
    _derive_game_status,
    _enrich_existing_child_team_identity,
    _ensure_game_stub,
    _ensure_player_basic_stubs,
    _extract_players_from_text,
    _format_notes,
    _has_game_child_rows,
    _infer_pitcher_from_children,
    _infer_score_from_children,
    _infer_team_code_from_children,
    _new_strict_player_resolver,
    _normalize_record_for_compare,
    _prepare_player_rows,
    _query_db_season_by_date_range,
    _records_match_existing,
    _records_match_existing_objects,
    _replace_records,
    _replace_records_for_side,
    _resolve_game_date_obj,
    _resolve_schedule_season_id,
    _resolve_season_id_fallback,
    _resolve_winner,
    _safe_time,
    _stat_float,
    _stat_int,
    _upsert_game_summary_entry,
    _upsert_metadata,
    _values_equal,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    Game.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GameSummary.__table__.create(engine)
    GameIdAlias.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    KboSeason.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


class TestDeriveGameStatus:
    def test_completed_with_batting(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=5,
            away_score=3,
            current_status="COMPLETED",
            has_metadata=True,
            has_inning_scores=True,
            has_lineups=True,
            has_batting=True,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "COMPLETED"

    def test_draw_game(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=5,
            away_score=5,
            current_status="COMPLETED",
            has_metadata=True,
            has_inning_scores=True,
            has_lineups=True,
            has_batting=True,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "DRAW"

    def test_scheduled_future_date(self):
        today = date.today()
        future = date(today.year, 12, 31)
        status_input = DerivedGameStatusInput(
            game_date=future,
            home_score=None,
            away_score=None,
            current_status=None,
            has_metadata=False,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "SCHEDULED"

    def test_cancelled_no_details(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=None,
            away_score=None,
            current_status="CANCELLED",
            has_metadata=True,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "CANCELLED"

    def test_postponed_no_details(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=None,
            away_score=None,
            current_status="POSTPONED",
            has_metadata=True,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "POSTPONED"

    def test_live_today_with_details(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=1,
            away_score=0,
            current_status="LIVE",
            has_metadata=True,
            has_inning_scores=True,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "LIVE"

    def test_unresolved_no_details(self):
        today = date.today()
        status_input = DerivedGameStatusInput(
            game_date=today,
            home_score=None,
            away_score=None,
            current_status=None,
            has_metadata=False,
            has_inning_scores=False,
            has_lineups=False,
            has_batting=False,
            has_pitching=False,
            today=today,
        )
        result = _derive_game_status(status_input)
        assert result == "UNRESOLVED_MISSING"

    def test_kwargs_form(self):
        today = date.today()
        result = _derive_game_status(
            game_date=today,
            home_score=5,
            away_score=3,
            current_status="COMPLETED",
            has_metadata=True,
            has_inning_scores=True,
            has_lineups=True,
            has_batting=True,
            has_pitching=False,
            today=today,
        )
        assert result == "COMPLETED"


class TestHasGameChildRows:
    def test_no_rows(self, session):
        assert _has_game_child_rows(session, GameBattingStat, "nonexistent") is False

    def test_with_row(self, session):
        session.add(
            GameBattingStat(
                game_id="20241015LGSS0",
                player_name="Kim",
                team_side="away",
                appearance_seq=1,
                at_bats=4,
            ),
        )
        session.commit()
        assert _has_game_child_rows(session, GameBattingStat, "20241015LGSS0") is True


class TestInferTeamCodeFromChildren:
    def test_from_inning_scores(self, session):
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=2, team_code="LG"))
        session.flush()
        result = _infer_team_code_from_children(session, "g1", "away", 2024)
        assert result == "LG"

    def test_from_game_id_segment_fallback(self, session):
        with patch("src.repositories.game_helpers.team_code_from_game_id_segment", return_value="SS"):
            result = _infer_team_code_from_children(session, "20241020LGSS0", "home", 2024)
            assert result == "SS"

    def test_no_children(self, session):
        with patch("src.repositories.game_helpers.team_code_from_game_id_segment", return_value=None):
            result = _infer_team_code_from_children(session, "g1", "away", 2024)
            assert result is None


class TestInferScoreFromChildren:
    def test_from_inning_scores(self, session):
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=3))
        session.add(GameInningScore(game_id="g1", team_side="away", inning=2, runs=2))
        session.flush()
        result = _infer_score_from_children(session, "g1", "away")
        assert result == 5

    def test_from_batting_stats(self, session):
        session.add(
            GameBattingStat(game_id="g1", player_name="Kim", team_side="away", appearance_seq=1, runs=4, at_bats=4),
        )
        session.flush()
        result = _infer_score_from_children(session, "g1", "away")
        assert result == 4

    def test_no_rows(self, session):
        result = _infer_score_from_children(session, "g1", "away")
        assert result is None


class TestInferPitcherFromChildren:
    def test_starting_pitcher(self, session):
        session.add(
            GamePitchingStat(
                game_id="g1",
                player_name="Park",
                team_side="home",
                appearance_seq=1,
                is_starting=True,
            ),
        )
        session.flush()
        result = _infer_pitcher_from_children(session, "g1", "home")
        assert result == "Park"

    def test_no_pitcher(self, session):
        session.add(
            GamePitchingStat(
                game_id="g1",
                player_name="Park",
                team_side="home",
                appearance_seq=1,
                is_starting=False,
            ),
        )
        session.flush()
        result = _infer_pitcher_from_children(session, "g1", "home")
        assert result is None


class TestEnrichExistingChildTeamIdentity:
    def test_enriches_rows(self, session):
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=2, team_code="LG"))
        session.flush()
        with patch("src.repositories.game_helpers._resolve_team_identity", return_value=(1, "LG", "LG")):
            _enrich_existing_child_team_identity(session, "g1", 2024)
            row = session.query(GameInningScore).filter(GameInningScore.game_id == "g1").first()
            assert row.franchise_id == 1
            assert row.canonical_team_code == "LG"

    def test_no_season_code(self, session):
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=2, team_code="LG"))
        session.flush()
        with patch("src.repositories.game_helpers._resolve_team_identity", return_value=(1, "LG", None)):
            _enrich_existing_child_team_identity(session, "g1", 2024)
            row = session.query(GameInningScore).filter(GameInningScore.game_id == "g1").first()
            assert row.franchise_id == 1


class TestEnsurePlayerBasicStubs:
    def test_no_mappings(self, session):
        result = _ensure_player_basic_stubs(session, [])
        assert result is False

    def test_no_player_id(self, session):
        result = _ensure_player_basic_stubs(session, [{"player_name": "Kim"}])
        assert result is False

    def test_existing_player(self, session):
        from src.models.player import PlayerBasic

        session.add(PlayerBasic(player_id=1001, name="Kim", status="ACTIVE", status_source="seed"))
        session.flush()
        result = _ensure_player_basic_stubs(session, [{"player_id": 1001, "player_name": "Kim", "team_code": "SS"}])
        assert result is False

    def test_create_stub(self, session):
        result = _ensure_player_basic_stubs(
            session,
            [{"player_id": 9999, "player_name": "NewPlayer", "team_code": "LG", "position": "P"}],
        )
        assert result is True
        stub = session.query(PlayerBasic).filter(PlayerBasic.player_id == 9999).one()
        assert stub.status == "STUB"
        assert stub.name == "NewPlayer"
        assert stub.team == "LG"
        assert stub.position == "P"


class TestReplaceRecords:
    def test_no_changes(self, session):
        from src.repositories.game_helpers import RecordReplaceContext, GameWriteSource

        session.add(GameBattingStat(game_id="g1", player_name="Kim", team_side="away", at_bats=4, appearance_seq=1))
        session.flush()

        source = GameWriteSource("detail", "DetailCrawler", "box_score")
        ctx = RecordReplaceContext(source=source, write_contract=None)
        mappings = [
            {
                "game_id": "g1",
                "player_name": "Kim",
                "team_side": "away",
                "appearance_seq": 1,
                "at_bats": 4,
                "player_id": None,
                "uniform_no": None,
                "batting_order": None,
                "is_starter": False,
                "position": None,
                "standard_position": None,
                "plate_appearances": 0,
                "runs": 0,
                "hits": 0,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "rbi": 0,
                "walks": 0,
                "intentional_walks": 0,
                "hbp": 0,
                "strikeouts": 0,
                "stolen_bases": 0,
                "caught_stealing": 0,
                "sacrifice_hits": 0,
                "sacrifice_flies": 0,
                "gdp": 0,
            },
        ]
        result = _replace_records(session, GameBattingStat, "g1", mappings, ctx)
        assert result is False

    def test_insert_changes(self, session):
        from src.repositories.game_helpers import RecordReplaceContext

        ctx = RecordReplaceContext()
        mappings = [
            {
                "game_id": "g1",
                "player_name": "New",
                "team_side": "away",
                "appearance_seq": 1,
                "at_bats": 3,
                "player_id": None,
                "uniform_no": None,
                "batting_order": None,
                "is_starter": False,
                "position": None,
                "standard_position": None,
                "plate_appearances": 0,
                "runs": 0,
                "hits": 0,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "rbi": 0,
                "walks": 0,
                "intentional_walks": 0,
                "hbp": 0,
                "strikeouts": 0,
                "stolen_bases": 0,
                "caught_stealing": 0,
                "sacrifice_hits": 0,
                "sacrifice_flies": 0,
                "gdp": 0,
            },
        ]
        result = _replace_records(session, GameBattingStat, "g1", mappings, ctx)
        assert result is True


class TestReplaceRecordsForSide:
    def test_with_matching_team_side(self, session):
        from src.repositories.game_helpers import RecordReplaceContext

        session.add(GameBattingStat(game_id="g1", player_name="Kim", team_side="away", at_bats=4, appearance_seq=1))
        session.flush()

        ctx = RecordReplaceContext()
        mappings = [
            {
                "game_id": "g1",
                "player_name": "Kim",
                "team_side": "away",
                "appearance_seq": 1,
                "at_bats": 4,
                "player_id": None,
                "uniform_no": None,
                "batting_order": None,
                "is_starter": False,
                "position": None,
                "standard_position": None,
                "plate_appearances": 0,
                "runs": 0,
                "hits": 0,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "rbi": 0,
                "walks": 0,
                "intentional_walks": 0,
                "hbp": 0,
                "strikeouts": 0,
                "stolen_bases": 0,
                "caught_stealing": 0,
                "sacrifice_hits": 0,
                "sacrifice_flies": 0,
                "gdp": 0,
            },
        ]
        record_key = RecordKey(model=GameBattingStat, game_id="g1", team_side="away")
        result = _replace_records_for_side(session, record_key, mappings, ctx)
        assert result is False


class TestRecordsMatchExistingObjects:
    def test_matching(self, session):
        session.add(GameBattingStat(game_id="g1", player_name="Kim", team_side="away", at_bats=4, appearance_seq=1))
        session.flush()

        existing = session.query(GameBattingStat).all()
        result = _records_match_existing_objects(existing, GameBattingStat, list(existing))
        assert result is True

    def test_mismatch(self, session):
        session.add(GameBattingStat(game_id="g1", player_name="Kim", team_side="away", at_bats=4, appearance_seq=1))
        session.flush()

        existing = session.query(GameBattingStat).all()
        new_record = GameBattingStat(game_id="g1", player_name="Park", team_side="away", at_bats=3, appearance_seq=1)
        result = _records_match_existing_objects(existing, GameBattingStat, [new_record])
        assert result is False


class TestNormalizeRecordForCompare:
    def test_decimal(self):
        record = {"val": Decimal("3.0")}
        result = _normalize_record_for_compare(record)
        assert result["val"] == "3"

    def test_datetime(self):
        record = {"dt": datetime(2024, 1, 15, 18, 0)}
        result = _normalize_record_for_compare(record)
        assert result["dt"] == "2024-01-15T18:00:00"

    def test_date(self):
        record = {"d": date(2024, 1, 15)}
        result = _normalize_record_for_compare(record)
        assert result["d"] == "2024-01-15"

    def test_plain_value(self):
        record = {"name": "Kim", "num": 42}
        result = _normalize_record_for_compare(record)
        assert result["name"] == "Kim"
        assert result["num"] == 42


class TestBuildInningScores:
    def test_builds_from_teams(self):
        with patch("src.repositories.game_helpers._apply_team_identity_to_mappings"):
            records = _build_inning_scores(
                "g1",
                {"away": {"line_score": [1, 0, 2], "code": "LG"}, "home": {"line_score": [0, 3], "code": "SS"}},
                season_year=2024,
            )
            assert len(records) == 5
            assert records[0]["team_side"] == "away"
            assert records[0]["runs"] == 1

    def test_extra_inning_flag(self):
        with patch("src.repositories.game_helpers._apply_team_identity_to_mappings"):
            records = _build_inning_scores(
                "g1",
                {"away": {"line_score": [1] * (MAX_INNINGS + 1), "code": "LG"}},
                season_year=2024,
            )
            assert records[MAX_INNINGS]["is_extra"] is True
            assert records[MAX_INNINGS - 1]["is_extra"] is False

    def test_none_runs_skipped(self):
        with patch("src.repositories.game_helpers._apply_team_identity_to_mappings"):
            records = _build_inning_scores(
                "g1",
                {"away": {"line_score": [1, None, 2], "code": "LG"}},
                season_year=2024,
            )
            assert len(records) == 2


class TestAutoSyncToOci:
    def test_disabled(self):
        with patch.dict("os.environ", {}, clear=False):
            with patch("os.getenv", return_value=None):
                _auto_sync_to_oci("g1")


class TestQueryDbSeasonByDateRange:
    def test_returns_none_on_error(self, session):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        from sqlalchemy.exc import SQLAlchemyError

        mock_session.execute.side_effect = SQLAlchemyError("error")

        with patch("src.repositories.game_helpers.SessionLocal", return_value=mock_session):
            result = _query_db_season_by_date_range(mock_session, 2024, date(2024, 6, 15))
            assert result is None


class TestResolveSeasonIdFallback:
    def test_returns_fallback_creates_and_returns_season_id(self, session):
        result = _resolve_season_id_fallback(session, {}, None, 2024)
        assert result == 202400

    def test_returns_existing(self, session):
        result = _resolve_season_id_fallback(session, {}, 42, None)
        assert result == 42

    def test_returns_none_when_both_none(self, session):
        result = _resolve_season_id_fallback(session, {}, None, None)
        assert result is None

    def test_returns_year_code_when_create_missing_false(self, session):
        result = _resolve_season_id_fallback(session, {"season_type": "wildcard"}, None, 2024, create_missing=False)
        assert result == 202402


class TestUpsertMetadata:
    def test_creates_new(self, session):
        source = MagicMock()
        wc = MagicMock()
        result = _upsert_metadata(
            session,
            "g1",
            {"stadium": "Gocheok", "attendance": 25000},
            source=source,
            write_contract=wc,
        )
        assert result is True
        meta = session.query(GameMetadata).one()
        assert meta.attendance == 25000

    def test_empty_values_ignored(self, session):
        result = _upsert_metadata(session, "g1", {"stadium": None, "attendance": ""})
        assert result is True


class TestApplyGameTeamIdentityWithContract:
    def test_no_changes_when_none(self):
        from src.repositories.game_helpers import GameWriteSource

        game = MagicMock()
        game.home_team = None
        game.away_team = None
        game.winning_team = None
        game.home_franchise_id = None
        game.away_franchise_id = None
        game.winning_franchise_id = None
        game.game_id = "g1"

        source = GameWriteSource("detail", "DetailCrawler", "box_score")
        wc = MagicMock()
        _apply_game_team_identity_with_contract(game, None, source=source, write_contract=wc)
        wc.field_updated.assert_not_called()


class TestAssignFieldIfChanged:
    def test_no_change(self):
        context = FieldChangeContext(game_id="g1", source="test", write_contract=None, field="score")
        target = MagicMock()
        target.score = 5
        result = _assign_field_if_changed(target, "score", 5, context)
        assert result is False

    def test_change(self):
        context = FieldChangeContext(game_id="g1", source="test", write_contract=None, field="score")
        target = MagicMock()
        target.score = 3
        result = _assign_field_if_changed(target, "score", 5, context)
        assert result is True
        assert target.score == 5

    def test_empty_not_allowed(self):
        context = FieldChangeContext(game_id="g1", source="test", write_contract=None, field="score", allow_empty=False)
        target = MagicMock()
        target.score = 3
        result = _assign_field_if_changed(target, "score", "", context)
        assert result is False


class TestValuesEqual:
    def test_decimal_equal(self):
        assert _values_equal(Decimal("3.0"), Decimal("3.00")) is True

    def test_decimal_vs_int(self):
        assert _values_equal(Decimal("3.0"), 3) is True

    def test_invalid_decimal(self):
        assert _values_equal("not_a_decimal", "also_not") is False


class TestExtractPlayersFromText:
    def test_empty_text(self):
        assert _extract_players_from_text("homerun", "") == []

    def test_simum_text(self):
        assert _extract_players_from_text("homerun", "없음") == []

    def test_umpire(self):
        result = _extract_players_from_text("심판", "김판수 박심판")
        assert len(result) == 2
        assert result[0][0] == "김판수"

    def test_korean_pattern(self):
        result = _extract_players_from_text("homerun", "강민호1호(2회1점 쿠에바스)")
        assert len(result) == 1
        assert result[0][0] == "강민호"

    def test_colon_fallback(self):
        result = _extract_players_from_text("homerun", "반즈: 어제")
        assert len(result) == 1
        assert result[0][0] == "반즈"

    def test_single_name_fallback(self):
        result = _extract_players_from_text("homerun", "강민호")
        assert len(result) == 1
        assert result[0][0] == "강민호"

    @pytest.mark.parametrize("name", ["가나", "가나다라마"])
    def test_name_length_boundaries_are_accepted(self, name):
        assert _extract_players_from_text("homerun", name) == [(name, None)]

    @pytest.mark.parametrize("name", ["가", "가나다라마바사"])
    def test_name_length_outside_boundaries_is_rejected(self, name):
        assert _extract_players_from_text("homerun", name) == []


class TestFormatNotes:
    def test_empty(self):
        assert _format_notes(None) is None

    def test_ignored_keys(self):
        result = _format_notes({"COL_0": "x", "선수명": "Kim"})
        assert result is None

    def test_single_value(self):
        result = _format_notes({"HR": 2})
        assert result == "2"

    def test_multiple_values(self):
        result = _format_notes({"HR": 2, "RBI": 3})
        assert "HR" in result


class TestNewStrictPlayerResolver:
    def test_type_error_fallback(self, session):
        with patch(
            "src.services.player_id_resolver.PlayerIdResolver",
            side_effect=TypeError("unexpected kwarg"),
        ):
            resolver = _new_strict_player_resolver(session)
            assert resolver is not None


class TestBuildLineups:
    def test_builds_with_player(self):
        from src.repositories.game_helpers import _build_lineups

        with patch("src.repositories.game_helpers._apply_team_identity_to_mappings"):
            records = _build_lineups(
                "g1",
                {"away": [{"player_name": "Kim", "batting_order": 1, "position": "CF", "is_starter": True}]},
                season_year=2024,
            )
            assert len(records) == 1
            assert records[0]["player_name"] == "Kim"
            assert records[0]["is_starter"] is True

    def test_ignores_empty_name(self):
        from src.repositories.game_helpers import _build_lineups

        with patch("src.repositories.game_helpers._apply_team_identity_to_mappings"):
            records = _build_lineups(
                "g1",
                {"away": [{"player_name": "", "batting_order": 1}]},
                season_year=2024,
            )
            assert len(records) == 0


class TestPreparePlayerRows:
    def test_dedupes_and_merges(self):
        with patch("src.repositories.game_helpers._assert_no_player_team_collisions"):
            result = _prepare_player_rows(
                "g1",
                "game_pitching_stats",
                [
                    {"player_id": 1001, "player_name": "Park", "team_side": "away"},
                    {"player_id": 1001, "player_name": "Park", "team_side": "away"},
                ],
            )
            assert len(result) == 1


class TestAssertNoPlayerTeamCollisions:
    def test_no_collisions(self, session):
        from src.repositories.game_helpers import _assert_no_player_team_collisions

        _assert_no_player_team_collisions(
            "g1",
            "game_batting_stats",
            [{"player_id": 1001, "team_side": "away", "team_code": "LG"}],
        )

    def test_collision_detected(self, session):
        from src.repositories.game_helpers import _assert_no_player_team_collisions

        with pytest.raises(ValueError, match="player_id team collisions"):
            _assert_no_player_team_collisions(
                "g1",
                "game_batting_stats",
                [
                    {"player_id": 1001, "team_side": "away", "team_code": "LG"},
                    {"player_id": 1001, "team_side": "home", "team_code": "SS"},
                ],
            )


class TestUpsertGameSummaryEntry:
    def test_create_new(self, session):
        entry = GameSummaryEntry(
            game_id="g1",
            summary_type="homerun",
            detail_text="강민호1호(2회)",
            player_name="강민호",
            player_id=1001,
        )
        _upsert_game_summary_entry(session, entry)
        session.commit()

        existing = session.query(GameSummary).one()
        assert existing.player_name == "강민호"

    def test_update_existing(self, session):
        session.add(
            GameSummary(
                game_id="g1",
                summary_type="homerun",
                player_name="강민호",
                player_id=1001,
                detail_text="old",
            ),
        )
        session.flush()

        entry = GameSummaryEntry(
            game_id="g1",
            summary_type="homerun",
            detail_text="강민호1호(2회)",
            player_name="강민호",
            player_id=2002,
        )
        _upsert_game_summary_entry(session, entry)
        session.commit()

        existing = session.query(GameSummary).one()
        assert existing.player_id == 2002
        assert existing.detail_text == "강민호1호(2회)"
