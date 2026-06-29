from __future__ import annotations

from datetime import date, datetime
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
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_save import (
    DetailSaveContext,
    PregameGameFieldInput,
    SnapshotContext,
    _apply_pregame_game_fields,
    _apply_snapshot_game_fields,
    _apply_snapshot_scores,
    _apply_snapshot_starting_pitchers,
    _apply_snapshot_status_and_winner,
    _build_summary_rows,
    _clean_pregame_text,
    _extract_existing_preview_payload,
    _get_or_create_game,
    _get_or_create_snapshot_game,
    _load_existing_preview_payload,
    _parse_detail_game_date,
    _replace_prepared_lineup_side,
    _resolve_pregame_starter,
    _summary_item_rows,
    _update_detail_children,
    _update_detail_core_fields,
    _update_detail_status,
    _update_detail_winner,
    _update_starting_pitchers,
    get_games_by_date,
    save_game_detail,
    save_game_snapshot,
    save_pregame_lineups,
    save_schedule_game,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    Game.__table__.create(engine)
    GameIdAlias.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GameSummary.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    KboSeason.__table__.create(engine)
    Team.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.game_save.SessionLocal", return_value=session),
        patch("src.repositories.game_save._auto_sync_to_oci"),
    ):
        yield


class TestUpdateDetailCoreFields:
    def test_all_fields_updated(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        metadata = {"stadium": "Jamsil"}
        home_info = {"code": "LG", "score": 5}
        away_info = {"code": "SS", "score": 3}
        _update_detail_core_fields(game, ctx, metadata, home_info, away_info)

        assert game.game_date == date(2024, 10, 15)
        assert game.stadium == "Jamsil"
        assert game.home_team == "LG"
        assert game.away_team == "SS"
        assert game.home_score == 5
        assert game.away_score == 3

    def test_empty_values_not_overwritten(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(
            game_id="20241015LGSS0", game_date=date(2024, 10, 15), stadium="Existing", home_team="LG", away_team="SS"
        )
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        _update_detail_core_fields(game, ctx, {}, {"code": None}, {"code": None})
        assert game.stadium == "Existing"
        assert game.home_team == "LG"
        assert game.away_team == "SS"


class TestUpdateDetailStatus:
    def test_completed_status(self, session):
        from src.services.game_write_contract import GameWriteSource
        from src.utils.game_status import GAME_STATUS_COMPLETED

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        teams = {"away": {}, "home": {}}
        changed, inning_rows, new_status = _update_detail_status(game, ctx, teams, GAME_STATUS_COMPLETED)
        assert changed is True
        assert new_status == GAME_STATUS_COMPLETED

    def test_no_inning_rows(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        teams = {"away": {}, "home": {}}
        changed, inning_rows, new_status = _update_detail_status(game, ctx, teams, None)
        assert len(inning_rows) == 0


class TestUpdateDetailWinner:
    def test_sets_winner(self, session):
        from src.services.game_write_contract import GameWriteSource
        from src.utils.game_status import GAME_STATUS_COMPLETED

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_score=5, away_score=3)
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        home_info = {"code": "LG", "score": 5}
        away_info = {"code": "SS", "score": 3}
        changed = _update_detail_winner(game, ctx, home_info, away_info, GAME_STATUS_COMPLETED)
        assert changed is True
        assert game.winning_team == "LG"
        assert game.winning_score == 5

    def test_no_change_when_not_terminal(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_score=5, away_score=3)
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        changed = _update_detail_winner(game, ctx, {}, {}, "LIVE")
        assert changed is False

    def test_no_change_when_scores_none(self, session):
        from src.services.game_write_contract import GameWriteSource
        from src.utils.game_status import GAME_STATUS_COMPLETED

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_score=None, away_score=None)
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        changed = _update_detail_winner(game, ctx, {}, {}, GAME_STATUS_COMPLETED)
        assert changed is False


class TestUpdateStartingPitchers:
    def test_sets_pitchers(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        pitchers = {
            "home": [{"player_name": "Kim", "is_starting": True}],
            "away": [{"player_name": "Park", "is_starting": True}],
        }
        source = GameWriteSource("detail", "test")
        _update_starting_pitchers(game, "20241015LGSS0", pitchers, source, None)
        assert game.home_pitcher == "Kim"
        assert game.away_pitcher == "Park"

    def test_no_starting_pitcher(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_pitcher="Existing")
        session.add(game)
        session.commit()

        pitchers = {"home": [], "away": []}
        source = GameWriteSource("detail", "test")
        _update_starting_pitchers(game, "20241015LGSS0", pitchers, source, None)
        assert game.home_pitcher == "Existing"


class TestUpdateDetailChildren:
    def test_with_empty_data(self, session):
        from src.services.game_write_contract import GameWriteSource

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        ctx = DetailSaveContext(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            source=GameWriteSource("detail", "test"),
            write_contract=None,
        )
        changed = _update_detail_children(session, ctx, {"away": [], "home": []}, {"away": [], "home": []}, [])
        assert changed is False


class TestBuildSummaryRows:
    def test_with_no_players(self, session):
        game_id = "20241015LGSS0"
        roster = {"hitters": {"away": [], "home": []}, "pitchers": {"away": [], "home": []}}
        summary_items = [{"summary_type": "MVP", "detail_text": "Kim: 3 hits"}]
        with patch("src.repositories.game_save._new_strict_player_resolver") as mock_resolver:
            mock_resolver.return_value = MagicMock()
            rows = _build_summary_rows(session, game_id, date(2024, 10, 15), roster, summary_items)
            assert len(rows) >= 1

    def test_empty_summary_items(self, session):
        rows = _build_summary_rows(session, "20241015LGSS0", date(2024, 10, 15), {}, [])
        assert rows == []


class TestSummaryItemRows:
    def test_no_entries(self):
        result = _summary_item_rows(
            {"summary_type": "MVP", "detail_text": ""},
            "20241015LGSS0",
            date(2024, 10, 15),
            {},
            MagicMock(),
        )
        assert len(result) == 1
        assert result[0]["player_name"] is None

    def test_with_player_map_match(self):
        resolver = MagicMock()
        resolver.resolve_id.return_value = 123
        result = _summary_item_rows(
            {"summary_type": "MVP", "detail_text": "Kim: 3 hits, 2 RBI"},
            "20241015LGSS0",
            date(2024, 10, 15),
            {"Kim": 123},
            resolver,
        )
        assert len(result) >= 1


class TestApplySnapshotGameFields:
    def test_updates_fields(self, session):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.commit()

        ctx = SnapshotContext(
            game_data={},
            game_date=date(2024, 10, 15),
            metadata={"stadium": "Jamsil"},
            away_info={"code": "SS"},
            home_info={"code": "LG"},
            pitchers={},
            status=None,
        )
        _apply_snapshot_game_fields(session, game, ctx)
        assert game.stadium == "Jamsil"
        assert game.away_team == "SS"
        assert game.home_team == "LG"


class TestApplySnapshotScores:
    def test_sets_scores(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        _apply_snapshot_scores(game, {"score": 3}, {"score": 5})
        assert game.away_score == 3
        assert game.home_score == 5

    def test_none_scores_not_overwritten(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), away_score=2, home_score=3)
        _apply_snapshot_scores(game, {"score": None}, {"score": None})
        assert game.away_score == 2
        assert game.home_score == 3


class TestApplySnapshotStartingPitchers:
    def test_sets_pitchers(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        pitchers = {
            "home": [{"player_name": "Kim", "is_starting": True}],
            "away": [{"player_name": "Park", "is_starting": True}],
        }
        _apply_snapshot_starting_pitchers(game, pitchers)
        assert game.home_pitcher == "Kim"
        assert game.away_pitcher == "Park"

    def test_no_pitchers(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_pitcher="Existing")
        _apply_snapshot_starting_pitchers(game, {})
        assert game.home_pitcher == "Existing"


class TestApplySnapshotStatusAndWinner:
    def test_sets_winner(self):
        from src.utils.game_status import GAME_STATUS_COMPLETED

        game = Game(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            home_score=5,
            away_score=3,
            home_team="LG",
            away_team="SS",
        )
        _apply_snapshot_status_and_winner(game, date(2024, 10, 15), GAME_STATUS_COMPLETED, has_inning_rows=True)
        assert game.winning_team == "LG"
        assert game.winning_score == 5

    def test_no_winner_when_not_terminal(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_score=5, away_score=3)
        _apply_snapshot_status_and_winner(game, date(2024, 10, 15), "LIVE", has_inning_rows=True)
        assert game.winning_team is None

    def test_no_when_scores_none(self):
        from src.utils.game_status import GAME_STATUS_COMPLETED

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), home_score=None, away_score=None)
        _apply_snapshot_status_and_winner(game, date(2024, 10, 15), GAME_STATUS_COMPLETED, has_inning_rows=True)
        assert game.winning_team is None


class TestGetOrCreateSnapshotGame:
    def test_creates_new(self, session):
        game = _get_or_create_snapshot_game(session, "20241015LGSS0", date(2024, 10, 15))
        assert game.game_id == "20241015LGSS0"
        assert game.game_date == date(2024, 10, 15)

    def test_returns_existing(self, session):
        existing = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(existing)
        session.commit()

        game = _get_or_create_snapshot_game(session, "20241015LGSS0", date(2024, 10, 15))
        assert game is existing


class TestApplyPregameGameFields:
    def test_sets_scheduled_status(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        input_data = PregameGameFieldInput(
            game_date=date(2024, 10, 15),
            away_code="SS",
            home_code="LG",
            away_starter="Kim",
            home_starter="Park",
        )
        _apply_pregame_game_fields(game, {}, input_data)
        assert game.game_status == "SCHEDULED"
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"

    def test_preserves_terminal_status(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), game_status="COMPLETED")
        input_data = PregameGameFieldInput(
            game_date=date(2024, 10, 15),
            away_code="SS",
            home_code="LG",
            away_starter="Kim",
            home_starter="Park",
        )
        _apply_pregame_game_fields(game, {}, input_data)
        assert game.game_status == "COMPLETED"

    def test_preserves_live_status(self):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), game_status="LIVE")
        input_data = PregameGameFieldInput(
            game_date=date(2024, 10, 15),
            away_code="SS",
            home_code="LG",
            away_starter="Kim",
            home_starter="Park",
        )
        _apply_pregame_game_fields(game, {}, input_data)
        assert game.game_status == "LIVE"


class TestLoadExistingPreviewPayload:
    def test_no_summary(self, session):
        result = _load_existing_preview_payload(session, "20241015LGSS0")
        assert result == {}

    def test_with_summary(self, session):
        summary = GameSummary(
            game_id="20241015LGSS0",
            summary_type="프리뷰",
            detail_text='{"stadium": "Jamsil"}',
        )
        session.add(summary)
        session.commit()

        result = _load_existing_preview_payload(session, "20241015LGSS0")
        assert result == {"stadium": "Jamsil"}


class TestReplacePreparedLineupSide:
    def test_with_matching_rows(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        prepared = [
            {"game_id": "20241015LGSS0", "team_side": "away", "player_name": "Kim", "appearance_seq": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "player_name": "Park", "appearance_seq": 1},
        ]
        _replace_prepared_lineup_side(session, "20241015LGSS0", "away", prepared)
        session.commit()

        lineups = session.query(GameLineup).filter(GameLineup.game_id == "20241015LGSS0").all()
        assert len(lineups) >= 1

    def test_no_matching_rows(self, session):
        prepared = []
        _replace_prepared_lineup_side(session, "20241015LGSS0", "away", prepared)


class TestSaveGameDetailExtended:
    def _make_detail_data(self, **overrides):
        data = {
            "game_id": "20241015LGSS0",
            "game_date": "2024-10-15",
            "teams": {
                "away": {"code": "SS", "score": 5},
                "home": {"code": "LG", "score": 3},
            },
            "metadata": {"stadium": "Jamsil"},
            "hitters": {"away": [], "home": []},
            "pitchers": {"away": [], "home": []},
            "game_status": "completed",
        }
        data.update(overrides)
        return data

    def test_save_game_detail_with_inning_scores(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        data["teams"]["away"]["innings"] = [{"inning": 1, "runs": 1}]
        result = save_game_detail(data)
        assert result is True

    def test_save_game_detail_with_hitters(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.add(PlayerBasic(player_id=1, name="Kim"))
        session.commit()

        data = self._make_detail_data()
        data["hitters"] = {
            "away": [{"player_name": "Kim", "player_id": 1, "appearance_seq": 1}],
            "home": [],
        }
        result = save_game_detail(data)
        assert result is True

    def test_save_game_detail_with_pitchers_data(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        data["pitchers"] = {
            "away": [{"player_name": "Kim", "is_starting": True, "appearance_seq": 1}],
            "home": [{"player_name": "Park", "is_starting": True, "appearance_seq": 1}],
        }
        result = save_game_detail(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game is not None
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"

    def test_save_game_detail_with_summary_data(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        data["summary"] = [
            {"summary_type": "MVP", "detail_text": "Kim (3 hits, 2 RBI)"},
            {"summary_type": "결승타", "detail_text": "Park (끝내기 안타)"},
        ]
        result = save_game_detail(data)
        assert result is True

        summaries = session.query(GameSummary).filter(GameSummary.game_id == "20241015SSLG0").all()
        assert len(summaries) >= 2

    def test_save_game_detail_with_alias(self, session):
        session.add(
            GameIdAlias(alias_game_id="OLD_ID", canonical_game_id="20241015LGSS0", source="test", reason="test")
        )
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        result = save_game_detail(data)
        assert result is True

        aliases = session.query(GameIdAlias).filter(GameIdAlias.canonical_game_id == "20241015LGSS0").all()
        assert len(aliases) >= 1


class TestSaveGameSnapshotExtended:
    def _make_snapshot_data(self, **overrides):
        data = {
            "game_id": "20241015LGSS0",
            "game_date": "2024-10-15",
            "teams": {
                "away": {"code": "SS", "score": 5},
                "home": {"code": "LG", "score": 3},
            },
            "metadata": {"stadium": "Jamsil"},
            "pitchers": {"away": [], "home": []},
        }
        data.update(overrides)
        return data

    def test_save_game_snapshot_with_pitchers_data(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_snapshot_data()
        data["pitchers"] = {
            "away": [{"player_name": "Kim", "is_starting": True}],
            "home": [{"player_name": "Park", "is_starting": True}],
        }
        result = save_game_snapshot(data, status="completed")
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"


class TestSavePregameLineupsExtended:
    def test_save_pregame_lineups_with_start_pitcher_announced(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_pregame_lineups(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_name": "SSG",
                "home_team_name": "LG",
                "start_pitcher_announced": True,
            }
        )
        assert result is True

        summaries = session.query(GameSummary).filter(GameSummary.game_id.like("20241015%")).all()
        assert len(summaries) >= 1

    def test_save_pregame_lineups_with_lineups(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.add(PlayerBasic(player_id=1, name="Kim"))
        session.add(PlayerBasic(player_id=2, name="Park"))
        session.commit()

        result = save_pregame_lineups(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_name": "SSG",
                "home_team_name": "LG",
                "away_lineup": [{"player_name": "Kim", "player_id": 1, "batting_order": 1, "position": "CF"}],
                "home_lineup": [{"player_name": "Park", "player_id": 2, "batting_order": 1, "position": "SS"}],
            }
        )
        assert result is True

        lineups = session.query(GameLineup).filter(GameLineup.game_id.like("20241015%")).all()
        assert len(lineups) >= 2


class TestSaveScheduleGameExtended:
    def test_save_schedule_game_with_doubleheader(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_schedule_game(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
                "doubleheader_no": 1,
            }
        )
        assert result is True

    def test_save_schedule_game_updates_existing(self, session):
        session.add(Game(game_id="20241015SSLG0", game_date=date(2024, 10, 15)))
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_schedule_game(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
                "game_time": "14:00",
            }
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game is not None

    def test_save_schedule_game_preserves_finalized_status(self, session):
        session.add(Game(game_id="20241015SSLG0", game_date=date(2024, 10, 15), game_status="COMPLETED"))
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_schedule_game(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
            }
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game.game_status == "COMPLETED"


class TestGetGamesByDateExtended:
    def test_no_games(self, session):
        results = get_games_by_date("20241015")
        assert results == []

    def test_multiple_games(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(Game(game_id="20241015SSLT0", game_date=date(2024, 10, 15)))
        session.add(Game(game_id="20241015HTWO0", game_date=date(2024, 10, 15)))
        session.commit()

        results = get_games_by_date("20241015")
        assert len(results) == 3


class TestParseDetailGameDateExtended:
    def test_with_dashes(self):
        game_date_str, game_date = _parse_detail_game_date({"game_date": "2024-10-15"}, None)
        assert game_date_str == "20241015"
        assert game_date == date(2024, 10, 15)

    def test_empty_game_date_uses_game_id(self):
        game_date_str, game_date = _parse_detail_game_date({}, "20241015LGSS0")
        assert game_date_str == "20241015"
        assert game_date == date(2024, 10, 15)


class TestGetOrCreateGameExtended:
    def test_returns_existing_game(self, session):
        from src.services.game_write_contract import GameWriteSource

        existing = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(existing)
        session.commit()

        source = GameWriteSource("detail", "test")
        game, created = _get_or_create_game(session, "20241015LGSS0", date(2024, 10, 15), source, None)
        assert created is False
        assert game is existing

    def test_creates_new_game(self, session):
        from src.services.game_write_contract import GameWriteSource

        source = GameWriteSource("detail", "test")
        game, created = _get_or_create_game(session, "20241015LGSS0", date(2024, 10, 15), source, None)
        assert created is True
        assert game.game_id == "20241015LGSS0"


class TestCleanPregameTextExtended:
    def test_whitespace_only(self):
        assert _clean_pregame_text("   ") == ""

    def test_tabs_and_newlines(self):
        assert _clean_pregame_text("\t\n  AAA \t\n") == "AAA"

    def test_integer_input(self):
        assert _clean_pregame_text(123) == "123"


class TestExtractExistingPreviewPayloadExtended:
    def test_empty_detail_text(self):
        summary = MagicMock()
        summary.detail_text = ""
        assert _extract_existing_preview_payload(summary) == {}

    def test_non_dict_json(self):
        summary = MagicMock()
        summary.detail_text = '["not", "a", "dict"]'
        assert _extract_existing_preview_payload(summary) == {}

    def test_null_detail_text(self):
        summary = MagicMock()
        summary.detail_text = None
        assert _extract_existing_preview_payload(summary) == {}


class TestResolvePregameStarterExtended:
    def test_no_starter_returns_empty_string(self):
        game = MagicMock()
        game.away_pitcher = None
        result, rid = _resolve_pregame_starter({}, game, {}, "away")
        assert result == ""
        assert rid is None

    def test_existing_payload_starter(self):
        game = MagicMock()
        game.away_pitcher = None
        existing = {"away_starter": "Kim"}
        result, rid = _resolve_pregame_starter({}, game, existing, "away")
        assert result == "Kim"

    def test_preview_overrides_existing(self):
        game = MagicMock()
        game.away_pitcher = None
        preview = {"away_starter": "Park"}
        existing = {"away_starter": "Kim"}
        result, rid = _resolve_pregame_starter(preview, game, existing, "away")
        assert result == "Park"
