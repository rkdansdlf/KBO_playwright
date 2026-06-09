from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.engine import Engine
from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
)
from src.models.team import Team, TeamDailyRoster
from src.repositories.player_repository import PlayerRepository


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    PlayerBasic.__table__.create(engine)
    Player.__table__.create(engine)
    PlayerIdentity.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    PlayerSeasonPitching.__table__.create(engine)
    PlayerMovement.__table__.create(engine)
    Team.__table__.create(engine)
    TeamDailyRoster.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.player_repository.SessionLocal", return_value=session),
        patch.object(Engine.dialect, "name", "sqlite"),
    ):
        yield


class TestPlayerRepository:
    def _add_basic(self, session, player_id=1001, name="Kim", team="LG"):
        pb = PlayerBasic(player_id=player_id, name=name, team=team)
        session.add(pb)
        session.flush()
        return pb

    def _add_team(self, session, team_id="LG", team_name="LG Twins"):
        t = Team(team_id=team_id, team_name=team_name, team_short_name="LG", city="Seoul", franchise_id=1)
        session.add(t)
        session.flush()
        return t

    # --- Profile tests ---

    def test_upsert_player_profile_creates_player(self, session):
        self._add_basic(session)
        repo = PlayerRepository()

        profile = MagicMock()
        profile.player_name = "Kim"
        profile.is_active = True
        profile.birth_date = "1990-01-15"
        profile.batting_hand = "R"
        profile.throwing_hand = "R"
        profile.height_cm = 185
        profile.weight_kg = 90
        profile.photo_url = None
        profile.salary_original = None
        profile.signing_bonus_original = None
        profile.entry_year = 2010
        profile.draft_year = 2010
        profile.draft_team_code = "LG"
        profile.draft_type = "2차"
        profile.draft_round = 1
        profile.draft_pick_overall = 10
        profile.education_or_career_path = ["School A", "School B"]
        profile.salary_amount = None
        profile.salary_currency = None
        profile.signing_bonus_amount = None
        profile.signing_bonus_currency = None
        profile.education_path = None
        profile.is_foreign = False

        player = repo.upsert_player_profile("1001", profile)
        assert player is not None
        assert player.kbo_person_id == "1001"
        assert player.bats == "R"
        assert player.throws == "R"

    def test_upsert_player_profile_raises_on_empty_id(self):
        repo = PlayerRepository()
        with pytest.raises(ValueError, match="kbo_player_id is required"):
            repo.upsert_player_profile("", MagicMock())

    # --- _split_movement_player_label ---

    def test_split_movement_player_label_with_position(self):
        name, pos = PlayerRepository._split_movement_player_label("김선수(투수)")
        assert name == "김선수"
        assert pos == "투수"

    def test_split_movement_player_label_without_position(self):
        name, pos = PlayerRepository._split_movement_player_label("홍길동")
        assert name == "홍길동"
        assert pos is None

    def test_split_movement_player_label_empty(self):
        name, pos = PlayerRepository._split_movement_player_label("")
        assert name == ""
        assert pos is None

    # --- _upsert_season_stats ---

    def test_upsert_season_batting(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()

        repo.upsert_season_batting(1001, {"season": 2024, "games": 10, "hits": 25})
        stats = session.query(PlayerSeasonBatting).all()
        assert len(stats) == 1
        assert stats[0].hits == 25
        assert stats[0].source == "PROFILE"

    def test_upsert_season_batting_updates_existing(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()
        repo.upsert_season_batting(1001, {"season": 2024, "league": "REGULAR", "level": "KBO1", "hits": 10})
        repo.upsert_season_batting(1001, {"season": 2024, "league": "REGULAR", "level": "KBO1", "hits": 20})

        stats = session.query(PlayerSeasonBatting).all()
        assert len(stats) == 1
        assert stats[0].hits == 20

    def test_upsert_season_pitching(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()
        repo.upsert_season_pitching(1001, {"season": 2024, "games": 10, "wins": 5})
        stats = session.query(PlayerSeasonPitching).all()
        assert len(stats) == 1
        assert stats[0].wins == 5

    def test_upsert_season_stats_empty_payload(self, session):
        repo = PlayerRepository()
        repo._upsert_season_stats(PlayerSeasonBatting, 1001, {})
        assert session.query(PlayerSeasonBatting).count() == 0

    def test_upsert_season_stats_missing_season(self):
        repo = PlayerRepository()
        with pytest.raises(ValueError, match="season_data must include 'season'"):
            repo._upsert_season_stats(PlayerSeasonBatting, 1001, {"games": 5})

    # --- Team code mapping ---

    def test_team_code_by_name_dict(self):
        assert PlayerRepository._TEAM_CODE_BY_NAME["KIA"] == "KIA"
        assert PlayerRepository._TEAM_CODE_BY_NAME["두산"] == "DB"
        assert PlayerRepository._TEAM_CODE_BY_NAME["LG"] == "LG"

    # --- _resolve_movement_team_id ---

    def test_resolve_movement_team_id(self, session):
        self._add_team(session, "LG")
        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "LG")
        assert result == "LG"

    def test_resolve_movement_team_id_with_mapping(self, session):
        self._add_team(session, "DB")
        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "두산")
        assert result == "DB"

    def test_resolve_movement_team_id_not_found(self, session):
        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "UNKNOWN")
        assert result is None

    # --- save_player_movements ---

    def test_save_player_movements(self, session):
        self._add_basic(session, player_id=1001, name="Kim")
        self._add_team(session, "LG")
        repo = PlayerRepository()

        movements = [
            {"date": "2024-01-15", "team_code": "LG", "player_name": "Kim", "section": "Trade", "remarks": "Test"},
        ]
        count = repo.save_player_movements(movements)
        assert count == 1

        saved = session.query(PlayerMovement).all()
        assert len(saved) == 1
        assert saved[0].player_name == "Kim"
        assert saved[0].section == "Trade"

    def test_save_player_movements_updates_existing(self, session):
        self._add_basic(session, player_id=1001, name="Kim")
        self._add_team(session, "LG")
        repo = PlayerRepository()

        movements = [
            {"date": "2024-01-15", "team_code": "LG", "player_name": "Kim", "section": "Trade", "remarks": "v1"},
        ]
        repo.save_player_movements(movements)
        movements[0]["remarks"] = "v2"
        repo.save_player_movements(movements)

        saved = session.query(PlayerMovement).all()
        assert len(saved) == 1
        assert saved[0].remarks == "v2"

    # --- _resolve_movement_player_id ---

    def test_resolve_movement_player_id_single(self, session):
        self._add_basic(session, player_id=1001, name="Kim")
        repo = PlayerRepository()
        result = repo._resolve_movement_player_id(session, "Kim", "LG", 2024)
        assert result == 1001

    def test_resolve_movement_player_id_rookie(self, session):
        repo = PlayerRepository()
        result = repo._resolve_movement_player_id(session, "신인", None, 2024)
        assert result is None

    def test_resolve_movement_player_id_ambiguous(self, session):
        self._add_basic(session, player_id=1001, name="Kim")
        self._add_basic(session, player_id=1002, name="Kim")
        repo = PlayerRepository()
        result = repo._resolve_movement_player_id(session, "Kim", None, 2024)
        assert result is None

    # --- _infer_movement_team_from_history ---

    def test_infer_movement_team_from_history_no_player(self, session):
        repo = PlayerRepository()
        result = repo._infer_movement_team_from_history(session, "Nobody", 2024)
        assert result is None

    # --- _unique_roster_movement_player_id ---

    def test_unique_roster_movement_player_id_no_roster(self, session):
        self._add_team(session, "LG")
        repo = PlayerRepository()
        result = repo._unique_roster_movement_player_id(session, "Kim", "LG", 2024, {1001})
        assert result is None

    # --- _unique_franchise_season_player_id ---

    def test_unique_franchise_season_player_id_no_team(self, session):
        repo = PlayerRepository()
        result = repo._unique_franchise_season_player_id(session, None, 2024, {1001})
        assert result is None

    def test_unique_franchise_season_player_id_no_candidates(self, session):
        self._add_team(session, "LG")
        repo = PlayerRepository()
        result = repo._unique_franchise_season_player_id(session, "LG", 2024, set())
        assert result is None

    # --- _canonical_player_basic_id ---

    def test_canonical_player_basic_id(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()
        result = repo._canonical_player_basic_id(session, "1001")
        assert result == 1001

    def test_canonical_player_basic_id_not_found(self, session):
        repo = PlayerRepository()
        result = repo._canonical_player_basic_id(session, "9999")
        assert result is None

    def test_canonical_player_basic_id_invalid(self, session):
        repo = PlayerRepository()
        result = repo._canonical_player_basic_id(session, "abc")
        assert result is None

    # --- _get_or_create_player ---

    def test_get_or_create_player_new(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()
        player = repo._get_or_create_player(session, "1001")
        assert player.id is not None
        assert player.kbo_person_id == "1001"

    def test_get_or_create_player_existing(self, session):
        self._add_basic(session, player_id=1001)
        repo = PlayerRepository()
        p1 = repo._get_or_create_player(session, "1001")
        p2 = repo._get_or_create_player(session, "1001")
        assert p1.id == p2.id
