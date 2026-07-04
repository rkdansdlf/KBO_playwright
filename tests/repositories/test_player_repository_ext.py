from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.repositories.player_repository import PlayerRepository


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


class TestSplitMovementPlayerLabel:
    def test_extracts_position(self):
        name, pos = PlayerRepository._split_movement_player_label("홍길동 (투수)")
        assert name == "홍길동"
        assert pos == "투수"

    def test_no_position(self):
        name, pos = PlayerRepository._split_movement_player_label("홍길동")
        assert name == "홍길동"
        assert pos is None

    def test_empty_string(self):
        name, pos = PlayerRepository._split_movement_player_label("")
        assert name == ""
        assert pos is None

    def test_none_input(self):
        name, pos = PlayerRepository._split_movement_player_label(None)
        assert name == ""
        assert pos is None

    def test_complex_position(self):
        name, pos = PlayerRepository._split_movement_player_label("김철수 (내야수)")
        assert name == "김철수"
        assert pos == "내야수"

    def test_multiple_spaces(self):
        name, pos = PlayerRepository._split_movement_player_label("이영희  (외야수)")
        assert name == "이영희"
        assert pos == "외야수"


class TestNarrowByPosition:
    def test_single_match(self, session):
        from src.models.player import PlayerBasic

        p1 = PlayerBasic(player_id=1, name="홍길동", position="투수")
        p2 = PlayerBasic(player_id=2, name="홍길동", position="내야수")
        session.add_all([p1, p2])
        session.commit()

        candidates, pid = PlayerRepository._narrow_by_position(
            list(session.query(PlayerBasic).filter_by(name="홍길동").all()),
            "투수",
        )
        assert pid == 1
        assert len(candidates) == 1

    def test_no_position_returns_all(self, session):
        from src.models.player import PlayerBasic

        p1 = PlayerBasic(player_id=1, name="홍길동", position="투수")
        session.add(p1)
        session.commit()

        candidates, pid = PlayerRepository._narrow_by_position(
            list(session.query(PlayerBasic).filter_by(name="홍길동").all()),
            None,
        )
        assert pid is None
        assert len(candidates) == 1

    def test_no_match_returns_all(self, session):
        from src.models.player import PlayerBasic

        p1 = PlayerBasic(player_id=1, name="홍길동", position="투수")
        p2 = PlayerBasic(player_id=2, name="홍길동", position="내야수")
        session.add_all([p1, p2])
        session.commit()

        candidates, pid = PlayerRepository._narrow_by_position(
            list(session.query(PlayerBasic).filter_by(name="홍길동").all()),
            "포수",
        )
        assert pid is None
        assert len(candidates) == 2


class TestNarrowByDebutTimeline:
    def test_exact_match_returns_single(self, session):
        from src.models.player import PlayerBasic

        p1 = PlayerBasic(player_id=1, name="홍길동", debut_year=2020)
        p2 = PlayerBasic(player_id=2, name="홍길동", debut_year=2010)
        session.add_all([p1, p2])
        session.commit()

        candidates, pid = PlayerRepository._narrow_by_debut_timeline(
            list(session.query(PlayerBasic).filter_by(name="홍길동").all()),
            2020,
        )
        assert pid == 1
        assert len(candidates) == 1

    def test_no_season_returns_all(self, session):
        from src.models.player import PlayerBasic

        p1 = PlayerBasic(player_id=1, name="홍길동", debut_year=2020)
        session.add(p1)
        session.commit()

        candidates, pid = PlayerRepository._narrow_by_debut_timeline(
            list(session.query(PlayerBasic).filter_by(name="홍길동").all()),
            2010,
        )
        assert pid is None
        assert len(candidates) == 1


class TestResolveMovementTeamId:
    def test_known_team_code(self, session):
        from src.models.team import Team

        team = Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울")
        session.add(team)
        session.commit()

        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "LG")
        assert result == "LG"

    def test_korean_name_lookup(self, session):
        from src.models.team import Team

        team = Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울")
        session.add(team)
        session.commit()

        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "LG")
        assert result == "LG"

    def test_unknown_team(self, session):
        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "ZZ")
        assert result is None

    def test_empty_team(self, session):
        repo = PlayerRepository()
        result = repo._resolve_movement_team_id(session, "")
        assert result is None
