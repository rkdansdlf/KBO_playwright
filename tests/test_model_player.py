"""Tests for src/models/player.py — all 8 model classes."""

from datetime import date

from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)
from src.models.team import Team
from tests.factories import (
    build_session,
    make_player,
    make_player_basic,
    make_player_identity,
    make_player_movement,
    make_player_season_baserunning,
    make_player_season_batting,
    make_player_season_fielding,
    make_player_season_pitching,
    make_team,
)


def _create_tables(session):
    tables = [
        PlayerBasic.__table__,
        Player.__table__,
        PlayerIdentity.__table__,
        Team.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        PlayerMovement.__table__,
        PlayerSeasonFielding.__table__,
        PlayerSeasonBaserunning.__table__,
    ]
    for table in tables:
        table.create(bind=session.bind, checkfirst=True)


class TestPlayerBasic:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        pb = make_player_basic()
        session.add(pb)
        session.commit()

        saved = session.query(PlayerBasic).filter_by(player_id=12345).one()
        assert saved.name == "테스트선수"
        assert saved.team == "LG"
        assert saved.position == "내야수"

    def test_optional_fields(self):
        pb = make_player_basic(height_cm=185, weight_kg=90, bats="R", throws="R")
        assert pb.height_cm == 185
        assert pb.bats == "R"

    def test_repr(self):
        pb = make_player_basic()
        assert "PlayerBasic" in repr(pb)
        assert "12345" in repr(pb)

    def test_unique_player_id(self):
        _, session = build_session()
        _create_tables(session)
        pb1 = make_player_basic()
        pb2 = make_player_basic()
        session.add(pb1)
        session.commit()
        session.add(pb2)
        import pytest
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            session.commit()


class TestPlayer:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        p = make_player(kbo_person_id="12345")
        session.add(p)
        session.commit()

        saved = session.query(Player).filter_by(kbo_person_id="12345").one()
        assert saved.status == "ACTIVE"

    def test_foreign_player_default(self):
        p = make_player()
        assert p.is_foreign_player is False

    def test_optional_fields(self):
        p = make_player(birth_date=date(1995, 5, 15), height_cm=180, weight_kg=80)
        assert p.birth_date == date(1995, 5, 15)
        assert p.height_cm == 180

    def test_repr(self):
        p = make_player()
        assert "Player" in repr(p)


class TestPlayerIdentity:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        p = Player(id=1, status="ACTIVE", is_foreign_player=False)
        session.add(p)
        session.commit()

        ident = make_player_identity(player_id=1)
        session.add(ident)
        session.commit()

        saved = session.query(PlayerIdentity).filter_by(player_id=1).one()
        assert saved.name_kor == "테스트선수"
        assert saved.is_primary is True

    def test_optional_name_eng(self):
        ident = make_player_identity(name_eng="Test Player")
        assert ident.name_eng == "Test Player"


def _seed_team_and_player(session):
    """Add a Team and PlayerBasic row to satisfy FK constraints."""
    session.add(make_team())
    session.add(make_player_basic())
    session.flush()


class TestPlayerSeasonBatting:
    def _insert(self, session, **kwargs):
        sb = make_player_season_batting(**kwargs)
        session.add(sb)
        session.commit()
        return sb

    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        self._insert(session)

        saved = session.query(PlayerSeasonBatting).filter_by(player_id=12345, season=2025).one()
        assert saved.league == "REGULAR"
        assert saved.level == "KBO1"

    def test_optional_stats(self):
        sb = make_player_season_batting(hits=100, home_runs=15, avg=0.300)
        assert sb.hits == 100
        assert sb.home_runs == 15

    def test_unique_constraint(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        s1 = make_player_season_batting()
        s2 = make_player_season_batting()
        session.add(s1)
        session.commit()
        session.add(s2)
        import pytest
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            session.commit()


class TestPlayerSeasonPitching:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        sp = make_player_season_pitching()
        session.add(sp)
        session.commit()

        saved = session.query(PlayerSeasonPitching).filter_by(player_id=12345, season=2025).one()
        assert saved.league == "REGULAR"
        assert saved.source == "TEST"

    def test_advanced_stats(self):
        sp = make_player_season_pitching(era=2.50, whip=1.10, k_per_nine=9.0)
        assert sp.era == 2.50
        assert sp.k_per_nine == 9.0

    def test_promoted_stats(self):
        sp = make_player_season_pitching(complete_games=3, shutouts=1, quality_starts=15)
        assert sp.complete_games == 3
        assert sp.shutouts == 1


class TestPlayerMovement:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        pm = make_player_movement()
        session.add(pm)
        session.commit()

        saved = session.query(PlayerMovement).filter_by(player_name="Test Player").one()
        assert saved.section == "Trade"
        assert saved.team_code == "LG"

    def test_default_resolution_status(self):
        pm = make_player_movement()
        assert pm.resolution_status == "unresolved"

    def test_optional_remarks(self):
        pm = make_player_movement(remarks="Test remark")
        assert pm.remarks == "Test remark"

    def test_repr(self):
        pm = make_player_movement()
        assert "PlayerMovement" in repr(pm)


class TestPlayerSeasonFielding:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        pf = make_player_season_fielding()
        session.add(pf)
        session.commit()

        saved = session.query(PlayerSeasonFielding).filter_by(player_id=12345, year=2025).one()
        assert saved.position_id == "3B"

    def test_catcher_metrics(self):
        pf = make_player_season_fielding(caught_stealing=10, passed_balls=2)
        assert pf.caught_stealing == 10


class TestPlayerSeasonBaserunning:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        pr = make_player_season_baserunning()
        session.add(pr)
        session.commit()

        saved = session.query(PlayerSeasonBaserunning).filter_by(player_id=12345, year=2025).one()
        assert saved.team_id == "LG"

    def test_optional_stats(self):
        pr = make_player_season_baserunning(stolen_bases=20, caught_stealing=3)
        assert pr.stolen_bases == 20
        assert pr.stolen_base_percentage is None


class TestPlayerAdvanced:
    def test_fk_violation_invalid_player_season_batting(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        sb = make_player_season_batting(player_id=99999)
        session.add(sb)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_fk_violation_invalid_player_movement_team(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        pm = make_player_movement(canonical_team_id="ZZ")
        session.add(pm)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_season_batting_composite_pk(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        _seed_team_and_player(session)
        s1 = make_player_season_batting()
        s2 = make_player_season_batting()
        session.add(s1)
        session.commit()
        session.add(s2)
        with pytest.raises(IntegrityError):
            session.commit()
