"""Tests for src/models/game.py — all 14 model classes."""

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameHighlight,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
    GameValidationMetrics,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.player import PlayerBasic
from tests.factories import (
    build_session,
    make_game,
    make_game_batting_stat,
    make_game_event,
    make_game_highlight,
    make_game_id_alias,
    make_game_inning_score,
    make_game_lineup,
    make_game_metadata,
    make_game_pitching_stat,
    make_game_play_by_play,
    make_game_summary,
    make_game_validation_metrics,
    make_player_basic,
    make_player_game_batting,
    make_player_game_pitching,
)


def _create_tables(session):
    """Create game-related tables in the in-memory DB."""
    tables = [
        PlayerBasic.__table__,
        Game.__table__,
        GameIdAlias.__table__,
        GameSummary.__table__,
        GamePlayByPlay.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameValidationMetrics.__table__,
        GameHighlight.__table__,
        PlayerGameBatting.__table__,
        PlayerGamePitching.__table__,
    ]
    for table in tables:
        table.create(bind=session.bind, checkfirst=True)


class TestGame:
    def test_create_game(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.commit()

        saved = session.query(Game).filter_by(game_id="20250401LGSS0").one()
        assert saved.home_team == "LG"
        assert saved.away_team == "SSG"
        assert saved.home_score == 5
        assert saved.away_score == 3
        assert saved.game_status == "COMPLETED"

    def test_game_defaults(self):
        g = make_game()
        assert g.is_primary is True
        assert g.id is None

    def test_game_unique_game_id(self):
        _, session = build_session()
        _create_tables(session)
        g1 = make_game()
        g2 = make_game()
        session.add(g1)
        session.commit()
        session.add(g2)
        import pytest
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            session.commit()

    def test_game_str_fields_optional(self):
        g = make_game(stadium=None, winning_team=None)
        _, session = build_session()
        _create_tables(session)
        session.add(g)
        session.commit()


class TestGameIdAlias:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.commit()

        alias = make_game_id_alias(canonical_game_id=g.game_id)
        session.add(alias)
        session.commit()

        saved = session.query(GameIdAlias).filter_by(alias_game_id="ALT_001").one()
        assert saved.source == "test"
        assert saved.canonical_game_id == "20250401LGSS0"


class TestGameSummary:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        s = make_game_summary()
        session.add(s)
        session.commit()

        saved = session.query(GameSummary).filter_by(game_id="20250401LGSS0").one()
        assert saved.summary_type == "STORY"
        assert saved.detail_text == "Test summary detail"

    def test_summary_with_player_id(self):
        s = make_game_summary(player_id=99999)
        assert s.player_id == 99999


class TestGamePlayByPlay:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        pbp = make_game_play_by_play()
        session.add(pbp)
        session.commit()

        saved = session.query(GamePlayByPlay).filter_by(game_id="20250401LGSS0").first()
        assert saved.inning == 1
        assert saved.inning_half == "초"
        assert saved.event_type == "K"
        assert saved.play_description == "Strikeout swinging"


class TestGameMetadata:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        meta = make_game_metadata()
        session.add(meta)
        session.commit()

        saved = session.query(GameMetadata).filter_by(game_id="20250401LGSS0").one()
        assert saved.attendance == 23750
        assert saved.game_time_minutes == 165

    def test_optional_payload(self):
        meta = make_game_metadata(source_payload={"key": "value"})
        assert meta.source_payload == {"key": "value"}


class TestGameInningScore:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        inning = make_game_inning_score()
        session.add(inning)
        session.commit()

        saved = session.query(GameInningScore).filter_by(game_id="20250401LGSS0").all()
        assert len(saved) == 1
        assert saved[0].runs == 2
        assert saved[0].team_side == "home"

    def test_unique_constraint(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()
        i1 = make_game_inning_score()
        i2 = make_game_inning_score()
        session.add_all([i1, i2])
        import pytest
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            session.commit()


def _seed_player_basic(session):
    session.add(make_player_basic())
    session.flush()


class TestGameLineup:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        _seed_player_basic(session)
        lineup = make_game_lineup()
        session.add(lineup)
        session.commit()

        saved = session.query(GameLineup).filter_by(game_id="20250401LGSS0").one()
        assert saved.batting_order == 1
        assert saved.position == "3B"
        assert saved.is_starter is True


class TestGameBattingStat:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        _seed_player_basic(session)
        stat = make_game_batting_stat()
        session.add(stat)
        session.commit()

        saved = session.query(GameBattingStat).filter_by(game_id="20250401LGSS0").one()
        assert saved.at_bats == 4
        assert saved.hits == 2
        assert saved.avg == 0.500

    def test_derived_stats_defaults(self):
        stat = make_game_batting_stat(avg=None, obp=None)
        assert stat.avg is None

    def test_extra_stats_json(self):
        stat = make_game_batting_stat(extra_stats={"xr": 1.5})
        assert stat.extra_stats == {"xr": 1.5}


class TestGamePitchingStat:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        _seed_player_basic(session)
        stat = make_game_pitching_stat()
        session.add(stat)
        session.commit()

        saved = session.query(GamePitchingStat).filter_by(game_id="20250401LGSS0").one()
        assert saved.innings_outs == 9
        assert saved.era == 2.00
        assert saved.whip == 1.00

    def test_decision_field(self):
        stat = make_game_pitching_stat(decision="W")
        assert stat.decision == "W"


class TestGameEvent:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        _seed_player_basic(session)
        session.commit()

        event = make_game_event()
        session.add(event)
        session.commit()

        saved = session.query(GameEvent).filter_by(game_id="20250401LGSS0").one()
        assert saved.event_seq == 1
        assert saved.event_type == "single"
        assert saved.outs == 2

    def test_wpa_fields(self):
        event = make_game_event(wpa=0.15, win_expectancy_before=0.5, win_expectancy_after=0.65)
        assert event.wpa == 0.15
        assert event.score_diff is None

    def test_at_bat_grouping(self):
        event = make_game_event(at_bat_seq=1, at_bat_event_role="result")
        assert event.at_bat_seq == 1


class TestGameValidationMetrics:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        vm = make_game_validation_metrics()
        session.add(vm)
        session.commit()

        saved = session.query(GameValidationMetrics).filter_by(game_id="20250401LGSS0").one()
        assert saved.validation_status == "verified"
        assert saved.duplicate_event_count == 0


class TestGameHighlight:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()

        hl = make_game_highlight()
        session.add(hl)
        session.commit()

        saved = session.query(GameHighlight).filter_by(game_id="20250401LGSS0").one()
        assert saved.highlight_type == "BIG_PLAY"
        assert saved.importance_score == 0.95


class TestPlayerGameBatting:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        _seed_player_basic(session)
        session.commit()

        stat = make_player_game_batting()
        session.add(stat)
        session.commit()

        saved = session.query(PlayerGameBatting).filter_by(game_id="20250401LGSS0").one()
        assert saved.player_id == 12345
        assert saved.plate_appearances == 4


class TestPlayerGamePitching:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        _seed_player_basic(session)
        session.commit()

        stat = make_player_game_pitching()
        session.add(stat)
        session.commit()

        saved = session.query(PlayerGamePitching).filter_by(game_id="20250401LGSS0").one()
        assert saved.player_id == 12345
        assert saved.innings_outs == 9


class TestGameRelationships:
    def test_game_has_summary(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.flush()
        _seed_player_basic(session)
        s = make_game_summary(game_id=g.game_id)
        session.add(s)
        session.commit()

        session.expire_all()
        loaded = session.query(Game).filter_by(game_id="20250401LGSS0").one()
        assert len(loaded.summary) == 1

    def test_game_has_highlight(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.flush()
        hl = make_game_highlight(game_id=g.game_id)
        session.add(hl)
        session.commit()

        session.expire_all()
        loaded = session.query(Game).filter_by(game_id="20250401LGSS0").one()
        assert len(loaded.highlights) == 1

    def test_game_has_events(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.flush()
        e = make_game_event(game_id=g.game_id)
        session.add(e)
        session.commit()

        session.expire_all()
        loaded = session.query(Game).filter_by(game_id="20250401LGSS0").one()
        assert len(loaded.events) == 1

    def test_game_delete_cascades_summary(self):
        from sqlalchemy import delete

        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.flush()
        s = make_game_summary(game_id=g.game_id)
        session.add(s)
        session.commit()

        session.execute(delete(Game).where(Game.game_id == g.game_id))
        session.commit()
        remaining = session.query(GameSummary).all()
        assert len(remaining) == 0

    def test_game_delete_cascades_events(self):
        from sqlalchemy import delete

        _, session = build_session()
        _create_tables(session)
        _seed_player_basic(session)
        g = make_game()
        session.add(g)
        session.flush()
        e = make_game_event(game_id=g.game_id)
        session.add(e)
        session.commit()

        session.execute(delete(Game).where(Game.game_id == g.game_id))
        session.commit()
        remaining = session.query(GameEvent).all()
        assert len(remaining) == 0

    def test_game_delete_cascades_batting_stat(self):
        from sqlalchemy import delete

        _, session = build_session()
        _create_tables(session)
        _seed_player_basic(session)
        g = make_game()
        session.add(g)
        session.flush()
        b = make_game_batting_stat(game_id=g.game_id)
        session.add(b)
        session.commit()

        session.execute(delete(Game).where(Game.game_id == g.game_id))
        session.commit()
        remaining = session.query(GameBattingStat).all()
        assert len(remaining) == 0

    def test_fk_violation_invalid_game_summary(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        s = make_game_summary(game_id="NONEXISTENT")
        session.add(s)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_fk_violation_invalid_player_game_batting(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        session.add(make_game())
        session.commit()
        stat = make_player_game_batting(player_id=99999)
        session.add(stat)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_empty_relationships(self):
        _, session = build_session()
        _create_tables(session)
        g = make_game()
        session.add(g)
        session.commit()

        session.expire_all()
        loaded = session.query(Game).filter_by(game_id="20250401LGSS0").one()
        assert loaded.summary == []
        assert loaded.highlights == []
        assert loaded.events == []
