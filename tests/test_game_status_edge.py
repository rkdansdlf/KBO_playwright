from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import src.repositories.game_status as game_status_module
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_status import refresh_game_status_for_date, update_game_status
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)


def _fake_canonicalize(gid):
    if not gid:
        return None, None
    return str(gid).strip().upper(), str(gid).strip().upper()


def _setup_game_tables():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    return engine, sessionmaker(bind=engine)()


@patch("src.repositories.game_status.SessionLocal")
@patch("src.repositories.game_status._canonicalize_game_id", side_effect=_fake_canonicalize)
def test_update_game_status_db_error_returns_false(MockCanon, MockSessionLocal):
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.one_or_none.return_value = None
    mock_session.commit.side_effect = Exception("DB error")
    MockSessionLocal.return_value.__enter__.return_value = mock_session
    MockSessionLocal.return_value.__exit__.return_value = None

    result = update_game_status("20241015LGSSG0", "completed")
    assert result is False


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_past_game_with_scores(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        home_score=5,
        away_score=3,
        game_status="unresolved",
    )
    session.add(g)
    session.add(
        GameBattingStat(game_id="20241015LGSSG0", team_side="home", player_id=1, player_name="test", appearance_seq=1)
    )
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert result["status_counts"].get(GAME_STATUS_COMPLETED) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_future_game(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status="unresolved",
    )
    session.add(g)
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 14))
    assert result["status_counts"].get(GAME_STATUS_SCHEDULED) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_today_with_lineups(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status="unresolved",
    )
    session.add(g)
    session.add(
        GameLineup(
            game_id="20241015LGSSG0",
            team_side="home",
            player_id=1,
            player_name="test",
            appearance_seq=1,
            is_starter=True,
        )
    )
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 15))
    assert result["status_counts"].get(GAME_STATUS_LIVE) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_cancelled_preserved(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status=GAME_STATUS_CANCELLED,
    )
    session.add(g)
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 15))
    assert result["status_counts"].get(GAME_STATUS_CANCELLED) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_postponed_preserved(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status=GAME_STATUS_POSTPONED,
    )
    session.add(g)
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 15))
    assert result["status_counts"].get(GAME_STATUS_POSTPONED) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_metadata_only(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status="unresolved",
    )
    session.add(g)
    session.add(GameMetadata(game_id="20241015LGSSG0", stadium_name="잠실"))
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 15))
    assert result["status_counts"].get(GAME_STATUS_CANCELLED) == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_multiple_games_by_status(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    session.add(
        Game(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            home_score=5,
            away_score=3,
            game_status="unresolved",
        )
    )
    session.add(
        Game(
            game_id="20241015KTWO0",
            game_date=date(2024, 10, 15),
            game_status="unresolved",
        )
    )
    session.add(
        GameBattingStat(game_id="20241015LGSS0", team_side="home", player_id=1, player_name="test", appearance_seq=1)
    )
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert result["total"] == 2
    assert "COMPLETED" in result["status_counts"]
    assert "SCHEDULED" in result["status_counts"] or "UNRESOLVED_MISSING" in result["status_counts"]


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_returns_sorted_ids(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    session.add(Game(game_id="20241015KTWO0", game_date=date(2024, 10, 15)))
    session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert result["game_ids"] == sorted(result["game_ids"])
    assert result["updated_game_ids"] == sorted(result["updated_game_ids"])


from sqlalchemy.exc import SQLAlchemyError


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_db_error_returns_empty(MockSessionLocal):
    mock_session = MagicMock()
    MockSessionLocal.return_value.__enter__.return_value = mock_session
    MockSessionLocal.return_value.__exit__.return_value = None
    mock_session.query.return_value.filter.return_value.all.side_effect = SQLAlchemyError("DB error", None, None)

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert result["total"] == 0
    assert result["updated"] == 0


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_inning_totals_partial(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    g = Game(
        game_id="20241015LGSSG0",
        game_date=date(2024, 10, 15),
        game_status="unresolved",
    )
    session.add(g)
    session.add(GameInningScore(game_id="20241015LGSSG0", team_side="away", inning=1, runs=3))
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert result["total"] == 1


@patch("src.repositories.game_status.SessionLocal")
def test_refresh_game_status_game_ids_by_status_structure(MockSessionLocal):
    engine, session = _setup_game_tables()
    MockSessionLocal.return_value.__enter__.return_value = session
    MockSessionLocal.return_value.__exit__.return_value = None

    session.add(
        Game(
            game_id="20241015LGSS0",
            game_date=date(2024, 10, 15),
            home_score=5,
            away_score=3,
            game_status="unresolved",
        )
    )
    session.add(
        GameBattingStat(game_id="20241015LGSS0", team_side="home", player_id=1, player_name="test", appearance_seq=1)
    )
    session.commit()

    result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
    assert "game_ids_by_status" in result
    for status, ids in result["game_ids_by_status"].items():
        assert ids == sorted(ids)
