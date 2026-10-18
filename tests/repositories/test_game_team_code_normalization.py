"""Game rows store the season team code, never the KBO technical token (issue #39).

The detail crawler's scoreboard fallback handed back GameCenter id segments
(``HT``/``OB``/``SK``/``WO``) and the save path wrote them straight to
``game.home_team``/``away_team``/``winning_team``. Child rows were already
normalized, the game row was not, so one franchise ended up under two codes.
"""

from __future__ import annotations

import contextlib
from datetime import date
from unittest.mock import patch

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
from src.models.quarantine import QuarantinedRecord
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_helpers import _apply_game_team_identity, _ensure_game_stub
from src.repositories.game_save import save_game_detail, save_schedule_game
from src.utils.team_codes import season_team_code

FULL_LINE = [1, 0, 0, 0, 0, 0, 0, 0, 0]
ZERO_LINE = [0, 0, 0, 0, 0, 0, 0, 0, 0]


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game,
        GameIdAlias,
        GameInningScore,
        GameLineup,
        GameMetadata,
        GameSummary,
        GameBattingStat,
        GamePitchingStat,
        PlayerBasic,
        PlayerSeasonBatting,
        KboSeason,
        Team,
        QuarantinedRecord,
    ):
        table.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    db.add(KboSeason(season_id=264, season_year=2026, league_type_code=0, league_type_name="정규시즌"))
    db.commit()

    @contextlib.contextmanager
    def _scoped():
        yield db

    with patch("src.repositories.game_save.get_db_session", side_effect=_scoped):
        yield db
    db.close()


@pytest.mark.parametrize(
    ("code", "year", "expected"),
    [
        ("HT", 2026, "KIA"),
        ("OB", 2026, "DB"),
        ("SK", 2026, "SSG"),
        ("WO", 2026, "KH"),
        ("KIA", 2026, "KIA"),
        ("ht ", 2026, "KIA"),
        ("LG", 2026, "LG"),
        ("EA", 2026, "EA"),
        ("HT", 1995, "HT"),
        ("OB", 1995, "OB"),
        ("SK", 2015, "SK"),
        ("WO", 2008, "WO"),
        ("SSG", 2001, "SSG"),
        ("KIA", 1995, "KIA"),
        (None, 2026, None),
        ("", 2026, None),
    ],
)
def test_season_team_code(code, year, expected):
    assert season_team_code(code, year) == expected


def test_apply_identity_rewrites_legacy_codes_and_keeps_franchise_ids():
    game = Game(game_id="20260926LGHT0", home_team="HT", away_team="LG", winning_team="HT")

    _apply_game_team_identity(game, 2026)

    assert (game.home_team, game.away_team, game.winning_team) == ("KIA", "LG", "KIA")
    assert game.home_franchise_id == game.winning_franchise_id == 5
    assert game.away_franchise_id == 3


def test_apply_identity_leaves_unknown_codes_alone():
    game = Game(game_id="20260712EAWE0", home_team="WE", away_team="EA", winning_team=None)

    _apply_game_team_identity(game, 2026)

    assert (game.home_team, game.away_team) == ("WE", "EA")


def test_apply_identity_does_not_rewrite_modern_brand_in_old_season():
    """Only KBO technical tokens are rewritten; existing historical rows stay as stored."""
    game = Game(game_id="20010509SKLT1", home_team="LT", away_team="SSG", winning_team="SSG")

    _apply_game_team_identity(game, 2001)

    assert (game.home_team, game.away_team, game.winning_team) == ("LT", "SSG", "SSG")
    assert game.away_franchise_id == 8


def test_apply_identity_keeps_historical_brand():
    game = Game(game_id="19950501HTOB0", home_team="OB", away_team="HT", winning_team="HT")

    _apply_game_team_identity(game, 1995)

    assert (game.home_team, game.away_team, game.winning_team) == ("OB", "HT", "HT")


def test_game_stub_from_legacy_game_id_stores_season_codes(session):
    _ensure_game_stub(session, "20260915HTSK0")

    game = session.query(Game).filter_by(game_id="20260915HTSK0").one()
    assert (game.away_team, game.home_team) == ("KIA", "SSG")


def test_schedule_save_with_legacy_codes_stores_season_codes(session):
    assert save_schedule_game(
        {
            "game_id": "20260921WOOB0",
            "game_date": "20260921",
            "season_year": 2026,
            "season_type": "regular",
            "away_team_code": "WO",
            "home_team_code": "OB",
            "doubleheader_no": 0,
            "game_status": "SCHEDULED",
        },
        session=session,
    )
    session.flush()

    game = session.query(Game).filter_by(game_id="20260921WOOB0").one()
    assert (game.away_team, game.home_team) == ("KH", "DB")


def test_detail_save_with_scoreboard_fallback_codes_stores_season_codes(session):
    """The 38 KIA games stored as HT: detail payload carrying the raw id segment."""
    payload = {
        "game_id": "20261005HTLG0",
        "game_date": "2026-10-05",
        "game_status": "completed",
        "teams": {
            "away": {"code": "HT", "score": 1, "line_score": FULL_LINE},
            "home": {"code": "LG", "score": 0, "line_score": ZERO_LINE},
        },
        "metadata": {"stadium": "잠실"},
        "hitters": {"away": [], "home": []},
        "pitchers": {"away": [], "home": []},
    }

    assert save_game_detail(payload, session=session)
    session.flush()

    game = session.query(Game).filter_by(game_id="20261005HTLG0").one()
    assert (game.away_team, game.home_team, game.winning_team) == ("KIA", "LG", "KIA")
    assert game.away_franchise_id == game.winning_franchise_id == 5
    inning_codes = {row.team_code for row in session.query(GameInningScore).filter_by(game_id=game.game_id)}
    assert inning_codes <= {"KIA", "LG"}


def test_detail_resave_does_not_regress_normalized_game_to_legacy_code(session):
    session.add(
        Game(
            game_id="20261005HTLG0",
            game_date=date(2026, 10, 5),
            away_team="KIA",
            home_team="LG",
            season_id=264,
        ),
    )
    session.commit()
    payload = {
        "game_id": "20261005HTLG0",
        "game_date": "2026-10-05",
        "game_status": "completed",
        "teams": {
            "away": {"code": "HT", "score": 1, "line_score": FULL_LINE},
            "home": {"code": "LG", "score": 0, "line_score": ZERO_LINE},
        },
        "metadata": {"stadium": "잠실"},
        "hitters": {"away": [], "home": []},
        "pitchers": {"away": [], "home": []},
    }

    assert save_game_detail(payload, session=session)
    session.flush()

    game = session.query(Game).filter_by(game_id="20261005HTLG0").one()
    assert game.away_team == "KIA"
