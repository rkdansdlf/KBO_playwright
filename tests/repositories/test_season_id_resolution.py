"""Season id resolution on save (issue #40).

The type is decided from the source signal and the season calendar, then mapped
to a kbo_seasons row by (year, league_type_code). Stale or overlapping
kbo_seasons spans must not decide the type.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.game import Game
from src.models.season import KboSeason
from src.repositories.game_helpers import _resolve_game_season_id, _resolve_schedule_season_id


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    KboSeason.__table__.create(engine)
    return sessionmaker(bind=engine)()


def _season(session: Session, season_id: int, code: int, name: str, start: date | None, end: date | None) -> None:
    session.add(
        KboSeason(
            season_id=season_id,
            season_year=2026,
            league_type_code=code,
            league_type_name=name,
            start_date=start,
            end_date=end,
        ),
    )
    session.flush()


@pytest.fixture
def misconfigured_2026(session: Session) -> Session:
    """kbo_seasons spans as the bug left them: regular covers the exhibition
    window, and the KS row claims early October.
    """
    _season(session, 202600, 0, "정규시즌", date(2026, 3, 12), date(2026, 10, 31))
    _season(session, 202605, 5, "한국시리즈", date(2026, 10, 6), date(2026, 11, 10))
    return session


def test_exhibition_game_is_not_stored_as_regular(misconfigured_2026: Session) -> None:
    payload = {
        "game_date": "2026-03-20",
        "season_year": 2026,
        "season_type": "exhibition",
        "season_type_confirmed": True,
    }
    season_id = _resolve_schedule_season_id(misconfigured_2026, payload, None)
    assert season_id == 202601
    created = misconfigured_2026.get(KboSeason, 202601)
    assert created.league_type_code == 1
    assert created.league_type_name == "시범경기"


def test_regular_game_in_october_is_not_korean_series(misconfigured_2026: Session) -> None:
    payload = {"game_date": "2026-10-06", "season_year": 2026, "season_type": "regular", "season_type_confirmed": True}
    assert _resolve_schedule_season_id(misconfigured_2026, payload, 202605) == 202600


def test_existing_wrong_season_is_corrected_on_recrawl(misconfigured_2026: Session) -> None:
    payload = {"game_date": "2026-10-12", "season_year": 2026, "season_type": "regular", "season_type_confirmed": True}
    assert _resolve_schedule_season_id(misconfigured_2026, payload, 202603) == 202600


def test_postseason_prefix_type_is_kept(misconfigured_2026: Session) -> None:
    payload = {
        "game_date": "2026-10-17",
        "season_year": 2026,
        "season_type": "semi_playoff",
        "season_type_confirmed": True,
    }
    assert _resolve_schedule_season_id(misconfigured_2026, payload, None) == 202603


def test_generic_postseason_uses_the_calendar_stage(misconfigured_2026: Session) -> None:
    payload = {"game_date": "20261014", "season_year": 2026, "season_type": "postseason"}
    assert _resolve_schedule_season_id(misconfigured_2026, payload, None) == 202602


def test_generic_postseason_without_stage_keeps_existing(session: Session) -> None:
    payload = {"game_date": "2031-10-16", "season_year": 2031, "season_type": "postseason"}
    assert _resolve_schedule_season_id(session, payload, 203104) == 203104


def test_unconfirmed_regular_default_follows_calendar(misconfigured_2026: Session) -> None:
    payload = {"game_date": "2026-03-21", "season_year": 2026, "season_type": "regular"}
    assert _resolve_schedule_season_id(misconfigured_2026, payload, None) == 202601


def test_detail_path_without_season_fills_regular_id(misconfigured_2026: Session) -> None:
    # 20260816HHSS0 had no season_id at all; any write path must fill one.
    assert _resolve_game_season_id(misconfigured_2026, {"game_id": "20260816HHSS0"}, date(2026, 8, 16), None) == 202600
