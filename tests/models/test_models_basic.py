from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.award import Award
from src.models.broadcast import GameBroadcast
from src.models.injury import InjuryEntry
from src.models.manager_change import ManagerChange
from src.models.player import PlayerBasic


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    Award.metadata.create_all(engine)
    GameBroadcast.metadata.create_all(engine)
    InjuryEntry.metadata.create_all(engine)
    ManagerChange.metadata.create_all(engine)
    PlayerBasic.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


class TestPlayerBasic:
    def test_defaults(self):
        player = PlayerBasic()
        assert player.player_id is None
        assert player.name is None

    def test_custom_values(self, session):
        player = PlayerBasic(
            player_id=12345,
            name="김하성",
            uniform_no="18",
            team="SS",
            position="유격수",
            height_cm=185,
            weight_kg=85,
        )
        session.add(player)
        session.commit()
        session.refresh(player)
        assert player.player_id == 12345
        assert player.name == "김하성"


class TestAward:
    def test_defaults(self):
        award = Award()
        assert award.year is None
        assert award.award_type is None


class TestGameBroadcast:
    def test_defaults(self):
        broadcast = GameBroadcast()
        assert broadcast.game_id is None


class TestInjuryEntry:
    def test_defaults(self):
        entry = InjuryEntry()
        assert entry.player_id is None


class TestManagerChange:
    def test_defaults(self):
        change = ManagerChange()
        assert change.team_id is None
