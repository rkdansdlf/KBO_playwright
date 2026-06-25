"""Tests for ORM model instantiation."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.award import Award
from src.models.broadcast import GameBroadcast
from src.models.embedding_cache import EmbeddingCache
from src.models.fa_contract import FAContract
from src.models.fan_culture import TeamRivalry
from src.models.foreign_player import ForeignPlayerChange
from src.models.franchise import Franchise
from src.models.game_mvp import GameMvp
from src.models.injury import InjuryEntry
from src.models.manager_change import ManagerChange
from src.models.parking_fee_rule import ParkingFeeRule
from src.models.parking_lot import ParkingLot
from src.models.player import PlayerBasic
from src.models.rag_chunk import RagChunk
from src.models.rankings import StatRanking
from src.models.roster_transaction import RosterTransaction
from src.models.season import KboSeason
from src.models.source_registry import DataSource
from src.models.stadium_food import StadiumFood
from src.models.team_event import TeamEvent
from src.models.ticket_open_rule import TicketOpenRule
from src.models.ticket_price import TicketPrice


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def session(engine):
    for cls in [
        Award,
        GameBroadcast,
        EmbeddingCache,
        FAContract,
        TeamRivalry,
        ForeignPlayerChange,
        Franchise,
        GameMvp,
        InjuryEntry,
        ManagerChange,
        ParkingFeeRule,
        ParkingLot,
        PlayerBasic,
        RagChunk,
        StatRanking,
        RosterTransaction,
        KboSeason,
        DataSource,
        StadiumFood,
        TeamEvent,
        TicketOpenRule,
        TicketPrice,
    ]:
        cls.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


class TestModels:
    def test_award(self):
        assert Award().id is None

    def test_broadcast(self):
        assert GameBroadcast().id is None

    def test_embedding_cache(self):
        assert EmbeddingCache().text_hash is None

    def test_fa_contract(self):
        assert FAContract().id is None

    def test_team_rivalry(self):
        assert TeamRivalry().id is None

    def test_foreign_player(self):
        assert ForeignPlayerChange().id is None

    def test_franchise(self):
        assert Franchise().id is None

    def test_game_mvp(self):
        assert GameMvp().id is None

    def test_injury(self):
        assert InjuryEntry().id is None

    def test_manager_change(self):
        assert ManagerChange().id is None

    def test_parking_fee_rule(self):
        assert ParkingFeeRule().id is None

    def test_parking_lot(self):
        assert ParkingLot().id is None

    def test_player_basic(self):
        assert PlayerBasic().player_id is None

    def test_rag_chunk(self):
        assert RagChunk().id is None

    def test_rankings(self):
        assert StatRanking().id is None

    def test_roster_transaction(self):
        assert RosterTransaction().id is None

    def test_season(self):
        assert KboSeason().season_id is None

    def test_data_source(self):
        assert DataSource().id is None

    def test_stadium_food(self):
        assert StadiumFood().id is None

    def test_team_event(self):
        assert TeamEvent().id is None

    def test_ticket_open_rule(self):
        assert TicketOpenRule().id is None

    def test_ticket_price(self):
        assert TicketPrice().id is None
