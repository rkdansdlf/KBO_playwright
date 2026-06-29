"""Tests for ORM model instantiation."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.award import Award
from src.models.broadcast import GameBroadcast
from src.models.embedding_cache import EmbeddingCache
from src.models.fa_contract import FAContract
from src.models.fan_culture import CheerChant, CheerSong, TeamRivalry
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
        CheerSong,
        CheerChant,
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


class TestModelRepr:
    """Tests for ORM model __repr__ methods (covers missing lines in each model)."""

    def test_award_repr(self):
        obj = Award(year=2025, award_type="MVP", category="Batter", player_name="Hong", team_name="LG")
        r = repr(obj)
        assert "MVP" in r
        assert "Hong" in r

    def test_broadcast_repr(self):
        obj = GameBroadcast(game_id="20260412LG0", broadcaster="KBS")
        assert "KBS" in repr(obj)

    def test_embedding_cache_repr(self):
        obj = EmbeddingCache(model_name="test-model", text_hash="abc12345")
        assert "test-model" in repr(obj)

    def test_fa_contract_repr(self):
        obj = FAContract(player_name="Kim", year=2025, fa_type="FA", new_team="LG", total_amount="100000000")
        assert "Kim" in repr(obj)

    def test_team_rivalry_repr(self):
        obj = TeamRivalry(rivalry_name="Metro")
        assert "Metro" in repr(obj)

    def test_foreign_player_repr(self):
        obj = ForeignPlayerChange(player_name="John", team_id="LG", change_type="contract", season=2025)
        assert "John" in repr(obj)

    def test_franchise_repr(self):
        obj = Franchise(name="LG Twins", original_code="LG", current_code="LG")
        assert "LG" in repr(obj)

    def test_game_mvp_repr(self):
        obj = GameMvp(game_id="20260412LG0", player_name="Hong")
        assert "Hong" in repr(obj)

    def test_injury_repr(self):
        obj = InjuryEntry(player_name="Kim", injury_type="shoulder")
        assert "Kim" in repr(obj)

    def test_manager_change_repr(self):
        obj = ManagerChange(team_id="LG", season=2025, new_manager="Kim", change_reason="dismiss")
        r = repr(obj)
        assert "LG" in r
        assert "Kim" in r

    def test_parking_fee_rule_repr(self):
        obj = ParkingFeeRule(parking_lot_id=1, vehicle_type="sedan", base_fee=5000, base_minutes=60)
        r = repr(obj)
        assert "sedan" in r
        assert "5000" in r

    def test_parking_lot_repr(self):
        obj = ParkingLot(name="Main", capacity=100)
        assert "Main" in repr(obj)

    def test_rag_chunk_repr(self):
        obj = RagChunk(
            league_type_code=1,
            source_table="games",
            source_row_id="1",
            title="Game 1",
            content="some content",
        )
        r = repr(obj)
        assert "games" in r
        assert "Game 1" in r

    def test_rankings_repr(self):
        obj = StatRanking(
            season=2025,
            metric="AVG",
            entity_id="P1",
            entity_label="Player1",
            entity_type="PLAYER",
            value=0.350,
            rank=1,
            source="kbo",
        )
        r = repr(obj)
        assert "AVG" in r
        assert "2025" in r

    def test_roster_transaction_repr(self):
        obj = RosterTransaction(
            player_id=1,
            roster_level="1ST",
            source_type="ROSTER",
            source_id=1,
            dedupe_key="dup1",
            transaction_date="2025-06-01",
            team_id="LG",
            player_name="Hong",
            action="registered",
        )
        r = repr(obj)
        assert "LG" in r
        assert "Hong" in r

    def test_season_repr(self):
        obj = KboSeason(season_year=2025, league_type_name="KBO")
        r = repr(obj)
        assert "2025" in r
        assert "KBO" in r

    def test_stadium_food_repr(self):
        obj = StadiumFood(stadium_name="Jamsil", restaurant_name="FoodCourt", menu_item="Ramen")
        r = repr(obj)
        assert "Jamsil" in r
        assert "Ramen" in r

    def test_team_event_repr(self):
        obj = TeamEvent(title="Event1", event_scope="ALL", status="ACTIVE")
        r = repr(obj)
        assert "Event1" in r
        assert "ACTIVE" in r

    def test_ticket_open_rule_repr(self):
        obj = TicketOpenRule(team_id="LG", platform="INTERPARK", open_offset_days=3)
        r = repr(obj)
        assert "LG" in r
        assert "INTERPARK" in r

    def test_ticket_price_repr(self):
        obj = TicketPrice(team_id="LG", seat_grade="VIP", price=50000)
        r = repr(obj)
        assert "LG" in r
        assert "50000" in r

    def test_cheer_song_repr(self):
        obj = CheerSong(team_id="LG", song_name="Hero", song_type="PERSONAL")
        r = repr(obj)
        assert "LG" in r
        assert "Hero" in r

    def test_cheer_chant_repr(self):
        obj = CheerChant(team_id="LG", chant_text="LG! LG! Fight!")
        assert "LG" in repr(obj)
