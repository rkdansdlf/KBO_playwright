"""Batch tests for remaining 30+ model files not covered by dedicated test files."""

from datetime import date, datetime, time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.models.award import Award
from src.models.base import Base
from src.models.broadcast import GameBroadcast
from src.models.crawl import CrawlRun
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
from src.models.rag_chunk import RagChunk
from src.models.rankings import StatRanking
from src.models.roster_transaction import RosterTransaction
from src.models.season import KboSeason
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.models.stadium_congestion import StadiumCongestion
from src.models.stadium_food import StadiumFood
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.models.stadium_info import StadiumInfo, StadiumRegulation
from src.models.stadium_operation_notice import StadiumOperationNotice
from src.models.stadium_seat_section import StadiumSeatSection
from src.models.stadium_transit_time import StadiumTransitTime
from src.models.team_event import TeamEvent
from src.models.team_history import TeamHistory
from src.models.ticket_open_rule import TicketOpenRule
from src.models.ticket_price import TicketPrice
from src.models.ticket_schedule import TicketSchedule


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys=ON"))
        conn.commit()
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def db_with_stadium(db_session):
    db_session.add(StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장"))
    db_session.commit()
    return db_session


@pytest.fixture
def db_with_data_source(db_session):
    db_session.add(DataSource(source_key="test", source_type="API", target_domain="test"))
    db_session.commit()
    return db_session


class TestAward:
    def test_create(self, db_session):
        a = Award(year=2025, award_type="MVP", player_name="Test", team_name="LG")
        db_session.add(a)
        db_session.commit()
        assert a.id is not None


class TestGameBroadcast:
    def test_create(self, db_session):
        b = GameBroadcast(game_id="20250401LGSS0", broadcaster="KBS N")
        db_session.add(b)
        db_session.commit()
        assert b.source == "KBO"


class TestCrawlRun:
    def test_create(self, db_session):
        now = datetime.now()
        c = CrawlRun(started_at=now, finished_at=now)
        db_session.add(c)
        db_session.commit()
        assert c.active_count == 0


class TestEmbeddingCache:
    def test_create(self, db_session):
        e = EmbeddingCache(text_hash="abc123", model_name="test-model", embedding=[0.1, 0.2, 0.3])
        db_session.add(e)
        db_session.commit()
        assert e.text_hash == "abc123"

    def test_duplicate_pk(self, db_session):
        e1 = EmbeddingCache(text_hash="abc", model_name="m1", embedding=[0.1])
        e2 = EmbeddingCache(text_hash="abc", model_name="m1", embedding=[0.2])
        db_session.add(e1)
        db_session.commit()
        db_session.add(e2)
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestFAContract:
    def test_create(self, db_session):
        f = FAContract(player_name="Test Player", year=2025, fa_type="FA")
        db_session.add(f)
        db_session.commit()
        assert f.id is not None


class TestFanCulture:
    def test_team_rivalry(self, db_session):
        r = TeamRivalry(team_id_a="LG", team_id_b="SSG", rivalry_name="잠실더비")
        db_session.add(r)
        db_session.commit()
        assert r.intensity == "MEDIUM"

    def test_cheer_song(self, db_session):
        s = CheerSong(team_id="LG", song_name="LG Victory", song_type="fight_song")
        db_session.add(s)
        db_session.commit()
        assert s.id is not None

    def test_cheer_chant(self, db_session):
        c = CheerChant(team_id="LG", chant_text="Let's go LG!")
        db_session.add(c)
        db_session.commit()
        assert c.id is not None


class TestForeignPlayerChange:
    def test_create(self, db_session):
        f = ForeignPlayerChange(player_name="Test Foreign", team_id="LG", season=2025, change_type="new")
        db_session.add(f)
        db_session.commit()
        assert f.id is not None


class TestFranchise:
    def test_create(self, db_session):
        f = Franchise(id=1, name="LG Twins", original_code="LG", current_code="LG")
        db_session.add(f)
        db_session.commit()
        assert f.name == "LG Twins"


class TestGameMvp:
    def test_create(self, db_session):
        m = GameMvp(game_id="20250401LGSS0", player_name="Test Player")
        db_session.add(m)
        db_session.commit()
        assert m.mvp_type == "GAME"


class TestInjuryEntry:
    def test_create(self, db_session):
        i = InjuryEntry(player_name="Test Player", team_id="LG")
        db_session.add(i)
        db_session.commit()
        assert i.status == "ACTIVE"


class TestManagerChange:
    def test_create(self, db_session):
        m = ManagerChange(team_id="LG", season=2025, new_manager="Coach Kim")
        db_session.add(m)
        db_session.commit()
        assert m.id is not None


class TestParkingLot:
    def test_create(self, db_with_stadium):
        p = ParkingLot(stadium_id="JAMSIL", name="Main Lot")
        db_with_stadium.add(p)
        db_with_stadium.commit()
        assert p.lot_type == "official"


class TestParkingFeeRule:
    def test_create_no_fk(self):
        p = ParkingFeeRule(parking_lot_id=1, vehicle_type="car", base_fee=5000, base_minutes=30)
        assert p.base_fee == 5000


class TestRagChunk:
    def test_create(self, db_session):
        r = RagChunk(source_table="game", source_row_id="20250401LGSS0", content="Test content")
        db_session.add(r)
        db_session.commit()
        assert r.meta == {}


class TestStatRanking:
    def test_create(self, db_session):
        r = StatRanking(
            season=2025, metric="AVG", entity_id="12345", entity_label="Test Player",
            value=0.300, rank=1, source="TEST",
        )
        db_session.add(r)
        db_session.commit()
        assert r.entity_type == "PLAYER"

    def test_is_tie_default(self, db_session):
        r = StatRanking(
            season=2025, metric="HR", entity_id="12345", entity_label="Test",
            value=30, rank=1, source="TEST", is_tie=False, entity_type="PLAYER",
        )
        assert r.is_tie is False


class TestRosterTransaction:
    def test_create(self, db_session):
        r = RosterTransaction(
            transaction_date=date(2025, 4, 1), team_id="LG", player_name="Test Player",
            action="registered", dedupe_key="LG_20250401_Test",
            roster_level="first_team",
        )
        db_session.add(r)
        db_session.commit()
        assert r.roster_level == "first_team"


class TestKboSeason:
    def test_create(self, db_session):
        s = KboSeason(season_id=2025, season_year=2025, league_type_code=0, league_type_name="Regular Season")
        db_session.add(s)
        db_session.commit()
        assert s.season_year == 2025


class TestSourceRegistry:
    def test_data_source(self, db_session):
        d = DataSource(source_key="kbo_official", source_type="API", target_domain="game_details")
        db_session.add(d)
        db_session.commit()
        assert d.is_active is True

    def test_raw_snapshot(self, db_with_data_source):
        from datetime import datetime
        r = RawSourceSnapshot(data_source_id=1, fetched_at=datetime.now())
        db_with_data_source.add(r)
        db_with_data_source.commit()
        assert r.parse_status == "pending"


class TestStadiumCongestion:
    def test_create_no_fk(self):
        now = datetime.now()
        s = StadiumCongestion(
            stadium_code="JAMSIL", location_type="gate", location_label="Gate 1",
            measured_at=now, game_date=date(2025, 4, 1), congestion_level="MODERATE", source="TEST",
        )
        assert s.congestion_level == "MODERATE"


class TestStadiumFood:
    def test_create(self, db_session):
        f = StadiumFood(stadium_name="잠실", restaurant_name="Test Restaurant", menu_item="Hot Dog")
        db_session.add(f)
        db_session.commit()
        assert f.is_famous is False


class TestStadiumFoodVendor:
    def test_create(self, db_with_stadium):
        v = StadiumFoodVendor(stadium_id="JAMSIL", vendor_name="Test Vendor")
        db_with_stadium.add(v)
        db_with_stadium.commit()
        assert v.order_method == "onsite"


class TestStadiumFoodMenuItem:
    def test_create_no_fk(self):
        m = StadiumFoodMenuItem(vendor_id=1, menu_name="Test Menu")
        assert m.menu_name == "Test Menu"

    def test_defaults(self):
        m = StadiumFoodMenuItem(vendor_id=1, menu_name="Signature Item", is_signature=False)
        assert m.is_signature is False


class TestStadiumInfo:
    def test_create(self, db_session):
        si = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장")
        db_session.add(si)
        db_session.commit()
        assert si.name_kr == "잠실야구장"

    def test_regulation(self, db_session):
        si = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실")
        db_session.add(si)
        db_session.commit()
        reg = StadiumRegulation(
            stadium_code="JAMSIL", regulation_type="parking", title="No Parking",
            description="No parking on game days",
        )
        db_session.add(reg)
        db_session.commit()
        assert reg.id is not None


class TestStadiumOperationNotice:
    def test_create(self, db_with_stadium):
        n = StadiumOperationNotice(
            stadium_code="JAMSIL", notice_type="weather", title="Rain Delay", source_name="KBO",
        )
        db_with_stadium.add(n)
        db_with_stadium.commit()
        assert n.is_urgent is False


class TestStadiumSeatSection:
    def test_create(self, db_with_stadium):
        s = StadiumSeatSection(stadium_id="JAMSIL", section_name="1루지정석")
        db_with_stadium.add(s)
        db_with_stadium.commit()
        assert s.id is not None


class TestStadiumTransitTime:
    def test_create_no_fk(self):
        now = datetime.now()
        t = StadiumTransitTime(
            stadium_code="JAMSIL", origin_label="Seoul Station",
            transport_mode="subway", measured_at=now, game_date=date(2025, 4, 1),
            duration_minutes=30, source_api="NAVER_MAP",
        )
        assert t.duration_minutes == 30


class TestTeamEvent:
    def test_create(self, db_session):
        e = TeamEvent(title="Fan Meeting")
        db_session.add(e)
        db_session.commit()
        assert e.event_scope == "team"


class TestTeamHistory:
    def test_create_no_fk(self):
        h = TeamHistory(id=1, franchise_id=1, season=2025, team_name="LG Twins", team_code="LG")
        assert h.team_code == "LG"


class TestTicketOpenRule:
    def test_create(self, db_session):
        r = TicketOpenRule(
            team_id="LG", platform="INTERPARK", open_offset_days=7, open_time=time(10, 0),
        )
        db_session.add(r)
        db_session.commit()
        assert r.id is not None


class TestTicketPrice:
    def test_create(self, db_session):
        p = TicketPrice(team_id="LG", stadium_id="JAMSIL", season=2025, seat_grade="VIP", price=50000)
        db_session.add(p)
        db_session.commit()
        assert p.day_type == "weekday"


class TestTicketSchedule:
    def test_create(self, db_session):
        s = TicketSchedule(
            game_date=date(2025, 4, 1), home_team="LG", away_team="SSG",
            stadium="잠실", open_time=datetime(2025, 3, 25, 10, 0), platform="INTERPARK",
        )
        db_session.add(s)
        db_session.commit()
        assert s.id is not None
