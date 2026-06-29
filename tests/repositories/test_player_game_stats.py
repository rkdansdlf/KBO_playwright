from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GamePitchingStat,
    GameLineup,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.stat_dataclasses import BattingStats, PitchingStats
from src.repositories.player_game_stats import (
    _compute_batting_rates,
    _compute_pitching_rates,
    _group_batting_by_game_player,
    _group_pitching_by_game_player,
    _upsert_bulk,
    aggregate_game_batting,
    aggregate_game_batting_batch,
    aggregate_game_pitching,
    aggregate_game_pitching_batch,
    bulk_upsert_player_game_batting,
    bulk_upsert_player_game_pitching,
    upsert_player_game_batting,
    upsert_player_game_pitching,
)


class TestRateComputation:
    def test_compute_batting_rates(self):
        stats = BattingStats(hits=2, at_bats=5, walks=1, hbp=0, sf=0, strikeouts=1, doubles=1, triples=0, home_runs=0)
        rates = _compute_batting_rates(stats)
        assert rates["avg"] == 0.400
        assert rates["obp"] == 0.500
        assert rates["slg"] == 0.600
        assert rates["ops"] == 1.100
        assert rates["iso"] == 0.200
        # BABIP = (H - HR) / (AB - K - HR + SF) = (2-0)/(5-1-0+0) = 2/4 = 0.5
        assert rates["babip"] == pytest.approx(0.5, abs=0.001)

    def test_compute_batting_rates_zero_ab(self):
        stats = BattingStats(hits=0, at_bats=0, walks=1, hbp=0, sf=0, strikeouts=0, doubles=0, triples=0, home_runs=0)
        rates = _compute_batting_rates(stats)
        assert rates["avg"] == 0.0
        assert rates["obp"] == 1.0
        assert rates["slg"] == 0.0
        assert rates["ops"] == 1.0

    def test_compute_pitching_rates(self):
        stats = PitchingStats(total_outs=15, hits=3, bb=1, er=1, k=5, hr=0)
        rates = _compute_pitching_rates(stats)
        assert rates["era"] == pytest.approx(1.80, abs=0.01)
        assert rates["whip"] == pytest.approx(0.80, abs=0.01)
        # FIP = (13*HR + 3*BB - 2*K) / IP + 3.10 = (0+3-10)/5+3.10 = 1.7
        assert rates["fip"] == pytest.approx(1.70, abs=0.01)
        assert rates["k_per_nine"] == pytest.approx(9.0, abs=0.01)
        assert rates["bb_per_nine"] == pytest.approx(1.80, abs=0.01)
        assert rates["kbb"] == 5.0

    def test_compute_pitching_rates_zero_ip(self):
        stats = PitchingStats(total_outs=0, hits=0, bb=0, er=0, k=0, hr=0)
        rates = _compute_pitching_rates(stats)
        assert rates["era"] == 0.0
        assert rates["whip"] == 0.0
        assert rates["fip"] == 0.0
        assert rates["k_per_nine"] == 0.0
        assert rates["bb_per_nine"] == 0.0
        assert rates["kbb"] == 0.0


class TestGroupFunctions:
    def test_group_batting_by_game_player(self):
        records = []
        for _i in range(2):
            r = MagicMock(spec=GameBattingStat)
            r.game_id = "20241015LGSS"
            r.player_id = 1001
            r.player_name = "Kim"
            r.team_side = "home"
            r.team_code = "LG"
            r.batting_order = 3
            r.appearance_seq = 1
            r.position = "CF"
            r.is_starter = True
            r.plate_appearances = 3
            r.at_bats = 2
            r.runs = 1
            r.hits = 1
            r.doubles = 0
            r.triples = 0
            r.home_runs = 1
            r.rbi = 2
            r.walks = 1
            r.intentional_walks = 0
            r.hbp = 0
            r.strikeouts = 1
            r.stolen_bases = 0
            r.caught_stealing = 0
            r.sacrifice_hits = 0
            r.sacrifice_flies = 0
            r.gdp = 0
            records.append(r)

        groups = _group_batting_by_game_player(records)
        assert len(groups) == 1
        key = ("20241015LGSS", 1001)
        assert key in groups
        assert groups[key]["hits"] == 2
        assert groups[key]["home_runs"] == 2
        assert groups[key]["is_starter"] is True

    def test_group_batting_skip_null_player_id(self):
        r = MagicMock(spec=GameBattingStat)
        r.player_id = None
        groups = _group_batting_by_game_player([r])
        assert len(groups) == 0

    def test_group_pitching_by_game_player(self):
        records = []
        for _i in range(2):
            r = MagicMock(spec=GamePitchingStat)
            r.game_id = "20241015LGSS"
            r.player_id = 2001
            r.player_name = "Park"
            r.team_side = "away"
            r.team_code = "SS"
            r.is_starting = True
            r.appearance_seq = 1
            r.decision = "W"
            r.innings_outs = 9
            r.hits_allowed = 3
            r.runs_allowed = 1
            r.earned_runs = 1
            r.home_runs_allowed = 0
            r.walks_allowed = 1
            r.strikeouts = 5
            r.hit_batters = 0
            r.wild_pitches = 0
            r.balks = 0
            r.wins = 1
            r.losses = 0
            r.saves = 0
            r.holds = 0
            r.batters_faced = 22
            records.append(r)

        groups = _group_pitching_by_game_player(records)
        assert len(groups) == 1
        entry = groups[("20241015LGSS", 2001)]
        assert entry["is_starting"] is True
        assert entry["decision"] == "W"
        assert entry["strikeouts"] == 10


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    PlayerGameBatting.__table__.create(engine)
    PlayerGamePitching.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


class TestAggregateFunctions:
    def _setup_completed_game(self, session):
        g = Game(game_id="20241015LGSS", game_date=date(2024, 10, 15), game_status="COMPLETED")
        session.add(g)
        session.flush()
        return g

    def test_aggregate_game_batting_no_game(self, session):
        result = aggregate_game_batting(session, "nonexistent")
        assert result == []

    def test_aggregate_game_batting_no_rows(self, session):
        self._setup_completed_game(session)
        result = aggregate_game_batting(session, "20241015LGSS")
        assert result == []

    def test_aggregate_game_batting_with_rows(self, session):
        self._setup_completed_game(session)
        session.add(
            GameBattingStat(
                game_id="20241015LGSS",
                player_id=1001,
                player_name="Kim",
                team_side="home",
                team_code="LG",
                batting_order=3,
                appearance_seq=1,
                is_starter=True,
                position="CF",
                plate_appearances=4,
                at_bats=4,
                hits=2,
                runs=1,
                doubles=0,
                triples=0,
                home_runs=0,
                rbi=1,
                walks=0,
                intentional_walks=0,
                hbp=0,
                strikeouts=0,
                stolen_bases=0,
                caught_stealing=0,
                sacrifice_hits=0,
                sacrifice_flies=0,
                gdp=0,
            ),
        )
        session.commit()

        results = aggregate_game_batting(session, "20241015LGSS")
        assert len(results) == 1
        assert results[0]["hits"] == 2
        assert results[0]["avg"] == 0.5

    def test_aggregate_game_pitching_with_rows(self, session):
        self._setup_completed_game(session)
        session.add(
            GamePitchingStat(
                game_id="20241015LGSS",
                player_id=2001,
                player_name="Park",
                team_side="away",
                team_code="SS",
                is_starting=True,
                appearance_seq=1,
                innings_outs=9,
                hits_allowed=3,
                runs_allowed=1,
                earned_runs=1,
                home_runs_allowed=0,
                walks_allowed=1,
                strikeouts=5,
                hit_batters=0,
                wild_pitches=0,
                balks=0,
                wins=1,
                losses=0,
                saves=0,
                holds=0,
                batters_faced=22,
            ),
        )
        session.commit()

        results = aggregate_game_pitching(session, "20241015LGSS")
        assert len(results) == 1
        assert results[0]["strikeouts"] == 5
        assert results[0]["era"] == pytest.approx(3.0, abs=0.01)

    def test_aggregate_game_batting_batch(self, session):
        self._setup_completed_game(session)
        session.add(
            GameBattingStat(
                game_id="20241015LGSS",
                player_id=1001,
                player_name="Kim",
                team_side="home",
                team_code="LG",
                batting_order=3,
                appearance_seq=1,
                is_starter=True,
                position="CF",
                plate_appearances=4,
                at_bats=4,
                hits=2,
                runs=1,
                doubles=0,
                triples=0,
                home_runs=0,
                rbi=1,
                walks=0,
                intentional_walks=0,
                hbp=0,
                strikeouts=0,
                stolen_bases=0,
                caught_stealing=0,
                sacrifice_hits=0,
                sacrifice_flies=0,
                gdp=0,
            ),
        )
        session.commit()

        results = aggregate_game_batting_batch(session, ["20241015LGSS"])
        assert len(results) == 1

    def test_aggregate_game_batting_batch_empty(self, session):
        results = aggregate_game_batting_batch(session, ["nonexistent"])
        assert results == []

    def test_aggregate_game_pitching_batch(self, session):
        self._setup_completed_game(session)
        session.add(
            GamePitchingStat(
                game_id="20241015LGSS",
                player_id=2001,
                player_name="Park",
                team_side="away",
                team_code="SS",
                is_starting=True,
                appearance_seq=1,
                innings_outs=9,
                hits_allowed=3,
                runs_allowed=1,
                earned_runs=1,
                home_runs_allowed=0,
                walks_allowed=1,
                strikeouts=5,
                hit_batters=0,
                wild_pitches=0,
                balks=0,
                wins=1,
                losses=0,
                saves=0,
                holds=0,
                batters_faced=22,
            ),
        )
        session.commit()

        results = aggregate_game_pitching_batch(session, ["20241015LGSS"])
        assert len(results) == 1


class TestUpsertBulk:
    def test_upsert_bulk_empty(self, session):
        assert _upsert_bulk(session, PlayerGameBatting, []) == 0

    def test_upsert_bulk_player_game_batting(self, session):
        records = [
            {
                "game_id": "20241015LGSS",
                "player_id": 1001,
                "player_name": "Kim",
                "team_side": "home",
                "team_code": "LG",
                "plate_appearances": 4,
                "at_bats": 4,
                "hits": 2,
            },
        ]
        count = upsert_player_game_batting(session, records)
        assert count == 1

        rows = session.query(PlayerGameBatting).all()
        assert len(rows) == 1
        assert rows[0].hits == 2

    def test_upsert_bulk_player_game_pitching(self, session):
        records = [
            {
                "game_id": "20241015LGSS",
                "player_id": 2001,
                "player_name": "Park",
                "team_side": "away",
                "team_code": "SS",
                "innings_outs": 9,
                "strikeouts": 5,
            },
        ]
        count = upsert_player_game_pitching(session, records)
        assert count == 1

    def test_bulk_upsert_player_game_batting(self, session):
        records = [
            {
                "game_id": "20241015LGSS",
                "player_id": 1001,
                "player_name": "Kim",
                "team_side": "home",
                "team_code": "LG",
                "plate_appearances": 4,
                "at_bats": 4,
                "hits": 2,
            },
        ]
        count = bulk_upsert_player_game_batting(session, records)
        assert count == 1

    def test_bulk_upsert_player_game_pitching(self, session):
        records = [
            {
                "game_id": "20241015LGSS",
                "player_id": 2001,
                "player_name": "Park",
                "team_side": "away",
                "team_code": "SS",
                "innings_outs": 9,
                "strikeouts": 5,
            },
        ]
        count = bulk_upsert_player_game_pitching(session, records)
        assert count == 1

    def test_upsert_bulk_dedup(self, session):
        records = [
            {
                "game_id": "G1",
                "player_id": 1,
                "player_name": "A",
                "team_side": "home",
                "team_code": "LG",
                "plate_appearances": 4,
                "at_bats": 4,
                "hits": 1,
            },
        ]
        upsert_player_game_batting(session, records)
        records[0]["hits"] = 2
        upsert_player_game_batting(session, records)
        rows = session.query(PlayerGameBatting).all()
        assert len(rows) == 1
        assert rows[0].hits == 2
