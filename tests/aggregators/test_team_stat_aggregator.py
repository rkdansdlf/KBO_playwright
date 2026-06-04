import datetime
import unittest
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.standings import TeamStandingsDaily
from src.models.team import Team
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.aggregators.team_stat_aggregator import TeamStatAggregator


class TestTeamStatAggregatorPure(unittest.TestCase):
    def setUp(self):
        self.aggregator = TeamStatAggregator()

    def test_aggregate_batting_calculation(self):
        # A팀 선수 1: AB=10, H=3, BB=1, HBP=0, SF=1, doubles=1, triples=0, home_runs=1, games=5
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="LG",
            games=5,
            plate_appearances=12,
            at_bats=10,
            runs=2,
            hits=3,
            doubles=1,
            triples=0,
            home_runs=1,
            rbi=3,
            walks=1,
            intentional_walks=0,
            hbp=0,
            strikeouts=2,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=1,
            gdp=0,
            league="REGULAR",
        )

        # A팀 선수 2: AB=20, H=5, BB=2, HBP=1, SF=0, doubles=1, triples=1, home_runs=1, games=10
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="LG",
            games=10,
            plate_appearances=23,
            at_bats=20,
            runs=4,
            hits=5,
            doubles=1,
            triples=1,
            home_runs=1,
            rbi=4,
            walks=2,
            intentional_walks=0,
            hbp=1,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=1,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
            league="REGULAR",
        )

        rows = [p1, p2]
        team_names = {"LG": "트윈스"}

        results = self.aggregator.aggregate_batting(rows, team_names=team_names)

        self.assertEqual(len(results), 1)
        res = results[0]

        # Counting Sum Checks
        self.assertEqual(res["team_id"], "LG")
        self.assertEqual(res["team_name"], "트윈스")
        self.assertEqual(res["season"], 2025)
        self.assertEqual(res["games"], 10)  # Max games from players

        self.assertEqual(res["at_bats"], 30)
        self.assertEqual(res["hits"], 8)
        self.assertEqual(res["walks"], 3)
        self.assertEqual(res["hbp"], 1)
        self.assertEqual(res["sacrifice_flies"], 1)

        # Ratio Checks
        # AVG = 8 / 30 = 0.267
        # OBP = (8 + 3 + 1) / (30 + 3 + 1 + 1) = 12 / 35 = 0.343
        # TB = 7 + 11 = 18. SLG = 18 / 30 = 0.600
        # OPS = 0.343 + 0.600 = 0.943
        self.assertAlmostEqual(res["avg"], 0.267, places=3)
        self.assertAlmostEqual(res["obp"], 0.343, places=3)
        self.assertAlmostEqual(res["slg"], 0.600, places=3)
        self.assertAlmostEqual(res["ops"], 0.943, places=3)

    def test_aggregate_pitching_calculation(self):
        # A팀 투수 1: IP=5.0 (outs=15), ER=2, H=4, BB=1, SO=5, games=2
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="LG",
            games=2,
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=15,
            hits_allowed=4,
            runs_allowed=2,
            earned_runs=2,
            home_runs_allowed=1,
            walks_allowed=1,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=5,
            league="REGULAR",
        )

        # A팀 투수 2: IP=4.0 (outs=12), ER=1, H=3, BB=2, SO=4, games=4
        p2 = PlayerSeasonPitching(
            id=2,
            season=2025,
            team_code="LG",
            games=4,
            wins=0,
            losses=1,
            saves=1,
            holds=1,
            innings_outs=12,
            hits_allowed=3,
            runs_allowed=1,
            earned_runs=1,
            home_runs_allowed=0,
            walks_allowed=2,
            intentional_walks=0,
            hit_batters=1,
            strikeouts=4,
            league="REGULAR",
        )

        rows = [p1, p2]
        team_names = {"LG": "트윈스"}

        results = self.aggregator.aggregate_pitching(rows, team_names=team_names)

        self.assertEqual(len(results), 1)
        res = results[0]

        # Counting Sum Checks
        self.assertEqual(res["team_id"], "LG")
        self.assertEqual(res["innings_outs"], 27)
        self.assertEqual(res["innings_pitched"], 9.0)
        self.assertEqual(res["earned_runs"], 3)
        self.assertEqual(res["hits_allowed"], 7)
        self.assertEqual(res["walks_allowed"], 3)
        self.assertEqual(res["strikeouts"], 9)

        # Ratio Checks
        # ERA = (3 / 9.0) * 9 = 3.00
        # WHIP = (3 + 7) / 9.0 = 10 / 9.0 = 1.11
        self.assertAlmostEqual(res["era"], 3.00, places=2)
        self.assertAlmostEqual(res["whip"], 1.11, places=2)

    def test_division_by_zero_safety(self):
        # Test case: 0 At Bats, 0 Innings Outs
        p_bat = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="LT",
            games=1,
            plate_appearances=0,
            at_bats=0,
            hits=0,
        )
        p_pit = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="LT",
            games=1,
            innings_outs=0,
            earned_runs=0,
        )

        bat_res = self.aggregator.aggregate_batting([p_bat])
        pit_res = self.aggregator.aggregate_pitching([p_pit])

        self.assertEqual(bat_res[0]["avg"], 0.0)
        self.assertEqual(bat_res[0]["obp"], 0.0)
        self.assertEqual(bat_res[0]["slg"], 0.0)

        self.assertEqual(pit_res[0]["era"], 0.0)
        self.assertEqual(pit_res[0]["whip"], 0.0)

    def test_invalid_team_exclusion(self):
        # Rows with placeholder team codes like "합계", "-", or None should be skipped
        p1 = PlayerSeasonBatting(id=1, season=2025, team_code=None)
        p2 = PlayerSeasonBatting(id=2, season=2025, team_code="합계")
        p3 = PlayerSeasonBatting(id=3, season=2025, team_code="TOTAL")
        p4 = PlayerSeasonBatting(id=4, season=2025, team_code="OB", at_bats=10, hits=3)

        results = self.aggregator.aggregate_batting([p1, p2, p3, p4])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["team_id"], "OB")


# Database integration tests mapped from the original test file
@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    # Create tables
    for table in (
        Team.__table__,
        TeamStandingsDaily.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        TeamSeasonBatting.__table__,
        TeamSeasonPitching.__table__,
    ):
        table.create(bind=engine)
    TestSession = sessionmaker(bind=engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture
def seed_teams(db_session):
    teams = [
        Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울"),
        Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"),
    ]
    for t in teams:
        db_session.add(t)
    db_session.commit()
    return db_session


@pytest.fixture
def seed_standings(seed_teams):
    session = seed_teams
    standings = [
        TeamStandingsDaily(
            standings_date=datetime.date(2025, 10, 1),
            team_code="OB",
            games_played=144,
            wins=80,
            losses=56,
            draws=8,
            win_pct=0.588,
            games_behind=0.0,
            current_streak=3,
            runs_scored=700,
            runs_allowed=550,
            run_differential=150,
            rank=2,
            top_5=True,
            recent_10_wins=6,
            recent_10_losses=4,
            recent_10_draws=0,
        ),
        TeamStandingsDaily(
            standings_date=datetime.date(2025, 10, 1),
            team_code="SS",
            games_played=144,
            wins=75,
            losses=65,
            draws=4,
            win_pct=0.536,
            games_behind=5.0,
            current_streak=-2,
            runs_scored=650,
            runs_allowed=620,
            run_differential=30,
            rank=3,
            top_5=True,
            recent_10_wins=4,
            recent_10_losses=6,
            recent_10_draws=0,
        ),
    ]
    for s in standings:
        session.add(s)
    session.commit()
    return session


class TestGetTeamGames:
    def test_returns_from_standings(self, seed_standings):
        session = seed_standings
        result = TeamStatAggregator._get_team_games(session, "OB", 2025)
        assert result == 144

    def test_standings_filtered_by_year(self, seed_standings):
        session = seed_standings
        result = TeamStatAggregator._get_team_games(session, "OB", 2024)
        assert result == 0


class TestAggregateTeamBatting:
    def seed_batting_data(self, session):
        players = [
            PlayerSeasonBatting(
                id=101,
                player_id=1,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=144,
                plate_appearances=600,
                at_bats=520,
                runs=85,
                hits=160,
                doubles=30,
                triples=5,
                home_runs=25,
                rbi=95,
                walks=60,
                intentional_walks=5,
                hbp=8,
                strikeouts=100,
                stolen_bases=20,
                caught_stealing=5,
                sacrifice_hits=5,
                sacrifice_flies=7,
                gdp=15,
            ),
            PlayerSeasonBatting(
                id=102,
                player_id=2,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=120,
                plate_appearances=450,
                at_bats=400,
                runs=60,
                hits=110,
                doubles=20,
                triples=2,
                home_runs=10,
                rbi=55,
                walks=40,
                intentional_walks=3,
                hbp=3,
                strikeouts=80,
                stolen_bases=10,
                caught_stealing=3,
                sacrifice_hits=3,
                sacrifice_flies=4,
                gdp=10,
            ),
            PlayerSeasonBatting(
                id=103,
                player_id=3,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="SS",
                games=130,
                plate_appearances=500,
                at_bats=450,
                runs=70,
                hits=120,
                doubles=25,
                triples=3,
                home_runs=15,
                rbi=70,
                walks=45,
                intentional_walks=4,
                hbp=2,
                strikeouts=90,
                stolen_bases=8,
                caught_stealing=4,
                sacrifice_hits=4,
                sacrifice_flies=3,
                gdp=12,
            ),
        ]
        for p in players:
            session.add(p)
        session.commit()

    def test_aggregates_correct_counting_stats(self, seed_standings):
        session = seed_standings
        self.seed_batting_data(session)
        results = TeamStatAggregator.aggregate_team_batting(session, 2025, "REGULAR")

        assert len(results) == 2

        ob = next(r for r in results if r["team_id"] == "OB")
        assert ob["games"] == 144
        assert ob["hits"] == 270
        assert ob["home_runs"] == 35
        assert ob["rbi"] == 150
        assert ob["at_bats"] == 920
        assert ob["plate_appearances"] == 1050
        assert ob["walks"] == 100
        assert ob["strikeouts"] == 180

        ss = next(r for r in results if r["team_id"] == "SS")
        assert ss["games"] == 144
        assert ss["hits"] == 120
        assert ss["home_runs"] == 15

    def test_computes_ratio_stats(self, seed_standings):
        session = seed_standings
        self.seed_batting_data(session)
        results = TeamStatAggregator.aggregate_team_batting(session, 2025, "REGULAR")

        ob = next(r for r in results if r["team_id"] == "OB")
        expected_avg = round(270 / 920, 3)
        assert ob["avg"] == pytest.approx(expected_avg, rel=1e-4)

        obp_numerator = 270 + 100 + 11
        obp_denominator = 920 + 100 + 11 + 11
        expected_obp = round(obp_numerator / obp_denominator, 3)
        assert ob["obp"] == pytest.approx(expected_obp, rel=1e-4)

        slg_numerator = 270 + 50 + 2 * 7 + 3 * 35
        expected_slg = round(slg_numerator / 920, 3)
        assert ob["slg"] == pytest.approx(expected_slg, rel=1e-4)

    def test_filters_out_other_leagues(self, seed_standings):
        session = seed_standings
        self.seed_batting_data(session)
        session.add(
            PlayerSeasonBatting(
                id=104,
                player_id=4,
                season=2025,
                league="POSTSEASON",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=5,
                plate_appearances=20,
                at_bats=18,
                hits=7,
                home_runs=2,
            )
        )
        session.commit()

        results = TeamStatAggregator.aggregate_team_batting(session, 2025, "REGULAR")
        ob = next(r for r in results if r["team_id"] == "OB")
        assert ob["hits"] == 270

    def test_no_data_returns_empty(self, db_session):
        results = TeamStatAggregator.aggregate_team_batting(db_session, 2025, "REGULAR")
        assert results == []


class TestAggregateTeamPitching:
    def seed_pitching_data(self, session):
        players = [
            PlayerSeasonPitching(
                id=201,
                player_id=1,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=30,
                wins=15,
                losses=8,
                saves=0,
                holds=0,
                innings_pitched=180.0,
                innings_outs=540,
                hits_allowed=150,
                runs_allowed=65,
                earned_runs=55,
                home_runs_allowed=15,
                walks_allowed=45,
                strikeouts=160,
            ),
            PlayerSeasonPitching(
                id=202,
                player_id=2,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=50,
                wins=5,
                losses=3,
                saves=20,
                holds=10,
                innings_pitched=70.0,
                innings_outs=210,
                hits_allowed=60,
                runs_allowed=25,
                earned_runs=20,
                home_runs_allowed=5,
                walks_allowed=20,
                strikeouts=70,
            ),
            PlayerSeasonPitching(
                id=203,
                player_id=3,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="SS",
                games=28,
                wins=12,
                losses=7,
                saves=0,
                holds=0,
                innings_pitched=165.0,
                innings_outs=495,
                hits_allowed=140,
                runs_allowed=75,
                earned_runs=65,
                home_runs_allowed=18,
                walks_allowed=40,
                strikeouts=140,
            ),
        ]
        for p in players:
            session.add(p)
        session.commit()

    def test_aggregates_correct_counting_stats(self, seed_standings):
        session = seed_standings
        self.seed_pitching_data(session)
        results = TeamStatAggregator.aggregate_team_pitching(session, 2025, "REGULAR")

        assert len(results) == 2

        ob = next(r for r in results if r["team_id"] == "OB")
        assert ob["games"] == 144
        assert ob["wins"] == 20
        assert ob["losses"] == 11
        assert ob["saves"] == 20
        assert ob["holds"] == 10
        assert ob["innings_outs"] == 750
        assert ob["innings_pitched"] == 250.0
        assert ob["earned_runs"] == 75
        assert ob["strikeouts"] == 230

    def test_computes_ratio_stats(self, seed_standings):
        session = seed_standings
        self.seed_pitching_data(session)
        results = TeamStatAggregator.aggregate_team_pitching(session, 2025, "REGULAR")

        ob = next(r for r in results if r["team_id"] == "OB")
        assert ob["era"] == pytest.approx(75 * 9 / 250.0, rel=1e-4)
        assert ob["whip"] == pytest.approx((45 + 20 + 150 + 60) / 250.0, rel=1e-4)

    def test_no_data_returns_empty(self, db_session):
        results = TeamStatAggregator.aggregate_team_pitching(db_session, 2025, "REGULAR")
        assert results == []


class TestTeamStatAggregatorInstanceMethods:
    def seed_batting_data(self, session):
        session.add(
            PlayerSeasonBatting(
                id=301,
                player_id=1,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=10,
                plate_appearances=40,
                at_bats=35,
                runs=5,
                hits=10,
                doubles=2,
                triples=0,
                home_runs=1,
                rbi=5,
                walks=4,
                intentional_walks=0,
                hbp=1,
                strikeouts=5,
                stolen_bases=1,
                caught_stealing=0,
                sacrifice_hits=0,
                sacrifice_flies=0,
                gdp=1,
            )
        )
        session.commit()

    def seed_pitching_data(self, session):
        session.add(
            PlayerSeasonPitching(
                id=302,
                player_id=1,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=5,
                wins=2,
                losses=1,
                saves=1,
                holds=0,
                innings_pitched=15.0,
                innings_outs=45,
                hits_allowed=10,
                runs_allowed=4,
                earned_runs=3,
                home_runs_allowed=1,
                walks_allowed=5,
                strikeouts=15,
                tbf=60,
                complete_games=0,
                shutouts=0,
                wild_pitches=0,
                balks=0,
                sacrifices_allowed=0,
                sacrifice_flies_allowed=0,
            )
        )
        session.commit()

    def test_aggregate_batting_instance(self, seed_standings):
        session = seed_standings
        self.seed_batting_data(session)
        
        aggregator = TeamStatAggregator(session)
        results = aggregator.aggregate_batting(2025)
        
        assert len(results) == 1
        assert results[0]["team_id"] == "OB"
        assert results[0]["hits"] == 10
        
        # Verify saved in DB
        db_records = session.query(TeamSeasonBatting).filter_by(season=2025).all()
        assert len(db_records) == 1
        assert db_records[0].team_id == "OB"
        assert db_records[0].hits == 10

    def test_aggregate_pitching_instance(self, seed_standings):
        session = seed_standings
        self.seed_pitching_data(session)
        
        aggregator = TeamStatAggregator(session)
        results = aggregator.aggregate_pitching(2025)
        
        assert len(results) == 1
        assert results[0]["team_id"] == "OB"
        assert results[0]["wins"] == 2
        assert results[0]["avg_against"] == pytest.approx(10 / (60 - 5), rel=1e-4) # hits / (tbf - bb)
        
        # Verify saved in DB
        db_records = session.query(TeamSeasonPitching).filter_by(season=2025).all()
        assert len(db_records) == 1
        assert db_records[0].team_id == "OB"
        assert db_records[0].wins == 2

    def test_aggregate_all_instance(self, seed_standings):
        session = seed_standings
        self.seed_batting_data(session)
        self.seed_pitching_data(session)

        aggregator = TeamStatAggregator(session)
        all_results = aggregator.aggregate_all(2025)
        
        assert "batting" in all_results
        assert "pitching" in all_results
        assert len(all_results["batting"]) == 1
        assert len(all_results["pitching"]) == 1
