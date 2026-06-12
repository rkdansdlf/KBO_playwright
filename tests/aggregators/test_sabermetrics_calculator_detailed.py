"""Tests for SabermetricsCalculator — wOBA, wRC+, FIP, WAR calculations."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.sabermetrics_calculator import SabermetricsCalculator
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    PlayerSeasonBatting.__table__.create(bind=engine)
    PlayerSeasonPitching.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_batter(
    session,
    player_id=10001,
    pa=600,
    ab=520,
    hits=160,
    doubles=30,
    triples=5,
    home_runs=25,
    walks=60,
    intentional_walks=5,
    hbp=8,
    sacrifice_flies=7,
    runs=85,
    rbi=95,
    strikeouts=100,
    stolen_bases=20,
    caught_stealing=5,
    gdp=15,
    avg=0.308,
    obp=0.380,
    slg=0.496,
):
    session.add(
        PlayerSeasonBatting(
            player_id=player_id,
            season=2025,
            league="REGULAR",
            plate_appearances=pa,
            at_bats=ab,
            hits=hits,
            doubles=doubles,
            triples=triples,
            home_runs=home_runs,
            walks=walks,
            intentional_walks=intentional_walks,
            hbp=hbp,
            sacrifice_flies=sacrifice_flies,
            runs=runs,
            rbi=rbi,
            strikeouts=strikeouts,
            stolen_bases=stolen_bases,
            caught_stealing=caught_stealing,
            gdp=gdp,
            avg=avg,
            obp=obp,
            slg=slg,
        )
    )
    session.commit()


def _add_pitcher(
    session,
    player_id=20001,
    ip_outs=540,
    earned_runs=65,
    home_runs_allowed=15,
    walks_allowed=45,
    hit_batters=5,
    strikeouts=160,
    runs_allowed=70,
    hits_allowed=140,
    era=3.25,
):
    session.add(
        PlayerSeasonPitching(
            player_id=player_id,
            season=2025,
            league="REGULAR",
            innings_outs=ip_outs,
            earned_runs=earned_runs,
            home_runs_allowed=home_runs_allowed,
            walks_allowed=walks_allowed,
            hit_batters=hit_batters,
            strikeouts=strikeouts,
            runs_allowed=runs_allowed,
            hits_allowed=hits_allowed,
            era=era,
        )
    )
    session.commit()


class TestSabermetricsCalculatorLeagueConstants:
    def test_with_data(self, session):
        _add_batter(
            session,
            pa=600,
            ab=520,
            hits=160,
            doubles=30,
            triples=5,
            home_runs=25,
            walks=60,
            intentional_walks=5,
            hbp=8,
            sacrifice_flies=7,
            runs=85,
        )
        _add_pitcher(
            session,
            ip_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
        )
        lg = SabermetricsCalculator.get_league_constants(session, 2025)
        assert lg["lg_woba"] > 0
        assert lg["woba_scale"] > 0
        assert lg["lg_r_per_pa"] > 0
        assert lg["fip_constant"] is not None
        assert lg["rpw"] > 0
        assert lg["lg_era"] > 0
        assert lg["lg_obp"] > 0
        assert lg["lg_slg"] > 0

    def test_empty_data_returns_defaults(self, session):
        lg = SabermetricsCalculator.get_league_constants(session, 2025)
        assert lg["lg_woba"] == 0.320
        assert lg["lg_r_per_pa"] == 0.0
        assert lg["rpw"] > 0

    def test_filters_by_year(self, session):
        _add_batter(session, player_id=10001)
        lg_2025 = SabermetricsCalculator.get_league_constants(session, 2025)
        lg_2024 = SabermetricsCalculator.get_league_constants(session, 2024)
        assert lg_2024["lg_woba"] == 0.320
        assert lg_2025["lg_woba"] > 0.320

    def test_filters_incomplete_data(self, session):
        session.add(
            PlayerSeasonBatting(
                player_id=1,
                season=2025,
                plate_appearances=5,
                home_runs=0,
                walks=0,
            )
        )
        session.commit()
        _add_batter(session, player_id=10001)
        lg = SabermetricsCalculator.get_league_constants(session, 2025)
        assert lg["lg_woba"] > 0


class TestSabermetricsCalculatorBattingMetrics:
    def test_calculate_woba(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=520,
            hits=160,
            doubles=30,
            triples=5,
            home_runs=25,
            walks=60,
            intentional_walks=5,
            hbp=8,
            sacrifice_flies=7,
            plate_appearances=600,
        )
        lg = {"lg_woba": 0.340, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.350, "lg_slg": 0.450}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["woba"] > 0
        assert result["wraa"] is not None
        assert result["wrc_plus"] is not None
        assert result["ops_plus"] is not None
        assert result["war"] is not None

    def test_batting_high_performer(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=400,
            hits=140,
            doubles=30,
            triples=5,
            home_runs=30,
            walks=80,
            intentional_walks=10,
            hbp=10,
            sacrifice_flies=5,
            plate_appearances=500,
            obp=0.440,
            slg=0.650,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.440}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["woba"] > lg["lg_woba"]
        assert result["wrc_plus"] > 100
        assert result["ops_plus"] > 100
        assert result["war"] > 0

    def test_batting_zero_pa(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=0,
            hits=0,
            plate_appearances=0,
            obp=0.0,
            slg=0.0,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.440}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["woba"] == 0.0
        assert result["war"] == 0.0

    def test_batting_exact_woba_calculation(self):
        # h_1b = 80 - 20 - 5 - 10
        # u_bb = 50 - 5
        # wOBA = (0.69*45 + 0.72*5 + 0.89*45 + 1.27*20 + 1.62*5 + 2.10*10) / (400 + 45 + 5 + 5)
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=400,
            hits=80,
            doubles=20,
            triples=5,
            home_runs=10,
            walks=50,
            intentional_walks=5,
            hbp=5,
            sacrifice_flies=5,
            plate_appearances=460,
            obp=0.300,
            slg=0.400,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.330, "lg_slg": 0.420}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        numerator = 0.69 * 45 + 0.72 * 5 + 0.89 * 45 + 1.27 * 20 + 1.62 * 5 + 2.10 * 10
        denominator = 400 + 45 + 5 + 5
        expected_woba = round(numerator / denominator, 3)
        assert result["woba"] == expected_woba

    def test_batting_war_calculation(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=500,
            hits=150,
            doubles=30,
            triples=3,
            home_runs=20,
            walks=60,
            intentional_walks=5,
            hbp=10,
            sacrifice_flies=8,
            plate_appearances=580,
            obp=0.360,
            slg=0.480,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.450}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        h_1b = 150 - 30 - 3 - 20
        u_bb = 60 - 5
        woba_num = 0.69 * u_bb + 0.72 * 10 + 0.89 * h_1b + 1.27 * 30 + 1.62 * 3 + 2.10 * 20
        woba_den = 500 + u_bb + 10 + 8
        woba = round(woba_num / woba_den, 3)
        wraa = round(((woba - 0.320) / 1.2) * 580, 1)
        expected_war = round(wraa / 10.0, 2)
        assert result["war"] == expected_war


class TestSabermetricsCalculatorPitchingMetrics:
    def test_calculate_fip(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        ip = 540 / 3.0  # 180.0
        fip = ((13 * 15 + 3 * (45 + 5) - 2 * 160) / ip) + 3.20
        assert result["fip_adj"] == pytest.approx(round(fip, 2), abs=0.01)
        assert result["war"] is not None

    def test_pitching_war(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        ip = 540 / 3.0
        fip = ((13 * 15 + 3 * (45 + 5) - 2 * 160) / ip) + 3.20
        war = round((4.00 - fip) * (ip / 9.0) / 10.0, 2)
        assert result["war"] == war

    def test_pitching_lob_pct(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        h, bb, hbp, r, hr = 140, 45, 5, 70, 15
        denom = h + bb + hbp - 1.4 * hr
        expected_lob = round(((h + bb + hbp - r) / denom), 3)
        assert result["lob_pct"] == expected_lob

    def test_pitching_zero_ip(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=0,
            earned_runs=0,
            home_runs_allowed=0,
            walks_allowed=0,
            hit_batters=0,
            strikeouts=0,
            runs_allowed=0,
            hits_allowed=0,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert result["fip_adj"] == 0.0
        assert result["war"] == 0.0
        assert result["lob_pct"] is None

    def test_pitching_high_performer(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=600,
            earned_runs=40,
            home_runs_allowed=8,
            walks_allowed=20,
            hit_batters=3,
            strikeouts=200,
            runs_allowed=45,
            hits_allowed=100,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.50, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert result["fip_adj"] < lg["lg_era"]
        assert result["war"] > 0


class TestSabermetricsCalculatorOpsPlus:
    def test_ops_plus_above_average(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            obp=0.400,
            slg=0.550,
            at_bats=500,
            hits=150,
            plate_appearances=550,
        )
        lg = {"lg_obp": 0.340, "lg_slg": 0.440, "lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        expected = round(((0.400 / 0.340) + (0.550 / 0.440) - 1) * 100, 0)
        assert result["ops_plus"] == int(expected)
        assert result["ops_plus"] > 100

    def test_ops_plus_below_average(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            obp=0.280,
            slg=0.350,
            at_bats=500,
            hits=100,
            plate_appearances=550,
        )
        lg = {"lg_obp": 0.340, "lg_slg": 0.440, "lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["ops_plus"] < 100

    def test_ops_plus_with_zero_lg_obp_slg(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            obp=0.300,
            slg=0.400,
            at_bats=500,
            hits=100,
            plate_appearances=550,
        )
        lg = {"lg_obp": 0.0, "lg_slg": 0.0, "lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["ops_plus"] == 100
