"""Tests for SabermetricsCalculator — wOBA, wRC+, WAR, FIP, LOB%."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.sabermetrics_calculator import (
    LEAGUE_BATTING_PA_STUB_LIMIT,
    MIN_LEAGUE_PLAYER_ID,
    SabermetricsCalculator,
)
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

# ── Test data ─────────────────────────────────────────────────────────────────

LG_BATTING = {
    "lg_woba": 0.340,
    "woba_scale": 1.2,
    "lg_r_per_pa": 0.12,
    "fip_constant": 3.2,
    "rpw": 10.0,
    "lg_era": 4.50,
    "lg_obp": 0.340,
    "lg_slg": 0.420,
}


def _make_batting(**overrides) -> PlayerSeasonBatting:
    """Factory for PlayerSeasonBatting with sensible defaults."""
    defaults = {
        "plate_appearances": 600,
        "at_bats": 500,
        "hits": 150,
        "doubles": 30,
        "triples": 5,
        "home_runs": 25,
        "walks": 80,
        "intentional_walks": 5,
        "hbp": 10,
        "sacrifice_flies": 5,
        "runs": 90,
        "obp": 0.380,
        "slg": 0.500,
    }
    defaults.update(overrides)
    return MagicMock(spec=PlayerSeasonBatting, **defaults)


def _make_pitching(**overrides) -> PlayerSeasonPitching:
    """Factory for PlayerSeasonPitching with sensible defaults."""
    defaults = {
        "innings_outs": 300,
        "earned_runs": 60,
        "home_runs_allowed": 15,
        "walks_allowed": 50,
        "hit_batters": 10,
        "strikeouts": 180,
        "runs_allowed": 70,
        "hits_allowed": 200,
    }
    defaults.update(overrides)
    return MagicMock(spec=PlayerSeasonPitching, **defaults)


# ── calculate_batting_metrics ────────────────────────────────────────────────


class TestCalculateBattingMetrics:
    def test_woba_zero_when_denominator_zero(self):
        stat = _make_batting(at_bats=0, walks=0, hbp=0, sacrifice_flies=0)
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["woba"] == 0.0

    def test_woba_formula(self):
        # 1B = 150 - 30 - 5 - 25 = 90
        # uBB = 80 - 5 = 75
        # numerator = 0.69*75 + 0.72*10 + 0.89*90 + 1.27*30 + 1.62*5 + 2.10*25
        #   = 51.75 + 7.2 + 80.1 + 38.1 + 8.1 + 52.5 = 237.75
        # denominator = 500 + 75 + 10 + 5 = 590
        # wOBA = 237.75 / 590 = 0.402966...
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["woba"] == pytest.approx(0.403, abs=0.001)

    def test_wraa_formula(self):
        # wRAA = ((0.403 - 0.340) / 1.2) * 600 = 31.5
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["wraa"] == pytest.approx(31.5, abs=0.1)

    def test_wrc_plus_formula(self):
        # wOBA = 0.403, wRC+ = (((0.403 - 0.340)/1.2 + 0.12) / 0.12) * 100 = 144
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["wrc_plus"] == 144

    def test_ops_plus_formula(self):
        # OPS+ = ((0.380/0.340 + 0.500/0.420) - 1) * 100 = (1.1176 + 1.1905 - 1) * 100 = 130.8 -> 131
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["ops_plus"] == 131

    def test_war_formula(self):
        # WAR = 31.5 / 10.0 = 3.15
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["war"] == pytest.approx(3.15, abs=0.01)

    def test_handles_missing_intentional_walks(self):
        stat = _make_batting(intentional_walks=None)
        result = SabermetricsCalculator.calculate_batting_metrics(stat, LG_BATTING)
        assert result["woba"] is not None

    def test_handles_zero_lg_r_per_pa(self):
        lg = {**LG_BATTING, "lg_r_per_pa": 0}
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["wrc_plus"] == 100

    def test_handles_zero_lg_obp_or_slg(self):
        lg = {**LG_BATTING, "lg_obp": 0, "lg_slg": 0}
        stat = _make_batting()
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["ops_plus"] == 100


# ── calculate_pitching_metrics ───────────────────────────────────────────────


class TestCalculatePitchingMetrics:
    def test_fip_formula(self):
        # IP = 300/3 = 100
        # FIP = ((13*15 + 3*(50+10) - 2*180) / 100) + 3.2
        #     = (195 + 180 - 360) / 100 + 3.2 = 0.15 + 3.2 = 3.35
        stat = _make_pitching()
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, LG_BATTING)
        assert result["fip_adj"] == pytest.approx(3.35, abs=0.01)

    def test_fip_zero_when_no_innings(self):
        stat = _make_pitching(innings_outs=0)
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, LG_BATTING)
        assert result["fip_adj"] == 0.0

    def test_lob_pct_formula(self):
        # LOB% = (200 + 50 + 10 - 70) / (200 + 50 + 10 - 1.4*15)
        #     = 190 / (260 - 21) = 190 / 239 = 0.795
        stat = _make_pitching()
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, LG_BATTING)
        assert result["lob_pct"] == pytest.approx(0.795, abs=0.001)

    def test_lob_pct_none_when_denominator_zero(self):
        stat = _make_pitching(hits_allowed=0, walks_allowed=0, hit_batters=0, runs_allowed=0, home_runs_allowed=0)
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, LG_BATTING)
        assert result["lob_pct"] is None

    def test_pitching_war(self):
        # WAR = (4.50 - 3.35) * (100 / 9) / 10.0 = 1.15 * 11.111 / 10 = 1.28
        stat = _make_pitching()
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, LG_BATTING)
        assert result["war"] == pytest.approx(1.28, abs=0.01)

    def test_war_zero_when_rpw_zero(self):
        lg = {**LG_BATTING, "rpw": 0}
        stat = _make_pitching()
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert result["war"] == 0.0


# ── get_league_constants ──────────────────────────────────────────────────────


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    PlayerSeasonBatting.__table__.create(bind=engine)
    PlayerSeasonPitching.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_batting(
    session,
    player_id=10001,
    season=2025,
    level="KBO1",
    pa=600,
    ab=500,
    hits=150,
    doubles=30,
    triples=5,
    hr=25,
    bb=80,
    ibb=5,
    hbp=10,
    sf=5,
    runs=90,
):
    session.add(
        PlayerSeasonBatting(
            player_id=player_id,
            season=season,
            level=level,
            plate_appearances=pa,
            at_bats=ab,
            hits=hits,
            doubles=doubles,
            triples=triples,
            home_runs=hr,
            walks=bb,
            intentional_walks=ibb,
            hbp=hbp,
            sacrifice_flies=sf,
            runs=runs,
        ),
    )
    session.commit()


def _add_pitching(
    session,
    player_id=10001,
    season=2025,
    level="KBO1",
    outs=300,
    er=60,
    hr_allowed=15,
    bb_allowed=50,
    hbp_allowed=10,
    so=180,
    r_allowed=70,
    hits_allowed=200,
):
    session.add(
        PlayerSeasonPitching(
            player_id=player_id,
            season=season,
            level=level,
            innings_outs=outs,
            earned_runs=er,
            home_runs_allowed=hr_allowed,
            walks_allowed=bb_allowed,
            hit_batters=hbp_allowed,
            strikeouts=so,
            runs_allowed=r_allowed,
            hits_allowed=hits_allowed,
        ),
    )
    session.commit()


class TestGetLeagueConstants:
    def test_returns_dict_with_all_keys(self, session):
        _add_batting(session)
        _add_pitching(session)
        result = SabermetricsCalculator.get_league_constants(session, 2025)
        expected_keys = {"lg_woba", "woba_scale", "lg_r_per_pa", "fip_constant", "rpw", "lg_era", "lg_obp", "lg_slg"}
        assert set(result.keys()) == expected_keys

    def test_filters_low_player_id(self, session):
        _add_batting(session, player_id=MIN_LEAGUE_PLAYER_ID - 1)
        result = SabermetricsCalculator.get_league_constants(session, 2025)
        # IDs below the league-player boundary are filtered out.
        assert result["lg_woba"] == 0.320
        assert result["lg_era"] == 4.50

    def test_filters_stub_rows(self, session):
        _add_batting(session, player_id=MIN_LEAGUE_PLAYER_ID, pa=LEAGUE_BATTING_PA_STUB_LIMIT, hr=0, bb=0)
        result = SabermetricsCalculator.get_league_constants(session, 2025)
        # A row exactly at the PA boundary remains a stub when it has no HR or BB.
        assert result["lg_woba"] == 0.320

    def test_computes_value_with_valid_data(self, session):
        _add_batting(session)
        _add_pitching(session)
        result = SabermetricsCalculator.get_league_constants(session, 2025)
        assert result["lg_woba"] > 0
        assert result["lg_era"] > 0
        assert result["rpw"] > 0
        assert result["fip_constant"] is not None

    def test_empty_db_returns_defaults(self, session):
        result = SabermetricsCalculator.get_league_constants(session, 2025)
        assert result["lg_woba"] == 0.320
        assert result["lg_era"] == 4.50

    def test_get_league_constants_respects_level(self, session):
        # Add KBO2 stats only
        _add_batting(session, level="KBO2")
        _add_pitching(session, level="KBO2")

        # Calculate for KBO1 (should return default since there is no KBO1 data)
        result_kbo1 = SabermetricsCalculator.get_league_constants(session, 2025, level="KBO1")
        assert result_kbo1["lg_woba"] == 0.320
        assert result_kbo1["lg_era"] == 4.50

        # Calculate for KBO2 (should return calculated values)
        result_kbo2 = SabermetricsCalculator.get_league_constants(session, 2025, level="KBO2")
        assert result_kbo2["lg_woba"] > 0.320
        assert result_kbo2["lg_era"] > 4.50
