"""Tests for MatchupEngine — BvP, splits, rate stats."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat
from src.models.matchup import (
    BatterSplit,
    BatterStadiumSplit,
    BatterTeamSplit,
    BatterVsStarter,
    MatchupBvP,
    PitcherSplit,
    PitcherTeamSplit,
)
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.services.matchup_engine import MatchupEngine


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameBattingStat.__table__,
        GameEvent.__table__,
        GamePitchingStat.__table__,
        MatchupBvP.__table__,
        BatterSplit.__table__,
        PitcherSplit.__table__,
        BatterTeamSplit.__table__,
        PitcherTeamSplit.__table__,
        BatterStadiumSplit.__table__,
        BatterVsStarter.__table__,
        PlayerBasic.__table__,
        KboSeason.__table__,

    ):
        table.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_game(session, game_id="20250101", stadium="잠실", home="LG", away="SS",
              home_pitcher="Kim", away_pitcher="Park"):
    session.add(Game(
        game_id=game_id, stadium=stadium,
        home_team=home, away_team=away,
        home_pitcher=home_pitcher, away_pitcher=away_pitcher,
        game_status="END", season_id=1,
        game_date=date(2025, 1, 1),
    ))
    session.commit()


def _ensure_player(session, player_id, name="홍길동", bats="L", throws="R"):
    if not session.get(PlayerBasic, player_id):
        session.add(PlayerBasic(player_id=player_id, name=name, bats=bats, throws=throws))
        session.commit()


def _add_event(session, game_id="20250101", batter_id=10001, pitcher_id=20001,
               batter_name="홍길동", pitcher_name="김투수",
               description="안타", rbi=1, bases_before="000", inning=1,
               event_seq=1):
    if batter_id is not None:
        _ensure_player(session, batter_id, name=batter_name)
    if pitcher_id is not None:
        _ensure_player(session, pitcher_id, name=pitcher_name)
    session.add(GameEvent(
        game_id=game_id, batter_id=batter_id, pitcher_id=pitcher_id,
        batter_name=batter_name, pitcher_name=pitcher_name,
        description=description, rbi=rbi, bases_before=bases_before,
        inning=inning, event_seq=event_seq,
    ))
    session.commit()


def _add_batting(session, game_id="20250101", player_id=10001, team_code="LG",
                 pa=5, ab=4, hits=2, doubles=1, triples=0, home_runs=0,
                 walks=1, hbp=0, strikeouts=1, stolen_bases=0,
                 caught_stealing=0, gdp=0, runs=1, rbi=1, player_name="홍길동",
                 team_side="home", appearance_seq=1):
    session.add(GameBattingStat(
        game_id=game_id, player_id=player_id, player_name=player_name,
        team_code=team_code, team_side=team_side, appearance_seq=appearance_seq,
        plate_appearances=pa, at_bats=ab, hits=hits,
        doubles=doubles, triples=triples, home_runs=home_runs,
        runs=runs, rbi=rbi, walks=walks, hbp=hbp,
        strikeouts=strikeouts, stolen_bases=stolen_bases,
        caught_stealing=caught_stealing, gdp=gdp,
    ))
    session.commit()


def _add_pitching(session, game_id="20250101", player_id=20001, team_code="SS",
                  ip_outs=15, hits_allowed=3, runs_allowed=1, earned_runs=1,
                  walks_allowed=1, strikeouts=5, batters_faced=18, pitches=60,
                  player_name="김투수", team_side="away", appearance_seq=1):
    session.add(GamePitchingStat(
        game_id=game_id, player_id=player_id, player_name=player_name,
        team_code=team_code, team_side=team_side, appearance_seq=appearance_seq,
        innings_outs=ip_outs, hits_allowed=hits_allowed,
        runs_allowed=runs_allowed, earned_runs=earned_runs,
        walks_allowed=walks_allowed, strikeouts=strikeouts,
        batters_faced=batters_faced, pitches=pitches,
    ))
    session.commit()


# ── _calc_rate_stats (pure function) ─────────────────────────────────────────


class TestCalcRateStats:
    def test_normal_triple_slash(self):
        engine = MatchupEngine()
        avg, obp, slg, ops = engine._calc_rate_stats(2, 5, 6, 1, 0, 1, 0, 0)
        assert avg == pytest.approx(0.400, abs=0.001)
        assert obp == pytest.approx(0.500, abs=0.001)  # (2+1+0)/6
        assert slg == pytest.approx(0.600, abs=0.001)  # TB=3, 3/5
        assert ops == pytest.approx(1.100, abs=0.001)

    def test_zero_ab_returns_zero_avg(self):
        engine = MatchupEngine()
        avg, obp, slg, ops = engine._calc_rate_stats(0, 0, 3, 3, 0, 0, 0, 0)
        assert avg == 0.0
        assert obp == 1.0  # 3/3
        assert slg == 0.0
        assert ops == 1.0

    def test_zero_pa_returns_zero_obp(self):
        engine = MatchupEngine()
        avg, obp, slg, ops = engine._calc_rate_stats(0, 0, 0, 0, 0, 0, 0, 0)
        assert avg == 0.0
        assert obp == 0.0
        assert slg == 0.0
        assert ops == 0.0

    def test_is_full_false_skips_slg(self):
        engine = MatchupEngine()
        avg, obp, slg, ops = engine._calc_rate_stats(2, 5, 6, 1, 0, 1, 0, 1, is_full=False)
        assert avg == pytest.approx(0.400, abs=0.001)
        assert obp == pytest.approx(0.500, abs=0.001)
        assert slg == 0.0
        assert ops == 0.500

    def test_handles_hr_in_tb(self):
        engine = MatchupEngine()
        # 1B=2, 2B=1, HR=1 => TB = 2*1 + 1*2 + 1*4 = 8
        avg, obp, slg, ops = engine._calc_rate_stats(4, 10, 11, 1, 0, 1, 0, 1)
        assert slg == pytest.approx(0.800, abs=0.001)  # 8/10

    def test_all_none_inputs(self):
        engine = MatchupEngine()
        avg, obp, slg, ops = engine._calc_rate_stats(None, None, None, None, None, None, None, None)
        assert avg == 0.0
        assert obp == 0.0
        assert slg == 0.0
        assert ops == 0.0


# ── _calc_precise_bvp ───────────────────────────────────────────────────────


class TestCalcPreciseBvP:
    def test_single_hit_event(self, session):
        _add_game(session)
        _add_event(session, description="안타")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp is not None
        assert bvp.batter_id == 10001
        assert bvp.pitcher_id == 20001
        assert bvp.plate_appearances == 1
        assert bvp.at_bats == 1
        assert bvp.hits == 1

    def test_korean_hit_types(self, session):
        _add_game(session)
        _add_event(session, description="2루타", game_id="20250101")
        _add_event(session, description="3루타", game_id="20250102")
        _add_game(session, game_id="20250102")
        _add_event(session, description="홈런", game_id="20250103")
        _add_game(session, game_id="20250103")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 3
        assert bvp.at_bats == 3
        assert bvp.hits == 3
        assert bvp.doubles == 1
        assert bvp.triples == 1
        assert bvp.home_runs == 1

    def test_walk_and_hbp(self, session):
        _add_game(session)
        _add_event(session, description="볼넷")
        _add_event(session, description="사구", game_id="20250102")
        _add_game(session, game_id="20250102")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 2
        assert bvp.at_bats == 0
        assert bvp.walks == 1
        assert bvp.hbp == 1

    def test_sacrifice_fly(self, session):
        _add_game(session)
        _add_event(session, description="희생플라이")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 1
        assert bvp.at_bats == 0
        assert bvp.sacrifice_flies == 1

    def test_sacrifice_bunt_excluded_from_ab(self, session):
        _add_game(session)
        _add_event(session, description="희생번트")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 1
        assert bvp.at_bats == 0

    def test_strikeout(self, session):
        _add_game(session)
        _add_event(session, description="삼진 루킹")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.at_bats == 1
        assert bvp.strikeouts == 1

    def test_out_counts_as_ab(self, session):
        _add_game(session)
        _add_event(session, description="2루수 땅볼")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.at_bats == 1
        assert bvp.hits == 0

    def test_rbi_accumulation(self, session):
        _add_game(session)
        _add_event(session, description="안타", rbi=2)
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.rbi == 2

    def test_upsert_accumulative(self, session):
        _add_game(session)
        _add_event(session, description="안타")  # year 2025
        _add_event(session, description="안타", game_id="20240101")
        _add_game(session, game_id="20240101")
        # ugly: game_id starts with 2024, won't match 2025 filter
        # Need to fix: add another game with 2025 prefix
        _add_event(session, description="안타", game_id="20250102")
        _add_game(session, game_id="20250102")
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 2  # 2025 only

        # Accumulative: commit then run again, stats should double
        session.commit()
        engine._calc_precise_bvp(session, 2025)
        session.commit()
        bvp = session.query(MatchupBvP).first()
        assert bvp.plate_appearances == 4  # doubled

    def test_filters_null_batter_or_pitcher(self, session):
        _add_game(session)
        _add_event(session, batter_id=None)
        engine = MatchupEngine()
        engine._calc_precise_bvp(session, 2025)
        assert session.query(MatchupBvP).count() == 0


# ── _calc_situational_splits ────────────────────────────────────────────────


class TestCalcSituationalSplits:
    def _setup_basic(self, session):
        _add_game(session)
    # players created via _add_event -> _ensure_player

    def test_batter_risp_split(self, session):
        self._setup_basic(session)
        _add_event(session, description="안타", bases_before="020")
        engine = MatchupEngine()
        engine._calc_situational_splits(session, 2025)
        splits = session.query(BatterSplit).all()
        assert len(splits) >= 1
        risp = next((s for s in splits if s.split_type == "RISP"), None)
        assert risp is not None
        assert risp.at_bats == 1
        assert risp.hits == 1

    def test_batter_vs_handedness_split(self, session):
        self._setup_basic(session)
        _add_event(session, description="안타")
        engine = MatchupEngine()
        engine._calc_situational_splits(session, 2025)
        splits = session.query(BatterSplit).all()
        vs_r = next((s for s in splits if s.split_type == "vsR"), None)
        assert vs_r is not None

    def test_pitcher_risp_split(self, session):
        self._setup_basic(session)
        _add_event(session, description="삼진", bases_before="003")
        engine = MatchupEngine()
        engine._calc_situational_splits(session, 2025)
        splits = session.query(PitcherSplit).all()
        risp = next((s for s in splits if s.split_type == "RISP"), None)
        assert risp is not None
        assert risp.batters_faced == 1
        assert risp.strikeouts == 1

    def test_pitcher_vs_batter_handedness_split(self, session):
        self._setup_basic(session)
        _add_event(session, description="안타")
        engine = MatchupEngine()
        engine._calc_situational_splits(session, 2025)
        splits = session.query(PitcherSplit).all()
        vs_l = next((s for s in splits if s.split_type == "vsL"), None)
        assert vs_l is not None

    def test_delete_then_reinsert_is_idempotent(self, session):
        self._setup_basic(session)
        _add_event(session, description="안타")
        engine = MatchupEngine()
        engine._calc_situational_splits(session, 2025)
        count1 = session.query(BatterSplit).count()
        engine._calc_situational_splits(session, 2025)
        count2 = session.query(BatterSplit).count()
        assert count1 == count2  # delete+insert = same count


# ── _calc_batter_team_splits ────────────────────────────────────────────────


class TestCalcBatterTeamSplits:
    def test_opponent_determined_correctly(self, session):
        _add_game(session, home="LG", away="SS")
        _add_batting(session, team_code="LG")  # LG plays SS -> opponent = SS
        engine = MatchupEngine()
        engine._calc_batter_team_splits(session, 2025)
        split = session.query(BatterTeamSplit).first()
        assert split is not None
        assert split.opponent_team_code == "SS"
        assert split.team_code == "LG"

    def test_delete_then_insert(self, session):
        _add_game(session, home="LG", away="SS")
        _add_batting(session, team_code="LG")
        engine = MatchupEngine()
        engine._calc_batter_team_splits(session, 2025)
        count1 = session.query(BatterTeamSplit).count()
        engine._calc_batter_team_splits(session, 2025)
        count2 = session.query(BatterTeamSplit).count()
        assert count1 == count2


# ── _calc_pitcher_team_splits ────────────────────────────────────────────────


class TestCalcPitcherTeamSplits:
    def test_opponent_and_era_whip(self, session):
        _add_game(session, home="LG", away="SS")
        _add_pitching(session, team_code="SS")  # SS plays LG -> opponent = LG
        engine = MatchupEngine()
        engine._calc_pitcher_team_splits(session, 2025)
        split = session.query(PitcherTeamSplit).first()
        assert split is not None
        assert split.opponent_team_code == "LG"
        # IP = 15/3 = 5.0, ERA = (1 * 9) / 5 = 1.80
        assert split.era == pytest.approx(1.80, abs=0.01)
        # WHIP = (3 + 1) / 5 = 0.80
        assert split.whip == pytest.approx(0.80, abs=0.01)


# ── _calc_batter_stadium_splits ──────────────────────────────────────────────


class TestCalcBatterStadiumSplits:
    def test_stadium_name_in_split(self, session):
        _add_game(session, stadium="잠실")
        _add_batting(session)
        engine = MatchupEngine()
        engine._calc_batter_stadium_splits(session, 2025)
        split = session.query(BatterStadiumSplit).first()
        assert split is not None
        assert split.stadium_name == "잠실"
        assert split.hits == 2


# ── _calc_batter_vs_starter ─────────────────────────────────────────────────


class TestCalcBatterVsStarter:
    def test_opposing_pitcher_name(self, session):
        _add_game(session, home="LG", away="SS", home_pitcher="Kim", away_pitcher="Park")
        _add_batting(session, team_code="LG")  # LG is home -> opposing = away_pitcher = Park
        engine = MatchupEngine()
        engine._calc_batter_vs_starter(session, 2025)
        split = session.query(BatterVsStarter).first()
        assert split is not None
        assert split.pitcher_name == "Park"
