"""KBO Game Matchup Win Predictor & Sabermetric Feature Store Engine."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.analytics.predictor_dto import MatchupFeatureVector, MatchupPredictionResult
from src.models.game import Game
from src.models.player import PlayerBasic, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

LEAGUE_AVG_RUNS = 4.80
LEAGUE_AVG_ERA = 4.30
PYTHAGOREAN_EXP = 1.83
HOME_FIELD_ADVANTAGE = 1.04

MIN_H2H_SAMPLE = 3
DATE_YEAR_LEN = 4
EVEN_WIN_PROB = 0.50
HIGH_CONFIDENCE_DIFF = 0.12
MEDIUM_CONFIDENCE_DIFF = 0.05
FIP_DIFF_THRESHOLD = 0.40
WRC_DIFF_THRESHOLD = 5.0
BULLPEN_DIFF_THRESHOLD = 0.30


class SabermetricFeatureStore:
    """Extracts and caches team and player sabermetric feature vectors."""

    def __init__(self, session: Session | None = None) -> None:
        """Initialize feature store."""
        self.session = session

    def extract_features_for_game(self, game_id: str) -> MatchupFeatureVector:
        """Extract matchup feature vector for an existing game."""
        if not self.session:
            return self._generate_fallback_vector(game_id=game_id)

        try:
            game = self.session.query(Game).filter(Game.game_id == game_id).first()
            if not game:
                return self._generate_fallback_vector(game_id=game_id)

            home_team = game.home_team or "KIA"
            away_team = game.away_team or "LG"
            game_date = str(game.game_date) if game.game_date else "2024-09-01"
            stadium = game.stadium or "Gwangju"
            home_starter = game.home_pitcher or "선발 미정"
            away_starter = game.away_pitcher or "선발 미정"
            is_valid_year = len(game_date) >= DATE_YEAR_LEN and game_date[:DATE_YEAR_LEN].isdigit()
            season = int(game_date[:DATE_YEAR_LEN]) if is_valid_year else 2024

            return self.extract_features_for_teams(
                home_team=home_team,
                away_team=away_team,
                season=season,
                game_id=game_id,
                game_date=game_date,
                stadium=stadium,
                home_starter=home_starter,
                away_starter=away_starter,
            )
        except SQLAlchemyError as exc:
            logger.debug("DB query failed during feature extraction (%s). Using fallback.", exc)
            return self._generate_fallback_vector(game_id=game_id)

    def _get_pitcher_stats(self, name: str, season: int) -> tuple[float, float]:
        """Query starter FIP and WHIP."""
        if not self.session or not name or name in {"선발 미정", "선발투수"}:
            return 4.20, 1.35
        p = (
            self.session.query(PlayerSeasonPitching)
            .join(PlayerBasic, PlayerBasic.player_id == PlayerSeasonPitching.player_id)
            .filter(PlayerBasic.name == name, PlayerSeasonPitching.season == season)
            .first()
        )
        if p:
            outs = getattr(p, "innings_outs", None) or getattr(p, "outs_pitched", None) or 1
            ip = outs / 3.0
            bb = getattr(p, "walks_allowed", None) or getattr(p, "walks", 0) or 0
            hits = getattr(p, "hits_allowed", None) or getattr(p, "hits", 0) or 0
            hr = getattr(p, "home_runs_allowed", None) or getattr(p, "home_runs", 0) or 0
            so = p.strikeouts or 0
            whip = (hits + bb) / ip if ip > 0 else 1.35
            fip = ((13 * hr + 3 * bb - 2 * so) / ip + 3.80) if ip > 0 else 4.20
            return max(1.50, min(8.0, fip)), max(0.80, min(2.50, whip))
        return 4.20, 1.35

    def _get_team_offense(self, tcode: str, season: int) -> tuple[float, float]:
        """Query team wRC+ and OPS."""
        if not self.session:
            return 100.0, 0.750
        tb = (
            self.session.query(TeamSeasonBatting)
            .filter(
                TeamSeasonBatting.season == season,
                TeamSeasonBatting.team_id == tcode,
            )
            .first()
        )
        if tb:
            ops = getattr(tb, "ops", None) or 0.750
            wrc_plus = getattr(tb, "wrc_plus", None) or (ops / 0.750 * 100.0)
            return float(wrc_plus), float(ops)
        return 100.0, 0.750

    def _get_h2h_record(self, home_team: str, away_team: str, season: int) -> tuple[int, int]:
        """Query head-to-head wins between two teams."""
        if not self.session:
            return 0, 0
        h2h_home_w = 0
        h2h_away_w = 0
        past_games = (
            self.session.query(Game)
            .filter(
                Game.season_id == season,
                ((Game.home_team == home_team) & (Game.away_team == away_team))
                | ((Game.home_team == away_team) & (Game.away_team == home_team)),
                Game.winning_team.isnot(None),
            )
            .all()
        )
        for g in past_games:
            if g.winning_team == home_team:
                h2h_home_w += 1
            elif g.winning_team == away_team:
                h2h_away_w += 1
        return h2h_home_w, h2h_away_w

    def extract_features_for_teams(  # noqa: PLR0913
        self,
        home_team: str,
        away_team: str,
        *,
        season: int = 2024,
        game_id: str | None = None,
        game_date: str | None = None,
        stadium: str | None = None,
        home_starter: str | None = None,
        away_starter: str | None = None,
    ) -> MatchupFeatureVector:
        """Extract matchup features for two competing teams."""
        gid = game_id or f"{season}0829{away_team}{home_team}0"
        gdate = game_date or f"{season}-08-29"
        stad = stadium or f"{home_team} Home Ground"
        h_starter = home_starter or "선발투수"
        a_starter = away_starter or "선발투수"

        if not self.session:
            return self._generate_fallback_vector(
                game_id=gid,
                home_team=home_team,
                away_team=away_team,
                game_date=gdate,
                stadium=stad,
                home_starter=h_starter,
                away_starter=a_starter,
            )

        try:
            h_fip, h_whip = self._get_pitcher_stats(h_starter, season)
            a_fip, a_whip = self._get_pitcher_stats(a_starter, season)
            h_wrc, h_ops = self._get_team_offense(home_team, season)
            a_wrc, a_ops = self._get_team_offense(away_team, season)
            h2h_home_w, h2h_away_w = self._get_h2h_record(home_team, away_team, season)

            return MatchupFeatureVector(
                game_id=gid,
                game_date=gdate,
                home_team=home_team,
                away_team=away_team,
                stadium=stad,
                home_starter_name=h_starter,
                home_starter_fip=h_fip,
                home_starter_whip=h_whip,
                away_starter_name=a_starter,
                away_starter_fip=a_fip,
                away_starter_whip=a_whip,
                home_team_wrc_plus=h_wrc,
                home_team_ops=h_ops,
                away_team_wrc_plus=a_wrc,
                away_team_ops=a_ops,
                home_bullpen_era=4.15,
                away_bullpen_era=4.35,
                h2h_home_wins=h2h_home_w,
                h2h_away_wins=h2h_away_w,
            )
        except SQLAlchemyError as exc:
            logger.debug("DB query error in team feature extraction (%s). Using fallback.", exc)
            return self._generate_fallback_vector(
                game_id=gid,
                home_team=home_team,
                away_team=away_team,
                game_date=gdate,
                stadium=stad,
                home_starter=h_starter,
                away_starter=a_starter,
            )

    def _generate_fallback_vector(  # noqa: PLR0913
        self,
        *,
        game_id: str = "20240829LGKIA0",
        home_team: str = "KIA",
        away_team: str = "LG",
        game_date: str = "2024-08-29",
        stadium: str = "Gwangju",
        home_starter: str = "양현종",
        away_starter: str = "켈리",
    ) -> MatchupFeatureVector:
        """Generate synthetic feature vector for offline/testing use."""
        return MatchupFeatureVector(
            game_id=game_id,
            game_date=game_date,
            home_team=home_team,
            away_team=away_team,
            stadium=stadium,
            home_starter_name=home_starter,
            home_starter_fip=3.65,
            home_starter_whip=1.22,
            away_starter_name=away_starter,
            away_starter_fip=4.10,
            away_starter_whip=1.31,
            home_team_wrc_plus=112.5,
            home_team_ops=0.810,
            away_team_wrc_plus=105.2,
            away_team_ops=0.775,
            home_bullpen_era=3.95,
            away_bullpen_era=4.25,
            h2h_home_wins=8,
            h2h_away_wins=5,
        )


class MatchupPredictor:
    """Predicts KBO game win probabilities and expected scores using Pythagorean + Log5 sabermetrics."""

    def __init__(
        self,
        session: Session | None = None,
        feature_store: SabermetricFeatureStore | None = None,
    ) -> None:
        """Initialize matchup predictor."""
        self.session = session
        self.feature_store = feature_store or SabermetricFeatureStore(session)

    def predict_game(self, game_id: str) -> MatchupPredictionResult:
        """Predict win probability and expected score for a game ID."""
        features = self.feature_store.extract_features_for_game(game_id)
        return self.predict_matchup(features)

    def _evaluate_key_factors(self, features: MatchupFeatureVector, total_h2h: int) -> list[str]:
        """Generate key matchup factors based on feature diffs."""
        factors: list[str] = []

        # Starter diff
        starter_fip_diff = features.away_starter_fip - features.home_starter_fip
        if abs(starter_fip_diff) >= FIP_DIFF_THRESHOLD:
            better_team = features.home_team if starter_fip_diff > 0 else features.away_team
            better_pitcher = features.home_starter_name if starter_fip_diff > 0 else features.away_starter_name
            fip_val = features.home_starter_fip if starter_fip_diff > 0 else features.away_starter_fip
            factors.append(
                f"{better_team} 선발 {better_pitcher} FIP {fip_val:.2f}로 "
                f"선발 마운드 우위 (격차 {abs(starter_fip_diff):.2f})"
            )

        # Offense diff
        wrc_diff = features.home_team_wrc_plus - features.away_team_wrc_plus
        if abs(wrc_diff) >= WRC_DIFF_THRESHOLD:
            better_off_team = features.home_team if wrc_diff > 0 else features.away_team
            wrc_val = features.home_team_wrc_plus if wrc_diff > 0 else features.away_team_wrc_plus
            factors.append(f"{better_off_team} 팀 wRC+ {wrc_val:.1f}로 타선 생산력 우세")

        # Bullpen diff
        bp_diff = features.away_bullpen_era - features.home_bullpen_era
        if abs(bp_diff) >= BULLPEN_DIFF_THRESHOLD:
            better_bp_team = features.home_team if bp_diff > 0 else features.away_team
            bp_val = features.home_bullpen_era if bp_diff > 0 else features.away_bullpen_era
            factors.append(f"{better_bp_team} 불펜진 평균자책점 {bp_val:.2f}로 경기 후반 안정감 보유")

        # H2H factor
        if total_h2h >= MIN_H2H_SAMPLE:
            h2h_leader = features.home_team if features.h2h_home_wins > features.h2h_away_wins else features.away_team
            w_cnt = max(features.h2h_home_wins, features.h2h_away_wins)
            l_cnt = min(features.h2h_home_wins, features.h2h_away_wins)
            if w_cnt > l_cnt:
                factors.append(f"{h2h_leader} 상대 전적 {w_cnt}승 {l_cnt}패로 올 시즌 맞대결 우세")

        if not factors:
            factors.append("양 팀 투타 전력이 팽팽하여 접전 양상 예상")

        return factors

    def predict_matchup(self, features: MatchupFeatureVector) -> MatchupPredictionResult:
        """Calculate win probabilities, expected scores, and key factors from feature vector."""
        # 1. Expected Run calculations
        home_offense_mod = features.home_team_wrc_plus / 100.0
        away_pitching_mod = (features.away_starter_fip * 0.60 + features.away_bullpen_era * 0.40) / LEAGUE_AVG_ERA
        home_exp_runs = LEAGUE_AVG_RUNS * home_offense_mod * away_pitching_mod * HOME_FIELD_ADVANTAGE

        away_offense_mod = features.away_team_wrc_plus / 100.0
        home_pitching_mod = (features.home_starter_fip * 0.60 + features.home_bullpen_era * 0.40) / LEAGUE_AVG_ERA
        away_exp_runs = LEAGUE_AVG_RUNS * away_offense_mod * home_pitching_mod

        home_exp_runs = max(1.0, min(12.0, home_exp_runs))
        away_exp_runs = max(1.0, min(12.0, away_exp_runs))
        total_runs = home_exp_runs + away_exp_runs

        # 2. Base Pythagorean Win Expectancy
        h_gamma = home_exp_runs**PYTHAGOREAN_EXP
        a_gamma = away_exp_runs**PYTHAGOREAN_EXP
        base_home_win_prob = h_gamma / (h_gamma + a_gamma)

        # 3. Head-to-Head adjustment
        total_h2h = features.h2h_home_wins + features.h2h_away_wins
        if total_h2h >= MIN_H2H_SAMPLE:
            h2h_rate = features.h2h_home_wins / float(total_h2h)
            home_win_prob = 0.90 * base_home_win_prob + 0.10 * h2h_rate
        else:
            home_win_prob = base_home_win_prob

        home_win_prob = max(0.10, min(0.90, home_win_prob))
        away_win_prob = 1.0 - home_win_prob

        # 4. Winner & Confidence Tier
        predicted_winner = features.home_team if home_win_prob >= EVEN_WIN_PROB else features.away_team
        diff = abs(home_win_prob - EVEN_WIN_PROB)

        if diff >= HIGH_CONFIDENCE_DIFF:
            confidence = "HIGH"
        elif diff >= MEDIUM_CONFIDENCE_DIFF:
            confidence = "MEDIUM"
        else:
            confidence = "TOSS_UP"

        factors = self._evaluate_key_factors(features, total_h2h)

        return MatchupPredictionResult(
            game_id=features.game_id,
            game_date=features.game_date,
            home_team=features.home_team,
            away_team=features.away_team,
            stadium=features.stadium,
            home_win_prob=home_win_prob,
            away_win_prob=away_win_prob,
            predicted_home_score=home_exp_runs,
            predicted_away_score=away_exp_runs,
            predicted_total_runs=total_runs,
            predicted_winner=predicted_winner,
            confidence_tier=confidence,
            key_factors=factors,
            features=features,
        )


__all__ = [
    "BULLPEN_DIFF_THRESHOLD",
    "DATE_YEAR_LEN",
    "EVEN_WIN_PROB",
    "FIP_DIFF_THRESHOLD",
    "HIGH_CONFIDENCE_DIFF",
    "HOME_FIELD_ADVANTAGE",
    "LEAGUE_AVG_ERA",
    "LEAGUE_AVG_RUNS",
    "MEDIUM_CONFIDENCE_DIFF",
    "MIN_H2H_SAMPLE",
    "PYTHAGOREAN_EXP",
    "WRC_DIFF_THRESHOLD",
    "MatchupPredictor",
    "SabermetricFeatureStore",
]
