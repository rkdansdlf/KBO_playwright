"""KBO Player Similarity Search & Head-to-Head Comparison Engine."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.analytics.similarity_dto import (
    HeadToHeadComparisonResult,
    PlayerRole,
    PlayerSimilarityResult,
    PlayerVector,
    SimilarPlayerMatch,
)
from src.reporting.scouting_engine import ScoutingReportEngine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

SIMILARITY_MATCH_LIMIT = 5
ADVANTAGE_THRESHOLD = 5.0
ELITE_SCORE_THRESHOLD = 80.0
HIGH_SCORE_THRESHOLD = 75.0
ABOVE_AVG_SCORE_THRESHOLD = 70.0
TOTAL_SCORE_TIE_DIFF = 10.0


def _classify_batter_style(d: dict[str, float]) -> str:
    """Derive a qualitative batter archetype style tag."""
    power = d.get("장타력 (Power)", d.get("Power", 50.0))
    speed = d.get("기동력 (Speed)", d.get("Speed", 50.0))
    contact = d.get("컨택 능력 (Contact)", d.get("Contact", 50.0))
    discipline = d.get("선구안 (Discipline)", d.get("Discipline", 50.0))

    if power >= HIGH_SCORE_THRESHOLD and speed >= HIGH_SCORE_THRESHOLD:
        return "호타준족 5툴 플레이어"
    if power >= ELITE_SCORE_THRESHOLD:
        return "슬러거 거포형 타자"
    if contact >= ELITE_SCORE_THRESHOLD and speed >= HIGH_SCORE_THRESHOLD:
        return "교타 스피드스타 외야수"
    if contact >= ELITE_SCORE_THRESHOLD and discipline >= HIGH_SCORE_THRESHOLD:
        return "출루 컨택형 테이블세터"
    if power >= ABOVE_AVG_SCORE_THRESHOLD:
        return "중장거리형 클러치 히터"
    return "올라운드 밸런스형 타자"


def _classify_pitcher_style(d: dict[str, float]) -> str:
    """Derive a qualitative pitcher archetype style tag."""
    stuff = d.get("구위 (Stuff)", d.get("Stuff", 50.0))
    command = d.get("제구력 (Command)", d.get("Command", 50.0))
    workhorse = d.get("이닝 소화력 (Workhorse)", d.get("Workhorse", 50.0))

    if stuff >= ELITE_SCORE_THRESHOLD and command >= HIGH_SCORE_THRESHOLD:
        return "에이스급 완성형 파이어볼러"
    if stuff >= ELITE_SCORE_THRESHOLD:
        return "파워스터프 탈삼진형 투수"
    if command >= ELITE_SCORE_THRESHOLD:
        return "정밀제구형 피네스볼러"
    if workhorse >= ELITE_SCORE_THRESHOLD:
        return "이닝이터 워크호스 선발"
    return "경기운영형 안정적 투수"


class PlayerSimilarityEngine:
    """Computes cosine similarity across 5-axis player performance vectors and 1:1 comparisons."""

    def __init__(
        self,
        session: Session | None = None,
        scouting_engine: ScoutingReportEngine | None = None,
    ) -> None:
        """Initialize PlayerSimilarityEngine."""
        self.session = session
        self.scouting_engine = scouting_engine or ScoutingReportEngine(session)

    @staticmethod
    def compute_cosine_similarity(vec1: PlayerVector, vec2: PlayerVector) -> float:
        """Calculate cosine similarity between two 5-dimensional player vectors."""
        dims1 = vec1.dimensions
        dims2 = vec2.dimensions

        common_keys = [k for k in dims1 if k in dims2]
        if not common_keys:
            return 0.0

        dot_product = sum(dims1[k] * dims2[k] for k in common_keys)
        mag1 = math.sqrt(sum(dims1[k] ** 2 for k in common_keys))
        mag2 = math.sqrt(sum(dims2[k] ** 2 for k in common_keys))

        if mag1 == 0.0 or mag2 == 0.0:
            return 0.0

        sim = dot_product / (mag1 * mag2)
        return max(0.0, min(1.0, sim))

    @staticmethod
    def classify_player_style(vec: PlayerVector) -> str:
        """Derive a qualitative player archetype style tag based on vector scores."""
        if vec.role == PlayerRole.BATTER:
            return _classify_batter_style(vec.dimensions)
        return _classify_pitcher_style(vec.dimensions)

    def extract_player_vector(
        self,
        name_or_id: str | int,
        season: int | None = None,
    ) -> PlayerVector:
        """Extract a 5-dimensional normalized vector for a player."""
        if self.session:
            try:
                report = self.scouting_engine.generate_scouting_report(name_or_id, year=season or 2024)
                dims = {d.name: d.score for d in report.dimensions}
                key_stats = {**report.classic_stats, **report.advanced_stats}
                return PlayerVector(
                    player_id=report.player_id,
                    player_name=report.player_name,
                    team_code=report.team_code,
                    season=report.season,
                    role=report.role,
                    dimensions=dims,
                    key_stats=key_stats,
                )
            except (SQLAlchemyError, ValueError, AttributeError) as exc:
                logger.debug("Failed to extract player vector from DB (%s). Using fallback vector.", exc)

        return self._get_fallback_player_vector(name_or_id, season)

    def find_similar_players(
        self,
        name_or_id: str | int,
        season: int | None = None,
        role: PlayerRole | None = None,
        top_k: int = SIMILARITY_MATCH_LIMIT,
    ) -> PlayerSimilarityResult:
        """Find the top-K most similar historical and contemporary KBO players."""
        target_vec = self.extract_player_vector(name_or_id, season)
        candidate_pool = self._get_historical_player_pool(target_vec.role if role is None else role)

        matches: list[SimilarPlayerMatch] = []
        for candidate in candidate_pool:
            if candidate.player_name == target_vec.player_name and candidate.season == target_vec.season:
                continue

            sim_score = self.compute_cosine_similarity(target_vec, candidate)
            style_tag = self.classify_player_style(candidate)

            common_strengths: list[str] = []
            for dim, score in candidate.dimensions.items():
                target_score = target_vec.dimensions.get(dim, 0.0)
                if score >= ABOVE_AVG_SCORE_THRESHOLD and target_score >= ABOVE_AVG_SCORE_THRESHOLD:
                    common_strengths.append(dim.split(" ")[0])

            matches.append(
                SimilarPlayerMatch(
                    player=candidate,
                    similarity_score=sim_score,
                    style_tag=style_tag,
                    common_strengths=common_strengths[:3],
                )
            )

        matches.sort(key=lambda m: m.similarity_score, reverse=True)
        return PlayerSimilarityResult(
            target_player=target_vec,
            matches=matches[:top_k],
        )

    def compare_players(
        self,
        player1: str | int,
        player2: str | int,
        season1: int | None = None,
        season2: int | None = None,
    ) -> HeadToHeadComparisonResult:
        """Perform a 1:1 head-to-head 5-axis sabermetric comparison."""
        vec1 = self.extract_player_vector(player1, season1)
        vec2 = self.extract_player_vector(player2, season2)

        sim_score = self.compute_cosine_similarity(vec1, vec2)

        diffs: dict[str, float] = {}
        adv1: list[str] = []
        adv2: list[str] = []

        all_keys = list(vec1.dimensions.keys())
        for k in all_keys:
            s1 = vec1.dimensions.get(k, 50.0)
            s2 = vec2.dimensions.get(k, 50.0)
            diff = s1 - s2
            diffs[k] = diff
            dim_short = k.split(" ")[0]
            if diff >= ADVANTAGE_THRESHOLD:
                adv1.append(f"{dim_short} (+{diff:.1f})")
            elif diff <= -ADVANTAGE_THRESHOLD:
                adv2.append(f"{dim_short} (+{abs(diff):.1f})")

        p1_score = sum(vec1.dimensions.values())
        p2_score = sum(vec2.dimensions.values())

        if abs(p1_score - p2_score) < TOTAL_SCORE_TIE_DIFF:
            verdict = "백중세 접전 (호각지세)"
        elif p1_score > p2_score:
            verdict = f"{vec1.player_name} 종합 우세"
        else:
            verdict = f"{vec2.player_name} 종합 우세"

        return HeadToHeadComparisonResult(
            player1=vec1,
            player2=vec2,
            similarity_score=sim_score,
            dimension_diffs=diffs,
            advantage_player1=adv1,
            advantage_player2=adv2,
            verdict_summary=verdict,
        )

    def _get_fallback_player_vector(
        self,
        name_or_id: str | int,
        season: int | None = None,
    ) -> PlayerVector:
        """Generate synthetic fallback player vector for offline/testing use."""
        name = str(name_or_id)
        target_season = season or 2024

        # Predefined known archetypes
        if name in {"김도영", "54640"}:
            return PlayerVector(
                player_id=54640,
                player_name="김도영",
                team_code="KIA",
                season=target_season,
                role=PlayerRole.BATTER,
                dimensions={
                    "컨택 능력 (Contact)": 92.5,
                    "장타력 (Power)": 94.0,
                    "선구안 (Discipline)": 82.0,
                    "기동력 (Speed)": 96.5,
                    "세이버 종합가치 (Value)": 95.0,
                },
                key_stats={"AVG": 0.347, "HR": 38, "SB": 40, "OPS": 1.060, "wRC+": 172.5},
            )
        if name in {"이종범", "74601"}:
            return PlayerVector(
                player_id=74601,
                player_name="이종범",
                team_code="해태",
                season=1994,
                role=PlayerRole.BATTER,
                dimensions={
                    "컨택 능력 (Contact)": 98.0,
                    "장타력 (Power)": 88.5,
                    "선구안 (Discipline)": 85.0,
                    "기동력 (Speed)": 99.5,
                    "세이버 종합가치 (Value)": 99.0,
                },
                key_stats={"AVG": 0.393, "HR": 19, "SB": 84, "OPS": 1.033, "wRC+": 198.2},
            )
        if name in {"이정후", "67001"}:
            return PlayerVector(
                player_id=67001,
                player_name="이정후",
                team_code="키움",
                season=2022,
                role=PlayerRole.BATTER,
                dimensions={
                    "컨택 능력 (Contact)": 99.0,
                    "장타력 (Power)": 85.0,
                    "선구안 (Discipline)": 94.0,
                    "기동력 (Speed)": 78.0,
                    "세이버 종합가치 (Value)": 97.0,
                },
                key_stats={"AVG": 0.349, "HR": 23, "SB": 5, "OPS": 0.996, "wRC+": 182.5},
            )
        if name in {"류현진", "76101"}:
            return PlayerVector(
                player_id=76101,
                player_name="류현진",
                team_code="한화",
                season=2006,
                role=PlayerRole.PITCHER,
                dimensions={
                    "구위 (Stuff)": 95.0,
                    "제구력 (Command)": 92.0,
                    "실점 억제력 (Damage Control)": 96.0,
                    "효율성 (Efficiency)": 93.0,
                    "이닝 소화력 (Workhorse)": 98.0,
                },
                key_stats={"ERA": 2.23, "FIP": 2.55, "WHIP": 1.05, "SO": 204, "IP": 201.2},
            )

        # Generic batter default
        return PlayerVector(
            player_id=99999,
            player_name=name,
            team_code="KBO",
            season=target_season,
            role=PlayerRole.BATTER,
            dimensions={
                "컨택 능력 (Contact)": 70.0,
                "장타력 (Power)": 65.0,
                "선구안 (Discipline)": 60.0,
                "기동력 (Speed)": 55.0,
                "세이버 종합가치 (Value)": 62.0,
            },
            key_stats={"AVG": 0.280, "HR": 15, "SB": 8, "OPS": 0.780, "wRC+": 105.0},
        )

    def _get_historical_player_pool(self, role: PlayerRole) -> list[PlayerVector]:
        """Return historical player pool for similarity matching."""
        if role == PlayerRole.BATTER:
            return [
                self._get_fallback_player_vector("김도영", 2024),
                self._get_fallback_player_vector("이종범", 1994),
                self._get_fallback_player_vector("이정후", 2022),
                PlayerVector(
                    player_id=72401,
                    player_name="이승엽",
                    team_code="삼성",
                    season=2003,
                    role=PlayerRole.BATTER,
                    dimensions={
                        "컨택 능력 (Contact)": 88.0,
                        "장타력 (Power)": 99.5,
                        "선구안 (Discipline)": 89.0,
                        "기동력 (Speed)": 45.0,
                        "세이버 종합가치 (Value)": 98.0,
                    },
                    key_stats={"AVG": 0.301, "HR": 56, "SB": 7, "OPS": 1.100, "wRC+": 190.0},
                ),
                PlayerVector(
                    player_id=64001,
                    player_name="강정호",
                    team_code="넥센",
                    season=2014,
                    role=PlayerRole.BATTER,
                    dimensions={
                        "컨택 능력 (Contact)": 89.0,
                        "장타력 (Power)": 97.0,
                        "선구안 (Discipline)": 86.0,
                        "기동력 (Speed)": 50.0,
                        "세이버 종합가치 (Value)": 96.0,
                    },
                    key_stats={"AVG": 0.356, "HR": 40, "SB": 3, "OPS": 1.198, "wRC+": 192.5},
                ),
                PlayerVector(
                    player_id=88001,
                    player_name="양준혁",
                    team_code="삼성",
                    season=1996,
                    role=PlayerRole.BATTER,
                    dimensions={
                        "컨택 능력 (Contact)": 95.0,
                        "장타력 (Power)": 91.0,
                        "선구안 (Discipline)": 98.0,
                        "기동력 (Speed)": 60.0,
                        "세이버 종합가치 (Value)": 97.5,
                    },
                    key_stats={"AVG": 0.346, "HR": 28, "SB": 8, "OPS": 1.045, "wRC+": 185.0},
                ),
            ]

        return [
            self._get_fallback_player_vector("류현진", 2006),
            PlayerVector(
                player_id=82101,
                player_name="선동열",
                team_code="해태",
                season=1986,
                role=PlayerRole.PITCHER,
                dimensions={
                    "구위 (Stuff)": 99.5,
                    "제구력 (Command)": 98.0,
                    "실점 억제력 (Damage Control)": 99.9,
                    "효율성 (Efficiency)": 99.0,
                    "이닝 소화력 (Workhorse)": 99.5,
                },
                key_stats={"ERA": 0.99, "FIP": 1.35, "WHIP": 0.74, "SO": 214, "IP": 262.2},
            ),
            PlayerVector(
                player_id=78201,
                player_name="김광현",
                team_code="SK",
                season=2008,
                role=PlayerRole.PITCHER,
                dimensions={
                    "구위 (Stuff)": 94.0,
                    "제구력 (Command)": 84.0,
                    "실점 억제력 (Damage Control)": 93.0,
                    "효율성 (Efficiency)": 89.0,
                    "이닝 소화력 (Workhorse)": 92.0,
                },
                key_stats={"ERA": 2.39, "FIP": 2.80, "WHIP": 1.15, "SO": 150, "IP": 162.0},
            ),
            PlayerVector(
                player_id=60001,
                player_name="양현종",
                team_code="KIA",
                season=2017,
                role=PlayerRole.PITCHER,
                dimensions={
                    "구위 (Stuff)": 88.0,
                    "제구력 (Command)": 89.0,
                    "실점 억제력 (Damage Control)": 89.0,
                    "효율성 (Efficiency)": 90.0,
                    "이닝 소화력 (Workhorse)": 96.0,
                },
                key_stats={"ERA": 3.44, "FIP": 3.65, "WHIP": 1.25, "SO": 158, "IP": 193.1},
            ),
        ]


__all__ = [
    "ABOVE_AVG_SCORE_THRESHOLD",
    "ADVANTAGE_THRESHOLD",
    "ELITE_SCORE_THRESHOLD",
    "HIGH_SCORE_THRESHOLD",
    "SIMILARITY_MATCH_LIMIT",
    "TOTAL_SCORE_TIE_DIFF",
    "PlayerSimilarityEngine",
]
