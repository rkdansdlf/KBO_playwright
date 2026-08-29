"""KBO Sabermetrics and Advanced Analytics Domain Package."""

from __future__ import annotations

from src.analytics.dto import (
    BattingSabermetrics,
    LeagueConstants,
    MatchupMatrix,
    PitchingSabermetrics,
    SplitMetrics,
)
from src.analytics.matchup import MatchupAnalyticsEngine
from src.analytics.predictor import MatchupPredictor, SabermetricFeatureStore
from src.analytics.predictor_dto import MatchupFeatureVector, MatchupPredictionResult
from src.analytics.sabermetrics import SabermetricsEngine
from src.analytics.similarity import PlayerSimilarityEngine
from src.analytics.similarity_dto import (
    HeadToHeadComparisonResult,
    PlayerRole,
    PlayerSimilarityResult,
    PlayerVector,
    SimilarPlayerMatch,
)

__all__ = [
    "BattingSabermetrics",
    "HeadToHeadComparisonResult",
    "LeagueConstants",
    "MatchupAnalyticsEngine",
    "MatchupFeatureVector",
    "MatchupMatrix",
    "MatchupPredictionResult",
    "MatchupPredictor",
    "PitchingSabermetrics",
    "PlayerRole",
    "PlayerSimilarityEngine",
    "PlayerSimilarityResult",
    "PlayerVector",
    "SabermetricFeatureStore",
    "SabermetricsEngine",
    "SimilarPlayerMatch",
    "SplitMetrics",
]
