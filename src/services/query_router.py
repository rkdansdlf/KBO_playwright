"""Route KBO questions to structured retrieval or document retrieval."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from src.services.kbo_entity_resolver import ResolvedKboEntities, resolve_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class QueryIntent(StrEnum):
    """Classify the dominant information need in a KBO question."""

    STAT_QUERY = "STAT_QUERY"
    ENTITY_LOOKUP = "ENTITY_LOOKUP"
    GAME_QUERY = "GAME_QUERY"
    RULE_QUERY = "RULE_QUERY"
    HISTORICAL_QUERY = "HISTORICAL_QUERY"
    EXPLANATION_QUERY = "EXPLANATION_QUERY"
    MIXED_QUERY = "MIXED_QUERY"


class RetrievalRoute(StrEnum):
    """Select the source of truth for a query plan."""

    STRUCTURED = "STRUCTURED"
    DOCUMENT = "DOCUMENT"
    MIXED = "MIXED"


STAT_TERMS = (
    "타율",
    "출루율",
    "장타율",
    "ops",
    "홈런",
    "안타",
    "타점",
    "득점",
    "도루",
    "삼진",
    "볼넷",
    "방어율",
    "평자",
    "승수",
    "세이브",
    "홀드",
    "이닝",
    "순위",
    "승률",
)
RULE_TERMS = (
    "규정",
    "규칙",
    "abs",
    "판정",
    "허용",
    "금지",
    "스트라이크존",
    "주자",
    "베이스",
    "투구",
    "떠나",
    "되나요",
    "되나",
)
EXPLANATION_TERMS = ("왜", "이유", "설명", "분석", "원인")
PROFILE_TERMS = ("프로필", "누구", "정보", "선수")
GAME_TERMS = ("경기", "스코어", "결과", "승부", "대진", "한국시리즈")


@dataclass(frozen=True)
class QueryPlan:
    """Describe routing, resolved entities, and retrieval filters for one query."""

    query: str
    intent: QueryIntent
    route: RetrievalRoute
    entities: ResolvedKboEntities
    stat_type: str | None
    filters: dict[str, Any]
    timings: dict[str, float] = field(default_factory=dict)

    def to_analysis(self) -> dict[str, Any]:
        """Serialize the plan for an internal retrieval trace."""
        extracted = self.entities.extracted
        return {
            "intent": self.intent.value,
            "route": self.route.value,
            "entities": {
                "team_id": extracted.team_id,
                "player_id": self.entities.player_id,
                "player_name": extracted.player_name,
                "season_year": extracted.season_year,
                "stadium": extracted.stadium,
                "stat_type": self.stat_type,
                "category": (
                    None if self.intent in {QueryIntent.STAT_QUERY, QueryIntent.MIXED_QUERY} else extracted.category
                ),
                "ambiguous_player": self.entities.ambiguous_player,
            },
            "filters": self.filters,
        }


def _stat_type(query: str) -> str | None:
    """Extract the canonical statistic requested by a query."""
    lowered = query.lower()
    stat_map = {
        "순위": "rank",
        "승률": "win_pct",
        "홈런": "home_runs",
        "안타": "hits",
        "타점": "rbi",
        "도루": "stolen_bases",
        "타율": "avg",
        "출루율": "obp",
        "장타율": "slg",
        "ops": "ops",
        "방어율": "era",
        "평자": "era",
        "승수": "wins",
        "세이브": "saves",
        "홀드": "holds",
        "이닝": "innings_pitched",
    }
    return next((value for term, value in stat_map.items() if term in lowered), None)


def _has_any(query: str, terms: tuple[str, ...]) -> bool:
    """Return whether a query contains at least one term."""
    lowered = query.lower()
    return any(term in lowered for term in terms)


def _classify(query: str, stat_type: str | None) -> QueryIntent:
    """Classify a query using deterministic domain signals."""
    has_explanation = _has_any(query, EXPLANATION_TERMS)
    has_stat = stat_type is not None or _has_any(query, STAT_TERMS)
    if has_stat and has_explanation:
        return QueryIntent.MIXED_QUERY
    if has_stat:
        return QueryIntent.STAT_QUERY
    return next(
        (
            intent
            for condition, intent in (
                (_has_any(query, RULE_TERMS), QueryIntent.RULE_QUERY),
                (_has_any(query, GAME_TERMS), QueryIntent.GAME_QUERY),
                (has_explanation, QueryIntent.EXPLANATION_QUERY),
                (_has_any(query, PROFILE_TERMS), QueryIntent.ENTITY_LOOKUP),
            )
            if condition
        ),
        QueryIntent.HISTORICAL_QUERY,
    )


class QueryRouter:
    """Build a source-aware retrieval plan without invoking an LLM."""

    def __init__(self, session: Session) -> None:
        """Initialize the router with the structured database session."""
        self.session = session

    def plan(
        self,
        query: str,
        *,
        category: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> QueryPlan:
        """Build a query plan and resolve entity filters before retrieval."""
        started = time.perf_counter()
        stat_started = time.perf_counter()
        stat_type = _stat_type(query)
        intent = _classify(query, stat_type)
        classify_ms = round((time.perf_counter() - stat_started) * 1000, 3)
        explicit_filters = dict(filters or {})
        resolve_started = time.perf_counter()
        entities = resolve_kbo_entities(
            self.session,
            query,
            explicit_filters,
            extract_player=intent not in {QueryIntent.RULE_QUERY, QueryIntent.EXPLANATION_QUERY},
        )
        resolver_ms = round((time.perf_counter() - resolve_started) * 1000, 3)
        merged_filters = entities.to_filters()
        merged_filters.update(explicit_filters)

        if category:
            merged_filters["document_type"] = category
        if intent in {QueryIntent.STAT_QUERY, QueryIntent.MIXED_QUERY}:
            merged_filters.pop("document_type", None)
        if intent in {QueryIntent.RULE_QUERY, QueryIntent.EXPLANATION_QUERY}:
            merged_filters.pop("player_name", None)

        has_structured_entity = bool(entities.player_id or entities.extracted.team_id or entities.extracted.season_year)
        if intent in {QueryIntent.STAT_QUERY, QueryIntent.ENTITY_LOOKUP} and has_structured_entity:
            route = RetrievalRoute.STRUCTURED
        elif intent is QueryIntent.MIXED_QUERY and has_structured_entity:
            route = RetrievalRoute.MIXED
        else:
            route = RetrievalRoute.DOCUMENT

        return QueryPlan(
            query=query,
            intent=intent,
            route=route,
            entities=entities,
            stat_type=stat_type,
            filters=merged_filters,
            timings={
                "classifier": classify_ms,
                "resolver": resolver_ms,
                "router": round((time.perf_counter() - started) * 1000, 3),
            },
        )
