"""Tests for query routing, entity resolution, and structured retrieval."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.services.kbo_entity_resolver import resolve_kbo_entities
from src.models.standings import TeamStandingsDaily
from src.services.query_router import QueryIntent, QueryRouter, RetrievalRoute
from src.services.structured_retriever import StructuredRetriever
from src.utils.kbo_entity_extractor import extract_kbo_entities


def _session() -> Session:
    """Create an isolated relational session for routing tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(PlayerBasic(player_id=100, name="김도영", team="KIA", position="내야수"))
    session.add(
        PlayerSeasonBatting(
            player_id=100,
            season=2024,
            league="REGULAR",
            level="KBO1",
            team_code="KIA",
            home_runs=38,
            hits=158,
            avg=0.347,
        )
    )
    session.add(
        TeamStandingsDaily(
            standings_date=date(2024, 10, 1),
            team_code="KIA",
            rank=1,
            wins=87,
            losses=55,
            draws=2,
            win_pct=0.613,
        )
    )
    session.commit()
    return session


def test_router_resolves_player_and_routes_stat_query_to_sql() -> None:
    """Route a season/player stat question to structured retrieval."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2024년 KIA 김도영의 홈런 수")

        assert plan.intent is QueryIntent.STAT_QUERY
        assert plan.route is RetrievalRoute.STRUCTURED
        assert plan.entities.player_id == "100"
        assert plan.stat_type == "home_runs"

        results = StructuredRetriever(session).retrieve(plan, top_k=5)
        assert len(results) == 1
        assert "홈런: 38" in results[0].content
        assert results[0].meta["player_id"] == "100"
    finally:
        session.close()


def test_structured_retriever_reads_latest_team_standing() -> None:
    """Return the latest available season snapshot for a ranking question."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2024년 KIA 최종 순위")
        results = StructuredRetriever(session).retrieve(plan, top_k=5)

        assert plan.stat_type == "rank"
        assert results[0].meta["rank"] == 1
        assert "1위" in results[0].content
    finally:
        session.close()


def test_router_keeps_rule_question_on_document_route() -> None:
    """Keep a regulation question on the document retrieval path."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("ABS 스트라이크존 규정이 어떻게 되나요?")
        assert plan.intent is QueryIntent.RULE_QUERY
        assert plan.route is RetrievalRoute.DOCUMENT
        assert plan.entities.extracted.player_name is None
        assert "player_name" not in plan.filters
    finally:
        session.close()


def test_extractor_preserves_three_syllable_foreign_player_name() -> None:
    """Do not strip a final 도 that is part of a three-syllable name."""
    entities = extract_kbo_entities("2024년 KT 후라도 이닝")
    assert entities.player_name == "후라도"


def test_resolver_uses_season_team_evidence_for_same_name_players() -> None:
    """Resolve a same-name player only when the requested season/team agrees."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(
        [
            PlayerBasic(player_id=201, name="김동현", team="KIA"),
            PlayerBasic(player_id=202, name="김동현", team="LG"),
            PlayerSeasonBatting(
                player_id=201,
                season=2024,
                league="REGULAR",
                level="KBO1",
                team_code="KIA",
                hits=10,
            ),
            PlayerSeasonBatting(
                player_id=202,
                season=2024,
                league="REGULAR",
                level="KBO1",
                team_code="LG",
                hits=20,
            ),
        ]
    )
    session.commit()

    resolved = resolve_kbo_entities(session, "2024년 KIA 김동현 타율")
    ambiguous = resolve_kbo_entities(session, "김동현 타율")

    assert resolved.player_id == "201"
    assert not resolved.ambiguous_player
    assert ambiguous.player_id is None
    assert ambiguous.ambiguous_player
    session.close()
