"""Contract tests for the four relational sources added to structured retrieval."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.award import Award
from src.models.base import Base
from src.models.franchise import Franchise
from src.models.game import Game
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.services.query_router import QueryRouter, RetrievalRoute
from src.services.structured_retriever import StructuredRetriever


def _session() -> Session:
    """Build an isolated database with one row per source under test."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(
        [
            Franchise(id=1, name="기아", original_code="KIA", current_code="KIA"),
            Franchise(id=2, name="한화", original_code="HH", current_code="HH"),
            Team(
                team_id="KIA",
                team_name="KIA 타이거즈",
                team_short_name="KIA",
                city="광주",
                stadium_name="광주-KIA 챔피언스필드",
            ),
            Team(team_id="HH", team_name="한화 이글스", team_short_name="한화", city="대전", stadium_name="이글스파크"),
            Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울", stadium_name="종합운동장"),
            TeamHistory(
                id=1,
                franchise_id=1,
                season=2003,
                team_name="기아 타이거즈",
                team_code="KIA",
                ranking=1,
                stadium="광주무등경기장 야구장",
                city="광주",
            ),
            Award(id=1, year=2025, award_type="MVP", player_name="코디 폰세", team_name="한화"),
            Award(
                id=2,
                year=2025,
                award_type="골든글러브",
                category="투수",
                player_name="코디 폰세",
                team_name="한화",
            ),
            Game(
                id=1,
                game_id="20260721HHKI0",
                game_date=date(2026, 7, 21),
                away_team="HH",
                home_team="KIA",
                away_score=3,
                home_score=5,
                game_status="COMPLETED",
            ),
            Game(
                id=2,
                game_id="20260721KTHH0",
                game_date=date(2026, 7, 21),
                away_team="KT",
                home_team="HH",
                away_score=1,
                home_score=2,
                game_status="COMPLETED",
            ),
            Game(
                id=3,
                game_id="20240701LGKI0",
                game_date=date(2024, 7, 1),
                away_team="LG",
                home_team="KIA",
                away_score=2,
                home_score=5,
                game_status="COMPLETED",
            ),
            Game(
                id=4,
                game_id="20240702KILG0",
                game_date=date(2024, 7, 2),
                away_team="KIA",
                home_team="LG",
                away_score=3,
                home_score=4,
                game_status="COMPLETED",
            ),
            Game(
                id=5,
                game_id="20240703HHKI0",
                game_date=date(2024, 7, 3),
                away_team="HH",
                home_team="KIA",
                away_score=1,
                home_score=7,
                game_status="COMPLETED",
            ),
        ]
    )
    session.commit()
    return session


def _retrieve(session: Session, query: str, source_table: str, top_k: int = 5):
    """Plan with an explicit source filter and run structured retrieval."""
    plan = QueryRouter(session).plan(query, filters={"source_table": source_table})
    return plan, StructuredRetriever(session).retrieve(plan, top_k)


def test_team_info_is_a_row_lookup() -> None:
    """A home-stadium question returns the canonical team row, not a text chunk."""
    session = _session()
    try:
        plan, results = _retrieve(session, "KIA 타이거즈의 연고지와 홈구장은 어디야?", "teams")
        assert plan.route is RetrievalRoute.STRUCTURED
        assert len(results) == 1
        assert results[0].source_table == "teams"
        assert results[0].source_row_id == "KIA"
        assert "광주" in results[0].content
        assert "챔피언스필드" in results[0].content
        assert results[0].to_hybrid_result().provenance["record_key"] == "teams:KIA"
    finally:
        session.close()


def test_awards_mvp_is_a_row_lookup() -> None:
    """An award-winner question resolves the awards table by year."""
    session = _session()
    try:
        _, results = _retrieve(session, "2025년 KBO MVP 수상자는 누구야?", "awards")
        assert [result.source_row_id for result in results] == ["1"]
        assert results[0].source_table == "awards"
        assert "코디 폰세" in results[0].content
    finally:
        session.close()


def test_the_question_word_winner_is_not_a_player_filter() -> None:
    """The resolver's '수상자' guess must not filter out the actual winner."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2025년 KBO MVP 수상자는 누구야?", filters={"source_table": "awards"})
        assert plan.entities.extracted.player_name == "수상자"
        assert "player_name" not in plan.filters
        _, results = _retrieve(session, "2025년 KBO MVP 수상자는 누구야?", "awards")
        assert [result.source_row_id for result in results] == ["1"]
    finally:
        session.close()


def test_award_category_is_not_ignored() -> None:
    """A position-specific award question must not return a different award type."""
    session = _session()
    try:
        _, results = _retrieve(session, "2025년 골든글러브 투수 수상자는?", "awards")
        assert len(results) == 1
        assert results[0].source_row_id == "2"
        assert "투수" in results[0].content
    finally:
        session.close()


def test_team_history_uses_season_and_team() -> None:
    """A historical team question resolves season plus franchise identity."""
    session = _session()
    try:
        _, results = _retrieve(session, "2003년 기아 타이거즈의 팀 역사는 어땠어?", "team_history")
        assert [result.source_row_id for result in results] == ["1"]
        assert results[0].source_table == "team_history"
        assert "2003" in results[0].content
        assert "무등경기장" in results[0].content
    finally:
        session.close()


def test_game_result_uses_date_and_both_teams() -> None:
    """A dated matchup returns the game row with its final score."""
    session = _session()
    try:
        _, results = _retrieve(session, "2026년 7월 21일 한화와 KIA 경기 결과", "game")
        assert [result.source_row_id for result in results] == ["20260721HHKI0"]
        assert results[0].source_table == "game"
        assert "5" in results[0].content and "3" in results[0].content
    finally:
        session.close()


def test_a_game_with_only_one_matching_team_is_not_returned() -> None:
    """A two-team matchup must not degenerate into an either-team filter."""
    session = _session()
    try:
        _, results = _retrieve(session, "2026년 7월 21일 한화와 KIA 경기 결과", "game")
        assert [result.source_row_id for result in results] == ["20260721HHKI0"]
    finally:
        session.close()


def test_all_four_sources_route_to_structured() -> None:
    """An explicit source filter is a routing signal, not just a text filter."""
    session = _session()
    try:
        for source_table, query in (
            ("teams", "KIA 타이거즈의 연고지와 홈구장은 어디야?"),
            ("awards", "2025년 KBO MVP 수상자는 누구야?"),
            ("team_history", "2003년 기아 타이거즈의 팀 역사는 어땠어?"),
            ("game", "2026년 7월 21일 한화와 KIA 경기 결과"),
        ):
            plan = QueryRouter(session).plan(query, filters={"source_table": source_table})
            assert plan.route is RetrievalRoute.STRUCTURED, source_table
    finally:
        session.close()


def test_a_season_team_result_works_without_a_date() -> None:
    """A season summary question returns that team's completed games, newest first."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2024년 KIA 경기 결과")
        assert plan.route is RetrievalRoute.STRUCTURED
        results = StructuredRetriever(session).retrieve(plan, 5)
        assert [result.source_row_id for result in results] == [
            "20240703HHKI0",
            "20240702KILG0",
            "20240701LGKI0",
        ]
    finally:
        session.close()


def test_a_season_head_to_head_requires_both_teams() -> None:
    """A 대진 question must not include games involving only one of the two teams."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2024년 KIA LG 대진 결과")
        assert plan.route is RetrievalRoute.STRUCTURED
        results = StructuredRetriever(session).retrieve(plan, 5)
        assert [result.source_row_id for result in results] == ["20240702KILG0", "20240701LGKI0"]
    finally:
        session.close()


def test_a_schedule_question_stays_a_document_query() -> None:
    """Fixtures are not results; the game branch must not answer '일정'."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2023년 두산 경기 일정")
        assert plan.route is RetrievalRoute.DOCUMENT
    finally:
        session.close()


def test_a_season_question_without_a_team_stays_a_document_query() -> None:
    """A league-wide question has no team key for the game branch."""
    session = _session()
    try:
        plan = QueryRouter(session).plan("2024년 한국시리즈 경기 결과")
        assert plan.route is RetrievalRoute.DOCUMENT
    finally:
        session.close()


def test_natural_language_questions_reach_the_new_branches() -> None:
    """The inferred source routes and answers questions without an explicit filter."""
    session = _session()
    try:
        cases = (
            ("KIA 타이거즈의 연고지와 홈구장은 어디야?", "teams", "KIA"),
            ("2025년 KBO MVP 수상자는 누구야?", "awards", "1"),
            ("2003년 기아 타이거즈의 팀 역사는 어땠어?", "team_history", "1"),
            ("2026년 7월 21일 한화와 KIA 경기 결과", "game", "20260721HHKI0"),
        )
        for query, source_table, row_id in cases:
            plan = QueryRouter(session).plan(query)
            assert plan.route is RetrievalRoute.STRUCTURED, query
            assert plan.filters.get("source_table") == source_table, query
            results = StructuredRetriever(session).retrieve(plan, 5)
            assert [result.source_row_id for result in results] == [row_id], query
    finally:
        session.close()


def test_a_caller_filter_overrides_the_inference() -> None:
    """An explicit source_table must win over the inferred one."""
    session = _session()
    try:
        plan = QueryRouter(session).plan(
            "KIA 타이거즈의 연고지와 홈구장은 어디야?", filters={"source_table": "no_such_table"}
        )
        assert plan.route is not RetrievalRoute.STRUCTURED
        assert plan.filters["source_table"] == "no_such_table"
    finally:
        session.close()


def test_an_unknown_source_table_does_not_invent_rows() -> None:
    """An unrecognized source filter falls back to the existing behavior."""
    session = _session()
    try:
        plan, results = _retrieve(session, "KBO 규정에 대해 알려줘", "no_such_table")
        assert plan.route is not RetrievalRoute.STRUCTURED
        assert results == []
    finally:
        session.close()


def test_a_team_question_without_a_team_returns_no_rows() -> None:
    """A broad question must not dump the whole team table."""
    session = _session()
    try:
        _, results = _retrieve(session, "연고지와 홈구장이 어디야?", "teams")
        assert results == []
    finally:
        session.close()
