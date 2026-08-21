"""Unit tests for KBO entity and metadata extractor."""

from __future__ import annotations

from src.utils.kbo_entity_extractor import extract_kbo_entities


def test_extract_team_and_year() -> None:
    """Test extracting team and season year from queries."""
    res1 = extract_kbo_entities("2026년 KIA 타이거즈 경기일정 알려줘")
    assert res1.season_year == 2026
    assert res1.team_id == "KIA"

    res2 = extract_kbo_entities("두산 베어스 2024 시즌 순위")
    assert res2.season_year == 2024
    assert res2.team_id == "DB"


def test_extract_game_date() -> None:
    """Extract a validated calendar date for date-scoped retrieval."""
    result = extract_kbo_entities("2015년 10월 19일 경기 결과")

    assert result.game_date == "2015-10-19"
    assert result.to_filters()["game_date"] == "2015-10-19"


def test_extract_stadium_and_category() -> None:
    """Test extracting stadium and category keywords."""
    res1 = extract_kbo_entities("잠실 야구장 주차장 요금 및 팁")
    assert res1.stadium == "잠실"
    assert res1.category == "stadium_facility"

    res2 = extract_kbo_entities("2025 KBO 최형우 마일스톤 대기록")
    assert res2.season_year == 2025
    assert res2.category == "milestone"


def test_extract_player_name() -> None:
    """Test extracting player candidate names."""
    res = extract_kbo_entities("김도영 득점권 타율 및 OPS")
    assert res.player_name == "김도영"
    assert res.category == "player_splits"


def test_to_filters_dict() -> None:
    """Test converting extracted entities to filter dict."""
    entities = extract_kbo_entities("2026 KIA 보도자료")
    filters = entities.to_filters()
    assert filters.get("season_year") == 2026
    assert filters.get("team_id") == "KIA"
    assert filters.get("document_type") == "press_release"


def test_facility_query_does_not_invent_player_name() -> None:
    """Test that facility nouns are not mistaken for player names."""
    result = extract_kbo_entities("잠실 야구장 주차장 요금 및 팁")

    assert result.stadium == "잠실"
    assert result.player_name is None
    assert result.to_filters()["stadium"] == "잠실"


def test_player_entity_is_exposed_as_a_metadata_filter() -> None:
    """Test that a player candidate is available to sparse metadata filtering."""
    result = extract_kbo_entities("2026 KIA 김도영 성적")

    assert result.player_name == "김도영"
    assert result.to_filters()["player_name"] == "김도영"
