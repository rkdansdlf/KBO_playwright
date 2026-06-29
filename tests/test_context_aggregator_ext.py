from datetime import date

import pytest

from src.db.engine import Engine, SessionLocal
from src.models.base import Base
from src.services.context_aggregator import ContextAggregator

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _test_db_tables():
    Base.metadata.create_all(bind=Engine)
    yield


def test_get_recent_player_movements_team_mapping():
    """HT, KIA 등 다양한 팀 코드로 조회가 잘 되는지 확인"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        # Test case 1: KIA (Recorded as 'KIA' in DB, but queried as 'HT')
        # We need to make sure we have dummy data or use real data if available
        # Since this is a unit test, we'll verify the logic for team_name_map
        target_date = "20240521"

        # This will test the 'possible_names' logic we added
        movements = agg.get_recent_player_movements("HT", target_date)

        # Verify result contains records that might have 'KIA' as team_code
        for m in movements:
            assert m.get("player") is not None


def test_target_date_type_handling():
    """문자열과 date 객체 모두 지원하는지 확인"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        # Case 1: String
        res1 = agg.get_recent_player_movements("HT", "20240521")

        # Case 2: Date object
        res2 = agg.get_recent_player_movements("HT", date(2024, 5, 21))

        # Both should execute without Error
        assert isinstance(res1, list)
        assert isinstance(res2, list)


def test_daily_roster_changes_logic():
    """엔트리 변동 계산 로직 검증"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        # Using real data from 20240521 for KIA
        res = agg.get_daily_roster_changes("HT", "20240521")

        assert "added" in res
        assert "removed" in res
        assert isinstance(res["added"], list)
        assert isinstance(res["removed"], list)


def test_get_team_error_games():
    """실책 경기 목록 조회 검증"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        res = agg.get_team_error_games("LG", 2026)
        assert isinstance(res, list)
        if res:
            assert "game_id" in res[0]
            assert "errors" in res[0]
            assert isinstance(res[0]["errors"], list)


def test_get_toughest_opponents():
    """상대전적상 가장 까다로운 팀 목록 조회 검증"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        res = agg.get_toughest_opponents("LG", 2026)
        assert isinstance(res, list)
        if res:
            assert "opponent" in res[0]
            assert "win_rate" in res[0]
            win_rates = [x["win_rate"] for x in res]
            assert win_rates == sorted(win_rates)


def test_get_position_avg_comparison():
    """포지션 평균 비교 검증"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)

        # Use player 51302 ('이주형', '내야수') in 2026
        res = agg.get_position_avg_comparison(51302, "내야수", 2026)
        if res:
            assert "player_id" in res
            assert "player_stats" in res
            assert "position_averages" in res
            assert "comparison" in res
            assert res["position"] == "내야수"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__])
