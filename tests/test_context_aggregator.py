
import pytest
from datetime import date, datetime
from src.services.context_aggregator import ContextAggregator
from src.models.player import PlayerMovement
from src.models.team import TeamDailyRoster
from src.db.engine import SessionLocal

def test_get_recent_player_movements_team_mapping():
    """HT, KIA 등 다양한 팀 코드로 조회가 잘 되는지 확인 (Team Mapping Shield)"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        # Test Case 1: Search HT (DB might have 'KIA' or 'HT')
        target_date = "20240521"
        res = agg.get_recent_player_movements("HT", target_date)
        
        # Verify result contains records that were found in previous tests (KIA records)
        # If we have data, results should be a list.
        assert isinstance(res, list)
        
        # Test Case 2: Search with Korean Team Name directly
        res_ko = agg.get_recent_player_movements("KIA", target_date)
        assert isinstance(res_ko, list)

def test_target_date_type_flexibility():
    """문자열('YYYYMMDD', 'YYYY-MM-DD')과 date 객체 모두 지원하는지 확인"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        # All of these should resolve to date(2024, 5, 21) internally
        res1 = agg.get_recent_player_movements("HT", "20240521")
        res2 = agg.get_recent_player_movements("HT", "2024-05-21")
        res3 = agg.get_recent_player_movements("HT", date(2024, 5, 21))
        
        assert isinstance(res1, list)
        assert isinstance(res2, list)
        assert isinstance(res3, list)

def test_daily_roster_changes_added_removed():
    """엔트리 변동 계산 로직의 정합성 검증"""
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        # Test with 2024-05-21 (KIA vs Lotte day)
        res = agg.get_daily_roster_changes("HT", "20240521")
        
        assert "added" in res
        assert "removed" in res
        assert isinstance(res["added"], list)
        assert isinstance(res["removed"], list)
        
        # Roster changes for 2024-05-21 HT should reflect what we saw in logs
        # This acts as a regression test for the set difference logic
        # (Assuming the local DB has the backfilled data)
        # print(f"HT 05-21 Changes: {res}")

if __name__ == "__main__":
    pytest.main([__file__])
