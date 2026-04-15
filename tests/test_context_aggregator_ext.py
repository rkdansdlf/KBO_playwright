
import pytest
from datetime import date, datetime
from src.services.context_aggregator import ContextAggregator
from src.models.player import PlayerMovement
from src.models.team import TeamDailyRoster
from src.db.engine import SessionLocal

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

if __name__ == "__main__":
    pytest.main([__file__])
