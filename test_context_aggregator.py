
import sys
import os
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from src.services.context_aggregator import ContextAggregator
from src.db.engine import SessionLocal
from src.models.player import PlayerMovement
from src.models.team import TeamDailyRoster

def test_aggregator():
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        # 1. Test Player Movements
        print("Testing get_recent_player_movements...")
        # Add a dummy record for testing if not exists? No, just check if it runs.
        movements = agg.get_recent_player_movements("LG", date(2023, 12, 23))
        print(f"Movements for LG on 2023-12-23: {movements}")
        
        # 2. Test Daily Roster Changes
        print("\nTesting get_daily_roster_changes...")
        # We might need to seed some data for this to be meaningful.
        changes = agg.get_daily_roster_changes("LG", date(2024, 5, 20))
        print(f"Roster changes for LG on 2024-05-20: {changes}")

if __name__ == "__main__":
    test_aggregator()
