
import sys
import os
from datetime import date
from src.services.context_aggregator import ContextAggregator
from src.db.engine import SessionLocal

def test_changes():
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        target = date(2024, 10, 1)
        changes = agg.get_daily_roster_changes("OB", target) # Doosan
        print(f"Roster changes for OB on {target}: {changes}")
        
        target2 = date(2024, 5, 21)
        changes2 = agg.get_daily_roster_changes("LG", target2)
        print(f"Roster changes for LG on {target2}: {changes2}")

if __name__ == "__main__":
    test_changes()
