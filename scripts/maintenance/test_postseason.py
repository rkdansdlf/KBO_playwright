import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.services.context_aggregator import ContextAggregator

def test_postseason_context():
    # 2024 Korean Series Game 3 (KIA vs SS)
    target_date = datetime.strptime("20241025", "%Y%m%d").date()
    away_team = "KIA" # KIA Tigers
    home_team = "SS"  # Samsung Lions
    
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        print(f"--- Testing Postseason Context for {away_team} vs {home_team} on {target_date} ---")
        
        series = agg.get_postseason_series_summary(away_team, home_team, 2024, target_date)
        if series:
            print(f"Result: {series['series_text']}")
            print(f"Details: {series}")
        else:
            print("No series context found. (Check if previous games are in local DB)")

if __name__ == "__main__":
    test_postseason_context()
