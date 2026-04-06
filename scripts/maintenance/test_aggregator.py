import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.services.context_aggregator import ContextAggregator

def test_aggregator():
    # Use 2024-09-24 as target date for testing against historical data
    target_date = datetime.strptime("20240924", "%Y%m%d").date()
    # Sample game: NC vs Doosan (DB)
    away_team = "NC"
    home_team = "DB"
    game_id = "20240924NCOB0"

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        print(f"--- Testing Aggregator for {away_team} vs {home_team} on {target_date} ---")
        
        # 1. L10 Summary
        away_l10 = agg.get_team_l10_summary(away_team, target_date)
        home_l10 = agg.get_team_l10_summary(home_team, target_date)
        print(f"Away L10: {away_l10}")
        print(f"Home L10: {home_l10}")
        
        # 2. Head to Head
        h2h = agg.get_head_to_head_summary(away_team, home_team, 2024, target_date)
        print(f"H2H: {h2h}")
        
        # 3. Recent Metrics
        away_metrics = agg.get_team_recent_metrics(away_team, target_date)
        print(f"Away Recent Metrics (AVG/ERA): {away_metrics}")
        
        # 4. Crucial Moments (if events exist)
        moments = agg.get_crucial_moments(game_id)
        print(f"Crucial Moments for {game_id}: {moments}")

if __name__ == "__main__":
    test_aggregator()
