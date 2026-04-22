import argparse
import sys
from typing import List, Dict, Any

from src.aggregators.ranking_aggregator import RankingAggregator
from src.repositories.player_season_batting_repository import PlayerSeasonBattingRepository
from src.repositories.player_season_pitching_repository import get_pitching_stats_by_season
from src.repositories.ranking_repository import RankingRepository
from src.db.engine import SessionLocal

def generate_for_season(year: int, total_games: int, save: bool):
    print(f"[*] Generating rankings for {year} (Season Games: {total_games})...")
    
    batting_repo = PlayerSeasonBattingRepository()
    ranking_repo = RankingRepository()
    
    # 1. Fetch Stats
    print("[*] Fetching batting stats...")
    batting_objs = batting_repo.get_by_season(year)
    batting_stats = [b.__dict__ for b in batting_objs]
    
    print("[*] Fetching pitching stats...")
    pitching_objs = get_pitching_stats_by_season(year)
    pitching_stats = [p.__dict__ for p in pitching_objs]
    
    # 2. Calculate Qualification Limits
    # KBO Standard: 3.1 PA per game, 1.0 IP per game
    min_pa = int(total_games * 3.1)
    min_ip_outs = int(total_games * 3) # 1.0 IP * 3 outs
    
    print(f"[*] Qualification Limits: Min PA={min_pa}, Min IP Outs={min_ip_outs}")
    
    # 3. Aggregate Rankings
    aggregator = RankingAggregator(repository=ranking_repo)
    rankings = aggregator.generate_rankings(
        season=year,
        batting_stats=batting_stats,
        pitching_stats=pitching_stats,
        min_pa=min_pa,
        min_ip_outs=min_ip_outs,
        persist=save
    )
    
    print(f"[+] Successfully generated {len(rankings)} ranking entries.")
    if save:
        print(f"[+] Rankings persisted to 'stat_rankings' table.")
    else:
        print("[!] Dry-run: Rankings NOT saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KBO Ranking Generator")
    parser.add_argument("--year", type=int, required=True, help="Year of the season to rank")
    parser.add_argument("--games", type=int, default=144, help="Total games in the season (default 144)")
    parser.add_argument("--save", action="store_true", help="Persist rankings to the database")
    
    args = parser.parse_args()
    
    try:
        generate_for_season(args.year, args.games, args.save)
    except Exception as e:
        print(f"[ERROR] Ranking generation failed: {e}")
        sys.exit(1)
