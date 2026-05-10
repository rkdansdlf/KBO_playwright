import argparse
from typing import List, Dict, Any
from sqlalchemy import func
from src.db.engine import SessionLocal
from src.models.game import GameBattingStat
from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.repositories.player_stats_repository import PlayerSeasonFieldingRepository, PlayerSeasonBaserunningRepository

def backfill_stats(years: List[int], series: str):
    fielding_repo = PlayerSeasonFieldingRepository()
    baserun_repo = PlayerSeasonBaserunningRepository()

    with SessionLocal() as session:
        for year in years:
            print(f"🛠️  Backfilling Advanced Stats for {year} {series}...")
            
            # 1. Resolve team_id for all players in this season (most frequent team)
            # This is needed because the aggregate methods only return player_id
            team_map = {}
            team_query = (
                session.query(
                    GameBattingStat.player_id,
                    GameBattingStat.team_code,
                    func.count(GameBattingStat.id).label('cnt')
                )
                .group_by(GameBattingStat.player_id, GameBattingStat.team_code)
                .all()
            )
            for pid, team, cnt in team_query:
                if pid not in team_map or cnt > team_map[pid][1]:
                    team_map[pid] = (team, cnt)

            # 2. Baserunning Backfill
            br_stats = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year, series, source='FALLBACK_BACKFILL')
            if br_stats:
                # Add team_id
                for stat in br_stats:
                    pid = stat['player_id']
                    stat['team_id'] = team_map.get(pid, (None, 0))[0]
                
                valid_br = [s for s in br_stats if s['team_id']]
                cnt = baserun_repo.upsert_many(valid_br)
                print(f"   ✅ Baserunning: {cnt} records saved.")

            # 3. Fielding Backfill
            fld_stats = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year, series, source='FALLBACK_BACKFILL')
            if fld_stats:
                # Add team_id
                for stat in fld_stats:
                    pid = stat['player_id']
                    stat['team_id'] = team_map.get(pid, (None, 0))[0]
                
                valid_fld = [s for s in fld_stats if s['team_id']]
                cnt = fielding_repo.upsert_many(valid_fld)
                print(f"   ✅ Fielding: {cnt} records saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing advanced stats from transactions.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--series", type=str, default="regular")
    
    args = parser.parse_args()
    
    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]
        
    backfill_stats(target_years, args.series)
