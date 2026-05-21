"""
CLI tool to recalculate team-level season stats by aggregating player stats.
Used as a fallback or for verification.
"""
from __future__ import annotations
import argparse
import asyncio
import sys
from typing import List, Dict, Any

from src.db.engine import SessionLocal
from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching

def save_team_batting_stats(session, stats_list: List[Dict[str, Any]]):
    for data in stats_list:
        existing = session.query(TeamSeasonBatting).filter_by(
            team_id=data['team_id'],
            season=data['season'],
            league=data['league']
        ).first()
        
        # Filter out keys not in model
        model_keys = TeamSeasonBatting.__table__.columns.keys()
        filtered_data = {k: v for k, v in data.items() if k in model_keys}
        
        if existing:
            for k, v in filtered_data.items():
                setattr(existing, k, v)
        else:
            new_rec = TeamSeasonBatting(**filtered_data)
            session.add(new_rec)
    session.commit()

def save_team_pitching_stats(session, stats_list: List[Dict[str, Any]]):
    for data in stats_list:
        existing = session.query(TeamSeasonPitching).filter_by(
            team_id=data['team_id'],
            season=data['season'],
            league=data['league']
        ).first()
        
        model_keys = TeamSeasonPitching.__table__.columns.keys()
        filtered_data = {k: v for k, v in data.items() if k in model_keys}
        
        if existing:
            for k, v in filtered_data.items():
                setattr(existing, k, v)
        else:
            new_rec = TeamSeasonPitching(**filtered_data)
            session.add(new_rec)
    session.commit()

async def run_recalc(args):
    with SessionLocal() as session:
        if args.type in ['batting', 'all']:
            print(f"⚾ Recalculating team BATTING for {args.year} {args.league}...")
            batting_stats = TeamStatAggregator.aggregate_team_batting(session, args.year, args.league)
            print(f"   Found {len(batting_stats)} teams.")
            if args.save:
                save_team_batting_stats(session, batting_stats)
                print("   ✅ Saved to database.")
            else:
                for s in batting_stats:
                    print(f"   - {s['team_id']} ({s.get('team_name')}): {s['hits']} H, {s['home_runs']} HR, {s['avg']} AVG")

        if args.type in ['pitching', 'all']:
            print(f"🥎 Recalculating team PITCHING for {args.year} {args.league}...")
            pitching_stats = TeamStatAggregator.aggregate_team_pitching(session, args.year, args.league)
            print(f"   Found {len(pitching_stats)} teams.")
            if args.save:
                save_team_pitching_stats(session, pitching_stats)
                print("   ✅ Saved to database.")
            else:
                for s in pitching_stats:
                    print(f"   - {s['team_id']} ({s.get('team_name')}): {s['wins']} W, {s['era']} ERA, {s['whip']} WHIP")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalculate Team Season Stats from Player Stats")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--league", type=str, default="regular", help="League type (regular, etc.)")
    parser.add_argument("--type", choices=['batting', 'pitching', 'all'], default='all', help="Stat type")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    
    args = parser.parse_args()
    asyncio.run(run_recalc(args))
