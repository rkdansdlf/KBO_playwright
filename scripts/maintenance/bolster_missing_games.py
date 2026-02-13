import asyncio
import sys
import os
from datetime import datetime, time
from sqlalchemy import text
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, os.getcwd())

from src.db.engine import SessionLocal
from src.crawlers.game_detail_crawler import GameDetailCrawler

async def bolster_missing_games(year_start=2010, year_end=2024):
    print(f"🚀 Starting Bolster operation for missing games ({year_start}-{year_end})...")
    
    with SessionLocal() as session:
        # Find games with NULL scores using game_id prefix which corresponds to year
        # Also filter by season range if possible, but game_id is more reliable here
        query = text("""
            SELECT game_id, game_date 
            FROM game 
            WHERE substr(game_id, 1, 4) BETWEEN :start AND :end 
            AND home_score IS NULL
            ORDER BY game_date
        """)
        missing_games = session.execute(query, {"start": str(year_start), "end": str(year_end)}).fetchall()
        
    print(f"📊 Found {len(missing_games)} games without scores in DB.")
    
    if not missing_games:
        return

    crawler = GameDetailCrawler(request_delay=1.0) # Adjusted delay
    
    success_count = 0
    fail_count = 0
    cancelled_count = 0

    # Process in chunks
    chunk_size = 10
    for i in range(0, len(missing_games), chunk_size):
        chunk = missing_games[i:i+chunk_size]
        batch_args = [{"game_id": g.game_id, "game_date": str(g.game_date).replace('-', '')} for g in chunk]
        
        print(f"\n📦 Processing batch {i//chunk_size + 1}/{(len(missing_games)-1)//chunk_size + 1} ({len(batch_args)} games)...")
        
        try:
            results = await crawler.crawl_games(batch_args, concurrency=3)
            
            for data in results:
                if data and data.get('teams', {}).get('home', {}).get('score') is not None:
                    game_id = data['game_id']
                    save_game_data(game_id, data)
                    print(f"✅ Bolstered: {game_id}")
                    success_count += 1
                else:
                    if data:
                        print(f"⚪ Cancelled/No Data: {data['game_id']}")
                    cancelled_count += 1
        except Exception as e:
            print(f"❌ Batch error: {e}")
            fail_count += len(batch_args)
        
    print(f"\n🏁 Bolster Summary:")
    print(f"   - Successfully Bolstered: {success_count}")
    print(f"   - Confirmed No Data (Rain/Exhibition/Cancel): {cancelled_count}")
    print(f"   - Failed: {fail_count}")

def save_game_data(game_id, data):
    """Save the crawled data to DB. Simplified version for bolster script."""
    from src.models.game import (
        Game, 
        GameInningScore, 
        GameMetadata, 
        GameBattingStat, 
        GamePitchingStat
    )
    
    with SessionLocal() as session:
        try:
            # 1. Update Game basic info
            game = session.query(Game).filter_by(game_id=game_id).first()
            if not game: return
            
            teams = data['teams']
            game.home_score = teams['home']['score']
            game.away_score = teams['away']['score']
            game.stadium = data['metadata'].get('stadium')
            
            # 2. Inning Scores
            session.query(GameInningScore).filter_by(game_id=game.game_id).delete()
            for idx, run_score in enumerate(teams['away']['line_score']):
                session.add(GameInningScore(
                    game_id=game.game_id, 
                    team_code=teams['away']['code'], 
                    team_side='away',
                    inning=idx+1, 
                    runs=run_score
                ))
            for idx, run_score in enumerate(teams['home']['line_score']):
                session.add(GameInningScore(
                    game_id=game.game_id, 
                    team_code=teams['home']['code'], 
                    team_side='home',
                    inning=idx+1, 
                    runs=run_score
                ))
                
            # 3. Metadata
            session.query(GameMetadata).filter_by(game_id=game.game_id).delete()
            meta = data['metadata']
            
            def _parse_time(t_str):
                if not t_str: return None
                try:
                    return datetime.strptime(t_str, "%H:%M").time()
                except ValueError:
                    return None

            session.add(GameMetadata(
                game_id=game.game_id,
                stadium_name=meta.get('stadium'),
                attendance=meta.get('attendance'),
                start_time=_parse_time(meta.get('start_time')),
                end_time=_parse_time(meta.get('end_time')),
                game_time_minutes=meta.get('duration_minutes')
            ))
            
            # 4. Lineups/Stats
            # Preserve existing player IDs before replacing stats.
            existing_batting_exact = {}
            existing_batting_order = {}
            existing_batting_name = {}
            for row in session.query(GameBattingStat).filter_by(game_id=game.game_id).all():
                if not row.player_id:
                    continue
                existing_batting_exact[(row.team_side, row.player_name, row.batting_order, row.appearance_seq)] = row.player_id
                existing_batting_order[(row.team_side, row.player_name, row.batting_order)] = row.player_id
                existing_batting_name[(row.team_side, row.player_name)] = row.player_id

            existing_pitching_exact = {}
            existing_pitching_start = {}
            existing_pitching_name = {}
            for row in session.query(GamePitchingStat).filter_by(game_id=game.game_id).all():
                if not row.player_id:
                    continue
                existing_pitching_exact[(row.team_side, row.player_name, row.is_starting, row.appearance_seq)] = row.player_id
                existing_pitching_start[(row.team_side, row.player_name, row.is_starting)] = row.player_id
                existing_pitching_name[(row.team_side, row.player_name)] = row.player_id

            # Batting
            hitters = data.get('hitters') or {}
            if hitters.get('away') or hitters.get('home'):
                session.query(GameBattingStat).filter_by(game_id=game.game_id).delete()
                for side in ['away', 'home']:
                    for idx, h in enumerate(hitters.get(side, []), start=1):
                        appearance_seq = h.get('appearance_seq') or idx
                        player_id = h.get('player_id')
                        if not player_id:
                            player_id = (
                                existing_batting_exact.get((side, h['player_name'], h.get('batting_order'), appearance_seq))
                                or existing_batting_order.get((side, h['player_name'], h.get('batting_order')))
                                or existing_batting_name.get((side, h['player_name']))
                            )

                        stat = GameBattingStat(
                            game_id=game.game_id,
                            player_id=player_id,
                            player_name=h['player_name'],
                            team_code=h['team_code'],
                            team_side=side,
                            batting_order=h['batting_order'],
                            position=h['position'],
                            is_starter=h['is_starter'],
                            appearance_seq=appearance_seq,
                            **h['stats']
                        )
                        # PA calculation logic
                        if stat.plate_appearances == 0:
                            stat.plate_appearances = (stat.at_bats or 0) + (stat.walks or 0) + (h['stats'].get('intentional_walks') or 0) + (stat.hbp or 0) + (stat.sacrifice_hits or 0) + (stat.sacrifice_flies or 0)
                        session.add(stat)
            else:
                print(f"⚠️ No hitter data for {game_id}; keeping existing batting stats.")

            # Pitching
            pitchers = data.get('pitchers') or {}
            if pitchers.get('away') or pitchers.get('home'):
                session.query(GamePitchingStat).filter_by(game_id=game.game_id).delete()
                for side in ['away', 'home']:
                    for idx, p in enumerate(pitchers.get(side, []), start=1):
                        appearance_seq = p.get('appearance_seq') or idx
                        player_id = p.get('player_id')
                        if not player_id:
                            player_id = (
                                existing_pitching_exact.get((side, p['player_name'], p['is_starting'], appearance_seq))
                                or existing_pitching_start.get((side, p['player_name'], p['is_starting']))
                                or existing_pitching_name.get((side, p['player_name']))
                            )

                        pstat = GamePitchingStat(
                            game_id=game.game_id,
                            player_id=player_id,
                            player_name=p['player_name'],
                            team_code=p['team_code'],
                            team_side=side,
                            is_starting=p['is_starting'],
                            appearance_seq=appearance_seq,
                            **p['stats']
                        )
                        session.add(pstat)
            else:
                print(f"⚠️ No pitcher data for {game_id}; keeping existing pitching stats.")
            
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"⚠️ Error saving {game_id}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2010)
    parser.add_argument("--end", type=int, default=2024)
    args = parser.parse_args()
    
    asyncio.run(bolster_missing_games(args.start, args.end))
