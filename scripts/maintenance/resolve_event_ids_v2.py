from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameBattingStat, GamePitchingStat
from sqlalchemy import func, or_

def resolve_ids_v2():
    with SessionLocal() as session:
        # Get all games with events
        games = session.query(Game.game_id, Game.home_team, Game.away_team, Game.home_pitcher, Game.away_pitcher).join(
            GameEvent, Game.game_id == GameEvent.game_id
        ).distinct().all()
        
        print(f"🔍 Processing {len(games)} games...")
        
        for g_id, home_team, away_team, home_starter, away_starter in games:
            # 1. Map names to IDs for this game
            bat_map = {row.player_name: row.player_id for row in session.query(GameBattingStat).filter_by(game_id=g_id).all()}
            pit_map = {row.player_name: row.player_id for row in session.query(GamePitchingStat).filter_by(game_id=g_id).all()}
            
            # 2. Track Current Pitcher
            # curr_p[side] = (name, id)
            curr_p = {
                'home': (home_starter, pit_map.get(home_starter)),
                'away': (away_starter, pit_map.get(away_starter))
            }
            
            events = session.query(GameEvent).filter_by(game_id=g_id).order_by(GameEvent.event_seq).all()
            
            for ev in events:
                inning_half = ev.inning_half or "top"
                # If it's top of the inning, the Away team is batting, so the Home team is pitching.
                # If it's bottom of the inning, the Home team is batting, so the Away team is pitching.
                pitching_side = 'home' if inning_half == 'top' else 'away'
                
                # Check for pitcher change in description
                # Example: "투수 교체 : 김도현" or "투수 : 김도현 (으)로 교체"
                if "투수" in (ev.description or "") and "교체" in (ev.description or ""):
                    # Try to extract name
                    new_p_name = None
                    # Simple split logic
                    if ":" in ev.description:
                        new_p_name = ev.description.split(":")[-1].strip().split(" ")[0]
                    elif "투수" in ev.description:
                        # "투수 김도현(으)로 교체"
                        parts = ev.description.replace("투수", "").replace("(으)로 교체", "").strip().split(" ")
                        if parts: new_p_name = parts[0]
                    
                    if new_p_name and new_p_name in pit_map:
                        curr_p[pitching_side] = (new_p_name, pit_map[new_p_name])
                
                # Assign Pitcher
                ev.pitcher_name = curr_p[pitching_side][0]
                ev.pitcher_id = curr_p[pitching_side][1]
                
                # Assign Batter ID
                if ev.batter_name and ev.batter_name in bat_map:
                    ev.batter_id = bat_map[ev.batter_name]
                
            session.flush()

        session.commit()
        print("✅ Precise ID resolution complete.")

if __name__ == "__main__":
    resolve_ids_v2()
