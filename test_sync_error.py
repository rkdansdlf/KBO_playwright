
import os
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.db.engine import SessionLocal
from src.models.game import Game
from src.sync.supabase_sync import SupabaseSync

def test_sync():
    supabase_url = os.getenv('SUPABASE_DB_URL')
    with SessionLocal() as session:
        sync = SupabaseSync(supabase_url, session)
        game = session.query(Game).first()
        if not game:
            print("No games found locally")
            return
            
        data = {
            'game_id': game.game_id,
            'game_date': game.game_date,
            'home_team': game.home_team,
            'away_team': game.away_team,
            'stadium': game.stadium,
            'home_score': game.home_score,
            'away_score': game.away_score,
            'winning_team': game.winning_team,
            'winning_score': game.winning_score,
            'season_id': game.season_id,
            'home_pitcher': game.home_pitcher,
            'away_pitcher': game.away_pitcher,
        }
        
        try:
            stmt = pg_insert(Game).values([data])
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'game_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id'],
                set_=update_dict
            )
            sync.supabase_session.execute(stmt)
            sync.supabase_session.commit()
            print("Sync successful")
        except Exception as e:
            print(f"Sync failed: {e}")

if __name__ == "__main__":
    test_sync()
