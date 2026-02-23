from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import sys

# Add src to path
sys.path.append(os.getcwd())

from src.sync.supabase_sync import SupabaseSync
from src.models.game import GameMetadata, GameInningScore

def isolate_sync():
    load_dotenv()
    target_url = os.getenv('TARGET_DATABASE_URL') or 'postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend'
    sqlite_url = 'sqlite:///./data/kbo_dev.db'
    
    source_engine = create_engine(sqlite_url)
    SessionSource = sessionmaker(bind=source_engine)
    
    with SessionSource() as session:
        syncer = SupabaseSync(target_url, session)
        
        # Test GameMetadata with a limit that covers many records but manageable
        print("\n2. Syncing GameMetadata (limit=10000)...")
        try:
            total_metadata = session.query(GameMetadata).count()
            print(f"Total metadata records available: {total_metadata}")
            res = syncer._sync_simple_table(GameMetadata, ['game_id'], exclude_cols=['created_at'], limit=10000)
            print(f"✅ Metadata sync result: {res}")
        except Exception as e:
            print(f"❌ Metadata sync failed: {e}")
            import traceback
            traceback.print_exc()

        # Test GameInningScore
        print("\n3. Syncing GameInningScore (limit=10000)...")
        try:
            total_scores = session.query(GameInningScore).count()
            print(f"Total score records available: {total_scores}")
            res = syncer._sync_simple_table(GameInningScore, ['game_id', 'team_side', 'inning'], exclude_cols=['id', 'created_at'], limit=10000)
            print(f"✅ Inning score sync result: {res}")
        except Exception as e:
            print(f"❌ Inning score sync failed: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    isolate_sync()
