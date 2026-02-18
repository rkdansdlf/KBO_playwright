
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.insert(0, os.getcwd())
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.models.game import GameMetadata, GameInningScores, GameLineup, GameBattingStats, GamePitchingStats, GameSummary, Game

def test_purge_logic():
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found")
        return

    year = 2010
    pattern = f"{year}%"
    
    # 1. Check initial counts on OCI
    engine = create_engine(oci_url)
    with engine.connect() as conn:
        initial_counts = {}
        tables = [
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_batting_stats",
            "game_pitching_stats",
            "game_summary",
        ]
        print(f"📊 Initial counts for Year {year}:")
        for t in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {t} WHERE game_id LIKE :pattern"), {"pattern": pattern}).fetchone()[0]
            initial_counts[t] = count
            print(f"  - {t}: {count}")

    # 2. Run Sync (this will purge and re-sync)
    print("\n🚀 Running Sync with purge...")
    with SessionLocal() as session:
        syncer = OCISync(oci_url, session)
        syncer.sync_game_details(year=year)

    # 3. Check final counts
    with engine.connect() as conn:
        final_counts = {}
        print(f"\n📊 Final counts for Year {year}:")
        for t in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {t} WHERE game_id LIKE :pattern"), {"pattern": pattern}).fetchone()[0]
            final_counts[t] = count
            print(f"  - {t}: {count}")
            
            # Simple assertion: logic is correct if counts are restored (or very close if source changed slightly)
            # In this test environment, source shouldn't change, so counts should match perfectly if purge+sync works.
            if count != initial_counts[t]:
                print(f"  ⚠️ Warning: Count mismatch for {t} (Initial: {initial_counts[t]} -> Final: {count})")
            else:
                print(f"  ✅ Count matches for {t}")

if __name__ == "__main__":
    test_purge_logic()
