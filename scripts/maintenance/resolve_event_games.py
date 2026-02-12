
import sys
import os
import argparse
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import Engine
from src.services.player_id_resolver import PlayerIdResolver

def resolve_event_games(dry_run=False):
    engine = Engine
    Session = sessionmaker(bind=engine)
    session = Session()
    resolver = PlayerIdResolver(session)

    # Teams to target: Real Event Teams + KH (which was unmapped)
    TARGET_TEAMS = ['EA', 'WE', 'KR', 'JP', 'TW', 'KH', 'NL', 'DL', 'DRE', 'NAN']
    teams_str = "', '".join(TARGET_TEAMS)
    
    print(f"🎯 Targeting teams: {TARGET_TEAMS}")

    updated_batting = 0
    updated_pitching = 0

    try:
        # 1. Batting Stats
        print("\n🏏 Checking Game Batting Stats...")
        stmt = text(f"""
            SELECT id, game_id, player_name, team_code, uniform_no
            FROM game_batting_stats
            WHERE player_id IS NULL
              AND team_code IN ('{teams_str}')
        """)
        
        rows = session.execute(stmt).fetchall()
        print(f"Found {len(rows)} unresolved batting records.")

        for row in rows:
            # Extract season from game_id (e.g. 20220716...) -> 2022
            try:
                season = int(row.game_id[:4])
            except:
                print(f"⚠️  Invalid game_id: {row.game_id}")
                continue
                
            pid = resolver.resolve_id(row.player_name, row.team_code, season, row.uniform_no)
            
            if pid:
                if not dry_run:
                    session.execute(
                        text("UPDATE game_batting_stats SET player_id = :pid WHERE id = :id"),
                        {'pid': pid, 'id': row.id}
                    )
                updated_batting += 1
                print(f"✅ Resolved Batting: {row.player_name} ({row.team_code}, {season}) -> {pid}")
            else:
                print(f"❌ Failed Batting: {row.player_name} ({row.team_code}, {season})")

        # 2. Pitching Stats
        print("\n⚾ Checking Game Pitching Stats...")
        stmt = text(f"""
            SELECT id, game_id, player_name, team_code, uniform_no
            FROM game_pitching_stats
            WHERE player_id IS NULL
              AND team_code IN ('{teams_str}')
        """)
        
        rows = session.execute(stmt).fetchall()
        print(f"Found {len(rows)} unresolved pitching records.")

        for row in rows:
            try:
                season = int(row.game_id[:4])
            except:
                continue

            pid = resolver.resolve_id(row.player_name, row.team_code, season, row.uniform_no)
            
            if pid:
                if not dry_run:
                    session.execute(
                        text("UPDATE game_pitching_stats SET player_id = :pid WHERE id = :id"),
                        {'pid': pid, 'id': row.id}
                    )
                updated_pitching += 1
                print(f"✅ Resolved Pitching: {row.player_name} ({row.team_code}, {season}) -> {pid}")
            else:
                print(f"❌ Failed Pitching: {row.player_name} ({row.team_code}, {season})")

        if not dry_run:
            session.commit()
            print("\n💾 Changes committed to database.")
        else:
            session.rollback()
            print("\n🛑 Dry run - no changes committed.")

        print(f"\nSummary:")
        print(f"Batting Updated: {updated_batting}")
        print(f"Pitching Updated: {updated_pitching}")

    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Do not commit changes")
    args = parser.parse_args()
    
    resolve_event_games(dry_run=args.dry_run)
