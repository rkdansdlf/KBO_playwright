"""
Sync validated data from SQLite to Supabase (PostgreSQL)
Dual-repository pattern: SQLite (dev/validation) → Supabase (production)
"""
import os
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.models.team import Franchise, TeamIdentity, Ballpark, HomeBallparkAssignment
from src.models.game import GameSchedule, Game, GameLineup, PlayerGameStats
from src.models.player import Player, PlayerIdentity, PlayerCode, PlayerStint
from src.utils.safe_print import safe_print as print


class SupabaseSync:
    """Sync data from SQLite to Supabase"""

    def __init__(self, supabase_url: str, sqlite_session: Session):
        """
        Initialize Supabase sync

        Args:
            supabase_url: PostgreSQL connection string for Supabase
            sqlite_session: Active SQLite session to read from
        """
        self.sqlite_session = sqlite_session

        # Create Supabase engine
        self.supabase_engine = create_engine(
            supabase_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 10,
                "application_name": "KBO_Crawler_Sync"
            }
        )

        # Create Supabase session
        SupabaseSession = sessionmaker(bind=self.supabase_engine)
        self.supabase_session = SupabaseSession()

    def test_connection(self) -> bool:
        """Test Supabase connection"""
        try:
            result = self.supabase_session.execute(text("SELECT 1"))
            print("✅ Supabase connection successful")
            return True
        except Exception as e:
            print(f"❌ Supabase connection failed: {e}")
            return False

    def sync_franchises(self) -> int:
        """Sync franchises from SQLite to Supabase"""
        franchises = self.sqlite_session.query(Franchise).all()
        synced = 0

        for franchise in franchises:
            data = {
                'key': franchise.key,
                'canonical_name': franchise.canonical_name,
                'first_season': franchise.first_season,
                'last_season': franchise.last_season,
                'status': franchise.status,
                'notes': franchise.notes,
            }

            # PostgreSQL UPSERT
            stmt = pg_insert(Franchise).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['key'],
                set_={
                    'canonical_name': stmt.excluded.canonical_name,
                    'first_season': stmt.excluded.first_season,
                    'last_season': stmt.excluded.last_season,
                    'status': stmt.excluded.status,
                    'notes': stmt.excluded.notes,
                    'updated_at': text('CURRENT_TIMESTAMP')
                }
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} franchises to Supabase")
        return synced

    def sync_team_identities(self) -> int:
        """Sync team identities from SQLite to Supabase"""
        # First, get franchise ID mappings (SQLite ID → Supabase ID)
        franchise_mapping = self._get_franchise_id_mapping()

        identities = self.sqlite_session.query(TeamIdentity).all()
        synced = 0

        for identity in identities:
            # Map SQLite franchise_id to Supabase franchise_id
            supabase_franchise_id = franchise_mapping.get(identity.franchise_id)
            if not supabase_franchise_id:
                print(f"⚠️  Skipping identity {identity.name_kor}: franchise_id {identity.franchise_id} not found in Supabase")
                continue

            data = {
                'franchise_id': supabase_franchise_id,
                'name_kor': identity.name_kor,
                'name_eng': identity.name_eng,
                'short_code': identity.short_code,
                'city_kor': identity.city_kor,
                'city_eng': identity.city_eng,
                'start_season': identity.start_season,
                'end_season': identity.end_season,
                'is_current': identity.is_current,
                'notes': identity.notes,
            }

            # PostgreSQL UPSERT
            stmt = pg_insert(TeamIdentity).values(**data)
            update_dict = {k: v for k, v in data.items() if k not in ['franchise_id', 'name_kor', 'start_season']}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['franchise_id', 'name_kor', 'start_season'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} team identities to Supabase")
        return synced

    def sync_ballparks(self) -> int:
        """Sync ballparks from SQLite to Supabase"""
        ballparks = self.sqlite_session.query(Ballpark).all()
        synced = 0

        for ballpark in ballparks:
            data = {
                'name_kor': ballpark.name_kor,
                'name_eng': ballpark.name_eng,
                'city_kor': ballpark.city_kor,
                'city_eng': ballpark.city_eng,
                'opened_year': ballpark.opened_year,
                'closed_year': ballpark.closed_year,
                'capacity': ballpark.capacity,
                'notes': ballpark.notes,
            }

            stmt = pg_insert(Ballpark).values(**data)
            update_dict = {k: v for k, v in data.items() if k != 'name_kor'}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['name_kor'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} ballparks to Supabase")
        return synced

    def sync_ballpark_assignments(self) -> int:
        """Sync ballpark assignments from SQLite to Supabase"""
        franchise_mapping = self._get_franchise_id_mapping()
        ballpark_mapping = self._get_ballpark_id_mapping()

        assignments = self.sqlite_session.query(HomeBallparkAssignment).all()
        synced = 0

        for assignment in assignments:
            supabase_franchise_id = franchise_mapping.get(assignment.franchise_id)
            supabase_ballpark_id = ballpark_mapping.get(assignment.ballpark_id)

            if not supabase_franchise_id or not supabase_ballpark_id:
                print(f"⚠️  Skipping assignment: franchise or ballpark not found")
                continue

            data = {
                'franchise_id': supabase_franchise_id,
                'ballpark_id': supabase_ballpark_id,
                'start_season': assignment.start_season,
                'end_season': assignment.end_season,
                'is_primary': assignment.is_primary,
                'notes': assignment.notes,
            }

            stmt = pg_insert(HomeBallparkAssignment).values(**data)
            update_dict = {k: v for k, v in data.items() if k not in ['franchise_id', 'ballpark_id', 'start_season']}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['franchise_id', 'ballpark_id', 'start_season'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} ballpark assignments to Supabase")
        return synced

    def sync_game_schedules(self, limit: int = None) -> int:
        """Sync game schedules from SQLite to Supabase"""
        query = self.sqlite_session.query(GameSchedule)
        if limit:
            query = query.limit(limit)

        schedules = query.all()
        synced = 0

        for schedule in schedules:
            data = {
                'game_id': schedule.game_id,
                'season_year': schedule.season_year,
                'season_type': schedule.season_type,
                'game_date': schedule.game_date,
                'game_time': schedule.game_time,
                'home_team_code': schedule.home_team_code,
                'away_team_code': schedule.away_team_code,
                'stadium': schedule.stadium,
                'game_status': schedule.game_status,
                'postpone_reason': schedule.postpone_reason,
                'doubleheader_no': schedule.doubleheader_no,
                'series_id': schedule.series_id,
                'series_name': schedule.series_name,
                'crawl_status': schedule.crawl_status,
                'crawl_attempts': schedule.crawl_attempts,
                'last_crawl_at': schedule.last_crawl_at,
                'crawl_error': schedule.crawl_error,
            }

            stmt = pg_insert(GameSchedule).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id'],
                set_={k: stmt.excluded[k] for k in data.keys() if k != 'game_id'}
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} game schedules to Supabase")
        return synced

    def _get_franchise_id_mapping(self) -> Dict[int, int]:
        """Get SQLite franchise_id → Supabase franchise_id mapping"""
        mapping = {}

        # Get all franchises from SQLite
        sqlite_franchises = self.sqlite_session.query(Franchise).all()

        for sf in sqlite_franchises:
            # Find corresponding Supabase franchise by key
            supabase_franchise = self.supabase_session.query(Franchise).filter_by(key=sf.key).first()
            if supabase_franchise:
                mapping[sf.id] = supabase_franchise.id

        return mapping

    def _get_ballpark_id_mapping(self) -> Dict[int, int]:
        """Get SQLite ballpark_id → Supabase ballpark_id mapping"""
        mapping = {}

        sqlite_ballparks = self.sqlite_session.query(Ballpark).all()

        for sb in sqlite_ballparks:
            supabase_ballpark = self.supabase_session.query(Ballpark).filter_by(name_kor=sb.name_kor).first()
            if supabase_ballpark:
                mapping[sb.id] = supabase_ballpark.id

        return mapping

    def sync_players(self) -> int:
        """Sync players from SQLite to Supabase"""
        players = self.sqlite_session.query(Player).all()
        synced = 0

        for player in players:
            data = {
                'kbo_person_id': player.kbo_person_id,
                'birth_date': player.birth_date,
                'birth_place': player.birth_place,
                'height_cm': player.height_cm,
                'weight_kg': player.weight_kg,
                'bats': player.bats,
                'throws': player.throws,
                'is_foreign_player': player.is_foreign_player,
                'debut_year': player.debut_year,
                'retire_year': player.retire_year,
                'status': player.status,
                'notes': player.notes,
            }

            stmt = pg_insert(Player).values(**data)
            update_dict = {k: v for k, v in data.items() if k != 'kbo_person_id'}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['kbo_person_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} players to Supabase")
        return synced

    def sync_player_identities(self) -> int:
        """Sync player identities from SQLite to Supabase"""
        player_mapping = self._get_player_id_mapping()
        identities = self.sqlite_session.query(PlayerIdentity).all()
        synced = 0

        for identity in identities:
            supabase_player_id = player_mapping.get(identity.player_id)
            if not supabase_player_id:
                continue

            data = {
                'player_id': supabase_player_id,
                'name_kor': identity.name_kor,
                'name_eng': identity.name_eng,
                'start_date': identity.start_date,
                'end_date': identity.end_date,
                'is_primary': identity.is_primary,
                'notes': identity.notes,
            }

            stmt = pg_insert(PlayerIdentity).values(**data)
            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player identities to Supabase")
        return synced

    def sync_player_codes(self) -> int:
        """Sync player codes from SQLite to Supabase"""
        player_mapping = self._get_player_id_mapping()
        codes = self.sqlite_session.query(PlayerCode).all()
        synced = 0

        for code in codes:
            supabase_player_id = player_mapping.get(code.player_id)
            if not supabase_player_id:
                continue

            data = {
                'player_id': supabase_player_id,
                'source': code.source,
                'code': code.code,
            }

            stmt = pg_insert(PlayerCode).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id', 'source'],
                set_={'code': stmt.excluded.code, 'updated_at': text('CURRENT_TIMESTAMP')}
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player codes to Supabase")
        return synced

    def _get_player_id_mapping(self) -> Dict[int, int]:
        """Get SQLite player_id → Supabase player_id mapping"""
        mapping = {}
        sqlite_players = self.sqlite_session.query(Player).all()

        for sp in sqlite_players:
            if sp.kbo_person_id:
                supabase_player = self.supabase_session.query(Player).filter_by(
                    kbo_person_id=sp.kbo_person_id
                ).first()
                if supabase_player:
                    mapping[sp.id] = supabase_player.id

        return mapping

    def sync_all_team_data(self) -> Dict[str, int]:
        """Sync all team-related data"""
        results = {
            'franchises': self.sync_franchises(),
            'team_identities': self.sync_team_identities(),
            'ballparks': self.sync_ballparks(),
            'ballpark_assignments': self.sync_ballpark_assignments(),
        }
        return results

    def sync_all_player_data(self) -> Dict[str, int]:
        """Sync all player-related data"""
        results = {
            'players': self.sync_players(),
            'player_identities': self.sync_player_identities(),
            'player_codes': self.sync_player_codes(),
        }
        return results

    def close(self):
        """Close Supabase session"""
        self.supabase_session.close()
        self.supabase_engine.dispose()


def main():
    """Test sync functionality"""
    from src.db.engine import SessionLocal

    # Get Supabase URL from environment
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if not supabase_url:
        print("❌ SUPABASE_DB_URL environment variable not set")
        print("   Set it in .env file or export it:")
        print("   export SUPABASE_DB_URL='postgresql://postgres.xxx:password@xxx.pooler.supabase.com:5432/postgres'")
        return

    print("\n" + "🔄" * 30)
    print("Supabase Sync Test")
    print("🔄" * 30 + "\n")

    with SessionLocal() as sqlite_session:
        try:
            sync = SupabaseSync(supabase_url, sqlite_session)

            # Test connection
            if not sync.test_connection():
                return

            # Sync team data
            print("\n📦 Syncing team data...")
            team_results = sync.sync_all_team_data()

            # Sync player data
            print("\n👥 Syncing player data...")
            player_results = sync.sync_all_player_data()

            print("\n" + "=" * 50)
            print("📈 Sync Summary")
            print("=" * 50)
            print("\nTeam Data:")
            for table, count in team_results.items():
                print(f"  {table}: {count} records")
            print("\nPlayer Data:")
            for table, count in player_results.items():
                print(f"  {table}: {count} records")

            sync.close()

        except Exception as e:
            print(f"\n❌ Sync error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
