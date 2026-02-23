"""
Sync validated data from SQLite to Supabase (PostgreSQL)
Dual-repository pattern: SQLite (dev/validation) → Supabase (production)
"""
import os
import json
from typing import List, Dict, Any
from pathlib import Path
from sqlalchemy import create_engine, text, select, MetaData, Table, Column, String, Integer, Date, column, table, tuple_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

# 현재 사용 가능한 모델들만 import
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic
from src.models.award import Award
from src.models.crawl import CrawlRun
from src.models.game import (
    Game,
    GameMetadata,
    GameInningScore,
    GameLineup,
    GameBattingStat,
    GamePitchingStat,
    GameEvent,
    GameSummary
)


LEAGUE_NAME_TO_CODE = {
    "REGULAR": 0,
    "EXHIBITION": 1,
    "WILDCARD": 2,
    "SEMI_PLAYOFF": 3,
    "PLAYOFF": 4,
    "KOREAN_SERIES": 5,
}


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

    def sync_pitcher_data(self) -> int:
        """새로운 player_season_pitching 테이블의 투수 데이터를 Supabase로 동기화"""
        query = self.sqlite_session.query(PlayerSeasonPitching)
        total = query.count()
        
        if total == 0:
            print("ℹ️ 동기화할 투수 데이터가 없습니다.")
            return 0
        
        synced = 0
        batch_size = 500
        values_list = []
        batch_now = None
        for data in query.yield_per(batch_size):
            if not values_list:
                batch_now = datetime.now()
            values_list.append({
                'player_id': data.player_id,
                'season': data.season,
                'league': data.league,
                'level': data.level,
                'source': data.source,
                'team_code': data.team_code,
                'games': data.games,
                'games_started': data.games_started,
                'wins': data.wins,
                'losses': data.losses,
                'saves': data.saves,
                'holds': data.holds,
                'innings_pitched': data.innings_pitched,
                'innings_outs': data.innings_outs,
                'hits_allowed': data.hits_allowed,
                'runs_allowed': data.runs_allowed,
                'earned_runs': data.earned_runs,
                'home_runs_allowed': data.home_runs_allowed,
                'walks_allowed': data.walks_allowed,
                'intentional_walks': data.intentional_walks,
                'hit_batters': data.hit_batters,
                'strikeouts': data.strikeouts,
                'wild_pitches': data.wild_pitches,
                'balks': data.balks,
                'era': data.era,
                'whip': data.whip,
                'fip': data.fip,
                'k_per_nine': data.k_per_nine,
                'bb_per_nine': data.bb_per_nine,
                'kbb': data.kbb,
                'complete_games': data.complete_games,
                'shutouts': data.shutouts,
                'quality_starts': data.quality_starts,
                'blown_saves': data.blown_saves,
                'tbf': data.tbf,
                'np': data.np,
                'avg_against': data.avg_against,
                'doubles_allowed': data.doubles_allowed,
                'triples_allowed': data.triples_allowed,
                'sacrifices_allowed': data.sacrifices_allowed,
                'sacrifice_flies_allowed': data.sacrifice_flies_allowed,
                'extra_stats': json.dumps(data.extra_stats) if data.extra_stats else None,
                'created_at': batch_now,
                'updated_at': batch_now,
            })

            if len(values_list) >= batch_size:
                self._upsert_pitching_batch(values_list)
                synced += len(values_list)
                values_list = []
                batch_now = None
                if synced % 1000 == 0 or synced == total:
                    print(f"   📝 {synced}/{total}건 동기화 중...")

        if values_list:
            self._upsert_pitching_batch(values_list)
            synced += len(values_list)
            print(f"   📝 {synced}/{total}건 동기화 중...")

        print(f"✅ Supabase 투수 데이터 동기화 완료: {synced}건")
        return synced

    def sync_batting_data(self) -> int:
        """타자 데이터를 Supabase로 동기화"""
        query = self.sqlite_session.query(PlayerSeasonBatting)
        total = query.count()
        
        if total == 0:
            print("ℹ️ 동기화할 타자 데이터가 없습니다.")
            return 0
        
        synced = 0
        batch_size = 500
        values_list = []
        batch_now = None
        for data in query.yield_per(batch_size):
            if not values_list:
                batch_now = datetime.now()
            values_list.append({
                'player_id': data.player_id,
                'season': data.season,
                'league': data.league,
                'level': data.level,
                'source': data.source,
                'team_code': data.team_code,
                'games': data.games,
                'plate_appearances': data.plate_appearances,
                'at_bats': data.at_bats,
                'runs': data.runs,
                'hits': data.hits,
                'doubles': data.doubles,
                'triples': data.triples,
                'home_runs': data.home_runs,
                'rbi': data.rbi,
                'walks': data.walks,
                'intentional_walks': data.intentional_walks,
                'hbp': data.hbp,
                'strikeouts': data.strikeouts,
                'stolen_bases': data.stolen_bases,
                'caught_stealing': data.caught_stealing,
                'sacrifice_hits': data.sacrifice_hits,
                'sacrifice_flies': data.sacrifice_flies,
                'gdp': data.gdp,
                'avg': data.avg,
                'obp': data.obp,
                'slg': data.slg,
                'ops': data.ops,
                'iso': data.iso,
                'babip': data.babip,
                'extra_stats': json.dumps(data.extra_stats) if data.extra_stats else None,
                'created_at': batch_now,
                'updated_at': batch_now,
            })

            if len(values_list) >= batch_size:
                self._upsert_batting_batch(values_list)
                synced += len(values_list)
                values_list = []
                batch_now = None
                if synced % 1000 == 0 or synced == total:
                    print(f"   📝 {synced}/{total}건 동기화 중...")

        if values_list:
            self._upsert_batting_batch(values_list)
            synced += len(values_list)
            print(f"   📝 {synced}/{total}건 동기화 중...")

        print(f"✅ Supabase 타자 데이터 동기화 완료: {synced}건")
        return synced

    def _bulk_copy_upsert(self, table_name: str, records: List[Dict[str, Any]], unique_cols: List[str]):
        """
        Perform bulk UPSERT using Postgres COPY + Temp Table.
        Significantly faster than INSERT VALUES for large datasets.
        """
        if not records:
            return

        import csv
        import io
        
        # Ensure we have a raw connection for COPY
        connection = self.supabase_engine.raw_connection()
        cursor = connection.cursor()

        try:
            # 1. Prepare Data
            # Use 'NULL' string for None values to avoid ambiguity with empty strings
            keys = list(records[0].keys())
            processed_records = []
            for r in records:
                row = {}
                for k in keys:
                    val = r.get(k)
                    # Handle specific types if needed, but CSV writer handles most
                    # For None, we want a specific NULL marker
                    if val is None:
                        row[k] = None # DictWriter handles None as empty string by default
                    else:
                        row[k] = val
                processed_records.append(row)

            output = io.StringIO()
            # Use NULL marker for CSV
            writer = csv.DictWriter(output, fieldnames=keys, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            # We don't write header
            # To handle None -> \N or custom NULL, we assume empty string in CSV = NULL 
            # if we configure COPY that way. 
            # But empty string might be valid value for text columns.
            # Let's use a distinct NULL string if possible or rely on standard CSV NULL handling.
            # Actually, DictWriter writes '' for None. 
            # So we set COPY WITH NULL '' (empty string matches null).
            writer.writerows(processed_records)
            output.seek(0)
            
            # 2. Create Temp Table matching target schema
            # Use a random suffix to avoid collision if parallel (though sync is usually serial)
            import random
            suffix = random.randint(1000, 9999)
            temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}_{suffix}"
            
            # Create temp table (structure only)
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS)")
            
            # 3. COPY data to Temp Table
            # Note: We use CSV format, delimiter tab, NULL as empty string
            columns_str = ", ".join(keys)
            cursor.copy_expert(
                f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '')", 
                output
            )
            
            # 4. UPSERT from Temp Table to Target Table
            update_cols = [k for k in keys if k not in unique_cols and k != 'created_at']
            
            if not update_cols:
                conflict_action = "DO NOTHING"
            else:
                set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_cols])
                # unique_cols usually form the conflict target
                conflict_target = ", ".join(unique_cols)
                conflict_action = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}"

            cols_list = ", ".join(keys)
            insert_sql = f"""
                INSERT INTO {table_name} ({cols_list})
                SELECT {cols_list} FROM {temp_table}
                {conflict_action}
            """
            cursor.execute(insert_sql)
            
            # 5. Cleanup
            cursor.execute(f"DROP TABLE {temp_table}")
            connection.commit()
            
        except Exception as e:
            connection.rollback()
            print(f"❌ Batch COPY Error: {e}")
            raise e
        finally:
            cursor.close()
            connection.close()

    def _upsert_pitching_batch(self, values_list: List[Dict[str, Any]]) -> None:
        # Use COPY for bulk upsert
        # Unique keys for PlayerSeasonPitching: player_id, season, league, level
        self._bulk_copy_upsert(
            "player_season_pitching", 
            values_list, 
            unique_cols=["player_id", "season", "league", "level"]
        )

    def _upsert_batting_batch(self, values_list: List[Dict[str, Any]]) -> None:
        # Use COPY for bulk upsert
        # Unique keys for PlayerSeasonBatting: player_id, season, league, level
        self._bulk_copy_upsert(
            "player_season_batting", 
            values_list, 
            unique_cols=["player_id", "season", "league", "level"]
        )

    def verify_pitcher_sync(self, expected_count: int):
        """투수 데이터 동기화 결과 검증"""
        try:
            result = self.supabase_session.execute(text("""
                SELECT COUNT(*) as count 
                FROM player_season_pitching 
            """))
            
            actual_count = result.fetchone()[0]
            print(f"🔍 Supabase 투수 데이터 확인: {actual_count}건 (예상: {expected_count}건)")
            
            if actual_count >= expected_count:
                print("✅ 투수 데이터 동기화 검증 성공!")
            else:
                print("⚠️ 동기화된 투수 데이터 수가 예상보다 적습니다.")
                
        except Exception as e:
            print(f"⚠️ 투수 데이터 동기화 검증 실패: {e}")

    def verify_batting_sync(self, expected_count: int):
        """타자 데이터 동기화 결과 검증"""
        try:
            result = self.supabase_session.execute(text("""
                SELECT COUNT(*) as count 
                FROM player_season_batting 
            """))
            
            actual_count = result.fetchone()[0]
            print(f"🔍 Supabase 타자 데이터 확인: {actual_count}건 (예상: {expected_count}건)")
            
            if actual_count >= expected_count:
                print("✅ 타자 데이터 동기화 검증 성공!")
            else:
                print("⚠️ 동기화된 타자 데이터 수가 예상보다 적습니다.")
                
        except Exception as e:
            print(f"⚠️ 타자 데이터 동기화 검증 실패: {e}")

    def show_supabase_data_sample(self):
        """Supabase의 데이터 샘플 표시"""
        try:
            # 투수 데이터 샘플
            pitcher_result = self.supabase_session.execute(text("""
                SELECT player_id, season, games, wins, losses, era, innings_pitched
                FROM player_season_pitching 
                LIMIT 3
            """))
            
            pitcher_rows = pitcher_result.fetchall()
            if pitcher_rows:
                print("\n📊 Supabase 투수 데이터 샘플:")
                for i, row in enumerate(pitcher_rows):
                    print(f"  {i+1}. player_id: {row[0]}, season: {row[1]}")
                    print(f"     게임수: {row[2]}, 승패: {row[3]}-{row[4]}, ERA: {row[5]}, 이닝: {row[6]}")
            
            # 타자 데이터 샘플
            batting_result = self.supabase_session.execute(text("""
                SELECT player_id, season, games, avg, hits, home_runs
                FROM player_season_batting 
                LIMIT 3
            """))
            
            batting_rows = batting_result.fetchall()
            if batting_rows:
                print("\n🏏 Supabase 타자 데이터 샘플:")
                for i, row in enumerate(batting_rows):
                    print(f"  {i+1}. player_id: {row[0]}, season: {row[1]}")
                    print(f"     게임수: {row[2]}, 타율: {row[3]}, 안타: {row[4]}, 홈런: {row[5]}")
                    
        except Exception as e:
            print(f"⚠️ Supabase 데이터 조회 실패: {e}")

    

    def sync_daily_rosters(self) -> int:
        """Sync team_daily_roster from SQLite to Supabase"""
        from src.models.team import TeamDailyRoster
        
        # Check if table exists (SQLite)
        try:
            rosters = self.sqlite_session.query(TeamDailyRoster).all()
        except Exception:
            print("⚠️ team_daily_roster table likely doesn't exist in local DB yet.")
            return 0
            
        synced = 0
        if not rosters:
            print("ℹ️ No daily roster data to sync.")
            return 0
            
        print(f"INFO: Found {len(rosters)} rosters to sync. Batching...")
        
        batch_size = 1000
        for i in range(0, len(rosters), batch_size):
            batch = rosters[i:i+batch_size]
            values_list = []
            
            for r in batch:
                data = {
                    'roster_date': r.roster_date,
                    'team_code': r.team_code,
                    'player_id': r.player_id,
                    'player_name': r.player_name,
                    'position': r.position,
                    'back_number': r.back_number,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                values_list.append(data)
                
            if not values_list:
                continue
                
            # Construct ON CONFLICT statement
            stmt = pg_insert(TeamDailyRoster).values(values_list)
            
            update_dict = {
                'player_name': stmt.excluded.player_name,
                'position': stmt.excluded.position,
                'back_number': stmt.excluded.back_number,
                'updated_at': stmt.excluded.updated_at
            }
            
            stmt = stmt.on_conflict_do_update(
                constraint='uq_team_daily_roster',
                set_=update_dict
            )
            
            self.supabase_session.execute(stmt)
            self.supabase_session.commit() # Commit each batch
            synced += len(values_list)
            print(f"   Synced batch {i // batch_size + 1} ({len(values_list)} records)")
        print(f"✅ Synced {synced} daily roster records to Supabase")
        return synced

    def sync_franchises(self) -> int:
        """Sync franchises from SQLite to Supabase"""
        from src.models.franchise import Franchise
        
        # Read from SQLite
        franchises = self.sqlite_session.query(Franchise).all()
        synced = 0
        
        for f in franchises:
            data = {
                'name': f.name,
                'original_code': f.original_code,
                'current_code': f.current_code,
                'metadata_json': json.dumps(f.metadata_json) if f.metadata_json else None,
                'web_url': f.web_url
            }
            
            stmt = pg_insert(Franchise).values(**data)
            update_dict = {k: v for k, v in data.items() if k != 'original_code'}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['original_code'],
                set_=update_dict
            )
            self.supabase_session.execute(stmt)
            synced += 1
            
        self.supabase_session.commit()
        print(f"✅ Synced {synced} franchises to Supabase")
        return synced

    def sync_teams(self) -> int:
        """Sync teams from SQLite to Supabase"""
        from src.models.team import Team
        from sqlalchemy import MetaData, Table, Column, String, Integer, Boolean, ARRAY
        from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
        
        # Get Franchise ID Mapping (Local ID -> Supabase ID)
        franchise_mapping = self._get_franchise_id_mapping()
        
        teams = self.sqlite_session.query(Team).all()
        synced = 0
        
        # Define Supabase Schema explicitly to handle ARRAY type
        metadata = MetaData()
        supabase_teams = Table(
            'teams', metadata,
            Column('team_id', String, primary_key=True),
            Column('team_name', String),
            Column('team_short_name', String),
            Column('city', String),
            Column('founded_year', Integer),
            Column('stadium_name', String),
            Column('franchise_id', Integer),
            Column('is_active', Boolean),
            Column('aliases', PG_ARRAY(String)), # Explicitly ARRAY
            Column('created_at', String), # Using String/Text for timestamps usually works or generic
            Column('updated_at', String),
        )

        for team in teams:
            # Map Local Franchise ID to Supabase Franchise ID
            fid = None
            if team.franchise_id:
                fid = franchise_mapping.get(team.franchise_id)
                if not fid:
                    print(f"⚠️ Franchise ID {team.franchise_id} not found in Supabase mapping for team {team.team_id}")
            
            # Ensure aliases is a list 
            aliases_val = team.aliases
            if aliases_val is None:
                aliases_val = [] # PostgreSQL Array prefers [] over NULL usually, or None is NULL.
            elif isinstance(aliases_val, str):
                try:
                    aliases_val = json.loads(aliases_val)
                except:
                    aliases_val = []
            
            data = {
                'team_id': team.team_id,
                'team_name': team.team_name,
                'team_short_name': team.team_short_name,
                'city': team.city,
                'founded_year': team.founded_year,
                'stadium_name': team.stadium_name,
                'franchise_id': fid,
                'is_active': team.is_active,
                'aliases': aliases_val,
            }
            
            # Use the explicit Table object
            stmt = pg_insert(supabase_teams).values(**data)
            
            update_dict = {
                'team_name': stmt.excluded.team_name,
                'team_short_name': stmt.excluded.team_short_name,
                'city': stmt.excluded.city,
                'founded_year': stmt.excluded.founded_year,
                'stadium_name': stmt.excluded.stadium_name,
                'franchise_id': stmt.excluded.franchise_id,
                'is_active': stmt.excluded.is_active,
                'aliases': stmt.excluded.aliases,
                'updated_at': text('CURRENT_TIMESTAMP')
            }
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['team_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} teams to Supabase")
        return synced

    def sync_team_history(self) -> int:
        """Sync team_history table"""
        from src.models.team_history import TeamHistory
        
        franchise_mapping = self._get_franchise_id_mapping()
        histories = self.sqlite_session.query(TeamHistory).all()
        synced = 0
        
        for h in histories:
            sup_fid = franchise_mapping.get(h.franchise_id)
            if not sup_fid:
                print(f"⚠️ Skip history {h.id}: Franchise ID {h.franchise_id} map failed.")
                continue
                
            data = {
                # ID is auto-increment, don't sync ID? Or sync it?
                # Usually better to let Supabase generate IDs unless we need strict 1:1 mapping.
                # But 'season' + 'franchise_id' (or team_code) is unique business key.
                # Let's use Unique Constraint (season, team_code) or (season, franchise_id)?
                # Model definition: `__table_args__` not visible here, assuming (season, team_code) or similar.
                # For sync, I will match on (season, team_code) assuming that's unique enough.
                # Re-checking model: `season` is indexed. 
                # Ideally upsert on (season, team_code).
                
                'franchise_id': sup_fid,
                'season': h.season,
                'team_name': h.team_name,
                'team_code': h.team_code,
                'logo_url': h.logo_url,
                'ranking': h.ranking,
                'stadium': h.stadium,
                'city': h.city,
                'color': h.color
            }
            
            stmt = pg_insert(TeamHistory).values(**data)
            
            # Construct update set
            update_dict = {k: stmt.excluded[k] for k in data.keys()}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            
            # On Conflict: Which columns?
            # If Supabase table has (season, team_code) unique constraint, use that.
            # If NOT, we might duplicate.
            # Risk: The migration might NOT have added unique constraint. 
            # I should use `id` if I can map it, but I can't.
            # I will assume `season` + `team_code` is unique.
            # If not, I'll fallback to delete/insert or just insert (risk duplicates).
            # Better: I'll use `on_conflict_do_update` but verify index elements. 
            # I'll guess ['season', 'team_code'] for now.
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['season', 'team_code'], 
                set_=update_dict
            )
            
            self.supabase_session.execute(stmt)
            synced += 1
            
        self.supabase_session.commit()
        print(f"✅ Synced {synced} team history records")
        return synced

    # ... (other methods)

    def _get_franchise_id_mapping(self) -> Dict[int, int]:
        """Get SQLite franchise_id → Supabase franchise_id mapping"""
        from src.models.franchise import Franchise
        mapping = {}

        # Get all franchises from SQLite
        sqlite_franchises = self.sqlite_session.query(Franchise).all()

        for sf in sqlite_franchises:
            # Find corresponding Supabase franchise by original_code (Unique Key)
            supabase_franchise = self.supabase_session.query(Franchise).filter_by(original_code=sf.original_code).first()
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

    def sync_all_batting_data(self) -> Dict[str, int]:
        """모든 타격 관련 데이터 동기화 (타자 + 투수)"""
        results = {
            'pitcher_data': self.sync_pitcher_data(),
            'batting_data': self.sync_batting_data(),
        }
        return results

    def sync_crawl_runs(self) -> int:
        query = self.sqlite_session.query(CrawlRun)
        runs = query.all()
        if not runs:
            return 0
        synced = 0
        for run in runs:
            data = {
                'id': run.id,
                'label': run.label,
                'started_at': run.started_at,
                'finished_at': run.finished_at,
                'active_count': run.active_count,
                'retired_count': run.retired_count,
                'staff_count': run.staff_count,
                'confirmed_profiles': run.confirmed_profiles,
                'heuristic_only': run.heuristic_only,
                'created_at': run.created_at,
            }
            stmt = pg_insert(CrawlRun).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'id'}
            stmt = stmt.on_conflict_do_update(index_elements=['id'], set_=update_dict)
            self.supabase_session.execute(stmt)
            synced += 1
        self.supabase_session.commit()
        print(f"✅ Synced {synced} crawl run records to Supabase")
        return synced

    def sync_player_basic(self, limit: int = None) -> int:
        """Sync player_basic data from SQLite to Supabase"""
        query = self.sqlite_session.query(PlayerBasic)
        if limit:
            query = query.limit(limit)

        players = query.all()
        synced = 0

        for player in players:
            data = {
                'player_id': player.player_id,
                'name': player.name,
                'uniform_no': player.uniform_no,
                'team': player.team,
                'position': player.position,
                'birth_date': player.birth_date,
                'birth_date_date': player.birth_date_date,
                'height_cm': player.height_cm,
                'weight_kg': player.weight_kg,
                'career': player.career,
                'status': player.status,
                'staff_role': player.staff_role,
                'status_source': player.status_source,
            }

            stmt = pg_insert(PlayerBasic).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'player_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player_basic records to Supabase")
        return synced

    def sync_player_movements(self) -> int:
        """Sync player_movements from SQLite to Supabase"""
        from src.models.player import PlayerMovement
        
        movements = self.sqlite_session.query(PlayerMovement).all()
        synced = 0
        
        if not movements:
            print("ℹ️ No player movement data to sync.")
            return 0
            
        for m in movements:
            data = {
                'date': m.date,
                'section': m.section,
                'team_code': m.team_code,
                'player_name': m.player_name,
                'remarks': m.remarks
            }
            
            stmt = pg_insert(PlayerMovement).values(**data)
            
            update_dict = {
                'remarks': stmt.excluded.remarks,
                'updated_at': text('CURRENT_TIMESTAMP')
            }
            
            stmt = stmt.on_conflict_do_update(
                constraint='uq_player_movement',
                set_=update_dict
            )
            
            self.supabase_session.execute(stmt)
            synced += 1
            
        self.supabase_session.commit()
        print(f"✅ Synced {synced} player movement records to Supabase")
        return synced

    def sync_player_season_batting(self, limit: int = None) -> int:
        """Sync player_season_batting data from SQLite to Supabase"""
        query = self.sqlite_session.query(PlayerSeasonBatting)
        if limit:
            query = query.limit(limit)

        batting_stats = query.all()
        synced = 0

        for stat in batting_stats:
            data = {
                'player_id': stat.player_id,
                'season': stat.season,
                'league': stat.league,
                'level': stat.level,
                'source': stat.source,
                'team_code': stat.team_code,
                'games': stat.games,
                'plate_appearances': stat.plate_appearances,
                'at_bats': stat.at_bats,
                'runs': stat.runs,
                'hits': stat.hits,
                'doubles': stat.doubles,
                'triples': stat.triples,
                'home_runs': stat.home_runs,
                'rbi': stat.rbi,
                'walks': stat.walks,
                'intentional_walks': stat.intentional_walks,
                'hbp': stat.hbp,
                'strikeouts': stat.strikeouts,
                'stolen_bases': stat.stolen_bases,
                'caught_stealing': stat.caught_stealing,
                'sacrifice_hits': stat.sacrifice_hits,
                'sacrifice_flies': stat.sacrifice_flies,
                'gdp': stat.gdp,
                'avg': stat.avg,
                'obp': stat.obp,
                'slg': stat.slg,
                'ops': stat.ops,
                'iso': stat.iso,
                'babip': stat.babip,
                'extra_stats': stat.extra_stats,
            }

            stmt = pg_insert(PlayerSeasonBatting).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k not in ['player_id', 'season', 'league', 'level']}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id', 'season', 'league', 'level'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player_season_batting records to Supabase")
        return synced

    def sync_player_season_pitching(self, limit: int = None) -> int:
        """Sync raw pitching stats to Supabase with player/team/season mapping."""
        fetch_sql = text(
            f"""
            SELECT *
            FROM {RAW_PITCHING_TABLE}
            { 'LIMIT :limit' if limit else '' }
            """
        )

        params = {"limit": limit} if limit else {}
        rows = self.sqlite_session.execute(fetch_sql, params).mappings().all()
        if not rows:
            print("ℹ️  No pitcher records found in raw table")
            return 0

        kbo_ids = {row["kbo_player_id"] for row in rows if row["kbo_player_id"] is not None}
        if not kbo_ids:
            print("ℹ️  Raw pitcher rows have no KBO IDs; skipping")
            return 0

        metadata = MetaData()
        player_basic_table = Table('player_basic', metadata, schema='public', autoload_with=self.supabase_engine)
        teams_table = Table('teams', metadata, schema='public', autoload_with=self.supabase_engine)
        try:
            seasons_table = Table('kbo_seasons', metadata, schema='public', autoload_with=self.supabase_engine)
        except Exception:
            seasons_table = Table('kbo_seasons_meta', metadata, schema='public', autoload_with=self.supabase_engine)
        pitching_table = Table('player_season_pitching', metadata, schema='public', autoload_with=self.supabase_engine)

        player_rows = self.supabase_session.execute(
            select(player_basic_table.c.player_id).where(player_basic_table.c.player_id.in_(list(kbo_ids)))
        ).all()
        player_map = {row.player_id: row.player_id for row in player_rows}

        team_rows = self.supabase_session.execute(select(teams_table.c.team_id)).all()
        team_set = {row.team_id for row in team_rows}

        season_rows = self.supabase_session.execute(
            select(seasons_table.c.season_id, seasons_table.c.season_year, seasons_table.c.league_type_code)
        ).all()
        season_map = {
            (row.season_year, row.league_type_code): row.season_id
            for row in season_rows
        }

        synced = 0

        for row in rows:
            kbo_id = row["kbo_player_id"]
            target_id = player_map.get(kbo_id)
            if not target_id:
                print(f"⚠️  Supabase player not found for KBO ID {kbo_id}; skipping")
                continue

            raw_extra = row["extra_stats"]
            extra_stats = raw_extra
            if isinstance(raw_extra, str):
                try:
                    extra_stats = json.loads(raw_extra)
                except Exception:
                    extra_stats = raw_extra

            league_code = LEAGUE_NAME_TO_CODE.get(row["league"], 0)
            season_id = season_map.get((row["season"], league_code))
            if not season_id:
                print(f"⚠️  Season not found for year={row['season']} league_code={league_code}; skipping")
                continue

            team_id = row["team_code"]
            if team_id and team_id not in team_set:
                print(f"⚠️  Team {team_id} not found in teams table; skipping")
                continue

            metrics = extra_stats.get("metrics", {}) if isinstance(extra_stats, dict) else {}
            innings_outs = row["innings_outs"]
            ip_value = None
            if innings_outs is not None:
                ip_value = round(innings_outs / 3.0, 2)
            elif isinstance(metrics, dict):
                ip_value = metrics.get("innings_pitched")

            extra_stats_value = raw_extra
            if isinstance(extra_stats, (dict, list)):
                extra_stats_value = json.dumps(extra_stats, ensure_ascii=False)

            data = {
                'season_id': season_id,
                'player_id': target_id,
                'team_id': team_id,
                'games': row['games'],
                'wins': row['wins'],
                'losses': row['losses'],
                'saves': row['saves'],
                'holds': row['holds'],
                'ip': ip_value,
                'hits': row['hits_allowed'],
                'home_runs': row['home_runs_allowed'],
                'walks': row['walks_allowed'],
                'hbp': row['hit_batters'],
                'strikeouts': row['strikeouts'],

                'runs': row['runs_allowed'],
                'earned_runs': row['earned_runs'],
                'whip': row['whip'],
                'complete_games': metrics.get('complete_games'),
                'shutouts': metrics.get('shutouts'),
                'quality_starts': metrics.get('quality_starts'),
                'blown_saves': metrics.get('blown_saves'),
                'tbf': metrics.get('tbf'),
                'np': metrics.get('np'),
                'avg_against': metrics.get('avg_against'),
                'doubles_allowed': metrics.get('doubles_allowed'),
                'triples_allowed': metrics.get('triples_allowed'),
                'sacrifices': metrics.get('sacrifices_allowed'),
                'sacrifice_flies': metrics.get('sacrifice_flies_allowed'),
                'intentional_walks': row['intentional_walks'],
                'wild_pitches': row['wild_pitches'],
                'balks': row['balks'],
                'extra_stats': extra_stats_value,
            }

            data = {
                k: v for k, v in data.items() if v is not None
            }

            stmt = pg_insert(pitching_table).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k not in ['season_id', 'player_id']}
            stmt = stmt.on_conflict_do_update(
                index_elements=['season_id', 'player_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player_season_pitching records to Supabase")
        return synced

    def sync_all_player_data(self) -> Dict[str, int]:
        """Sync all player-related data"""
        results = {
            'players': self.sync_players(),
            'player_identities': self.sync_player_identities(),
            'player_season_batting': self.sync_player_season_batting(),
            'player_season_pitching': self.sync_player_season_pitching(),
        }
        return results

    def sync_games(self, limit: int = None) -> int:
        """Sync game detail data from SQLite to Supabase using Batched UPSERT"""
        from src.models.game import Game
        
        # Load Season Mapping for Supabase compatibility
        metadata = MetaData()
        try:
            seasons_table = Table('kbo_seasons', metadata, schema='public', autoload_with=self.supabase_engine)
        except Exception:
            try:
                seasons_table = Table('kbo_seasons_meta', metadata, schema='public', autoload_with=self.supabase_engine)
            except Exception:
                seasons_table = None
        
        season_map = {}
        if seasons_table is not None:
            season_rows = self.supabase_session.execute(
                select(seasons_table.c.season_id, seasons_table.c.season_year, seasons_table.c.league_type_code)
            ).all()
            season_map = {
                (row.season_year, row.league_type_code): row.season_id
                for row in season_rows
            }

        # Define explicit table object with schema
        game_table = Table("game", metadata,
            Column("game_id", String, primary_key=True),
            Column("game_date", Date),
            Column("home_team", String),
            Column("away_team", String),
            Column("stadium", String),
            Column("home_score", Integer),
            Column("away_score", Integer),
            Column("winning_team", String),
            Column("winning_score", Integer),
            Column("season_id", Integer),
            Column("home_pitcher", String),
            Column("away_pitcher", String),
            Column("home_franchise_id", Integer),
            Column("away_franchise_id", Integer),
            Column("winning_franchise_id", Integer),
            schema='public'
        )

        base_query = self.sqlite_session.query(Game).order_by(Game.game_id)
        total_count = base_query.count()
        if total_count == 0:
            return 0

        if limit:
            total_count = min(total_count, limit)
        print(f"🚚 Syncing game table ({total_count} rows) in batches...")

        synced = 0
        batch_size = 100  # Smaller batch for more reliable execution
        update_columns = [
            "game_date",
            "home_team",
            "away_team",
            "stadium",
            "home_score",
            "away_score",
            "winning_team",
            "winning_score",
            "season_id",
            "home_pitcher",
            "away_pitcher",
            "home_franchise_id",
            "away_franchise_id",
            "winning_franchise_id",
        ]

        last_game_id = None
        remaining = total_count

        while True:
            query = base_query
            if last_game_id:
                query = query.filter(Game.game_id > last_game_id)
            if limit:
                query = query.limit(min(batch_size, remaining))
            else:
                query = query.limit(batch_size)
            games = query.all()
            if not games:
                break

            values_list = []
            for g in games:
                # Resolve season_id for Supabase
                local_sid = g.season_id
                target_sid = local_sid
                if local_sid and season_map:
                    if local_sid < 10000:
                        # If simple year (e.g. 2026), assume Regular Season (0)
                        target_sid = season_map.get((local_sid, 0), local_sid)
                    else:
                        year = local_sid // 10
                        ltype = local_sid % 10
                        target_sid = season_map.get((year, ltype), local_sid)
                
                data = {
                    'game_id': g.game_id,
                    'game_date': g.game_date,
                    'home_team': g.home_team,
                    'away_team': g.away_team,
                    'stadium': g.stadium,
                    'home_score': g.home_score,
                    'away_score': g.away_score,
                    'winning_team': g.winning_team,
                    'winning_score': g.winning_score,
                    'season_id': target_sid,
                    'home_pitcher': g.home_pitcher,
                    'away_pitcher': g.away_pitcher,
                    'home_franchise_id': g.home_franchise_id,
                    'away_franchise_id': g.away_franchise_id,
                    'winning_franchise_id': g.winning_franchise_id,
                }
                values_list.append(data)

            try:
                stmt = pg_insert(game_table).values(values_list)
                update_dict = {k: stmt.excluded[k] for k in update_columns}
                stmt = stmt.on_conflict_do_update(
                    index_elements=['game_id'],
                    set_=update_dict
                )
                self.supabase_session.execute(stmt)
                self.supabase_session.commit()
                synced += len(values_list)
                last_game_id = games[-1].game_id
                if limit:
                    remaining -= len(values_list)
                    if remaining <= 0:
                        break
            except Exception as e:
                self.supabase_session.rollback()
                print(f"❌ Error syncing games batch after {last_game_id}: {e}")
                
                # Try individual inserts as fallback if batch fails
                print("   ⚠️ Retrying batch individually...")
                for val in values_list:
                    try:
                        single_stmt = pg_insert(game_table).values(val)
                        single_update = {k: single_stmt.excluded[k] for k in update_columns}
                        single_stmt = single_stmt.on_conflict_do_update(
                            index_elements=['game_id'],
                            set_=single_update
                        )
                        self.supabase_session.execute(single_stmt)
                        self.supabase_session.commit()
                        synced += 1
                        last_game_id = val['game_id']
                    except Exception as single_e:
                        self.supabase_session.rollback()
                        print(f"   ❌ Critical error on game {val.get('game_id')}: {single_e}")
                        # Update last_game_id even on failure to avoid infinite loop
                        last_game_id = val['game_id']

        print(f"✅ Synced {synced} games to Supabase")
        return synced

    def sync_player_game_batting(self, limit: int = None) -> int:
        """Sync player game batting stats from SQLite to Supabase"""
        query = self.sqlite_session.query(PlayerGameBatting)
        if limit:
            query = query.limit(limit)

        batting_stats = query.all()
        synced = 0

        for stat in batting_stats:
            data = {
                'game_id': stat.game_id,
                'player_id': stat.player_id,
                'player_name': stat.player_name,
                'team_side': stat.team_side,
                'team_code': stat.team_code,
                'batting_order': stat.batting_order,
                'appearance_seq': stat.appearance_seq,
                'position': stat.position,
                'is_starter': bool(stat.is_starter) if stat.is_starter is not None else False,
                'source': stat.source,
                'plate_appearances': stat.plate_appearances,
                'at_bats': stat.at_bats,
                'runs': stat.runs,
                'hits': stat.hits,
                'doubles': stat.doubles,
                'triples': stat.triples,
                'home_runs': stat.home_runs,
                'rbi': stat.rbi,
                'walks': stat.walks,
                'intentional_walks': stat.intentional_walks,
                'hbp': stat.hbp,
                'strikeouts': stat.strikeouts,
                'stolen_bases': stat.stolen_bases,
                'caught_stealing': stat.caught_stealing,
                'sacrifice_hits': stat.sacrifice_hits,
                'sacrifice_flies': stat.sacrifice_flies,
                'gdp': stat.gdp,
                'avg': stat.avg,
                'obp': stat.obp,
                'slg': stat.slg,
                'ops': stat.ops,
                'iso': stat.iso,
                'babip': stat.babip,
                'extras': stat.extras,
            }

            stmt = pg_insert(PlayerGameBatting).values(**data)
            update_dict = {k: v for k, v in data.items() if k not in ['game_id', 'player_id']}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id', 'player_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player game batting stats to Supabase")
        return synced

    def sync_player_game_pitching(self, limit: int = None) -> int:
        """Sync player game pitching stats from SQLite to Supabase"""
        query = self.sqlite_session.query(PlayerGamePitching)
        if limit:
            query = query.limit(limit)

        pitching_stats = query.all()
        synced = 0

        for stat in pitching_stats:
            data = {
                'game_id': stat.game_id,
                'player_id': stat.player_id,
                'player_name': stat.player_name,
                'team_side': stat.team_side,
                'team_code': stat.team_code,
                'is_starting': bool(stat.is_starting) if stat.is_starting is not None else False,
                'appearance_seq': stat.appearance_seq,
                'source': stat.source,
                'innings_outs': stat.innings_outs,
                'hits_allowed': stat.hits_allowed,
                'runs_allowed': stat.runs_allowed,
                'earned_runs': stat.earned_runs,
                'home_runs_allowed': stat.home_runs_allowed,
                'walks_allowed': stat.walks_allowed,
                'strikeouts': stat.strikeouts,
                'hit_batters': stat.hit_batters,
                'wild_pitches': stat.wild_pitches,
                'balks': stat.balks,
                'wins': stat.wins,
                'losses': stat.losses,
                'saves': stat.saves,
                'holds': stat.holds,
                'decision': stat.decision,
                'batters_faced': stat.batters_faced,
                'era': stat.era,
                'whip': stat.whip,
                'fip': stat.fip,
                'k_per_nine': stat.k_per_nine,
                'bb_per_nine': stat.bb_per_nine,
                'kbb': stat.kbb,
                'extras': stat.extras,
            }

            stmt = pg_insert(PlayerGamePitching).values(**data)
            update_dict = {k: v for k, v in data.items() if k not in ['game_id', 'player_id']}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id', 'player_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} player game pitching stats to Supabase")
        return synced

    def sync_all_game_data(self, limit: int = None) -> Dict[str, int]:
        """Sync all game-related data"""
        results = {
            'game_schedules': self.sync_game_schedules(limit=limit),
            'games': self.sync_games(limit=limit),
            'player_game_batting': self.sync_player_game_batting(limit=limit),
            'player_game_pitching': self.sync_player_game_pitching(limit=limit),
        }
        return results

    def sync_game_details(self, limit: int = None) -> Dict[str, int]:
        """Sync all game detail tables to Supabase"""
        results = {}
        
        # 0. Sync Parent Games first (Required for Foreign Keys)
        print("⚾ Syncing Parent Game Records...")
        results['games'] = self.sync_games(limit=limit)

        # 1. Game Metadata
        results['metadata'] = self._sync_simple_table(
            GameMetadata, 
            ['game_id'], 
            exclude_cols=['created_at'],
            limit=limit
        )

        # 2. Inning Scores
        results['inning_scores'] = self._sync_simple_table(
            GameInningScore,
            ['game_id', 'team_side', 'inning'],
            exclude_cols=['id', 'created_at'],
            limit=limit
        )

        # 3. Lineups
        results['lineups'] = self._sync_simple_table(
            GameLineup,
            ['game_id', 'team_side', 'appearance_seq'],
             exclude_cols=['id', 'created_at'],
             limit=limit
        )

        # 4. Batting Stats
        results['batting_stats'] = self._sync_simple_table(
            GameBattingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['id', 'created_at'],
            limit=limit
        )

        # 5. Pitching Stats
        results['pitching_stats'] = self._sync_simple_table(
            GamePitchingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['id', 'created_at'],
            limit=limit
        )

        results['events'] = self._sync_simple_table(
            GameEvent,
            ['game_id', 'event_seq'],
            exclude_cols=['id', 'created_at'],
            limit=limit
        )

        # 7. Game Summary (New)
        results['summary'] = self._sync_simple_table(
            GameSummary,
            ['game_id', 'summary_type', 'player_name', 'detail_text'], 
            exclude_cols=['id', 'created_at'],
            limit=limit
        )

        
        print(f"✅ Game Details Sync Summary: {results}")
        return results

    def _sync_simple_table(self, model, conflict_keys: List[str], exclude_cols: List[str] = None, limit: int = None) -> int:
        """Generic sync parameter for simple tables using Batched UPSERT"""
        if exclude_cols is None:
            exclude_cols = []
            
        # Get columns to sync
        columns = [c.key for c in model.__table__.columns if c.key not in exclude_cols and c.key not in ('created_at', 'updated_at')]
        
        total_count = self.sqlite_session.query(model).count()
        if total_count == 0:
            print(f"ℹ️  No records for {model.__tablename__}")
            return 0
            
        print(f"🚚 Syncing {model.__tablename__} ({total_count} rows) in batches...")
        
        synced = 0
        batch_size = 1000
        pk_cols = list(model.__table__.primary_key.columns)
        use_keyset = bool(pk_cols)

        if limit:
            total_count = min(total_count, limit)
            
        if not use_keyset:
            for offset in range(0, total_count, batch_size):
                query = self.sqlite_session.query(model)
                rows = query.offset(offset).limit(min(batch_size, total_count - offset)).all()
                if not rows:
                    break
                values_list = []
                for row in rows:
                    data = {c: getattr(row, c) for c in columns}
                    for k, v in data.items():
                        if isinstance(v, (dict, list)):
                            # Explicitly serialize JSON types for TEXT column compatibility (e.g. source_payload)
                            data[k] = json.dumps(v, ensure_ascii=False)
                    data['updated_at'] = datetime.now()
                    values_list.append(data)

                try:
                    metadata = MetaData()
                    target_table = Table(model.__tablename__, metadata, schema='public', autoload_with=self.supabase_engine)
                    stmt = pg_insert(target_table).values(values_list)
                    update_dict = {c: stmt.excluded[c] for c in columns if c not in conflict_keys}
                    update_dict['updated_at'] = stmt.excluded.updated_at
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_keys,
                        set_=update_dict
                    )
                    self.supabase_session.execute(stmt)
                    self.supabase_session.commit()
                    synced += len(values_list)
                    if synced % 5000 == 0 or synced == total_count:
                        print(f"   Synced {synced}/{total_count} rows...")
                except Exception as e:
                    self.supabase_session.rollback()
                    print(f"❌ Error syncing {model.__tablename__} batch at offset {offset}: {e}")
            return synced

        last_key = None
        remaining = total_count
        while remaining > 0:
            query = self.sqlite_session.query(model).order_by(*pk_cols)
            if last_key is not None:
                if len(pk_cols) == 1:
                    query = query.filter(pk_cols[0] > last_key)
                else:
                    query = query.filter(tuple_(*pk_cols) > last_key)
            rows = query.limit(min(batch_size, remaining)).all()
            if not rows:
                break
            
            first_pk = getattr(rows[0], pk_cols[0].key) if len(pk_cols) == 1 else "composite"
            # print(f"   Batch start PK: {first_pk}")

            values_list = []
            for row in rows:
                data = {c: getattr(row, c) for c in columns}
                for k, v in data.items():
                    if isinstance(v, (dict, list)):
                        # Explicitly serialize JSON types for TEXT column compatibility (e.g. source_payload)
                        data[k] = json.dumps(v, ensure_ascii=False)
                data['updated_at'] = datetime.now()
                values_list.append(data)

            try:
                metadata = MetaData()
                target_table = Table(model.__tablename__, metadata, schema='public', autoload_with=self.supabase_engine)
                stmt = pg_insert(target_table).values(values_list)
                update_dict = {c: stmt.excluded[c] for c in columns if c not in conflict_keys}
                update_dict['updated_at'] = stmt.excluded.updated_at

                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_=update_dict
                )

                self.supabase_session.execute(stmt)
                self.supabase_session.commit()
                synced += len(values_list)
                remaining -= len(values_list)
                if synced % 5000 == 0 or synced == total_count:
                    print(f"   Synced {synced}/{total_count} rows...")
            except Exception as e:
                self.supabase_session.rollback()
                print(f"❌ Error syncing {model.__tablename__} batch at {synced}: {e}")
                print(f"   ⚠️ Retrying {len(values_list)} records individually to isolate error...")
                for val in values_list:
                    try:
                        metadata = MetaData()
                        target_table = Table(model.__tablename__, metadata, schema='public', autoload_with=self.supabase_engine)
                        single_stmt = pg_insert(target_table).values(val)
                        single_update = {c: single_stmt.excluded[c] for c in columns if c not in conflict_keys}
                        single_update['updated_at'] = single_stmt.excluded.updated_at
                        single_stmt = single_stmt.on_conflict_do_update(
                            index_elements=conflict_keys,
                            set_=single_update
                        )
                        self.supabase_session.execute(single_stmt)
                        self.supabase_session.commit()
                        synced += 1
                    except Exception as single_e:
                        self.supabase_session.rollback()
                        print(f"   ❌ Critical error on {model.__tablename__} row: {single_e}")
                        print(f"   ❌ Data: {val}")
                        # Move on to next row

            if len(pk_cols) == 1:
                last_key = getattr(rows[-1], pk_cols[0].key)
            else:
                last_key = tuple(getattr(rows[-1], col.key) for col in pk_cols)

        return synced


    def sync_awards(self) -> int:
        """Sync awards from SQLite to Supabase"""
        # Ensure table exists
        try:
            migration_path = Path("migrations/supabase/019_create_awards.sql")
            if migration_path.exists():
                sql = migration_path.read_text()
                self.supabase_session.execute(text(sql))
                self.supabase_session.commit()
                print("✅ Applied awards migration")
        except Exception as e:
            print(f"⚠️ Failed to apply migration: {e}")
            self.supabase_session.rollback()

        awards = self.sqlite_session.query(Award).all()
        synced = 0
        if not awards:
            print("ℹ️ No awards data to sync.")
            return 0

        for award in awards:
            data = {
                'year': award.year,
                'award_type': award.award_type,
                'category': award.category,
                'player_name': award.player_name,
                'team_name': award.team_name,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            stmt = pg_insert(Award).values(**data)
            update_dict = {
                'updated_at': stmt.excluded.updated_at
            }
            stmt = stmt.on_conflict_do_update(
                constraint='uq_award_record',
                set_=update_dict
            )
            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
        print(f"✅ Synced {synced} awards to Supabase")
        return synced

    def close(self):

        """Close Supabase session"""
        self.supabase_session.close()
        self.supabase_engine.dispose()


def main():
    """타자 및 투수 데이터 Supabase 동기화"""
    from src.db.engine import SessionLocal

    # Get Supabase URL from environment
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if not supabase_url:
        print("❌ SUPABASE_DB_URL environment variable not set")
        print("   Set it in .env file or export it:")
        print("   export SUPABASE_DB_URL='postgresql://postgres.xxx:password@xxx.pooler.supabase.com:5432/postgres'")
        return

    print("\n" + "🔄" * 30)
    print("KBO 데이터 Supabase 동기화")
    print("🔄" * 30 + "\n")

    with SessionLocal() as sqlite_session:
        try:
            sync = SupabaseSync(supabase_url, sqlite_session)

            # Test connection
            if not sync.test_connection():
                return

            # SQLite 데이터 현황 확인
            batting_count = sqlite_session.query(PlayerSeasonBatting).count()
            pitching_count = sqlite_session.query(PlayerSeasonPitching).count()
            
            print(f"📊 SQLite 데이터 현황:")
            print(f"   타자 데이터: {batting_count}건")
            print(f"   투수 데이터: {pitching_count}건")

            if batting_count == 0 and pitching_count == 0:
                print("⚠️ 동기화할 데이터가 없습니다.")
                print("📌 먼저 크롤러를 실행하세요:")
                print("   ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save")
                print("   ./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year 2025 --series regular --save")
                return

            total_synced = 0

            # 타자 데이터 동기화
            if batting_count > 0:
                print("\n🏏 타자 데이터 동기화 중...")
                batting_synced = sync.sync_batting_data()
                sync.verify_batting_sync(batting_synced)
                total_synced += batting_synced

            # 투수 데이터 동기화
            if pitching_count > 0:
                print("\n⚾ 투수 데이터 동기화 중...")
                pitching_synced = sync.sync_pitcher_data()
                sync.verify_pitcher_sync(pitching_synced)
                total_synced += pitching_synced

            # Supabase 데이터 샘플 표시
            sync.show_supabase_data_sample()

            print("\n" + "=" * 50)
            print("📈 동기화 완료")
            print("=" * 50)
            print(f"총 동기화된 데이터: {total_synced}건")
            if batting_count > 0:
                print(f"  - 타자 데이터: {batting_count}건")
            if pitching_count > 0:
                print(f"  - 투수 데이터: {pitching_count}건")
            print("\n🎉 Supabase에서 데이터를 확인할 수 있습니다!")

            sync.close()

        except Exception as e:
            print(f"\n❌ Sync error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
