"""
Sync validated data from SQLite to OCI (Oracle Cloud Infrastructure) PostgreSQL
Dual-repository pattern: SQLite (dev/validation) → OCI (production)
"""
import os
import json
from typing import List, Dict, Any, Optional, Callable, Type
from pathlib import Path
from sqlalchemy import create_engine, text, select, MetaData, Table, column, table
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
from src.models.matchup import BatterTeamSplit, PitcherTeamSplit, BatterStadiumSplit, BatterVsStarter


LEAGUE_NAME_TO_CODE = {
    "REGULAR": 0,
    "EXHIBITION": 1,
    "WILDCARD": 2,
    "SEMI_PLAYOFF": 3,
    "PLAYOFF": 4,
    "KOREAN_SERIES": 5,
}


class OCISync:
    """Sync data from SQLite to OCI"""

    def __init__(self, oci_url: str, sqlite_session: Session):
        """
        Initialize OCI sync

        Args:
            oci_url: PostgreSQL connection string for OCI
            sqlite_session: Active SQLite session to read from
        """
        self.sqlite_session = sqlite_session

        # Create OCI engine
        self.oci_engine = create_engine(
            oci_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 60,
                "application_name": "KBO_Crawler_Sync"
            }
        )

        # Create OCI session
        target_session_factory = sessionmaker(bind=self.oci_engine)
        self.target_session = target_session_factory()

    @staticmethod
    def _chunked(items: List[str], size: int) -> List[List[str]]:
        return [items[idx: idx + size] for idx in range(0, len(items), size)]

    def test_connection(self) -> bool:
        """Test OCI connection"""
        try:
            result = self.target_session.execute(text("SELECT 1"))
            print("✅ OCI connection successful")
            return True
        except Exception as e:
            print(f"❌ OCI connection failed: {e}")
            return False

    def sync_pitcher_data(self) -> int:
        """새로운 player_season_pitching 테이블의 투수 데이터를 OCI로 동기화 (고속 Batch COPY)"""
        return self._sync_simple_table(
            PlayerSeasonPitching,
            conflict_keys=['player_id', 'season', 'league', 'level'],
            exclude_cols=['id', 'created_at']
        )

    def sync_batting_data(self) -> int:
        """타자 데이터를 OCI로 동기화 (고속 Batch COPY)"""
        return self._sync_simple_table(
            PlayerSeasonBatting,
            conflict_keys=['player_id', 'season', 'league', 'level'],
            exclude_cols=['id', 'created_at']
        )

    def verify_pitcher_sync(self, expected_count: int):
        """투수 데이터 동기화 결과 검증"""
        try:
            result = self.target_session.execute(text("""
                SELECT COUNT(*) as count 
                FROM player_season_pitching 
            """))
            
            actual_count = result.fetchone()[0]
            print(f"🔍 OCI 투수 데이터 확인: {actual_count}건 (예상: {expected_count}건)")
            
            if actual_count >= expected_count:
                print("✅ 투수 데이터 동기화 검증 성공!")
            else:
                print("⚠️ 동기화된 투수 데이터 수가 예상보다 적습니다.")
                
        except Exception as e:
            print(f"⚠️ 투수 데이터 동기화 검증 실패: {e}")

    def verify_batting_sync(self, expected_count: int):
        """타자 데이터 동기화 결과 검증"""
        try:
            result = self.target_session.execute(text("""
                SELECT COUNT(*) as count 
                FROM player_season_batting 
            """))
            
            actual_count = result.fetchone()[0]
            print(f"🔍 OCI 타자 데이터 확인: {actual_count}건 (예상: {expected_count}건)")
            
            if actual_count >= expected_count:
                print("✅ 타자 데이터 동기화 검증 성공!")
            else:
                print("⚠️ 동기화된 타자 데이터 수가 예상보다 적습니다.")
                
        except Exception as e:
            print(f"⚠️ 타자 데이터 동기화 검증 실패: {e}")

    def show_supabase_data_sample(self):
        """OCI의 데이터 샘플 표시"""
        try:
            # 투수 데이터 샘플
            pitcher_result = self.target_session.execute(text("""
                SELECT player_id, season, games, wins, losses, era, innings_pitched
                FROM player_season_pitching 
                LIMIT 3
            """))
            
            pitcher_rows = pitcher_result.fetchall()
            if pitcher_rows:
                print("\n📊 OCI 투수 데이터 샘플:")
                for i, row in enumerate(pitcher_rows):
                    print(f"  {i+1}. player_id: {row[0]}, season: {row[1]}")
                    print(f"     게임수: {row[2]}, 승패: {row[3]}-{row[4]}, ERA: {row[5]}, 이닝: {row[6]}")
            
            # 타자 데이터 샘플
            batting_result = self.target_session.execute(text("""
                SELECT player_id, season, games, avg, hits, home_runs
                FROM player_season_batting 
                LIMIT 3
            """))
            
            batting_rows = batting_result.fetchall()
            if batting_rows:
                print("\n🏏 OCI 타자 데이터 샘플:")
                for i, row in enumerate(batting_rows):
                    print(f"  {i+1}. player_id: {row[0]}, season: {row[1]}")
                    print(f"     게임수: {row[2]}, 타율: {row[3]}, 안타: {row[4]}, 홈런: {row[5]}")
                    
        except Exception as e:
            print(f"⚠️ OCI 데이터 조회 실패: {e}")

    

    def sync_daily_rosters(self) -> int:
        """Sync team_daily_roster from SQLite to OCI"""
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
            
            self.target_session.execute(stmt)
            self.target_session.commit() # Commit each batch
            synced += len(values_list)
            print(f"   Synced batch {i // batch_size + 1} ({len(values_list)} records)")
        print(f"✅ Synced {synced} daily roster records to OCI")
        return synced

    def sync_franchises(self) -> int:
        """Sync franchises from SQLite to OCI"""
        from src.models.franchise import Franchise
        from src.cli.sync_oci import clone_row
        
        franchises = self.sqlite_session.query(Franchise).all()
        synced = 0
        
        for f in franchises:
            clone = clone_row(f, Franchise)
            self.target_session.merge(clone)
            synced += 1
            
        self.target_session.commit()
        print(f"✅ Synced {synced} franchises to OCI")
        return synced

    def sync_teams(self) -> int:
        """Sync teams from SQLite to OCI"""
        from src.models.team import Team
        from src.cli.sync_oci import clone_row
        
        # Get Franchise ID Mapping (Local ID -> OCI ID)
        franchise_mapping = self._get_franchise_id_mapping()
        
        teams = self.sqlite_session.query(Team).all()
        synced = 0
        
        for team in teams:
            # Map Local Franchise ID to OCI Franchise ID
            fid = None
            if team.franchise_id:
                fid = franchise_mapping.get(team.franchise_id)

            clone = clone_row(team, Team)
            clone.franchise_id = fid
            
            # Ensure aliases is a list for merge to handle it as PG ARRAY
            if clone.aliases is None:
                clone.aliases = []
            elif isinstance(clone.aliases, str):
                try:
                    clone.aliases = json.loads(clone.aliases)
                except:
                    clone.aliases = [clone.aliases]

            self.target_session.merge(clone)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} teams to OCI")
        return synced

    def sync_team_history(self) -> int:
        """Sync team_history table"""
        from src.models.team_history import TeamHistory
        from src.cli.sync_oci import clone_row
        
        franchise_mapping = self._get_franchise_id_mapping()
        histories = self.sqlite_session.query(TeamHistory).all()
        synced = 0
        
        for h in histories:
            sup_fid = franchise_mapping.get(h.franchise_id)
            if not sup_fid:
                continue
                
            clone = clone_row(h, TeamHistory)
            clone.franchise_id = sup_fid
            self.target_session.merge(clone)
            synced += 1
            
        self.target_session.commit()
        print(f"✅ Synced {synced} team history records to OCI")
        return synced

    # ... (other methods)

    def _get_franchise_id_mapping(self) -> Dict[int, int]:
        """Get SQLite franchise_id → OCI franchise_id mapping"""
        from src.models.franchise import Franchise
        mapping = {}

        # Get all franchises from SQLite
        sqlite_franchises = self.sqlite_session.query(Franchise).all()

        for sf in sqlite_franchises:
            # Find corresponding OCI franchise by original_code (Unique Key)
            supabase_franchise = self.target_session.query(Franchise).filter_by(original_code=sf.original_code).first()
            if supabase_franchise:
                mapping[sf.id] = supabase_franchise.id
        
        return mapping

    def _get_ballpark_id_mapping(self) -> Dict[int, int]:
        """Get SQLite ballpark_id → OCI ballpark_id mapping"""
        mapping = {}

        sqlite_ballparks = self.sqlite_session.query(Ballpark).all()

        for sb in sqlite_ballparks:
            supabase_ballpark = self.target_session.query(Ballpark).filter_by(name_kor=sb.name_kor).first()
            if supabase_ballpark:
                mapping[sb.id] = supabase_ballpark.id

        return mapping

    def sync_players(self) -> int:
        """Sync players from SQLite to OCI"""
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

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} players to OCI")
        return synced

    def sync_player_identities(self) -> int:
        """Sync player identities from SQLite to OCI"""
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
            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player identities to OCI")
        return synced



    def _get_player_id_mapping(self) -> Dict[int, int]:
        """Get SQLite player_id → OCI player_id mapping"""
        mapping = {}
        sqlite_players = self.sqlite_session.query(Player).all()

        for sp in sqlite_players:
            if sp.kbo_person_id:
                supabase_player = self.target_session.query(Player).filter_by(
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
            self.target_session.execute(stmt)
            synced += 1
        self.target_session.commit()
        print(f"✅ Synced {synced} crawl run records to OCI")
        return synced

    def sync_player_basic(self, limit: int = None) -> int:
        """Sync player_basic data from SQLite to OCI"""
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
                'photo_url': player.photo_url,
                'bats': player.bats,
                'throws': player.throws,
                'debut_year': player.debut_year,
                'salary_original': player.salary_original,
                'signing_bonus_original': player.signing_bonus_original,
                'draft_info': player.draft_info,
            }

            stmt = pg_insert(PlayerBasic).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'player_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id'],
                set_=update_dict
            )

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player_basic records to OCI")
        return synced

    def sync_player_movements(self) -> int:
        """Sync player_movements from SQLite to OCI"""
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
            
            self.target_session.execute(stmt)
            synced += 1
            
        self.target_session.commit()
        print(f"✅ Synced {synced} player movement records to OCI")
        return synced

    def sync_player_season_batting(self, limit: int = None) -> int:
        """Sync player_season_batting data from SQLite to OCI"""
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

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player_season_batting records to OCI")
        return synced

    def sync_player_season_pitching(self, limit: int = None) -> int:
        """Sync raw pitching stats to OCI with player/team/season mapping."""
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
        player_basic_table = Table('player_basic', metadata, autoload_with=self.oci_engine)
        teams_table = Table('teams', metadata, autoload_with=self.oci_engine)
        try:
            seasons_table = Table('kbo_seasons', metadata, autoload_with=self.oci_engine)
        except Exception:
            seasons_table = Table('kbo_seasons_meta', metadata, autoload_with=self.oci_engine)
        pitching_table = Table('player_season_pitching', metadata, autoload_with=self.oci_engine)

        player_rows = self.target_session.execute(
            select(player_basic_table.c.player_id).where(player_basic_table.c.player_id.in_(list(kbo_ids)))
        ).all()
        player_map = {row.player_id: row.player_id for row in player_rows}

        team_rows = self.target_session.execute(select(teams_table.c.team_id)).all()
        team_set = {row.team_id for row in team_rows}

        season_rows = self.target_session.execute(
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
                print(f"⚠️  OCI player not found for KBO ID {kbo_id}; skipping")
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

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player_season_pitching records to OCI")
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

    def sync_games(self, limit: int = None, filters: List = None) -> int:
        """Sync game detail data from SQLite to OCI using Batched UPSERT or COPY"""
        from src.models.game import Game
        
        # Load season map for mapping SQLite season_id (year) to OCI season_id (int)
        season_map = self._get_season_map()
        
        def transform(data):
            # If season_id looks like a year (e.g. > 1900), map it
            raw_sid = data.get('season_id')
            if raw_sid and raw_sid > 1900:
                # Default to Regular season (type 0) for these legacy years
                key = (raw_sid, 0)
                if key in season_map:
                    data['season_id'] = season_map[key]
            return data

        # Exclude columns that don't exist on OCI side
        # We must exclude 'id' to avoid PK conflicts, as SQLite and OCI use different surrogate ID sequences.
        # Business key for deduplication/upsert is 'game_id'.
        exclude_cols = ['id', 'created_at', 'updated_at', 'home_franchise_id', 'away_franchise_id', 'winning_franchise_id']
        
        return self._sync_simple_table(
            Game, 
            ['game_id'], 
            exclude_cols=exclude_cols,
            filters=filters,
            transform_fn=transform
        )

    def sync_player_game_batting(self, limit: int = None) -> int:
        """Sync player game batting stats from SQLite to OCI"""
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

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player game batting stats to OCI")
        return synced

    def sync_player_game_pitching(self, limit: int = None) -> int:
        """Sync player game pitching stats from SQLite to OCI"""
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

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player game pitching stats to OCI")
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

    def _purge_game_detail_children_for_year(self, year: int) -> None:
        """
        Delete year-scoped child detail rows on OCI before re-sync.
        This prevents stale duplicates when mutable fields (e.g. player_id) change.
        """
        pattern = f"{year}%"
        child_tables = [
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_batting_stats",
            "game_pitching_stats",
            "game_play_by_play",
            "game_events",
            "game_summary",
        ]
        for table_name in child_tables:
            self.target_session.execute(
                text(f"DELETE FROM {table_name} WHERE game_id LIKE :pattern"),
                {"pattern": pattern},
            )
        self.target_session.commit()
        print(f"🧹 Purged OCI child game-detail rows for year {year}")

    def get_unsynced_or_modified_game_ids(self) -> List[str]:
        """로컬 SQLite와 OCI 간의 차집합(누락되거나 상태가 변경된 게임) 계산"""
        from src.models.game import Game
        
        # OCI에서 전체 game_id와 game_status를 가져옴
        oci_games = {
            row[0]: row[1] 
            for row in self.target_session.execute(text("SELECT game_id, game_status FROM game")).fetchall()
        }
        
        # SQLite에서 전체 게임 가져옴
        local_games = self.sqlite_session.query(Game.game_id, Game.game_status).all()
        
        sync_ids = []
        for l_id, l_status in local_games:
            oci_status = oci_games.get(l_id)
            if l_id not in oci_games:
                # OCI에 없는 게임
                sync_ids.append(l_id)
            elif l_status != oci_status:
                # 상태가 변경된 게임 (예: 경기 전 -> 종료)
                sync_ids.append(l_id)
                
        return sync_ids

    def sync_game_details(self, days: int = None, year: int = None, unsynced_only: bool = False) -> Dict[str, int]:
        """Sync all game detail tables to OCI"""
        results = {}
        from src.models.game import (
            Game,
            GameMetadata,
            GameInningScore,
            GameLineup,
            GameBattingStat,
            GamePitchingStat,
            GamePlayByPlay,
            GameEvent,
            GameSummary,
        )
        
        filters = []
        target_game_ids = None

        if unsynced_only:
            print("🔍 식별 중: OCI에 없거나 로컬에서 최근에 갱신된 게임 데이터를 검사합니다...")
            target_game_ids = self.get_unsynced_or_modified_game_ids()
            if not target_game_ids:
                print("🎉 모든 게임 데이터가 이미 최신 상태입니다. 동기화를 건너뜁니다.")
                return results
            print(f"🎯 총 {len(target_game_ids)}개의 변경/누락된 게임을 발견했습니다.")
            # target_game_ids가 너무 길면 sqlite in_ 절 한도를 초과할 수 있지만, 부분 업데이트라 대개 수십 건 내외임.
            filters.append(Game.game_id.in_(target_game_ids))
        else:
            if days:
                from datetime import datetime, timedelta
                since_date = (datetime.now() - timedelta(days=days)).date()
                filters.append(Game.game_date >= since_date)
            if year:
                filters.append(Game.game_id.like(f"{year}%"))
            
        # 0. Sync Parent Games first (Required for Foreign Keys)
        print("⚾ Syncing Parent Game Records...")
        results['games'] = self.sync_games(filters=filters if filters else None)

        # Build filters for child tables (they often use game_id instead of game_date)
        child_filters = []
        if year:
            child_filters.append(text(f"game_id LIKE '{year}%'"))
        elif days:
            game_ids = [g.game_id for g in self.sqlite_session.query(Game.game_id).filter(*filters).all()]
            if game_ids:
                quoted_ids = [f"'{gid}'" for gid in game_ids]
                child_filters.append(text(f"game_id IN ({','.join(quoted_ids)})"))
            else:
                print("ℹ️ No games found for the specified period.")
                return results

        if year:
            # Remove existing year-scoped child rows first to avoid stale/null duplicates.
            self._purge_game_detail_children_for_year(year)

        def get_child_filters(model_cls):
            if unsynced_only and target_game_ids:
                return [model_cls.game_id.in_(target_game_ids)]
            return child_filters if child_filters else None

        # 1. Game Metadata
        results['metadata'] = self._sync_simple_table(
            GameMetadata, 
            ['game_id'], 
            exclude_cols=['created_at'],
            filters=get_child_filters(GameMetadata)
        )

        # 2. Inning Scores
        results['inning_scores'] = self._sync_simple_table(
            GameInningScore,
            ['game_id', 'team_side', 'inning'],
            exclude_cols=['created_at', 'id'],
            filters=get_child_filters(GameInningScore)
        )

        # 3. Lineups
        results['lineups'] = self._sync_simple_table(
            GameLineup,
            ['game_id', 'team_side', 'appearance_seq'],
             exclude_cols=['created_at', 'id'],
             filters=get_child_filters(GameLineup)
        )

        # 4. Batting Stats
        results['batting_stats'] = self._sync_simple_table(
            GameBattingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['created_at', 'id'],
            filters=get_child_filters(GameBattingStat)
        )

        # 5. Pitching Stats
        results['pitching_stats'] = self._sync_simple_table(
            GamePitchingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['created_at', 'id'],
            filters=get_child_filters(GamePitchingStat)
        )

        results['play_by_play'] = self._sync_game_play_by_play(
            filters=get_child_filters(GamePlayByPlay)
        )

        results['events'] = self._sync_simple_table(
            GameEvent,
            ['game_id', 'event_seq'],
            exclude_cols=['created_at', 'id'],
            filters=get_child_filters(GameEvent)
        )

        # 7. Game Summary
        results['summary'] = self._sync_simple_table(
            GameSummary,
            ['game_id', 'summary_type', 'player_name', 'detail_text'],
            exclude_cols=['created_at', 'id'],
            filters=get_child_filters(GameSummary)
        )
        
        print(f"✅ Game Details Sync Summary: {results}")
        return results

    def sync_specific_game(self, game_id: str) -> Dict[str, int]:
        """Sync all related data for a single game_id"""
        # We need Game model for filtering
        from src.models.game import (
            Game,
            GameMetadata,
            GameInningScore,
            GameLineup,
            GameBattingStat,
            GamePitchingStat,
            GamePlayByPlay,
            GameEvent,
            GameSummary,
        )
        
        results = {}
        filters = [Game.game_id == game_id]

        # Sync Game record
        results['game'] = self._sync_simple_table(Game, ['game_id'], exclude_cols=['created_at', 'updated_at'], filters=filters)
        
        # Sync children
        results['metadata'] = self._sync_simple_table(GameMetadata, ['game_id'], exclude_cols=['created_at'], filters=[GameMetadata.game_id == game_id])
        results['inning_scores'] = self._sync_simple_table(GameInningScore, ['game_id', 'team_side', 'inning'], exclude_cols=['created_at'], filters=[GameInningScore.game_id == game_id])
        results['lineups'] = self._sync_simple_table(GameLineup, ['game_id', 'team_side', 'appearance_seq'], exclude_cols=['created_at'], filters=[GameLineup.game_id == game_id])
        results['batting_stats'] = self._sync_simple_table(GameBattingStat, ['game_id', 'player_id', 'appearance_seq'], exclude_cols=['created_at'], filters=[GameBattingStat.game_id == game_id])
        results['pitching_stats'] = self._sync_simple_table(GamePitchingStat, ['game_id', 'player_id', 'appearance_seq'], exclude_cols=['created_at'], filters=[GamePitchingStat.game_id == game_id])
        results['play_by_play'] = self._sync_game_play_by_play(filters=[GamePlayByPlay.game_id == game_id])
        results['events'] = self._sync_simple_table(GameEvent, ['game_id', 'event_seq'], exclude_cols=['created_at'], filters=[GameEvent.game_id == game_id])
        results['summary'] = self._sync_simple_table(GameSummary, ['game_id', 'summary_type', 'player_name', 'detail_text'], exclude_cols=['created_at', 'id'], filters=[GameSummary.game_id == game_id])
        
        return results

    def _sync_game_play_by_play(self, filters: List = None) -> int:
        from src.models.game import GamePlayByPlay

        query = self.sqlite_session.query(GamePlayByPlay)
        if filters:
            for filter_clause in filters:
                query = query.filter(filter_clause)

        rows = query.all()
        game_ids = sorted({row.game_id for row in rows})
        if not game_ids:
            return 0

        for batch in self._chunked(game_ids, 500):
            self.target_session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id.in_(batch)).delete(
                synchronize_session=False
            )

        mappings = []
        for row in rows:
            mappings.append(
                {
                    "game_id": row.game_id,
                    "inning": row.inning,
                    "inning_half": row.inning_half,
                    "pitcher_name": row.pitcher_name,
                    "batter_name": row.batter_name,
                    "play_description": row.play_description,
                    "event_type": row.event_type,
                    "result": row.result,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                }
            )

        if mappings:
            self.target_session.execute(GamePlayByPlay.__table__.insert(), mappings)
        self.target_session.commit()
        print(f"✅ Synced {len(mappings)} game_play_by_play rows to OCI")
        return len(mappings)

    def _sync_simple_table(
        self, 
        model: Type, 
        conflict_keys: List[str], 
        exclude_cols: List[str] = None, 
        filters: List = None,
        transform_fn: Optional[Callable] = None
    ) -> int:
        """Generic sync parameter for simple tables using Batched UPSERT or COPY"""
        if exclude_cols is None:
            exclude_cols = []
            
        columns = [c.key for c in model.__table__.columns if c.key not in exclude_cols and c.key not in ('created_at', 'updated_at')]
        
        query = self.sqlite_session.query(model)
        if filters:
            query = query.filter(*filters)
            
        total_count = query.count()
        if total_count == 0:
            print(f"ℹ️  No records for {model.__tablename__}")
            return 0
            
        print(f"🚚 Syncing {model.__tablename__} ({total_count} rows)...")
        
        # Always use Bulk COPY Upsert to be safe from schema mismatches (e.g. created_at/updated_at missing on OCI)
        batch_size = 10000
        synced = 0
        for offset in range(0, total_count, batch_size):
            rows = query.offset(offset).limit(batch_size).all()
            records = []
            for row in rows:
                data = {c: getattr(row, c) for c in columns if hasattr(row, c)}
                
                # Apply transformation if provided
                if transform_fn:
                    data = transform_fn(data)

                # Handle JSON/Dict serialization for COPY
                for k, v in data.items():
                    if isinstance(v, (dict, list)):
                        data[k] = json.dumps(v, ensure_ascii=False)
                records.append(data)
            
            # Deduplicate records to avoid postgres error
            if conflict_keys:
                seen = set()
                deduped_records = []
                for r in records:
                    key = tuple(r.get(k) for k in conflict_keys)
                    if key not in seen:
                        seen.add(key)
                        deduped_records.append(r)
                records = deduped_records

            self._bulk_copy_upsert(
                model.__tablename__, 
                records, 
                conflict_keys, 
                update_timestamp=('updated_at' not in exclude_cols)
            )
            synced += len(records)
            print(f"   Synced {synced}/{total_count} rows via COPY...")
            
        return synced


    def sync_crawl_runs(self) -> int:
        """Sync crawl runs from SQLite to OCI"""
        from src.models.crawl import CrawlRun
        from src.cli.sync_oci import clone_row
        
        runs = self.sqlite_session.query(CrawlRun).all()
        if not runs:
            return 0
            
        synced = 0
        for run in runs:
            clone = clone_row(run, CrawlRun)
            self.target_session.merge(clone)
            synced += 1
            if synced % 100 == 0:
                self.target_session.commit()
                
        self.target_session.commit()
        print(f"✅ Synced {synced} crawl run records to OCI")
        return synced

    def _get_season_map(self) -> Dict[tuple, int]:
        """Fetch OCI season mapping (year, league_type_code) -> season_id via raw SQL"""
        # We try different table names just in case, but using raw SQL is faster than reflection
        queries = [
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons",
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons_meta",
            "SELECT season_id, season_year, league_type_code FROM seasons"
        ]
        
        for q in queries:
            try:
                rows = self.target_session.execute(text(q)).all()
                return {(row.season_year, row.league_type_code): row.season_id for row in rows}
            except Exception:
                continue
        
        print("⚠️ Warning: Could not fetch season map from OCI")
        return {}

    def _bulk_copy_upsert(self, table_name: str, records: List[Dict[str, Any]], unique_cols: List[str], update_timestamp: bool = True):
        """
        Perform bulk UPSERT using Postgres COPY + Temp Table.
        Significantly faster than INSERT VALUES for large datasets.
        """
        if not records:
            return

        import csv
        import io
        import random
        from datetime import datetime
        
        # Ensure we have a raw connection for COPY
        connection = self.oci_engine.raw_connection()
        cursor = connection.cursor()

        try:
            # 1. Prepare Data
            keys = list(records[0].keys())
            output = io.StringIO()
            # Use NULL marker for CSV (empty string will be NULL)
            writer = csv.DictWriter(output, fieldnames=keys, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            writer.writerows(records)
            output.seek(0)
            
            # 2. Create Temp Table matching target schema
            suffix = random.randint(1000, 9999)
            temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}_{suffix}"
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS)")
            
            # 3. COPY data to Temp Table
            columns_str = ", ".join([f'"{k}"' for k in keys])
            cursor.copy_expert(
                f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '')", 
                output
            )
            
            # 4. UPSERT from Temp Table to Target Table
            update_cols = [k for k in keys if k not in unique_cols and k not in ('created_at', 'updated_at', 'id')]
            
            if not update_cols:
                conflict_action = "DO NOTHING"
            else:
                set_clause = ", ".join([f'"{k}" = EXCLUDED."{k}"' for k in update_cols])
                if update_timestamp:
                    set_clause += ', "updated_at" = CURRENT_TIMESTAMP'
                conflict_target = ", ".join([f'"{k}"' for k in unique_cols])
                conflict_action = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}"

            cols_list = ", ".join([f'"{k}"' for k in keys])
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
            print(f"❌ Batch COPY Error on {table_name}: {e}")
            raise e
        finally:
            cursor.close()
            connection.close()

    def sync_standings(self, year: int = None, days: int = None) -> int:
        """Sync calculated daily standings snapshots to OCI"""
        from src.models.standings import TeamStandingsDaily
        from src.models.base import Base
        print("📁 Ensure Standings table exists on OCI...")
        Base.metadata.create_all(self.oci_engine) # Ensure table exists
        
        filters = []
        if year:
            from sqlalchemy import extract
            filters.append(extract('year', TeamStandingsDaily.standings_date) == year)
        if days:
            from datetime import datetime, timedelta
            since_date = (datetime.now() - timedelta(days=days)).date()
            filters.append(TeamStandingsDaily.standings_date >= since_date)
            
        return self._sync_simple_table(
            TeamStandingsDaily,
            ['standings_date', 'team_code'],
            exclude_cols=['created_at', 'id'],
            filters=filters
        )

    def sync_awards(self) -> int:
        """Sync awards from SQLite to OCI"""
        # Ensure table exists
        try:
            migration_path = Path("migrations/supabase/019_create_awards.sql")
            if migration_path.exists():
                sql = migration_path.read_text()
                self.target_session.execute(text(sql))
                self.target_session.commit()
                print("✅ Applied awards migration")
        except Exception as e:
            print(f"⚠️ Failed to apply migration: {e}")
            self.target_session.rollback()

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
            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} awards to OCI")
        return synced

    def sync_matchups(self, year: int = None) -> Dict[str, int]:
        """Sync Matchup Split tables (Batter/Pitcher vs Team, Stadium, Starter) to OCI"""
        from src.models.base import Base
        print("📁 Ensuring matchup tables exist on OCI...")
        Base.metadata.create_all(self.oci_engine)

        results = {}
        filters = [text(f"season_year = {year}")] if year else None

        # 1. Batter Team Splits
        results['batter_team'] = self._sync_simple_table(
            BatterTeamSplit,
            ['season_year', 'league_type_code', 'player_id', 'team_code', 'opponent_team_code'],
            exclude_cols=['created_at', 'id'],
            filters=filters
        )

        # 2. Pitcher Team Splits
        results['pitcher_team'] = self._sync_simple_table(
            PitcherTeamSplit,
            ['season_year', 'league_type_code', 'player_id', 'team_code', 'opponent_team_code'],
            exclude_cols=['created_at', 'id'],
            filters=filters
        )

        # 3. Batter Stadium Splits
        results['batter_stadium'] = self._sync_simple_table(
            BatterStadiumSplit,
            ['season_year', 'league_type_code', 'player_id', 'team_code', 'stadium_name'],
            exclude_cols=['created_at', 'id'],
            filters=filters
        )

        # 4. Batter vs Starter (Starter Heuristic)
        results['batter_vs_starter'] = self._sync_simple_table(
            BatterVsStarter,
            ['season_year', 'league_type_code', 'player_id', 'pitcher_name'],
            exclude_cols=['created_at', 'id'],
            filters=filters
        )

        print(f"✅ Matchup Splits Sync Summary: {results}")
        return results

    def close(self):

        """Close OCI session"""
        self.target_session.close()
        self.oci_engine.dispose()


def main():
    """타자 및 투수 데이터 OCI 동기화"""
    from src.db.engine import SessionLocal

    # Get OCI URL from environment
    oci_url = os.getenv('OCI_DB_URL')
    if not oci_url:
        print("❌ OCI_DB_URL environment variable not set")
        print("   Set it in .env file or export it:")
        print("   export OCI_DB_URL='postgresql://user:pass@host:5432/dbname'")
        return

    print("\n" + "🔄" * 30)
    print("KBO 데이터 OCI 동기화")
    print("🔄" * 30 + "\n")

    with SessionLocal() as sqlite_session:
        try:
            sync = OCISync(oci_url, sqlite_session)

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

            # OCI 데이터 샘플 표시
            sync.show_supabase_data_sample()

            print("\n" + "=" * 50)
            print("📈 동기화 완료")
            print("=" * 50)
            print(f"총 동기화된 데이터: {total_synced}건")
            if batting_count > 0:
                print(f"  - 타자 데이터: {batting_count}건")
            if pitching_count > 0:
                print(f"  - 투수 데이터: {pitching_count}건")
            print("\n🎉 OCI에서 데이터를 확인할 수 있습니다!")

            sync.close()

        except Exception as e:
            print(f"\n❌ Sync error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
