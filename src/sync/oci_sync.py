"""
Sync validated data from SQLite to OCI (Oracle Cloud Infrastructure) PostgreSQL
Dual-repository pattern: SQLite (dev/validation) → OCI (production)
"""
import os
import json
from typing import List, Dict, Any, Optional, Callable, Type
from pathlib import Path
from sqlalchemy import bindparam, create_engine, text, select, MetaData, Table, column, table
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
    GameSummary,
    GameIdAlias,
)
from src.models.matchup import BatterTeamSplit, PitcherTeamSplit, BatterStadiumSplit, BatterVsStarter
from src.models.rankings import StatRanking
from src.utils.game_status import GAME_STATUS_SCHEDULED


LEAGUE_NAME_TO_CODE = {
    "REGULAR": 0,
    "EXHIBITION": 1,
    "WILDCARD": 2,
    "SEMI_PLAYOFF": 3,
    "PLAYOFF": 4,
    "KOREAN_SERIES": 5,
}

GAME_SIGNATURE_CHILD_TABLES = (
    "game_metadata",
    "game_inning_scores",
    "game_lineups",
    "game_events",
    "game_summary",
    "game_play_by_play",
)


def _serialize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _dedupe_records_for_conflict_keys(
    records: List[Dict[str, Any]],
    conflict_keys: List[str],
) -> List[Dict[str, Any]]:
    """Mirror Postgres unique semantics while removing duplicate upsert keys.

    Postgres unique indexes allow multiple rows when any indexed column is NULL.
    Python tuple-based dedupe would otherwise collapse rows such as away/home
    pitching lines that share ``(game_id, NULL, appearance_seq)``.
    """
    if not conflict_keys:
        return records

    seen = set()
    deduped_records = []
    for record in records:
        key = tuple(record.get(column) for column in conflict_keys)
        if any(value is None for value in key):
            deduped_records.append(record)
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped_records.append(record)
    return deduped_records


def _execute_signature_query(session_or_conn, sql: str, *, game_ids: List[str] | None = None):
    stmt = text(sql)
    params = {}
    if game_ids is not None:
        stmt = stmt.bindparams(bindparam("game_ids", expanding=True))
        params["game_ids"] = list(game_ids)
    return session_or_conn.execute(stmt, params)


def load_game_sync_signatures(session_or_conn, *, game_ids: List[str] | None = None) -> Dict[str, Dict[str, Any]]:
    filter_sql = "WHERE g.game_id IN :game_ids" if game_ids is not None else ""
    game_rows = _execute_signature_query(
        session_or_conn,
        f"""
        SELECT
            g.game_id,
            g.game_status,
            g.home_score,
            g.away_score,
            g.home_pitcher,
            g.away_pitcher,
            g.home_team,
            g.away_team,
            g.updated_at
        FROM game g
        {filter_sql}
        """,
        game_ids=game_ids,
    ).mappings().all()

    signatures: Dict[str, Dict[str, Any]] = {}
    for row in game_rows:
        game_id = str(row["game_id"])
        signatures[game_id] = {
            "game": {
                "game_status": row["game_status"],
                "home_score": row["home_score"],
                "away_score": row["away_score"],
                "home_pitcher": row["home_pitcher"],
                "away_pitcher": row["away_pitcher"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "updated_at": _serialize_scalar(row["updated_at"]),
            }
        }
        for table_name in GAME_SIGNATURE_CHILD_TABLES:
            signatures[game_id][table_name] = {
                "row_count": 0,
                "max_updated_at": None,
            }

    if not signatures:
        return signatures

    child_game_ids = sorted(signatures.keys())
    metadata_rows = _execute_signature_query(
        session_or_conn,
        """
        SELECT
            g.game_id,
            COUNT(g.game_id) AS row_count,
            MAX(g.updated_at) AS max_updated_at,
            MAX(g.start_time) AS start_time
        FROM game_metadata g
        WHERE g.game_id IN :game_ids
        GROUP BY g.game_id
        """,
        game_ids=child_game_ids,
    ).mappings().all()
    for row in metadata_rows:
        signatures[str(row["game_id"])]["game_metadata"] = {
            "row_count": int(row["row_count"] or 0),
            "max_updated_at": _serialize_scalar(row["max_updated_at"]),
            "start_time": _serialize_scalar(row["start_time"]),
        }

    for table_name in GAME_SIGNATURE_CHILD_TABLES:
        if table_name == "game_metadata":
            continue
        rows = _execute_signature_query(
            session_or_conn,
            f"""
            SELECT
                t.game_id,
                COUNT(*) AS row_count,
                MAX(t.updated_at) AS max_updated_at
            FROM {table_name} t
            WHERE t.game_id IN :game_ids
            GROUP BY t.game_id
            """,
            game_ids=child_game_ids,
        ).mappings().all()
        for row in rows:
            signatures[str(row["game_id"])][table_name] = {
                "row_count": int(row["row_count"] or 0),
                "max_updated_at": _serialize_scalar(row["max_updated_at"]),
            }

    return signatures


def detect_dirty_game_ids(local_session_or_conn, remote_session_or_conn, *, game_ids: List[str] | None = None) -> List[str]:
    local_signatures = load_game_sync_signatures(local_session_or_conn, game_ids=game_ids)
    remote_signatures = load_game_sync_signatures(remote_session_or_conn, game_ids=list(local_signatures.keys()))

    dirty: List[str] = []
    for game_id, local_signature in local_signatures.items():
        remote_signature = remote_signatures.get(game_id)
        if remote_signature is None:
            dirty.append(game_id)
            continue

        local_game = local_signature["game"]
        remote_game = remote_signature.get("game", {})
        for key in ("game_status", "home_score", "away_score", "home_pitcher", "away_pitcher", "home_team", "away_team"):
            if local_game.get(key) != remote_game.get(key):
                dirty.append(game_id)
                break
        else:
            if (
                local_game.get("updated_at") is not None
                and (
                    remote_game.get("updated_at") is None
                    or str(local_game.get("updated_at")) > str(remote_game.get("updated_at"))
                )
            ):
                dirty.append(game_id)
                continue

        if dirty and dirty[-1] == game_id:
            continue

        metadata_local = local_signature.get("game_metadata", {})
        metadata_remote = remote_signature.get("game_metadata", {})
        if metadata_local.get("row_count") != metadata_remote.get("row_count"):
            dirty.append(game_id)
            continue
        if metadata_local.get("start_time") != metadata_remote.get("start_time"):
            dirty.append(game_id)
            continue
        if (
            metadata_local.get("max_updated_at") is not None
            and (
                metadata_remote.get("max_updated_at") is None
                or str(metadata_local.get("max_updated_at")) > str(metadata_remote.get("max_updated_at"))
            )
        ):
            dirty.append(game_id)
            continue

        for table_name in GAME_SIGNATURE_CHILD_TABLES:
            if table_name == "game_metadata":
                continue
            local_child = local_signature.get(table_name, {})
            remote_child = remote_signature.get(table_name, {})
            if local_child.get("row_count") != remote_child.get("row_count"):
                dirty.append(game_id)
                break
            if (
                local_child.get("max_updated_at") is not None
                and (
                    remote_child.get("max_updated_at") is None
                    or str(local_child.get("max_updated_at")) > str(remote_child.get("max_updated_at"))
                )
            ):
                dirty.append(game_id)
                break

    return dirty


def filter_game_ids_by_year(game_ids: List[str], year: int | None) -> List[str]:
    if year is None:
        return list(game_ids)
    prefix = str(int(year))
    return [game_id for game_id in game_ids if str(game_id).startswith(prefix)]


def filter_publishable_game_ids(session, game_ids: List[str]) -> List[str]:
    """Restrict parent-game sync to rows that are more than schedule-only stubs."""
    if not game_ids:
        return []

    rows = (
        session.query(
            Game.game_id,
            Game.game_status,
            Game.home_score,
            Game.away_score,
        )
        .filter(Game.game_id.in_(game_ids))
        .all()
    )
    publishable: List[str] = []
    for game_id, game_status, home_score, away_score in rows:
        if home_score is not None or away_score is not None:
            publishable.append(game_id)
            continue
        if str(game_status or "").upper() != GAME_STATUS_SCHEDULED:
            publishable.append(game_id)
            continue

        has_detail = any(
            session.query(model.game_id).filter(model.game_id == game_id).first() is not None
            for model in (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat, GameEvent, GameSummary)
        )
        if has_detail:
            publishable.append(game_id)

    return sorted(set(publishable))


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
        """Sync master player records (players table) from SQLite to OCI"""
        from src.models.player import Player
        players = self.sqlite_session.query(Player).all()
        synced = 0

        print(f"🚚 Syncing Master Players ({len(players)} rows)...")

        for player in players:
            # Map all relevant fields including the new photo_url and profile details
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
                'photo_url': player.photo_url,
                'salary_original': player.salary_original,
                'signing_bonus_original': player.signing_bonus_original,
                'draft_info': player.draft_info,
            }

            stmt = pg_insert(Player).values(**data)
            
            # Update all fields on conflict except kbo_person_id
            update_dict = {k: v for k, v in data.items() if k != 'kbo_person_id'}
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['kbo_person_id'],
                set_=update_dict
            )

            self.target_session.execute(stmt)
            synced += 1
            if synced % 500 == 0:
                self.target_session.commit()
                print(f"   Synced {synced} players...")

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
                'movement_date': m.movement_date,
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
                'extra_stats': stat.extra_stats if stat.extra_stats not in [None, 'null', 'None'] else None,
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

    def sync_player_season_batting(self, limit: int = None) -> int:
        """Sync player_season_batting data from SQLite to OCI using bulk upsert"""
        query = self.sqlite_session.query(PlayerSeasonBatting)
        if limit:
            query = query.limit(limit)

        pitcher_stats = query.all()
        total = len(pitcher_stats)
        synced = 0
        chunk_size = 500

        print(f"  - Starting bulk sync for {total} batting records...")

        for i in range(0, total, chunk_size):
            chunk = pitcher_stats[i:i + chunk_size]
            data_list = []
            for stat in chunk:
                data_list.append({
                    'player_id': stat.player_id,
                    'season': stat.season,
                    'league': stat.league,
                    'level': stat.level,
                    'source': stat.source,
                    'team_code': stat.team_code,
                    'franchise_id': stat.franchise_id,
                    'canonical_team_code': stat.canonical_team_code,
                    'games': stat.games,
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
                    'extra_stats': stat.extra_stats if stat.extra_stats not in [None, 'null', 'None'] else None,
                })

            if data_list:
                stmt = pg_insert(PlayerSeasonBatting).values(data_list)
                update_cols = {k: stmt.excluded[k] for k in data_list[0].keys() if k not in ['player_id', 'season', 'league', 'level']}
                stmt = stmt.on_conflict_do_update(
                    index_elements=['player_id', 'season', 'league', 'level'],
                    set_=update_cols
                )
                self.target_session.execute(stmt)
                synced += len(data_list)
                self.target_session.commit()
                print(f"    ✅ Synced {synced}/{total} batting records...")

        print(f"  ✅ Finished syncing {synced} player_season_batting records to OCI")
        return synced

    def sync_player_season_pitching(self, limit: int = None) -> int:
        """Sync player_season_pitching data from SQLite to OCI using bulk upsert"""
        query = self.sqlite_session.query(PlayerSeasonPitching)
        if limit:
            query = query.limit(limit)

        pitching_stats = query.all()
        total = len(pitching_stats)
        synced = 0
        chunk_size = 500

        print(f"  - Starting bulk sync for {total} pitching records...")

        for i in range(0, total, chunk_size):
            chunk = pitching_stats[i:i + chunk_size]
            data_list = []
            for stat in chunk:
                data_list.append({
                    'player_id': stat.player_id,
                    'season': stat.season,
                    'league': stat.league,
                    'level': stat.level,
                    'source': stat.source,
                    'team_code': stat.team_code,
                    'franchise_id': stat.franchise_id,
                    'canonical_team_code': stat.canonical_team_code,
                    'games': stat.games,
                    'games_started': stat.games_started,
                    'wins': stat.wins,
                    'losses': stat.losses,
                    'saves': stat.saves,
                    'holds': stat.holds,
                    'innings_pitched': stat.innings_pitched,
                    'innings_outs': stat.innings_outs,
                    'hits_allowed': stat.hits_allowed,
                    'runs_allowed': stat.runs_allowed,
                    'earned_runs': stat.earned_runs,
                    'home_runs_allowed': stat.home_runs_allowed,
                    'walks_allowed': stat.walks_allowed,
                    'intentional_walks': stat.intentional_walks,
                    'hit_batters': stat.hit_batters,
                    'strikeouts': stat.strikeouts,
                    'wild_pitches': stat.wild_pitches,
                    'balks': stat.balks,
                    'era': stat.era,
                    'whip': stat.whip,
                    'fip': stat.fip,
                    'k_per_nine': stat.k_per_nine,
                    'bb_per_nine': stat.bb_per_nine,
                    'kbb': stat.kbb,
                    'complete_games': stat.complete_games,
                    'shutouts': stat.shutouts,
                    'quality_starts': stat.quality_starts,
                    'blown_saves': stat.blown_saves,
                    'tbf': stat.tbf,
                    'np': stat.np,
                    'avg_against': stat.avg_against,
                    'doubles_allowed': stat.doubles_allowed,
                    'triples_allowed': stat.triples_allowed,
                    'sacrifices_allowed': stat.sacrifices_allowed,
                    'sacrifice_flies_allowed': stat.sacrifice_flies_allowed,
                    'extra_stats': stat.extra_stats if stat.extra_stats not in [None, 'null', 'None'] else None,
                })

            if data_list:
                stmt = pg_insert(PlayerSeasonPitching).values(data_list)
                update_cols = {k: stmt.excluded[k] for k in data_list[0].keys() if k not in ['player_id', 'season', 'league', 'level']}
                stmt = stmt.on_conflict_do_update(
                    index_elements=['player_id', 'season', 'league', 'level'],
                    set_=update_cols
                )
                self.target_session.execute(stmt)
                synced += len(data_list)
                self.target_session.commit()
                print(f"    ✅ Synced {synced}/{total} pitching records...")

        print(f"  ✅ Finished syncing {synced} player_season_pitching records to OCI")
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
        """Detect dirty game_ids by comparing game + child-table signatures across local/OCI."""
        return detect_dirty_game_ids(self.sqlite_session, self.target_session)

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
            GameIdAlias,
        )
        
        filters = []
        target_game_ids = None

        publishable_parent_game_ids = None

        if unsynced_only:
            print("🔍 식별 중: OCI에 없거나 로컬에서 최근에 갱신된 게임 데이터를 검사합니다...")
            target_game_ids = self.get_unsynced_or_modified_game_ids()
            target_game_ids = filter_game_ids_by_year(target_game_ids, year)
            if not target_game_ids:
                year_msg = f" ({year})" if year else ""
                print(f"🎉 모든 게임 데이터{year_msg}가 이미 최신 상태입니다. 동기화를 건너뜁니다.")
                return results
            print(f"🎯 총 {len(target_game_ids)}개의 변경/누락된 게임을 발견했습니다.")
            # target_game_ids가 너무 길면 sqlite in_ 절 한도를 초과할 수 있지만, 부분 업데이트라 대개 수십 건 내외임.
            filters.append(Game.game_id.in_(target_game_ids))
            publishable_parent_game_ids = filter_publishable_game_ids(self.sqlite_session, target_game_ids)
            skipped_schedule_only = sorted(set(target_game_ids) - set(publishable_parent_game_ids))
            if skipped_schedule_only:
                print(
                    "⚠️ Skipping parent game sync for schedule-only dirty rows: "
                    f"{', '.join(skipped_schedule_only[:10])}"
                    + (" ..." if len(skipped_schedule_only) > 10 else "")
                )
        else:
            if days:
                from datetime import datetime, timedelta
                since_date = (datetime.now() - timedelta(days=days)).date()
                filters.append(Game.game_date >= since_date)
            if year:
                filters.append(Game.game_id.like(f"{year}%"))
            
        # 0. Sync Parent Games first (Required for Foreign Keys)
        print("⚾ Syncing Parent Game Records...")
        if unsynced_only and target_game_ids is not None:
            if publishable_parent_game_ids:
                results['games'] = self.sync_games(filters=[Game.game_id.in_(publishable_parent_game_ids)])
            else:
                results['games'] = 0
                print("ℹ️ No publishable parent game rows beyond schedule-only stubs.")
        else:
            results['games'] = self.sync_games(filters=filters if filters else None)

        alias_filters = None
        if unsynced_only and target_game_ids:
            alias_filters = [GameIdAlias.canonical_game_id.in_(target_game_ids)]
        elif year:
            alias_filters = [GameIdAlias.canonical_game_id.like(f"{year}%")]
        elif days and filters:
            game_ids = [g.game_id for g in self.sqlite_session.query(Game.game_id).filter(*filters).all()]
            alias_filters = [GameIdAlias.canonical_game_id.in_(game_ids)] if game_ids else []

        if alias_filters != []:
            results['game_id_aliases'] = self._sync_simple_table(
                GameIdAlias,
                ['alias_game_id'],
                exclude_cols=['created_at'],
                filters=alias_filters,
            )

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

        if year and not unsynced_only:
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
            GameIdAlias,
        )
        
        results = {}
        filters = [Game.game_id == game_id]

        # Sync Game record
        results['game'] = self._sync_simple_table(Game, ['game_id'], exclude_cols=['created_at', 'updated_at'], filters=filters)
        results['game_id_aliases'] = self._sync_simple_table(GameIdAlias, ['alias_game_id'], exclude_cols=['created_at'], filters=[GameIdAlias.canonical_game_id == game_id])

        # Player IDs can be repaired after an initial crawl. Because Postgres
        # treats NULL values as distinct in unique constraints, an upsert keyed
        # by player_id would otherwise leave stale NULL-player rows beside the
        # repaired rows. For one-game publishing, replace child snapshots.
        for child_model in (
            GameMetadata,
            GameInningScore,
            GameLineup,
            GameBattingStat,
            GamePitchingStat,
            GameEvent,
            GameSummary,
        ):
            self.target_session.query(child_model).filter(child_model.game_id == game_id).delete(
                synchronize_session=False
            )
        self.target_session.commit()
        
        # Sync children
        results['metadata'] = self._sync_simple_table(GameMetadata, ['game_id'], exclude_cols=['created_at'], filters=[GameMetadata.game_id == game_id])
        results['inning_scores'] = self._sync_simple_table(GameInningScore, ['game_id', 'team_side', 'inning'], exclude_cols=['created_at'], filters=[GameInningScore.game_id == game_id])
        results['lineups'] = self._sync_simple_table(GameLineup, ['game_id', 'team_side', 'appearance_seq'], exclude_cols=['id', 'created_at'], filters=[GameLineup.game_id == game_id])
        results['batting_stats'] = self._sync_simple_table(GameBattingStat, ['game_id', 'player_id', 'appearance_seq'], exclude_cols=['id', 'created_at'], filters=[GameBattingStat.game_id == game_id])
        results['pitching_stats'] = self._sync_simple_table(GamePitchingStat, ['game_id', 'player_id', 'appearance_seq'], exclude_cols=['id', 'created_at'], filters=[GamePitchingStat.game_id == game_id])
        results['play_by_play'] = self._sync_game_play_by_play(filters=[GamePlayByPlay.game_id == game_id])
        results['events'] = self._sync_simple_table(GameEvent, ['game_id', 'event_seq'], exclude_cols=['id', 'created_at'], filters=[GameEvent.game_id == game_id])
        results['summary'] = self._sync_simple_table(GameSummary, ['game_id', 'summary_type', 'player_name', 'detail_text'], exclude_cols=['id', 'created_at'], filters=[GameSummary.game_id == game_id])
        
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
            exclude_cols = ['id'] # Default to exclude ID for auto-inc compatibility
        elif 'id' not in exclude_cols:
            exclude_cols.append('id')
            
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
            
            # Deduplicate records to avoid Postgres "affect row a second time"
            # errors, while preserving SQL's distinct-NULL unique semantics.
            records = _dedupe_records_for_conflict_keys(records, conflict_keys)

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

    def sync_stat_rankings(self, year: int | None = None) -> int:
        """Sync derived stat_rankings rows to OCI."""
        filters = [StatRanking.season == year] if year else None
        return self._sync_simple_table(
            StatRanking,
            ["season", "metric", "entity_id", "entity_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
        )

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
