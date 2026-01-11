"""
Sync validated data from SQLite to Supabase (PostgreSQL)
Dual-repository pattern: SQLite (dev/validation) → Supabase (production)
"""
import os
import json
from typing import List, Dict, Any
from sqlalchemy import create_engine, text, select, MetaData, Table, column, table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

# 현재 사용 가능한 모델들만 import
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic
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
        pitcher_data = self.sqlite_session.query(PlayerSeasonPitching).all()
        
        if not pitcher_data:
            print("ℹ️ 동기화할 투수 데이터가 없습니다.")
            return 0
        
        synced = 0
        
        for data in pitcher_data:
            # UPSERT 쿼리 직접 실행 (PostgreSQL)
            upsert_sql = text("""
                INSERT INTO player_season_pitching (
                    player_id, season, league, level, source, team_code,
                    games, games_started, wins, losses, saves, holds,
                    innings_pitched, innings_outs, hits_allowed, runs_allowed,
                    earned_runs, home_runs_allowed, walks_allowed, intentional_walks,
                    hit_batters, strikeouts, wild_pitches, balks,
                    era, whip, fip, k_per_nine, bb_per_nine, kbb,
                    complete_games, shutouts, quality_starts, blown_saves,
                    tbf, np, avg_against, doubles_allowed, triples_allowed,
                    sacrifices_allowed, sacrifice_flies_allowed, extra_stats,
                    created_at, updated_at
                ) VALUES (
                    :player_id, :season, :league, :level, :source, :team_code,
                    :games, :games_started, :wins, :losses, :saves, :holds,
                    :innings_pitched, :innings_outs, :hits_allowed, :runs_allowed,
                    :earned_runs, :home_runs_allowed, :walks_allowed, :intentional_walks,
                    :hit_batters, :strikeouts, :wild_pitches, :balks,
                    :era, :whip, :fip, :k_per_nine, :bb_per_nine, :kbb,
                    :complete_games, :shutouts, :quality_starts, :blown_saves,
                    :tbf, :np, :avg_against, :doubles_allowed, :triples_allowed,
                    :sacrifices_allowed, :sacrifice_flies_allowed, :extra_stats,
                    NOW(), NOW()
                )
                ON CONFLICT (player_id, season, league, level) DO UPDATE SET
                    source = EXCLUDED.source,
                    team_code = EXCLUDED.team_code,
                    games = EXCLUDED.games,
                    games_started = EXCLUDED.games_started,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    saves = EXCLUDED.saves,
                    holds = EXCLUDED.holds,
                    innings_pitched = EXCLUDED.innings_pitched,
                    innings_outs = EXCLUDED.innings_outs,
                    hits_allowed = EXCLUDED.hits_allowed,
                    runs_allowed = EXCLUDED.runs_allowed,
                    earned_runs = EXCLUDED.earned_runs,
                    home_runs_allowed = EXCLUDED.home_runs_allowed,
                    walks_allowed = EXCLUDED.walks_allowed,
                    intentional_walks = EXCLUDED.intentional_walks,
                    hit_batters = EXCLUDED.hit_batters,
                    strikeouts = EXCLUDED.strikeouts,
                    wild_pitches = EXCLUDED.wild_pitches,
                    balks = EXCLUDED.balks,
                    era = EXCLUDED.era,
                    whip = EXCLUDED.whip,
                    fip = EXCLUDED.fip,
                    k_per_nine = EXCLUDED.k_per_nine,
                    bb_per_nine = EXCLUDED.bb_per_nine,
                    kbb = EXCLUDED.kbb,
                    complete_games = EXCLUDED.complete_games,
                    shutouts = EXCLUDED.shutouts,
                    quality_starts = EXCLUDED.quality_starts,
                    blown_saves = EXCLUDED.blown_saves,
                    tbf = EXCLUDED.tbf,
                    np = EXCLUDED.np,
                    avg_against = EXCLUDED.avg_against,
                    doubles_allowed = EXCLUDED.doubles_allowed,
                    triples_allowed = EXCLUDED.triples_allowed,
                    sacrifices_allowed = EXCLUDED.sacrifices_allowed,
                    sacrifice_flies_allowed = EXCLUDED.sacrifice_flies_allowed,
                    extra_stats = EXCLUDED.extra_stats,
                    updated_at = NOW()
            """)
            
            # 데이터 준비
            params = {
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
                'extra_stats': json.dumps(data.extra_stats) if data.extra_stats else None
            }
            
            # UPSERT 실행
            self.supabase_session.execute(upsert_sql, params)
            synced += 1
            
            if synced % 10 == 0:
                print(f"   📝 {synced}건 동기화 중...")
        
        self.supabase_session.commit()
        print(f"✅ Supabase 투수 데이터 동기화 완료: {synced}건")
        return synced

    def sync_batting_data(self) -> int:
        """타자 데이터를 Supabase로 동기화"""
        batting_data = self.sqlite_session.query(PlayerSeasonBatting).all()
        
        if not batting_data:
            print("ℹ️ 동기화할 타자 데이터가 없습니다.")
            return 0
        
        synced = 0
        
        for data in batting_data:
            # UPSERT 쿼리 직접 실행 (PostgreSQL)
            upsert_sql = text("""
                INSERT INTO player_season_batting (
                    player_id, season, league, level, source, team_code,
                    games, plate_appearances, at_bats, runs, hits, doubles,
                    triples, home_runs, rbi, walks, intentional_walks, hbp,
                    strikeouts, stolen_bases, caught_stealing, sacrifice_hits,
                    sacrifice_flies, gdp, avg, obp, slg, ops, iso, babip,
                    extra_stats, created_at, updated_at
                ) VALUES (
                    :player_id, :season, :league, :level, :source, :team_code,
                    :games, :plate_appearances, :at_bats, :runs, :hits, :doubles,
                    :triples, :home_runs, :rbi, :walks, :intentional_walks, :hbp,
                    :strikeouts, :stolen_bases, :caught_stealing, :sacrifice_hits,
                    :sacrifice_flies, :gdp, :avg, :obp, :slg, :ops, :iso, :babip,
                    :extra_stats, NOW(), NOW()
                )
                ON CONFLICT (player_id, season, league, level) DO UPDATE SET
                    source = EXCLUDED.source,
                    team_code = EXCLUDED.team_code,
                    games = EXCLUDED.games,
                    plate_appearances = EXCLUDED.plate_appearances,
                    at_bats = EXCLUDED.at_bats,
                    runs = EXCLUDED.runs,
                    hits = EXCLUDED.hits,
                    doubles = EXCLUDED.doubles,
                    triples = EXCLUDED.triples,
                    home_runs = EXCLUDED.home_runs,
                    rbi = EXCLUDED.rbi,
                    walks = EXCLUDED.walks,
                    intentional_walks = EXCLUDED.intentional_walks,
                    hbp = EXCLUDED.hbp,
                    strikeouts = EXCLUDED.strikeouts,
                    stolen_bases = EXCLUDED.stolen_bases,
                    caught_stealing = EXCLUDED.caught_stealing,
                    sacrifice_hits = EXCLUDED.sacrifice_hits,
                    sacrifice_flies = EXCLUDED.sacrifice_flies,
                    gdp = EXCLUDED.gdp,
                    avg = EXCLUDED.avg,
                    obp = EXCLUDED.obp,
                    slg = EXCLUDED.slg,
                    ops = EXCLUDED.ops,
                    iso = EXCLUDED.iso,
                    babip = EXCLUDED.babip,
                    extra_stats = EXCLUDED.extra_stats,
                    updated_at = NOW()
            """)
            
            # 데이터 준비
            params = {
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
                'extra_stats': json.dumps(data.extra_stats) if data.extra_stats else None
            }
            
            # UPSERT 실행
            self.supabase_session.execute(upsert_sql, params)
            synced += 1
            
            if synced % 10 == 0:
                print(f"   📝 {synced}건 동기화 중...")
        
        self.supabase_session.commit()
        print(f"✅ Supabase 타자 데이터 동기화 완료: {synced}건")
        return synced

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
            
        for r in rosters:
            data = {
                'roster_date': r.roster_date,
                'team_code': r.team_code,
                'player_id': r.player_id,
                'player_name': r.player_name,
                'position': r.position,
                'back_number': r.back_number
            }
            
            stmt = pg_insert(TeamDailyRoster).values(**data)
            
            # Update fields on conflict
            update_dict = {
                'player_name': stmt.excluded.player_name,
                'position': stmt.excluded.position,
                'back_number': stmt.excluded.back_number,
                'updated_at': text('CURRENT_TIMESTAMP')
            }
            
            # Constraint name 'uq_team_daily_roster' maps to (roster_date, team_code, player_id)
            stmt = stmt.on_conflict_do_update(
                constraint='uq_team_daily_roster',
                set_=update_dict
            )
            
            self.supabase_session.execute(stmt)
            synced += 1
            
        self.supabase_session.commit()
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
        player_basic_table = Table('player_basic', metadata, autoload_with=self.supabase_engine)
        teams_table = Table('teams', metadata, autoload_with=self.supabase_engine)
        try:
            seasons_table = Table('kbo_seasons', metadata, autoload_with=self.supabase_engine)
        except Exception:
            seasons_table = Table('kbo_seasons_meta', metadata, autoload_with=self.supabase_engine)
        pitching_table = Table('player_season_pitching', metadata, autoload_with=self.supabase_engine)

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
        """Sync game detail data from SQLite to Supabase"""
        query = self.sqlite_session.query(Game)
        if limit:
            query = query.limit(limit)

        games = query.all()
        synced = 0

        # Define a lightweight table object to avoid Model schema mismatches (e.g. missing created_at)
        game_table = table("game",
            column("game_id"),
            column("game_date"),
            column("home_team"),
            column("away_team"),
            column("stadium"),
            column("home_score"),
            column("away_score"),
            column("winning_team"),
            column("winning_score"),
            column("season_id"),
            column("home_pitcher"),
            column("away_pitcher")
        )

        for game in games:
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

            stmt = pg_insert(game_table).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'game_id'}
            # Note: We are ignoring updated_at for now to avoid invalid column errors.
            # If the column exists, it won't be updated, which is acceptable for now.
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id'],
                set_=update_dict
            )

            self.supabase_session.execute(stmt)
            synced += 1

        self.supabase_session.commit()
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

    def sync_game_details(self, days: int = None) -> Dict[str, int]:
        """Sync all game detail tables to Supabase"""
        results = {}
        
        # 0. Sync Parent Games first (Required for Foreign Keys)
        print("⚾ Syncing Parent Game Records...")
        results['games'] = self.sync_games()

        # 1. Game Metadata
        results['metadata'] = self._sync_simple_table(
            GameMetadata, 
            ['game_id'], 
            exclude_cols=['created_at']
        )

        # 2. Inning Scores
        results['inning_scores'] = self._sync_simple_table(
            GameInningScore,
            ['game_id', 'team_side', 'inning'],
            exclude_cols=['id', 'created_at']
        )

        # 3. Lineups
        results['lineups'] = self._sync_simple_table(
            GameLineup,
            ['game_id', 'team_side', 'appearance_seq'],
             exclude_cols=['id', 'created_at']
        )

        # 4. Batting Stats
        results['batting_stats'] = self._sync_simple_table(
            GameBattingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['id', 'created_at']
        )

        # 5. Pitching Stats
        results['pitching_stats'] = self._sync_simple_table(
            GamePitchingStat,
            ['game_id', 'player_id', 'appearance_seq'],
            exclude_cols=['id', 'created_at']
        )

        results['events'] = self._sync_simple_table(
            GameEvent,
            ['game_id', 'event_seq'],
            exclude_cols=['id', 'created_at']
        )

        # 7. Game Summary (New)
        results['summary'] = self._sync_simple_table(
            GameSummary,
            ['game_id', 'summary_type', 'detail_text'],
            exclude_cols=['id', 'created_at']
        )
        
        print(f"✅ Game Details Sync Summary: {results}")
        return results

    def _sync_simple_table(self, model, conflict_keys: List[str], exclude_cols: List[str] = None) -> int:
        """Generic sync parameter for simple tables"""
        if exclude_cols is None:
            exclude_cols = []
            
        rows = self.sqlite_session.query(model).all()
        if not rows:
            print(f"ℹ️  No records for {model.__tablename__}")
            return 0
            
        synced = 0
        table_name = model.__tablename__
        columns = [c.key for c in model.__table__.columns if c.key not in exclude_cols and c.key not in ('created_at', 'updated_at')]
        
        # Build SQL dynamically
        col_str = ", ".join(columns)
        val_str = ", ".join([f":{c}" for c in columns])
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_keys])
        
        upsert_sql = text(f"""
            INSERT INTO {table_name} ({col_str}, updated_at)
            VALUES ({val_str}, NOW())
            ON CONFLICT ({", ".join(conflict_keys)}) 
            DO UPDATE SET {update_set}, updated_at = NOW()
        """)
        
        for row in rows:
            data = {c: getattr(row, c) for c in columns}
            # Handle JSON serialization if needed (sqlalchemy might handle it, but being safe)
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    data[k] = json.dumps(v, ensure_ascii=False)
            
            try:
                self.supabase_session.execute(upsert_sql, data)
                synced += 1
            except Exception as e:
                self.supabase_session.rollback()
                print(f"❌ Error syncing {table_name} row: {e}")
                
        self.supabase_session.commit()
        print(f"✅ Synced {synced} rows to {table_name}")
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

