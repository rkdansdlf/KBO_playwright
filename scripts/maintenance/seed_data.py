
import csv
import os
import sys
from pathlib import Path
from typing import Optional, Union
from datetime import datetime

# Ensure project root is on sys.path when run as a script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from sqlalchemy.orm import Session

from src.db.engine import SessionLocal, Engine
from src.models.team import Team
from src.models.season import KboSeason

DEFAULT_TEAMS = [
    {"team_id": "SS", "team_name": "삼성 라이온즈", "team_short_name": "삼성", "city": "대구", "founded_year": 1982, "stadium_name": "대구 삼성 라이온즈 파크"},
    {"team_id": "LT", "team_name": "롯데 자이언츠", "team_short_name": "롯데", "city": "부산", "founded_year": 1982, "stadium_name": "부산 사직 야구장"},
    {"team_id": "MBC", "team_name": "MBC 청룡", "team_short_name": "MBC", "city": "서울", "founded_year": 1982, "stadium_name": "잠실야구장"},
    {"team_id": "LG", "team_name": "LG 트윈스", "team_short_name": "LG", "city": "서울", "founded_year": 1990, "stadium_name": "잠실야구장"},
    {"team_id": "OB", "team_name": "OB 베어스", "team_short_name": "OB", "city": "서울", "founded_year": 1982, "stadium_name": "잠실야구장"},
    {"team_id": "DO", "team_name": "두산 베어스", "team_short_name": "두산", "city": "서울", "founded_year": 1996, "stadium_name": "잠실야구장"},
    {"team_id": "HT", "team_name": "해태 타이거즈", "team_short_name": "해태", "city": "광주", "founded_year": 1982, "stadium_name": "광주 무등경기장 야구장"},
    {"team_id": "KIA", "team_name": "KIA 타이거즈", "team_short_name": "KIA", "city": "광주", "founded_year": 2001, "stadium_name": "광주-기아 챔피언스 필드"},
    {"team_id": "SM", "team_name": "삼미 슈퍼스타즈", "team_short_name": "삼미", "city": "인천", "founded_year": 1982, "stadium_name": "인천공설운동장 야구장"},
    {"team_id": "CB", "team_name": "청보 핀토스", "team_short_name": "청보", "city": "인천", "founded_year": 1985, "stadium_name": "인천공설운동장 야구장"},
    {"team_id": "TP", "team_name": "태평양 돌핀스", "team_short_name": "태평양", "city": "인천", "founded_year": 1988, "stadium_name": "인천공설운동장 야구장"},
    {"team_id": "HU", "team_name": "현대 유니콘스", "team_short_name": "현대", "city": "수원", "founded_year": 1996, "stadium_name": "수원야구장"},
    {"team_id": "WO", "team_name": "우리 히어로즈", "team_short_name": "우리", "city": "서울", "founded_year": 2008, "stadium_name": "목동야구장"},
    {"team_id": "NX", "team_name": "넥센 히어로즈", "team_short_name": "넥센", "city": "서울", "founded_year": 2010, "stadium_name": "고척스카이돔"},
    {"team_id": "KI", "team_name": "키움 히어로즈", "team_short_name": "키움", "city": "서울", "founded_year": 2019, "stadium_name": "고척스카이돔"},
    {"team_id": "BE", "team_name": "빙그레 이글스", "team_short_name": "빙그레", "city": "대전", "founded_year": 1986, "stadium_name": "대전한밭야구장"},
    {"team_id": "HH", "team_name": "한화 이글스", "team_short_name": "한화", "city": "대전", "founded_year": 1993, "stadium_name": "대전 한화생명 이글스 파크"},
    {"team_id": "SL", "team_name": "쌍방울 레이더스", "team_short_name": "쌍방울", "city": "전주", "founded_year": 1990, "stadium_name": "전주야구장"},
    {"team_id": "SK", "team_name": "SK 와이번스", "team_short_name": "SK", "city": "인천", "founded_year": 2000, "stadium_name": "인천문학야구장"},
    {"team_id": "SSG", "team_name": "SSG 랜더스", "team_short_name": "SSG", "city": "인천", "founded_year": 2021, "stadium_name": "인천SSG랜더스필드"},
    {"team_id": "NC", "team_name": "NC 다이노스", "team_short_name": "NC", "city": "창원", "founded_year": 2011, "stadium_name": "창원NC파크"},
    {"team_id": "KT", "team_name": "kt wiz", "team_short_name": "kt", "city": "수원", "founded_year": 2013, "stadium_name": "수원 kt wiz 파크"},
    {"team_id": "EA", "team_name": "나눔 올스타", "team_short_name": "나눔", "city": "KBO", "founded_year": 1982, "stadium_name": "-"},
    {"team_id": "WE", "team_name": "드림 올스타", "team_short_name": "드림", "city": "KBO", "founded_year": 1982, "stadium_name": "-"},
    # National Teams
    {"team_id": "KR", "team_name": "대한민국", "team_short_name": "한국", "city": "Seoul", "founded_year": None, "stadium_name": None},
    {"team_id": "JP", "team_name": "Japan", "team_short_name": "일본", "city": "Tokyo", "founded_year": None, "stadium_name": None},
    {"team_id": "TW", "team_name": "Taiwan", "team_short_name": "대만", "city": "Taipei", "founded_year": None, "stadium_name": None},
    {"team_id": "CU", "team_name": "Cuba", "team_short_name": "쿠바", "city": "Havana", "founded_year": None, "stadium_name": None},
    {"team_id": "AU", "team_name": "Australia", "team_short_name": "호주", "city": "Canberra", "founded_year": None, "stadium_name": None},
    {"team_id": "DOM", "team_name": "Dominican Rep.", "team_short_name": "도미니카", "city": "Santo Domingo", "founded_year": None, "stadium_name": None},
    {"team_id": "PA", "team_name": "Panama", "team_short_name": "파나마", "city": "Panama City", "founded_year": None, "stadium_name": None},
    {"team_id": "NL", "team_name": "Netherlands", "team_short_name": "네덜란드", "city": "Amsterdam", "founded_year": None, "stadium_name": None},
    {"team_id": "US", "team_name": "USA", "team_short_name": "미국", "city": "Washington, D.C.", "founded_year": None, "stadium_name": None},
    {"team_id": "VE", "team_name": "Venezuela", "team_short_name": "베네수엘라", "city": "Caracas", "founded_year": None, "stadium_name": None},
    {"team_id": "MX", "team_name": "Mexico", "team_short_name": "멕시코", "city": "Mexico City", "founded_year": None, "stadium_name": None},
    {"team_id": "PR", "team_name": "Puerto Rico", "team_short_name": "푸에르토리코", "city": "San Juan", "founded_year": None, "stadium_name": None},
    {"team_id": "CN", "team_name": "China", "team_short_name": "중국", "city": "Beijing", "founded_year": None, "stadium_name": None},
    {"team_id": "CA", "team_name": "Canada", "team_short_name": "캐나다", "city": "Ottawa", "founded_year": None, "stadium_name": None},
    {"team_id": "IT", "team_name": "Italy", "team_short_name": "이탈리아", "city": "Rome", "founded_year": None, "stadium_name": None},
    {"team_id": "CZ", "team_name": "Czechia", "team_short_name": "체코", "city": "Prague", "founded_year": None, "stadium_name": None},
]


def get_project_root() -> Path:
    """Get the project root directory (2 levels up from scripts/maintenance/)."""
    return Path(__file__).resolve().parent.parent.parent


def to_int_or_none(value: str) -> Optional[int]:
    """Convert a string to an integer, returning None if conversion fails."""
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def to_date_or_none(value: str, fmt: str = "%Y-%m-%d") -> Optional[datetime.date]:
    """Convert a string to a date object, returning None if conversion fails."""
    if not value:
        return None
    try:
        return datetime.strptime(value, fmt).date()
    except (ValueError, TypeError):
        return None


INVALID_TOKENS = {"team_id", "team_name", "team_short_name", "city", "founded_year", "stadium_name"}


def _is_valid_team_row(row: dict) -> bool:
    team_id = (row.get("team_id") or "").strip()
    team_name = (row.get("team_name") or "").strip()
    if not team_id or not team_name:
        return False
    lowered = team_id.lower()
    if lowered in INVALID_TOKENS or "varchar" in lowered:
        return False
    lowered_name = team_name.lower()
    if lowered_name in INVALID_TOKENS or "varchar" in lowered_name:
        return False
    return True


def _seed_default_teams(session: Session):
    print("   ℹ️  Falling back to built-in team seed list.")
    for team_data in DEFAULT_TEAMS:
        session.merge(Team(**team_data))
    session.commit()
    print(f"✅ Upserted {len(DEFAULT_TEAMS)} default teams.")


def seed_teams(session: Session, csv_path: Union[str, Path]):
    """Seed the teams table from a CSV file."""
    print(f"\n🌱 Seeding teams from {csv_path}...")
    teams_to_upsert = []
    csv_path = Path(csv_path)

    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not _is_valid_team_row(row):
                    continue
                team = Team(
                    team_id=row["team_id"].strip(),
                    team_name=row["team_name"].strip(),
                    team_short_name=row["team_short_name"].strip(),
                    city=row["city"].strip(),
                    founded_year=to_int_or_none(row.get("founded_year")),
                    stadium_name=(row.get("stadium_name") or "").strip() or None,
                )
                session.merge(team)
                teams_to_upsert.append(team)

    if teams_to_upsert:
        session.commit()
        print(f"✅ Upserted {len(teams_to_upsert)} teams from CSV.")
    else:
        print("⚠️  No valid teams in CSV; using default seed data.")
        _seed_default_teams(session)


LEAGUE_TYPES = [
    (0, "정규시즌"),
    (1, "시범경기"),
    (3, "포스트시즌"),
    (4, "올스타전"),
    (5, "퓨처스리그"),
    (7, "WBC"),
    (9, "프리미어12"),
]


def _seed_default_seasons(session: Session):
    """Programmatically seed KBO season entries (1982-2030) using INSERT OR IGNORE."""
    from sqlalchemy import text
    count = 0
    for year in range(1982, 2031):
        for code, name in LEAGUE_TYPES:
            sid = (year - 1982) * len(LEAGUE_TYPES) + code + 1
            session.execute(text(
                "INSERT OR IGNORE INTO kbo_seasons "
                "(season_id, season_year, league_type_code, league_type_name, created_at, updated_at) "
                "VALUES (:sid, :year, :code, :name, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ), {"sid": sid, "year": year, "code": code, "name": name})
            count += 1
    session.commit()
    print(f"   ✅ Upserted {count} default season entries.")


def seed_kbo_seasons(session: Session, csv_path: Union[str, Path]):
    """Seed the kbo_seasons table from a CSV file, with programmatic fallback."""
    print(f"\n🌱 Seeding KBO seasons from {csv_path}...")
    csv_path = Path(csv_path)
    seasons_to_add = []

    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                season_id = to_int_or_none(row.get("season_id") or row.get("필드명"))
                if not season_id:
                    continue

                exists = session.query(KboSeason).filter_by(season_id=season_id).first()
                if exists:
                    continue

                season = KboSeason(
                    season_id=season_id,
                    season_year=to_int_or_none(row.get("season_year") or row.get("시즌 연도")),
                    league_type_code=to_int_or_none(row.get("league_type_code") or row.get("시즌 종류 코드")),
                    league_type_name=row.get("league_type_name") or row.get("시즌 종류 이름"),
                    start_date=to_date_or_none(row.get("start_date") or row.get("시즌 시작일")),
                    end_date=to_date_or_none(row.get("end_date") or row.get("시즌 종료일")),
                )
                seasons_to_add.append(season)
    else:
        print(f"⚠️  Seasons CSV not found: {csv_path}. Generating defaults...")
        _seed_default_seasons(session)
        return

    if seasons_to_add:
        for s in seasons_to_add:
            session.merge(s)
        session.commit()
        print(f"✅ Upserted {len(seasons_to_add)} seasons.")
    else:
        print("✅ No new seasons to add.")


def drop_raw_table(engine, table_name: str):
    """Drop a table if it exists."""
    print(f"\n🗑️ Attempting to drop raw table: {table_name}...")
    try:
        with engine.connect() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"✅ Table {table_name} dropped successfully (if it existed).")
    except Exception as e:
        print(f"⚠️ Could not drop table {table_name}. It might not exist. Error: {e}")


if __name__ == "__main__":
    print("--- Starting Database Seeding ---")
    project_root = get_project_root()
    
    # Define paths to CSV files
    teams_csv = project_root / "Docs" / "schema" / "teams (구단 정보).csv"
    seasons_csv = project_root / "Docs" / "schema" / "KBO_시즌별 메타 테이블 제약조건.csv"

    # Create a new session
    db_session = SessionLocal()

    try:
        # Seed data
        seed_teams(db_session, teams_csv)
        seed_kbo_seasons(db_session, seasons_csv)

        # Drop the old raw table
        drop_raw_table(Engine, "kbo_season_pitching_raw")
        # Also drop batting raw table if it exists
        drop_raw_table(Engine, "kbo_season_batting_raw")

    except Exception as e:
        print(f"\n❌ An error occurred during seeding: {e}")
        db_session.rollback()
    finally:
        db_session.close()
        print("\n--- Database Seeding Complete ---")
