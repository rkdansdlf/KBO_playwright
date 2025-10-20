
import csv
import os
from pathlib import Path
from typing import Optional, Union
from datetime import datetime

from sqlalchemy.orm import Session

from src.db.engine import SessionLocal, Engine
from src.models.team import Team
from src.models.season import KboSeason


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.resolve()


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


def seed_teams(session: Session, csv_path: Union[str, Path]):
    """Seed the teams table from a CSV file."""
    print(f"\n🌱 Seeding teams from {csv_path}...")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        teams_to_upsert = []
        for row in reader:
            team = Team(
                team_id=row["team_id"],
                team_name=row["team_name"],
                team_short_name=row["team_short_name"],
                city=row["city"],
                founded_year=to_int_or_none(row["founded_year"]),
                stadium_name=row["stadium_name"],
            )
            # Use merge to perform an UPSERT operation
            session.merge(team)
            teams_to_upsert.append(team)
    
    if teams_to_upsert:
        session.commit()
        print(f"✅ Upserted {len(teams_to_upsert)} teams.")
    else:
        print("✅ No teams data found in CSV.")


def seed_kbo_seasons(session: Session, csv_path: Union[str, Path]):
    """Seed the kbo_seasons table from a CSV file."""
    print(f"\n🌱 Seeding KBO seasons from {csv_path}...")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        seasons_to_add = []
        for row in reader:
            season_id = to_int_or_none(row.get("season_id") or row.get("필드명"))
            if not season_id:
                continue

            exists = session.query(KboSeason).filter_by(season_id=season_id).first()
            if exists:
                print(f"  - Skipping existing season: {season_id}")
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

    if seasons_to_add:
        session.add_all(seasons_to_add)
        session.commit()
        print(f"✅ Added {len(seasons_to_add)} new seasons.")
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
