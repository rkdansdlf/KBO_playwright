import sqlite3
import psycopg2
from datetime import datetime

pg_conn = psycopg2.connect("postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend")
pg_cur = pg_conn.cursor()

sqlite_conn = sqlite3.connect("data/kbo_dev.db")
sqlite_cur = sqlite_conn.cursor()

pg_cur.execute("""
    SELECT season_id, season_year, league_type_code, league_type_name, start_date, end_date, created_at, updated_at 
    FROM kbo_seasons WHERE season_year = 2026
""")
oci_rows = pg_cur.fetchall()

sqlite_cur.execute("DELETE FROM kbo_seasons WHERE season_year = 2026")
print(f"Deleted {sqlite_cur.rowcount} bad 2026 rows from SQLite.")

for r in oci_rows:
    season_id = r[0]
    season_year = r[1]
    league_type_code = r[2]
    name = r[3]
    start_date = r[4]
    end_date = r[5]
    created_at = r[6]
    updated_at = r[7]
    sqlite_cur.execute("""
        INSERT INTO kbo_seasons 
        (season_id, season_year, league_type_code, league_type_name, start_date, end_date, created_at, updated_at) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (season_id, season_year, league_type_code, name, start_date, end_date, created_at, updated_at))

sqlite_conn.commit()
print(f"Inserted {len(oci_rows)} correct 2026 rows into SQLite kbo_seasons.")

sqlite_cur.execute("SELECT season_id FROM kbo_seasons WHERE season_id=265")
if not sqlite_cur.fetchone():
    sqlite_cur.execute("""
        INSERT INTO kbo_seasons 
        (season_id, season_year, league_type_code, league_type_name, created_at, updated_at) 
        VALUES (265, 2019, 5, '포스트시즌', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)
    sqlite_conn.commit()
    print("Migrated season_id=265 to 2019/5 in SQLite.")

