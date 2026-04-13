import sqlite3
import psycopg2

print("Fixing games in SQLite...")
# Local SQLite
sqlite_conn = sqlite3.connect("data/kbo_dev.db")
sqlite_cur = sqlite_conn.cursor()
sqlite_cur.execute("UPDATE game SET season_id = 2026 WHERE game_id LIKE '2026%' AND season_id = 265;")
sqlite_conn.commit()
print(f"✅ Updated {sqlite_cur.rowcount} rows in Local SQLite.")

print("Fixing games in OCI PostgreSQL...")
# OCI PG
pg_conn = psycopg2.connect("postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend")
pg_cur = pg_conn.cursor()
pg_cur.execute("UPDATE game SET season_id = 2026 WHERE game_id LIKE '2026%' AND season_id = 265;")
pg_conn.commit()
print(f"✅ Updated {pg_cur.rowcount} rows in OCI Postgres.")

