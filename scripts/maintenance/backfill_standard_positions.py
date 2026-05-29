import io
import os
import sqlite3

import psycopg2
from dotenv import load_dotenv

from src.utils.player_positions import get_primary_position

load_dotenv()


def backfill_local():
    print("🏠 Backfilling local SQLite database...")
    db_path = "data/kbo_dev.db"
    if not os.path.exists(db_path):
        print(f"❌ Local DB not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1. Pitching stats are easy
    print("  Updating game_pitching_stats...")
    cur.execute("UPDATE game_pitching_stats SET standard_position = 'P' WHERE standard_position IS NULL")

    # 2. Batting and Lineups need processing
    for table in ["game_batting_stats", "game_lineups"]:
        print(f"  Processing {table}...")
        cur.execute(f"SELECT id, position FROM {table} WHERE standard_position IS NULL AND position IS NOT NULL")
        rows = cur.fetchall()
        print(f"    Found {len(rows)} rows to update.")

        updates = []
        for row_id, pos in rows:
            std_pos = get_primary_position(pos).value
            updates.append((std_pos, row_id))

        if updates:
            # For SQLite, executemany is fine for 100k rows
            cur.executemany(f"UPDATE {table} SET standard_position = ? WHERE id = ?", updates)

    conn.commit()
    conn.close()
    print("✅ Local backfill finished.")


def backfill_oci():
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("⚠️ OCI_DB_URL not found. Skipping OCI backfill.")
        return

    print("☁️ Backfilling OCI PostgreSQL database (Optimized via COPY)...")
    try:
        conn = psycopg2.connect(oci_url)
        cur = conn.cursor()

        # 1. Pitching
        print("  Updating game_pitching_stats...")
        cur.execute('UPDATE "game_pitching_stats" SET "standard_position" = \'P\' WHERE "standard_position" IS NULL')
        conn.commit()

        # 2. Batting and Lineups
        for table in ["game_batting_stats", "game_lineups"]:
            print(f"  Processing {table}...")
            cur.execute(
                f'SELECT "id", "position" FROM "{table}" WHERE "standard_position" IS NULL AND "position" IS NOT NULL'
            )
            rows = cur.fetchall()
            print(f"    Found {len(rows)} rows to update.")

            if not rows:
                continue

            # Create a CSV-like buffer for COPY
            buf = io.StringIO()
            for row_id, pos in rows:
                std_pos = get_primary_position(pos).value
                buf.write(f"{row_id}\t{std_pos}\n")
            buf.seek(0)

            print("    Streaming data into temporary table...")
            cur.execute(f"CREATE TEMP TABLE temp_pos_update_{table} (id INT, std_pos VARCHAR(10)) ON COMMIT DROP")
            cur.copy_from(buf, f"temp_pos_update_{table}", columns=("id", "std_pos"))

            print("    Performing mass update from temp table...")
            cur.execute(f"""
                UPDATE "{table}"
                SET "standard_position" = temp.std_pos
                FROM temp_pos_update_{table} temp
                WHERE "{table}"."id" = temp.id
            """)
            conn.commit()
            print(f"    ✅ Finished {table}")

        conn.close()
        print("✅ OCI optimized backfill finished.")
    except Exception as e:
        print(f"❌ Failed to backfill OCI: {e}")


if __name__ == "__main__":
    backfill_local()
    backfill_oci()
