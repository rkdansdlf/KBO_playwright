import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("OCI_DB_URL") or "postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend"


def migrate_fielding():
    print(f"Connecting to {url}...")
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cols = [
            ("caught_stealing", "INTEGER"),
            ("stolen_bases_allowed", "INTEGER"),
            ("passed_balls", "INTEGER"),
            ("cs_pct", "FLOAT"),
        ]

        for col_name, col_type in cols:
            print(f"Adding column {col_name}...")
            try:
                cur.execute(f"ALTER TABLE player_season_fielding ADD COLUMN {col_name} {col_type};")
                print(f"  Column {col_name} added.")
            except psycopg2.errors.DuplicateColumn:
                print(f"  Column {col_name} already exists.")
            except Exception as e:
                print(f"  Error adding {col_name}: {e}")

        cur.close()
        conn.close()
        print("Migration complete.")
    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    migrate_fielding()
