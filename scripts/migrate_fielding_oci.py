import logging
import os

import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()
url = os.getenv("OCI_DB_URL") or "postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend"


def migrate_fielding():
    logger.info("Connecting to %s...", url)
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
            logger.info("Adding column %s...", col_name)
            try:
                cur.execute(f"ALTER TABLE player_season_fielding ADD COLUMN {col_name} {col_type};")
                logger.info("  Column %s added.", col_name)
            except psycopg2.errors.DuplicateColumn:
                logger.info("  Column %s already exists.", col_name)
            except psycopg2.Error as e:
                logger.error("  Error adding %s: %s", col_name, e)

        cur.close()
        conn.close()
        logger.info("Migration complete.")
    except psycopg2.Error as e:
        logger.error("Connection failed: %s", e)


if __name__ == "__main__":
    migrate_fielding()
