import logging

logger = logging.getLogger(__name__)

import os

from sqlalchemy import create_engine, text


def main():
    engine = create_engine(os.environ["OCI_DB_URL"])
    with engine.connect() as conn:
        logger.info("Unique Constraints:")
        res = conn.execute(
            text("""
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'game_lineups'::regclass AND contype = 'u';
        """)
        ).fetchall()
        for row in res:
            logger.info(row)

        logger.info("\nPrimary Key:")
        res = conn.execute(
            text("""
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'game_lineups'::regclass AND contype = 'p';
        """)
        ).fetchall()
        for row in res:
            logger.info(row)


if __name__ == "__main__":
    main()
