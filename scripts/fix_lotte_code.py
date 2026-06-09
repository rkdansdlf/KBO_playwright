import logging
import os
import sys

logger = logging.getLogger(__name__)

# Add the project root to the python path
sys.path.append(os.getcwd())

from sqlalchemy import text  # noqa: E402

from src.db.engine import SessionLocal  # noqa: E402


def fix_lotte_code():
    """
    Updates the Lotte Giants team code in the team_franchises table from 'LOT' to 'LT'.
    """
    logger.info("Starting Lotte team code fix...")

    with SessionLocal() as session:
        try:
            # Check current state
            check_sql = text(
                "SELECT id, name, original_code, current_code FROM team_franchises WHERE original_code = 'LT'"
            )
            result = session.execute(check_sql).fetchone()

            if not result:
                logger.error("Error: Could not find Lotte franchise with original_code='LT'")
                return

            logger.info(
                "Current state: ID=%s, Name=%s, Original=%s, Current=%s",
                result.id,
                result.name,
                result.original_code,
                result.current_code,
            )

            if result.current_code == "LT":
                logger.info("Code is already 'LT'. No action needed.")
                return

            # Update
            update_sql = text("UPDATE team_franchises SET current_code = 'LT' WHERE original_code = 'LT'")
            session.execute(update_sql)
            session.commit()

            # Verify
            result_after = session.execute(check_sql).fetchone()
            logger.info(
                "New state: ID=%s, Name=%s, Original=%s, Current=%s",
                result_after.id,
                result_after.name,
                result_after.original_code,
                result_after.current_code,
            )

            if result_after.current_code == "LT":
                logger.info("SUCCESS: Lotte team code updated to 'LT'.")
            else:
                logger.info("FAILURE: Lotte team code was not updated.")

        except Exception as e:
            session.rollback()
            logger.error("An error occurred: %s", e)


if __name__ == "__main__":
    fix_lotte_code()
