"""
Initialize database (create all tables)
"""

import logging

logger = logging.getLogger(__name__)

from src.db.engine import init_db

if __name__ == "__main__":
    logger.info("🔧 Initializing database...")
    init_db()
    logger.info("✅ Database initialization complete!")
