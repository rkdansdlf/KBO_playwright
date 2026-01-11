"""
Initialize database (create all tables)
"""
from src.db.engine import init_db

if __name__ == "__main__":
    print("🔧 Initializing database...")
    init_db()
    print("✅ Database initialization complete!")
