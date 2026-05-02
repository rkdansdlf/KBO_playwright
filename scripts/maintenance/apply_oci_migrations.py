import os
import glob
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

def apply_migrations():
    load_dotenv()
    oci_url = os.getenv('OCI_DB_URL') or os.getenv('TARGET_DATABASE_URL')
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return

    engine = create_engine(oci_url)
    Session = sessionmaker(bind=engine)
    
    migration_files = sorted(glob.glob("migrations/oci/*.sql"))
    
    if not migration_files:
        print("ℹ️ No OCI migration files found.")
        return

    with Session() as session:
        for file_path in migration_files:
            print(f"🚀 Applying migration: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                
                # PostgreSQL allows running multiple statements in one execute() 
                # if they are separated by semicolons.
                session.execute(text(sql))
                session.commit()
                print(f"✅ Successfully applied {os.path.basename(file_path)}")
            except Exception as e:
                session.rollback()
                print(f"❌ Failed to apply {file_path}: {e}")

    engine.dispose()

if __name__ == "__main__":
    apply_migrations()
