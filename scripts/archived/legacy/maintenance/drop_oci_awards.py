import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
if not oci_url:
    print("OCI_DB_URL not found")
    exit(1)

engine = create_engine(oci_url)
with engine.connect() as conn:
    print("Dropping table awards in OCI...")
    conn.execute(text("DROP TABLE IF EXISTS awards CASCADE"))
    conn.commit()
    print("Done.")
