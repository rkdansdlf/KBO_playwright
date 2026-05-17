from sqlalchemy import create_engine, text
import os

engine = create_engine(os.environ['OCI_DB_URL'])
with engine.connect() as conn:
    # Check unique constraints
    print("Unique Constraints:")
    res = conn.execute(text("""
        SELECT conname, pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE conrelid = 'game_lineups'::regclass AND contype = 'u';
    """)).fetchall()
    for row in res:
        print(row)
        
    # Check primary key
    print("\nPrimary Key:")
    res = conn.execute(text("""
        SELECT conname, pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE conrelid = 'game_lineups'::regclass AND contype = 'p';
    """)).fetchall()
    for row in res:
        print(row)
