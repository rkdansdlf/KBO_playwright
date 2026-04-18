import sqlite3

db_path = 'data/kbo_dev.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]

keyword = '삼성'

for table in tables:
    try:
        cursor.execute(f"PRAGMA table_info({table});")
        columns = [row[1] for row in cursor.fetchall()]
        
        for col in columns:
            query = f"SELECT * FROM {table} WHERE {col} LIKE ?"
            cursor.execute(query, (f'%{keyword}%',))
            results = cursor.fetchall()
            if results:
                print(f"Match in Table: {table}, Column: {col}")
                for res in results:
                    print(res)
    except Exception as e:
        # Some tables might have virtual columns or other issues
        pass

conn.close()
