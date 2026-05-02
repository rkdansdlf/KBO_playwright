import psycopg2
import sys

def run_query(query):
    try:
        conn = psycopg2.connect("postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend")
        cur = conn.cursor()
        cur.execute(query)
        
        if cur.description:
            colnames = [desc[0] for desc in cur.description]
            print(f"{' | '.join(colnames)}")
            print("-" * 50)
            rows = cur.fetchall()
            for row in rows:
                print(f"{' | '.join(map(str, row))}")
        else:
            conn.commit()
            print("Query executed successfully (no results).")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_query(sys.argv[1])
    else:
        print("Usage: python3 query_oci.py \"SQL_QUERY\"")
