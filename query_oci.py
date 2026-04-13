import psycopg2
conn = psycopg2.connect("postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend")
cur = conn.cursor()
cur.execute("SELECT count(*) FROM game WHERE game_id LIKE '202604%';")
print(cur.fetchone()[0])
