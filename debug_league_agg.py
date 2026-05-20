import sqlite3
import pandas as pd

conn = sqlite3.connect('data/kbo_dev.db')
query = """
SELECT 
    SUM(plate_appearances) as pa,
    SUM(at_bats) as ab,
    SUM(hits) as h,
    SUM(doubles) as d2,
    SUM(triples) as d3,
    SUM(home_runs) as hr,
    SUM(walks) as bb,
    SUM(hbp) as hbp,
    SUM(sacrifice_flies) as sf,
    SUM(runs) as r
FROM player_season_batting
WHERE season = 2024 AND league = 'REGULAR'
"""
df = pd.read_sql_query(query, conn)
print(df.to_string())

pa = df['pa'].iloc[0]
ab = df['ab'].iloc[0]
h = df['h'].iloc[0]
d2 = df['d2'].iloc[0]
d3 = df['d3'].iloc[0]
hr = df['hr'].iloc[0]
bb = df['bb'].iloc[0]
hbp = df['hbp'].iloc[0]
sf = df['sf'].iloc[0]
r = df['r'].iloc[0]

h_1b = h - d2 - d3 - hr
numerator = (0.69 * bb) + (0.72 * hbp) + (0.89 * h_1b) + (1.27 * d2) + (1.62 * d3) + (2.10 * hr)
denominator = ab + bb + hbp + sf
lg_woba = numerator / denominator
print(f"\nCalculated lg_woba: {lg_woba:.3f}")
print(f"Calculated lg_obp: {(h+bb+hbp)/denominator:.3f}")
print(f"Calculated R/PA: {r/pa:.3f}")

conn.close()
