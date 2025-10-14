"""Debug retired player parser directly."""
from src.parsers.retired_player_parser import parse_retired_hitter_tables, _select_tables

# Simulate a table with type marker and some data
test_table = {
    '_table_type': 'HITTER',
    'caption': '타격 기록표',
    'summary': '타격(선수,타율,경기,안타,타점)',
    'headers': ['선수', '타율', '경기', '안타', '타점'],
    'rows': [
        ['박동원(롯데)', '0.336', '32', '36', '19'],
        ['신범수(SSG)', '0.219', '18', '14', '6'],
    ]
}

print("=== Testing _select_tables ===")
base, adv = _select_tables([test_table])
print(f"Base rows: {len(base)}")
print(f"Adv rows: {len(adv)}")

if base:
    print("\nFirst base row dict:")
    print(base[0])

print("\n=== Testing parse_retired_hitter_tables ===")
result = parse_retired_hitter_tables([test_table], league="FUTURES", level="KBO2")
print(f"Parsed records: {len(result)}")
for rec in result:
    print(f"  {rec}")
