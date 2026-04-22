
from src.utils.team_codes import normalize_kbo_game_id

test_ids = [
    "20200827KHLT0",
    "20200827WOLT0",
    "20240323SKSS0",
    "20240323SSGSS0"
]

for gid in test_ids:
    norm = normalize_kbo_game_id(gid)
    print(f"{gid} -> {norm}")
