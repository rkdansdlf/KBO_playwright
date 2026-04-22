
"""Collect international schedule pages.

International fixtures do not use the regular KBO season schedule pages, so
this script keeps a dedicated crawler. DB writes still go through the shared
game snapshot persistence path instead of direct ORM upserts.
"""
import asyncio
import argparse

from src.crawlers.international_crawler import InternationalScheduleCrawler
from src.repositories.game_repository import save_game_snapshot
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED

# List of target international URLs
# Ideally these would be dynamic, but for now we target the user's specific request (Premier 2024)
TARGET_URLS = [
    "https://www.koreabaseball.com/Schedule/International/Etc/Premier2024.aspx",
    # Add more if needed (e.g. APBC 2023)
    # "https://www.koreabaseball.com/Schedule/International/Etc/APBC2023.aspx"
]

async def collect_international_games(save: bool = False):
    crawler = InternationalScheduleCrawler()
    total_games = []
    
    try:
        for url in TARGET_URLS:
            games = await crawler.crawl_schedule(url)
            total_games.extend(games)
            
        print(f"\n📊 Collected {len(total_games)} total international games.")
        
        if save:
            saved_count = save_games(total_games)
            print(f"✅ Saved/Updated {saved_count}/{len(total_games)} international games.")
            
    finally:
        await crawler.close()

def _normalize_status(status: str | None) -> str:
    value = str(status or "").strip().lower()
    if value in {"end", "ended", "final", "completed", "complete"}:
        return GAME_STATUS_COMPLETED
    return GAME_STATUS_SCHEDULED


def _score_for_status(score: int | None, status: str) -> int | None:
    return score if status == GAME_STATUS_COMPLETED else None


def _to_snapshot_payload(data: dict) -> tuple[dict, str]:
    status = _normalize_status(data.get("status"))
    payload = {
        "game_id": data["game_id"],
        "game_date": data["game_date"],
        "season_id": data.get("season_id"),
        "metadata": {
            "stadium": data.get("stadium"),
            "start_time": data.get("game_time"),
            "source": "international_schedule",
            "series_id": data.get("series_id"),
        },
        "teams": {
            "away": {
                "code": data.get("away_team"),
                "score": _score_for_status(data.get("away_score"), status),
                "line_score": [],
            },
            "home": {
                "code": data.get("home_team"),
                "score": _score_for_status(data.get("home_score"), status),
                "line_score": [],
            },
        },
    }
    return payload, status


def save_games(games_data: list) -> int:
    saved_count = 0
    for data in games_data:
        payload, status = _to_snapshot_payload(data)
        if save_game_snapshot(payload, status=status):
            saved_count += 1
        else:
            print(f"❌ Failed to save international game {data.get('game_id')}")
    return saved_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect International KBO Games")
    parser.add_argument("--save", action="store_true", help="Save collected games to SQLite DB")
    args = parser.parse_args()
    
    asyncio.run(collect_international_games(save=args.save))
