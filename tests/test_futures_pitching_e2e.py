"""
End-to-end test: Fetch Futures pitching stats and save to database.
"""

import asyncio

from src.crawlers.futures.futures_pitching import fetch_and_parse_futures_pitching
from src.repositories.player_repository import PlayerRepository
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.utils.safe_print import safe_print as print

PLAYER_ID = "50030"  # KBO player ID (string) for So Hyeong-jun (pitcher)
PLAYER_URL = f"https://www.koreabaseball.com/Futures/Player/PitcherTotal.aspx?playerId={PLAYER_ID}"


async def main():
    print("=== Futures Pitching E2E Test ===\n")

    # Step 1: Crawl and parse
    print(f"Step 1: Crawling Futures pitching stats for player {PLAYER_ID}...")
    rows = await fetch_and_parse_futures_pitching(PLAYER_ID, PLAYER_URL)
    print(f"✓ Parsed {len(rows)} season records\n")

    if not rows:
        print("No data to save. Exiting.")
        return

    # Show sample
    for row in rows[:3]:
        print(
            f"  {row.get('season')}: ERA={row.get('era')}, G={row.get('games')}, W={row.get('wins')}, L={row.get('losses')}, IP={row.get('innings_pitched')}"
        )
    print()

    # Step 2: Get or create player in database
    print(f"Step 2: Ensuring player {PLAYER_ID} exists in database...")
    repo = PlayerRepository()

    from src.parsers.player_profile_parser import PlayerProfileParsed

    player = repo.upsert_player_profile(PLAYER_ID, PlayerProfileParsed(is_active=True, player_name="소형준"))

    if not player:
        print("Failed to create player record")
        return

    print(f"✓ Player DB ID: {player.id}, Player Basic ID: {player.player_basic_id}\n")

    # Step 3: Save Futures pitching stats
    print(f"Step 3: Saving {len(rows)} Futures pitching records to database...")
    payloads = []
    for r in rows:
        payloads.append(
            {
                "player_id": int(player.player_basic_id),
                "player_name": "소형준",
                "season": r.get("season"),
                "league": "FUTURES",
                "level": "KBO2",
                "source": "PROFILE",
                "team_code": r.get("team_code"),
                "games": r.get("games"),
                "complete_games": r.get("complete_games"),
                "shutouts": r.get("shutouts"),
                "wins": r.get("wins"),
                "losses": r.get("losses"),
                "saves": r.get("saves"),
                "holds": r.get("holds"),
                "innings_pitched": r.get("innings_pitched"),
                "innings_outs": r.get("innings_outs"),
                "hits_allowed": r.get("hits_allowed"),
                "runs_allowed": r.get("runs_allowed"),
                "earned_runs": r.get("earned_runs"),
                "home_runs_allowed": r.get("home_runs_allowed"),
                "walks_allowed": r.get("walks_allowed"),
                "hit_batters": r.get("hit_batters"),
                "strikeouts": r.get("strikeouts"),
                "era": r.get("era"),
                "tbf": r.get("tbf"),
            }
        )

    saved = save_pitching_stats_to_db(payloads)
    print(f"✓ Saved {saved} records\n")

    # Step 4: Verify
    print("Step 4: Verifying records in database...")
    from sqlalchemy import select

    from src.db.engine import SessionLocal
    from src.models.player import PlayerSeasonPitching

    with SessionLocal() as session:
        stmt = (
            select(PlayerSeasonPitching)
            .where(PlayerSeasonPitching.player_id == player.player_basic_id, PlayerSeasonPitching.league == "FUTURES")
            .order_by(PlayerSeasonPitching.season)
        )

        results = session.execute(stmt).scalars().all()

        print(f"✓ Found {len(results)} Futures pitching records in database:")
        for record in results:
            print(
                f"  {record.season}: ERA={record.era}, G={record.games}, W={record.wins}, L={record.losses}, IP={record.innings_pitched}"
            )

    print("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
