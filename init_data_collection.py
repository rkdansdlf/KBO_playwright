"""
Initial Data Collection Script
Follows the correct order from ProjectOverview.md:

1. Player List Crawler - Collect all players by team
2. Player Profile Crawler - Collect player basic info (ID, physical stats, position)
3. Retired/Inactive Players - (TODO: separate script)
4. Futures League - (TODO: separate script)
5. Season Game Schedule - Collect game IDs
6. Game Detail - Collect box scores, player stats
"""
import argparse
import asyncio
import json
from pathlib import Path
from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.crawl_run_repository import CrawlRunRepository
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.services.player_status_confirmer import PlayerStatusConfirmer


async def step1_collect_player_list(season_year: int = 2024, confirm_limit: int = 200):
    """1단계: KBO 리그의 모든 선수(타자, 투수) 목록을 수집합니다."""
    print("\n" + "=" * 60)
    print("STEP 1: Collect Player List")
    print("=" * 60)

    crawler = PlayerListCrawler()
    result = await crawler.crawl_all_players(season_year)
    confirmer = PlayerStatusConfirmer(max_confirmations=confirm_limit)
    confirm_stats = await confirmer.confirm_entries(result.get("retired", []) + result.get("staff", []))

    # Save to JSON
    output_dir = Path("data/players")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"player_list_{season_year}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved player list to: {output_file}")
    retired_count = len(result.get("retired", []))
    staff_count = len(result.get("staff", []))
    confirmed = confirm_stats.get("confirmed", 0)
    print(
        f"📊 Active: {len(result['hitters'])} hitters, {len(result['pitchers'])} pitchers "
        f"| Retired: {retired_count} | Staff: {staff_count} | Profile-confirmed: {confirmed}"
    )
    if confirm_stats.get("attempted"):
        print(f"   ↳ Profile checks attempted: {confirm_stats['attempted']}")

    return result, confirm_stats


async def step2_collect_player_profiles(player_list: dict, limit: int = 5, concurrency: int = 5):
    """Step 2: Collect player profile data (limited for POC)"""
    print("\n" + "=" * 60)
    print("STEP 2: Collect Player Profiles")
    print("=" * 60)

    crawler = PlayerProfileCrawler()
    profiles = []

    all_players = player_list['hitters'][:limit] + player_list['pitchers'][:limit]
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def fetch(idx: int, player: dict):
        player_id = player.get('player_id')
        if not player_id:
            print(f"⚠️  Skipping {player.get('player_name')} - no player ID")
            return None
        async with semaphore:
            print(f"\n[{idx}/{len(all_players)}] Crawling profile: {player.get('player_name')}")
            return await crawler.crawl_player_profile(player_id)

    tasks = [fetch(i + 1, player) for i, player in enumerate(all_players)]
    results = await asyncio.gather(*tasks)
    profiles.extend([profile for profile in results if profile])

    # Save to JSON
    output_dir = Path("data/players")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"player_profiles_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved {len(profiles)} player profiles to: {output_file}")
    status_updates = []
    id_to_entry = {}
    for bucket in ("hitters", "pitchers", "retired", "staff"):
        for entry in player_list.get(bucket, []):
            id_to_entry[str(entry.get("player_id"))] = entry

    for profile in profiles:
        status = profile.get("status")
        if status:
            update = {
                "player_id": profile["player_id"],
                "status": status,
                "staff_role": profile.get("staff_role"),
                "status_source": profile.get("status_source") or "profile",
            }
            status_updates.append(update)
            entry = id_to_entry.get(str(profile["player_id"]))
            if entry:
                entry.update(update)

    if status_updates:
        repo = PlayerBasicRepository()
        repo.update_statuses(status_updates)

    return profiles


async def step5_collect_schedule(year: int, month: int):
    """Step 5: Collect game schedule"""
    print("\n" + "=" * 60)
    print("STEP 5: Collect Game Schedule")
    print("=" * 60)

    crawler = ScheduleCrawler()
    games = await crawler.crawl_schedule(year, month)

    # Save to JSON
    output_dir = Path("data/schedules")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"schedule_{year}_{month:02d}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(games, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved schedule to: {output_file}")
    print(f"📊 Total games: {len(games)}")

    return games


async def step6_collect_game_details(games: list, limit: int = 3, concurrency: int = 3):
    """Step 6: Collect game detail data (limited for POC)"""
    print("\n" + "=" * 60)
    print("STEP 6: Collect Game Details")
    print("=" * 60)

    crawler = GameDetailCrawler()
    game_details = []

    semaphore = asyncio.Semaphore(max(1, concurrency))
    targets = games[:limit]

    async def fetch(idx: int, game: dict):
        async with semaphore:
            print(f"\n[{idx}/{len(targets)}] Crawling game: {game['game_id']}")
            return await crawler.crawl_game(game['game_id'], game['game_date'])

    tasks = [fetch(i + 1, game) for i, game in enumerate(targets)]
    results = await asyncio.gather(*tasks)
    game_details.extend([data for data in results if data])

    # Save to JSON
    output_dir = Path("data/games")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"game_details_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(game_details, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved {len(game_details)} game details to: {output_file}")

    return game_details


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the initial data collection pipeline")
    parser.add_argument("--season-year", type=int, default=2024, help="Season year for player list crawl")
    parser.add_argument("--schedule-year", type=int, default=2024, help="Year for schedule crawl")
    parser.add_argument("--schedule-month", type=int, default=10, help="Month for schedule crawl")
    parser.add_argument("--profile-limit", type=int, default=5, help="Number of player profiles to fetch")
    parser.add_argument("--profile-concurrency", type=int, default=5, help="Concurrent profile fetches")
    parser.add_argument("--confirm-limit", type=int, default=200, help="Max profile confirmations")
    parser.add_argument("--game-limit", type=int, default=3, help="Number of games to fetch details for")
    parser.add_argument("--game-concurrency", type=int, default=3, help="Concurrent game detail fetches")
    parser.add_argument("--skip-profiles", action="store_true", help="Skip profile collection step")
    parser.add_argument("--skip-games", action="store_true", help="Skip schedule/detail steps")
    parser.add_argument("--label", type=str, default=None, help="Optional crawl label for crawl_runs table")
    return parser


async def run_pipeline(args: argparse.Namespace):
    """Run initial data collection in correct order"""
    print("\n" + "🚀" * 30)
    print("KBO Initial Data Collection")
    print("Following ProjectOverview.md order")
    print("🚀" * 30)
    run_label = args.label or datetime.utcnow().strftime("init_run_%Y%m%d_%H%M%S")
    started_at = datetime.utcnow()

    try:
        # Step 1: Player List
        player_list, confirm_stats = await step1_collect_player_list(
            season_year=args.season_year,
            confirm_limit=args.confirm_limit,
        )

        # Step 2: Player Profiles (limited to 5 players for POC)
        profiles = []
        if not args.skip_profiles and (player_list['hitters'] or player_list['pitchers']):
            profiles = await step2_collect_player_profiles(
                player_list,
                limit=args.profile_limit,
                concurrency=args.profile_concurrency,
            )
        else:
            print("\n⚠️  Skipping profile collection (requested or no players)")

        # Step 3-4: Retired/Futures (TODO: implement later)
        print("\n" + "⏭️ " * 30)
        print("Step 3-4 (Retired/Futures): TODO - will implement later")
        print("⏭️ " * 30)

        # Step 5: Schedule
        games = []
        game_details = []
        if not args.skip_games:
            games = await step5_collect_schedule(args.schedule_year, args.schedule_month)

            # Step 6: Game Details (limited for POC)
            if games:
                game_details = await step6_collect_game_details(
                    games,
                    limit=args.game_limit,
                    concurrency=args.game_concurrency,
                )
            else:
                print("\n⚠️  No games found, skipping game detail collection")

        print("\n" + "✅" * 30)
        print("Initial Data Collection Complete!")
        print("✅" * 30)

        # Summary
        print("\n=== Player Classification Summary ===")
        all_entries = (
            player_list.get('hitters', [])
            + player_list.get('pitchers', [])
            + player_list.get('retired', [])
            + player_list.get('staff', [])
        )
        confirmed_profiles = sum(1 for entry in all_entries if entry.get("status_source") == "profile")
        heuristic_only = len(all_entries) - confirmed_profiles
        active_total = len(player_list.get('hitters', [])) + len(player_list.get('pitchers', []))
        retired_total = len(player_list.get('retired', []))
        staff_total = len(player_list.get('staff', []))
        print(f"Active players: {active_total}")
        print(f"Retired players: {retired_total}")
        print(f"Staff entries: {staff_total}")
        print(f"Confirmed by profile: {confirmed_profiles}")
        print(f"Heuristic only: {heuristic_only}")
        print(f"\n📊 Collection Summary:")
        print(f"  Profiles collected: {len(profiles)}")
        print(f"  Games scheduled: {len(games)}")
        print(f"  Game details collected: {len(game_details)}")

        repo = CrawlRunRepository()
        repo.create_run(
            label=run_label,
            started_at=started_at,
            finished_at=datetime.utcnow(),
            active_count=active_total,
            retired_count=retired_total,
            staff_count=staff_total,
            confirmed_profiles=confirmed_profiles,
            heuristic_only=heuristic_only,
        )

    except Exception as e:
        print(f"\n❌ Error during data collection: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_pipeline(args))
