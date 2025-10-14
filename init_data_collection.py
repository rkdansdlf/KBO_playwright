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
import asyncio
import json
from pathlib import Path
from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler


async def step1_collect_player_list(season_year: int = 2024):
    """Step 1: Collect all players (hitters and pitchers)"""
    print("\n" + "=" * 60)
    print("STEP 1: Collect Player List")
    print("=" * 60)

    crawler = PlayerListCrawler()
    result = await crawler.crawl_all_players(season_year)

    # Save to JSON
    output_dir = Path("data/players")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"player_list_{season_year}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved player list to: {output_file}")
    print(f"📊 Total: {len(result['hitters'])} hitters, {len(result['pitchers'])} pitchers")

    return result


async def step2_collect_player_profiles(player_list: dict, limit: int = 5):
    """Step 2: Collect player profile data (limited for POC)"""
    print("\n" + "=" * 60)
    print("STEP 2: Collect Player Profiles")
    print("=" * 60)

    crawler = PlayerProfileCrawler()
    profiles = []

    # Collect profiles for first N players (POC limit)
    all_players = player_list['hitters'][:limit] + player_list['pitchers'][:limit]

    for i, player in enumerate(all_players, 1):
        player_id = player.get('player_id')
        if not player_id:
            print(f"⚠️  Skipping {player['player_name']} - no player ID")
            continue

        print(f"\n[{i}/{len(all_players)}] Crawling profile: {player['player_name']}")

        profile = await crawler.crawl_player_profile(player_id)
        if profile:
            profiles.append(profile)

    # Save to JSON
    output_dir = Path("data/players")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"player_profiles_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved {len(profiles)} player profiles to: {output_file}")

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


async def step6_collect_game_details(games: list, limit: int = 3):
    """Step 6: Collect game detail data (limited for POC)"""
    print("\n" + "=" * 60)
    print("STEP 6: Collect Game Details")
    print("=" * 60)

    crawler = GameDetailCrawler()
    game_details = []

    # Collect details for first N games (POC limit)
    for i, game in enumerate(games[:limit], 1):
        print(f"\n[{i}/{min(limit, len(games))}] Crawling game: {game['game_id']}")

        game_data = await crawler.crawl_game(game['game_id'], game['game_date'])
        if game_data:
            game_details.append(game_data)

    # Save to JSON
    output_dir = Path("data/games")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"game_details_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(game_details, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved {len(game_details)} game details to: {output_file}")

    return game_details


async def main():
    """Run initial data collection in correct order"""
    print("\n" + "🚀" * 30)
    print("KBO Initial Data Collection")
    print("Following ProjectOverview.md order")
    print("🚀" * 30)

    try:
        # Step 1: Player List
        player_list = await step1_collect_player_list(season_year=2024)

        # Step 2: Player Profiles (limited to 5 players for POC)
        if player_list['hitters'] or player_list['pitchers']:
            profiles = await step2_collect_player_profiles(player_list, limit=5)
        else:
            print("\n⚠️  No players found, skipping profile collection")

        # Step 3-4: Retired/Futures (TODO: implement later)
        print("\n" + "⏭️ " * 30)
        print("Step 3-4 (Retired/Futures): TODO - will implement later")
        print("⏭️ " * 30)

        # Step 5: Schedule
        year = 2024
        month = 10
        games = await step5_collect_schedule(year, month)

        # Step 6: Game Details (limited to 3 games for POC)
        if games:
            game_details = await step6_collect_game_details(games, limit=3)
        else:
            print("\n⚠️  No games found, skipping game detail collection")

        print("\n" + "✅" * 30)
        print("Initial Data Collection Complete!")
        print("✅" * 30)

        # Summary
        print(f"\n📊 Collection Summary:")
        print(f"  Players collected: {len(player_list.get('hitters', []))} hitters + {len(player_list.get('pitchers', []))} pitchers")
        print(f"  Profiles collected: {len(profiles) if 'profiles' in locals() else 0}")
        print(f"  Games scheduled: {len(games) if 'games' in locals() else 0}")
        print(f"  Game details collected: {len(game_details) if 'game_details' in locals() else 0}")

    except Exception as e:
        print(f"\n❌ Error during data collection: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
