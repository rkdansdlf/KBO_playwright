"""
Enrich Player Stubs
Targets players in 'player_basic' with missing metadata and fills them using PlayerProfileCrawler.
Focuses on 'active' players to ensure 100% integrity for current/recent seasons.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Mapping

from dotenv import load_dotenv
from sqlalchemy import bindparam, text

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.engine import SessionLocal
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.playwright_pool import AsyncPlaywrightPool


def parse_player_ids(raw_ids: str | None) -> list[int]:
    if not raw_ids:
        return []
    player_ids: list[int] = []
    for raw_id in raw_ids.split(","):
        value = raw_id.strip()
        if value:
            player_ids.append(int(value))
    return player_ids


def build_stub_query(player_ids: list[int] | None):
    query = """
        SELECT
          player_id,
          name,
          uniform_no,
          team,
          position,
          birth_date,
          birth_date_date,
          height_cm,
          weight_kg,
          career,
          status,
          staff_role,
          status_source,
          photo_url,
          bats,
          throws,
          debut_year,
          salary_original,
          signing_bonus_original,
          draft_info
        FROM player_basic
        WHERE status = 'active'
          AND (photo_url IS NULL OR height_cm IS NULL OR bats IS NULL)
    """
    params: dict[str, object] = {}
    if player_ids:
        query += "          AND player_id IN :player_ids\n"
        query += "        ORDER BY player_id\n"
        params["player_ids"] = player_ids
        return text(query).bindparams(bindparam("player_ids", expanding=True)), params

    query += "        ORDER BY player_id\n"
    query += "        LIMIT :limit\n"
    params["limit"] = 200
    return text(query), params


def _row_value(row, field: str):
    if isinstance(row, Mapping):
        return row.get(field)
    return getattr(row, field, None)


def _profile_or_existing(profile_data: dict, row, field: str):
    value = profile_data.get(field)
    return value if value is not None else _row_value(row, field)


async def enrich_stubs(limit: int = 200, player_ids: list[int] | None = None) -> int:
    load_dotenv()

    session = SessionLocal()
    pool = None

    try:
        # 1. Identify active players with missing metadata
        # We prioritize photo_url or height_cm being NULL
        query, params = build_stub_query(player_ids)
        if not player_ids:
            params["limit"] = limit

        stubs = session.execute(query, params).fetchall()

        if not stubs:
            print("✨ No active stubs found. Player metadata is already enriched!")
            return 0

        print(f"🎯 Found {len(stubs)} active players to enrich.")

        repo = PlayerBasicRepository()
        pool = AsyncPlaywrightPool(max_pages=2)
        crawler = PlayerProfileCrawler(pool=pool)
        await pool.start()

        enriched_count = 0
        for idx, row in enumerate(stubs, 1):
            pid = str(row.player_id)
            name = row.name
            pos = row.position

            print(f"[{idx}/{len(stubs)}] Processing {name} ({pid})...")

            try:
                data = await crawler.crawl_player_profile(pid, position=pos)
                if data:
                    # Merge with original row data for safety
                    update_payload = {
                        "player_id": int(pid),
                        "name": name,
                        "uniform_no": _row_value(row, "uniform_no"),
                        "team": _row_value(row, "team"),
                        "position": pos,
                        "birth_date": _row_value(row, "birth_date"),
                        "birth_date_date": _row_value(row, "birth_date_date"),
                        "height_cm": _row_value(row, "height_cm"),
                        "weight_kg": _row_value(row, "weight_kg"),
                        "career": _row_value(row, "career"),
                        "status": _row_value(row, "status"),
                        "staff_role": _row_value(row, "staff_role"),
                        "status_source": _row_value(row, "status_source"),
                        "photo_url": _profile_or_existing(data, row, "photo_url"),
                        "bats": _profile_or_existing(data, row, "bats"),
                        "throws": _profile_or_existing(data, row, "throws"),
                        "debut_year": _profile_or_existing(data, row, "debut_year"),
                        "salary_original": _profile_or_existing(data, row, "salary_original"),
                        "signing_bonus_original": _profile_or_existing(data, row, "signing_bonus_original"),
                        "draft_info": _profile_or_existing(data, row, "draft_info"),
                    }

                    # Note: PlayerProfileCrawler doesn't fetch height/weight currently
                    # as they were in the search result, but some stubs might be missing them.
                    # We might want to expand the JS extractor if needed.

                    repo.upsert_players([update_payload])
                    enriched_count += 1
                    print(f"   ✅ Enriched {name}")
                else:
                    print(f"   ⚠️  Could not find profile for {name}")
            except Exception as exc:
                print(f"   ❌ Error processing {name}: {exc}")

            # Periodic sleep to be extra safe
            if idx % 10 == 0:
                await asyncio.sleep(2)

        print(f"\n🚀 Enrichment complete! Successfully updated {enriched_count} players.")
        return enriched_count

    finally:
        if pool is not None:
            await pool.close()
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Player Basic Metadata")
    parser.add_argument("--limit", type=int, default=200, help="Max players to process")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")
    args = parser.parse_args()

    if args.ids:
        target_ids = parse_player_ids(args.ids)
        asyncio.run(enrich_stubs(limit=len(target_ids), player_ids=target_ids))
    else:
        asyncio.run(enrich_stubs(limit=args.limit))


if __name__ == "__main__":
    main()
