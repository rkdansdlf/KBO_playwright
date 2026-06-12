#!/usr/bin/env python3
"""Backfill verified player_basic profiles referenced by season stats."""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.player_validation import validate_player_payload

logger = logging.getLogger(__name__)
DEFAULT_REPORT_DIR = Path("data/player_profile_backfill")


@dataclass
class Candidate:
    player_id: int
    team_code: str | None
    existing_name: str | None
    position: str | None
    source: str


def parse_player_ids(raw_ids: str | None) -> list[int]:
    if not raw_ids:
        return []
    return [int(token.strip()) for token in raw_ids.split(",") if token.strip()]


def _unknown_name_sql(alias: str) -> str:
    return f"UPPER(TRIM(COALESCE({alias}.name, ''))) LIKE 'UNKNOWN %'"


def _execute_candidate_query(session, sql: str, params: dict[str, Any], player_ids: list[int]):
    query = text(sql)
    if player_ids:
        query = query.bindparams(bindparam("player_ids", expanding=True))
    return session.execute(query, params).fetchall()


def _table_exists(session, table_name: str) -> bool:
    row = session.execute(
        text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = :table_name"),
        {"table_name": table_name},
    ).first()
    return row is not None


def _collect_missing_from_table(session, table_name: str, source: str, player_ids: list[int]) -> list[Candidate]:
    player_filter = "AND t.player_id IN :player_ids" if player_ids else ""
    rows = _execute_candidate_query(
        session,
        f"""
        SELECT DISTINCT t.player_id, t.team_code
        FROM {table_name} t
        LEFT JOIN player_basic p ON t.player_id = p.player_id
        WHERE p.player_id IS NULL
          {player_filter}
        ORDER BY t.player_id
        """,
        {"player_ids": player_ids} if player_ids else {},
        player_ids,
    )
    return [
        Candidate(
            player_id=int(row[0]),
            team_code=row[1],
            existing_name=None,
            position="투수" if source == "pitching_missing" else None,
            source=source,
        )
        for row in rows
        if row[0] is not None
    ]


def _collect_unknown_stubs(session, table_names: list[tuple[str, str]], player_ids: list[int]) -> list[Candidate]:
    candidates: dict[int, Candidate] = {}
    for table_name, source in table_names:
        player_filter = "AND p.player_id IN :player_ids" if player_ids else ""
        rows = _execute_candidate_query(
            session,
            f"""
            SELECT DISTINCT p.player_id, COALESCE(t.team_code, p.team), p.name, p.position
            FROM player_basic p
            JOIN {table_name} t ON p.player_id = t.player_id
            WHERE {_unknown_name_sql("p")}
              {player_filter}
            ORDER BY p.player_id
            """,
            {"player_ids": player_ids} if player_ids else {},
            player_ids,
        )
        for row in rows:
            player_id = int(row[0])
            candidates.setdefault(
                player_id,
                Candidate(
                    player_id=player_id,
                    team_code=row[1],
                    existing_name=row[2],
                    position=row[3] or ("투수" if source == "pitching_unknown_stub" else None),
                    source=source,
                ),
            )
    return list(candidates.values())


def find_candidates(
    *,
    include_pitching: bool = False,
    include_unknown_stubs: bool = False,
    player_ids: list[int] | None = None,
    limit: int | None = None,
) -> list[Candidate]:
    player_ids = player_ids or []
    candidates: dict[int, Candidate] = {}
    with SessionLocal() as session:
        for candidate in _collect_missing_from_table(
            session,
            "player_season_batting",
            "batting_missing",
            player_ids,
        ):
            candidates.setdefault(candidate.player_id, candidate)

        if include_pitching:
            for candidate in _collect_missing_from_table(
                session,
                "player_season_pitching",
                "pitching_missing",
                player_ids,
            ):
                candidates.setdefault(candidate.player_id, candidate)

        if include_unknown_stubs:
            source_tables = [("player_season_batting", "batting_unknown_stub")]
            if include_pitching:
                source_tables.append(("player_season_pitching", "pitching_unknown_stub"))
            for candidate in _collect_unknown_stubs(session, source_tables, player_ids):
                candidates.setdefault(candidate.player_id, candidate)

    rows = sorted(candidates.values(), key=lambda item: item.player_id)
    return rows[:limit] if limit is not None else rows


def _clean_optional_photo_url(value: str | None) -> str | None:
    if not value or "no-Image.png" in value:
        return None
    return value


def _normalize_status(value: str | None) -> str:
    if not value:
        return "backfilled"
    return value.strip().lower()


def load_local_canonical_profiles(player_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Return verified names already present in canonical players/player_identities."""
    if not player_ids:
        return {}

    with SessionLocal() as session:
        if not (_table_exists(session, "players") and _table_exists(session, "player_identities")):
            return {}

        query = text(
            """
            SELECT
              p.id AS player_id,
              i.name_kor AS identity_name,
              kb.name AS kbo_profile_name,
              COALESCE(NULLIF(p.height_cm, 0), NULLIF(kb.height_cm, 0)) AS height_cm,
              COALESCE(NULLIF(p.weight_kg, 0), NULLIF(kb.weight_kg, 0)) AS weight_kg,
              COALESCE(p.bats, kb.bats) AS bats,
              COALESCE(p.throws, kb.throws) AS throws,
              COALESCE(p.debut_year, kb.debut_year) AS debut_year,
              COALESCE(p.photo_url, kb.photo_url) AS photo_url,
              COALESCE(p.salary_original, kb.salary_original) AS salary_original,
              COALESCE(p.signing_bonus_original, kb.signing_bonus_original) AS signing_bonus_original,
              COALESCE(p.draft_info, kb.draft_info) AS draft_info,
              p.status
            FROM players p
            LEFT JOIN player_identities i
              ON i.player_id = p.id
             AND i.is_primary = 1
            LEFT JOIN player_basic kb
              ON kb.player_id = CAST(p.kbo_person_id AS INTEGER)
             AND p.kbo_person_id GLOB '[0-9]*'
             AND UPPER(TRIM(COALESCE(kb.name, ''))) NOT LIKE 'UNKNOWN %'
            WHERE p.id IN :player_ids
              AND COALESCE(NULLIF(TRIM(i.name_kor), ''), NULLIF(TRIM(kb.name), '')) IS NOT NULL
            """
        ).bindparams(bindparam("player_ids", expanding=True))
        rows = session.execute(query, {"player_ids": player_ids}).mappings().fetchall()

    profiles: dict[int, dict[str, Any]] = {}
    for row in rows:
        player_id = int(row["player_id"])
        name = row["identity_name"] or row["kbo_profile_name"]
        profiles[player_id] = {
            "player_id": player_id,
            "name": name,
            "height_cm": row["height_cm"],
            "weight_kg": row["weight_kg"],
            "bats": row["bats"],
            "throws": row["throws"],
            "debut_year": row["debut_year"],
            "photo_url": _clean_optional_photo_url(row["photo_url"]),
            "salary_original": row["salary_original"],
            "signing_bonus_original": row["signing_bonus_original"],
            "draft_info": row["draft_info"],
            "status": _normalize_status(row["status"]),
            "status_source": "canonical_fb" if row["identity_name"] else "kbo_person_fb",
        }
    return profiles


def _build_payload(candidate: Candidate, profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "player_id": candidate.player_id,
        "name": profile.get("name"),
        "team": profile.get("team") or candidate.team_code,
        "position": profile.get("position") or candidate.position,
        "photo_url": profile.get("photo_url"),
        "bats": profile.get("bats"),
        "throws": profile.get("throws"),
        "height_cm": profile.get("height_cm"),
        "weight_kg": profile.get("weight_kg"),
        "debut_year": profile.get("debut_year"),
        "salary_original": profile.get("salary_original"),
        "signing_bonus_original": profile.get("signing_bonus_original"),
        "draft_info": profile.get("draft_info"),
        "status": profile.get("status") or "backfilled",
        "status_source": profile.get("status_source") or "profile_backfill",
    }


def _write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "player_id",
        "candidate_source",
        "team_code",
        "existing_name",
        "profile_name",
        "status",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


async def backfill_players(
    *,
    include_pitching: bool = False,
    include_unknown_stubs: bool = False,
    apply: bool = False,
    limit: int | None = None,
    player_ids: list[int] | None = None,
    report_dir: Path = DEFAULT_REPORT_DIR,
    delay: float = 1.0,
) -> dict[str, Any]:
    candidates = find_candidates(
        include_pitching=include_pitching,
        include_unknown_stubs=include_unknown_stubs,
        player_ids=player_ids,
        limit=limit,
    )
    if not candidates:
        logger.info("No missing or stub player profiles found.")
        return {
            "candidates": 0,
            "prepared": 0,
            "saved": 0,
            "skipped": 0,
            "report_csv": None,
        }

    logger.info(f"Found {len(candidates)} player profile candidates.")
    local_profiles = load_local_canonical_profiles([candidate.player_id for candidate in candidates])
    crawler: PlayerProfileCrawler | None = None
    repo = PlayerBasicRepository()
    report_rows: list[dict[str, Any]] = []
    prepared = 0
    saved = 0
    skipped = 0

    for idx, candidate in enumerate(candidates, start=1):
        player_id = str(candidate.player_id)
        logger.info(f"[{idx}/{len(candidates)}] Processing player {player_id} ({candidate.source})...")
        profile = local_profiles.get(candidate.player_id)
        if profile:
            reason = "local_canonical_profile"
        else:
            if crawler is None:
                crawler = PlayerProfileCrawler()
            try:
                profile = await crawler.crawl_player_profile(player_id, position=candidate.position)
            except Exception as exc:  # noqa: BLE001
                profile = None
                reason = f"crawl_error:{exc}"
            else:
                reason = "profile_not_found" if not profile else "official_profile"

        if not profile:
            skipped += 1
            report_rows.append(
                {
                    "player_id": candidate.player_id,
                    "candidate_source": candidate.source,
                    "team_code": candidate.team_code,
                    "existing_name": candidate.existing_name,
                    "profile_name": "",
                    "status": "skipped",
                    "reason": reason,
                }
            )
            continue

        payload = _build_payload(candidate, profile)
        ok, validation_reason = validate_player_payload(payload)
        if not ok:
            skipped += 1
            report_rows.append(
                {
                    "player_id": candidate.player_id,
                    "candidate_source": candidate.source,
                    "team_code": candidate.team_code,
                    "existing_name": candidate.existing_name,
                    "profile_name": profile.get("name") or "",
                    "status": "skipped",
                    "reason": validation_reason or "invalid_profile_payload",
                }
            )
            continue

        prepared += 1
        if apply:
            saved += repo.upsert_players([payload])
        report_rows.append(
            {
                "player_id": candidate.player_id,
                "candidate_source": candidate.source,
                "team_code": candidate.team_code,
                "existing_name": candidate.existing_name,
                "profile_name": payload["name"],
                "status": "saved" if apply else "prepared",
                "reason": reason,
            }
        )

        if delay and idx < len(candidates):
            await asyncio.sleep(delay)

    report_path = report_dir / f"missing_player_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    _write_report(report_path, report_rows)
    logger.info(
        f"Backfill {'applied' if apply else 'dry-run'} complete: prepared={prepared} saved={saved} skipped={skipped}"
    )
    logger.info(f"report_csv={report_path}")
    return {
        "candidates": len(candidates),
        "prepared": prepared,
        "saved": saved,
        "skipped": skipped,
        "report_csv": str(report_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill verified player_basic profiles.")
    parser.add_argument("--include-pitching", action="store_true", help="Include player_season_pitching references.")
    parser.add_argument(
        "--include-unknown-stubs", action="store_true", help="Include player_basic rows named Unknown <id>."
    )
    parser.add_argument("--apply", action="store_true", help="Persist verified profiles. Default is dry-run.")
    parser.add_argument("--limit", type=int, help="Maximum candidates to process.")
    parser.add_argument("--ids", help="Comma-separated player IDs to process.")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between profile requests in seconds.")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="CSV report directory.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(
        backfill_players(
            include_pitching=args.include_pitching,
            include_unknown_stubs=args.include_unknown_stubs,
            apply=args.apply,
            limit=args.limit,
            player_ids=parse_player_ids(args.ids),
            report_dir=Path(args.report_dir),
            delay=args.delay,
        )
    )


if __name__ == "__main__":
    main()
