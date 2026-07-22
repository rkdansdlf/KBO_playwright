"""Look up unresolved players on the official KBO player search page."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

from sqlalchemy import select

from src.constants import KST
from src.crawlers.player_search_crawler import (
    PLAYER_SEARCH_EXCEPTIONS,
    SEARCH_URL,
    PlayerSearchCrawler,
)
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameLineup, GamePitchingStat
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.team_codes import TEAM_NAME_TO_CODE, resolve_kbo_legacy_team_code, resolve_team_code

logger = logging.getLogger(__name__)
INLINE_TARGET_PART_COUNT = 3

if TYPE_CHECKING:
    from collections.abc import Sequence


class LookupInputError(ValueError):
    """Raised when a lookup target cannot be parsed."""


def _input_error(message: str) -> LookupInputError:
    error = LookupInputError()
    error.args = (message,)
    return error


@dataclass(frozen=True)
class LookupTarget:
    """Describe one name, team, and season lookup target."""

    name: str
    team_code: str | None
    season: int


def _team_aliases(value: str | None, season: int) -> frozenset[str]:
    if not value:
        return frozenset()
    raw = value.strip().upper()
    if not raw:
        return frozenset()
    aliases = {raw}
    resolved = resolve_team_code(value, season)
    legacy = resolve_kbo_legacy_team_code(value, season)
    if resolved:
        aliases.add(resolved.upper())
    if legacy:
        aliases.add(legacy.upper())
    return frozenset(aliases)


def team_matches(target: LookupTarget, official_team: str | None) -> bool:
    """Return whether an official result belongs to the requested team."""
    if not target.team_code:
        return False
    return bool(_team_aliases(target.team_code, target.season) & _team_aliases(official_team, target.season))


def _team_tokens(value: str | None, season: int) -> frozenset[str]:
    aliases = _team_aliases(value, season)
    if not aliases:
        return frozenset()
    labels = {label.upper() for label, code in TEAM_NAME_TO_CODE.items() if _team_aliases(code, season) & aliases}
    return frozenset(aliases | labels)


def _career_team_matches(target: LookupTarget, career: object) -> bool:
    career_tokens = {token.upper() for token in re.split(r"[-/,()\s]+", str(career or "")) if token.strip()}
    return bool(_team_tokens(target.team_code, target.season) & career_tokens)


def _career_contains_season(career: object, season: int) -> bool:
    return str(season) in str(career or "")


def summarize_candidate(target: LookupTarget, candidate: dict[str, object]) -> dict[str, object]:
    """Serialize one official search result with matching evidence."""
    selected_keys = (
        "player_id",
        "name",
        "uniform_no",
        "team",
        "position",
        "birth_date",
        "height_cm",
        "weight_kg",
        "career",
    )
    summary = {key: candidate.get(key) for key in selected_keys}
    current_team_match = team_matches(target, _as_optional_text(candidate.get("team")))
    career_team_match = _career_team_matches(target, candidate.get("career"))
    summary["current_team_match"] = current_team_match
    summary["career_team_match"] = career_team_match
    summary["team_match"] = current_team_match or career_team_match
    summary["career_mentions_season"] = _career_contains_season(candidate.get("career"), target.season)
    return summary


def _as_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _result_for_target(target: LookupTarget, candidates: list[dict[str, object]]) -> dict[str, object]:
    summarized = [summarize_candidate(target, candidate) for candidate in candidates]
    return {
        "target": asdict(target),
        "search_url": f"{SEARCH_URL}?searchWord={quote(target.name)}",
        "candidate_count": len(summarized),
        "team_match_count": sum(bool(row["team_match"]) for row in summarized),
        "season_evidence_count": sum(bool(row["career_mentions_season"]) for row in summarized),
        "candidates": summarized,
    }


def _parse_season(raw: str | None, default: int | None) -> int:
    value = raw.strip() if raw else ""
    if not value:
        if default is None:
            message = "season is required in the input or via --year"
            raise _input_error(message)
        return default
    try:
        return int(value)
    except ValueError as exc:
        message = f"invalid season: {value}"
        raise _input_error(message) from exc


def _target_from_row(row: dict[str, str], default_year: int | None) -> LookupTarget:
    name = row.get("name", "").strip()
    if not name:
        message = "input row is missing name"
        raise _input_error(message)
    team_code = _as_optional_text(row.get("team_code"))
    return LookupTarget(name=name, team_code=team_code, season=_parse_season(row.get("season"), default_year))


def load_targets_from_csv(path: Path, default_year: int | None = None) -> list[LookupTarget]:
    """Load lookup targets from a CSV with name, team_code, and season columns."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [_target_from_row(row, default_year) for row in csv.DictReader(handle)]


def load_targets_from_db(year: int) -> list[LookupTarget]:
    """Read distinct NULL-player targets from game-level tables without writing."""
    start_date = f"{year:04d}-01-01"
    end_date = f"{year + 1:04d}-01-01"
    models = (GameLineup, GameBattingStat, GamePitchingStat)
    values: set[tuple[str, str | None]] = set()
    with SessionLocal() as session:
        for model in models:
            rows = session.execute(
                select(model.player_name, model.team_code)
                .join(Game, model.game_id == Game.game_id)
                .where(
                    model.player_id.is_(None),
                    Game.game_date >= start_date,
                    Game.game_date < end_date,
                ),
            ).all()
            values.update((str(name).strip(), _as_optional_text(team)) for name, team in rows if str(name).strip())
    return [LookupTarget(name=name, team_code=team, season=year) for name, team in sorted(values)]


def _parse_inline_target(raw: str, default_year: int | None) -> LookupTarget:
    parts = [part.strip() for part in raw.split(",", maxsplit=2)]
    if len(parts) != INLINE_TARGET_PART_COUNT:
        message = "--target must use NAME,TEAM_CODE,SEASON"
        raise _input_error(message)
    return LookupTarget(name=parts[0], team_code=parts[1] or None, season=_parse_season(parts[2], default_year))


def _deduplicate_targets(targets: Sequence[LookupTarget]) -> list[LookupTarget]:
    unique = {(target.name, target.team_code, target.season): target for target in targets}
    return [unique[key] for key in sorted(unique)]


async def lookup_targets(
    targets: Sequence[LookupTarget],
    *,
    request_delay: float,
    headless: bool,
) -> list[dict[str, object]]:
    """Search official KBO profiles for each target using one browser session."""
    pool = AsyncPlaywrightPool(max_pages=1, headless=headless)
    await pool.start()
    try:
        crawler = PlayerSearchCrawler(pool=pool, request_delay=request_delay, headless=headless)
        results: list[dict[str, object]] = []
        for target in targets:
            try:
                candidates = await crawler.search_player(target.name)
            except PLAYER_SEARCH_EXCEPTIONS as exc:
                logger.warning("Official player search failed for %s: %s", target.name, exc)
                results.append({"target": asdict(target), "error": str(exc), "candidates": []})
            else:
                results.append(_result_for_target(target, candidates))
        return results
    finally:
        await pool.close()


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the official player lookup argument parser."""
    parser = argparse.ArgumentParser(description="Read-only lookup against the official KBO player search page.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="CSV file with name, team_code, and season columns")
    source.add_argument("--from-db", action="store_true", help="Load NULL player targets from game-level SQLite tables")
    source.add_argument("--target", action="append", help="One NAME,TEAM_CODE,SEASON target; repeatable")
    parser.add_argument("--year", type=int, help="Default season for CSV rows or --target values")
    parser.add_argument("--limit", type=int, help="Limit targets after de-duplication")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between official requests in seconds")
    parser.add_argument("--headed", action="store_true", help="Show the browser window")
    parser.add_argument("--output", type=Path, help="Write the JSON report to this path")
    return parser


def _targets_from_args(args: argparse.Namespace) -> tuple[list[LookupTarget], str]:
    if args.input:
        return load_targets_from_csv(args.input, args.year), f"csv:{args.input}"
    if args.from_db:
        if args.year is None:
            message = "--from-db requires --year"
            raise _input_error(message)
        return load_targets_from_db(args.year), f"db:{args.year}"
    return [_parse_inline_target(raw, args.year) for raw in args.target], "inline"


def main(argv: Sequence[str] | None = None) -> None:
    """Run the read-only official player lookup CLI."""
    args = build_arg_parser().parse_args(argv)
    targets, source = _targets_from_args(args)
    targets = _deduplicate_targets(targets)
    if args.limit is not None:
        if args.limit < 1:
            message = "--limit must be positive"
            raise _input_error(message)
        targets = targets[: args.limit]
    if not targets:
        message = "no lookup targets found"
        raise _input_error(message)

    results = asyncio.run(lookup_targets(targets, request_delay=args.delay, headless=not args.headed))
    report = {
        "generated_at": datetime.now(KST).isoformat(),
        "source": source,
        "read_only": True,
        "target_count": len(targets),
        "results": results,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
        sys.stdout.write(f"Wrote official player lookup report: {args.output}\n")
    else:
        sys.stdout.write(payload)


if __name__ == "__main__":
    main()
