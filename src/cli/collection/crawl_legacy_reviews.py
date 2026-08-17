"""Crawl legacy (2001-2009) KBO boxscore REVIEW pages into local HTML cache.

Reads game rows from the local SQLite DB (game table) for the requested
season range, derives the GameCenter REVIEW URL for each game, downloads
the page, verifies a minimal boxscore marker, and writes:

- data/schedules/legacy_html/{game_id}.html
- data/schedules/legacy_html/manifest.csv (game_id, game_date, status,
  sha256, bytes, fetched_at)

Idempotent: games already present in the manifest with a matching sha256
are skipped. Failing games are recorded as status=error and retried on a
later run (re-download when the manifest entry is missing or errored).
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from sqlalchemy import text

from src.constants import GAME_ID_MIN_LEN
from src.db.engine import SessionLocal
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.type_helpers import safe_int_or_none

REVIEW_URL = (
    "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={date}&gameId={game_id}&section=REVIEW"
)
OUT_DIR = Path("data/schedules/legacy_html")
MANIFEST_CSV = OUT_DIR / "manifest.csv"
MIN_HTML_BYTES = 20_000
BOXSCORE_MARKER = "tbl-type"
BOXSCORE_MARKER_MIN_COUNT = 3
MANIFEST_FIELD_COUNT = 6
MANIFEST_MIN_PARTS = 2
PROGRESS_INTERVAL = 50
DEFAULT_YEAR_RANGE = (2001, 2009)

# Segment mapping: DB canonical code -> KBO site game_id segment (2001-2009).
# The schedule crawler stored normalized codes (HU) in game_id, but the KBO
# GameCenter site only serves review pages under its original segments (HD).
SITE_GAME_ID_SEGMENT = {
    "HU": "HD",
}


def _to_site_game_id(db_game_id: str) -> str:
    """Map a DB game_id to the KBO site game_id used by review URLs."""
    if len(db_game_id) < GAME_ID_MIN_LEN:
        return db_game_id
    away_seg = db_game_id[8:10]
    home_seg = db_game_id[10:12]
    away_mapped = SITE_GAME_ID_SEGMENT.get(away_seg, away_seg)
    home_mapped = SITE_GAME_ID_SEGMENT.get(home_seg, home_seg)
    if away_mapped == away_seg and home_mapped == home_seg:
        return db_game_id
    return db_game_id[:8] + away_mapped + home_mapped + db_game_id[12:]


def _load_target_games(start_year: int, end_year: int) -> list[tuple[str, str]]:
    start = f"{start_year}-01-01"
    end = f"{end_year + 1}-01-01"
    with SessionLocal() as session:
        rows = session.execute(
            text(
                "SELECT game_id, game_date FROM game "
                "WHERE game_date >= :start AND game_date < :end "
                "ORDER BY game_date, game_id"
            ),
            {"start": start, "end": end},
        ).fetchall()
    return [(row[0], row[1]) for row in rows]


def _load_manifest() -> dict[str, dict[str, str]]:
    if not MANIFEST_CSV.exists():
        return {}
    entries: dict[str, dict[str, str]] = {}
    for line in MANIFEST_CSV.read_text(encoding="utf-8").splitlines()[1:]:
        if not line.strip():
            continue
        parts = line.strip().split(",")
        if len(parts) >= MANIFEST_FIELD_COUNT:
            site_game_id = parts[1] if len(parts) >= MANIFEST_MIN_PARTS else parts[0]
            entries[parts[0]] = {
                "site_game_id": site_game_id,
                "game_date": parts[2],
                "status": parts[3],
                "sha256": parts[4],
                "size": parts[5],
            }
    return entries


def _write_manifest(entries: dict[str, dict[str, str]]) -> None:
    lines = ["game_id,site_game_id,game_date,status,sha256,bytes"]
    for game_id in sorted(entries):
        e = entries[game_id]
        lines.append(",".join([game_id, e["site_game_id"], e["game_date"], e["status"], e["sha256"], e["size"]]))
    MANIFEST_CSV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _skip(
    entries: dict[str, dict[str, str]],
    game_id: str,
    *,
    retry_errors: bool,
) -> bool:
    """Return whether a game should be skipped by the manifest state."""
    entry = entries.get(game_id)
    if entry is None:
        return False
    if entry["status"] == "ok":
        return True
    return not retry_errors


def crawl_reviews(
    start_year: int,
    end_year: int,
    limit: int | None = None,
    *,
    retry_errors: bool = False,
) -> dict[str, int]:
    """Crawl boxscore REVIEW pages for a season range into the HTML cache."""
    targets = _load_target_games(start_year, end_year)
    if limit is not None:
        targets = targets[:limit]
    entries = _load_manifest()

    counters = {"ok": 0, "skipped": 0, "error": 0}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        install_sync_resource_blocking(page)
        try:
            for db_game_id, game_date in targets:
                if _skip(entries, db_game_id, retry_errors=retry_errors):
                    counters["skipped"] += 1
                    continue

                site_game_id = _to_site_game_id(db_game_id)
                date_compact = game_date.replace("-", "")
                url = REVIEW_URL.format(date=date_compact, game_id=site_game_id)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                    time.sleep(1.5)
                    html = page.content()
                except PlaywrightError as exc:
                    entries[db_game_id] = {
                        "site_game_id": site_game_id,
                        "game_date": game_date,
                        "status": "error",
                        "sha256": "",
                        "size": "0",
                    }
                    counters["error"] += 1
                    sys.stdout.write(f"ERROR {db_game_id}: {exc}\n")
                    continue

                marker_count = html.count(BOXSCORE_MARKER)
                if len(html) < MIN_HTML_BYTES or marker_count < BOXSCORE_MARKER_MIN_COUNT:
                    entries[db_game_id] = {
                        "site_game_id": site_game_id,
                        "game_date": game_date,
                        "status": "error",
                        "sha256": "",
                        "size": str(len(html)),
                    }
                    counters["error"] += 1
                    sys.stdout.write(f"EMPTY {db_game_id}: len={len(html)} marker={marker_count}\n")
                    continue

                (OUT_DIR / f"{site_game_id}.html").write_text(html, encoding="utf-8")
                digest = hashlib.sha256(html.encode("utf-8")).hexdigest()
                entries[db_game_id] = {
                    "site_game_id": site_game_id,
                    "game_date": game_date,
                    "status": "ok",
                    "sha256": digest,
                    "size": str(len(html)),
                }
                counters["ok"] += 1
                if counters["ok"] % PROGRESS_INTERVAL == 0:
                    sys.stdout.write(f"  progress: ok={counters['ok']} error={counters['error']}\n")
        finally:
            browser.close()

    _write_manifest(entries)
    return counters


def main(argv: list[str] | None = None) -> None:
    """Run the legacy REVIEW page crawl CLI."""
    parser = argparse.ArgumentParser(description="Crawl legacy 2001-2009 boxscore REVIEW pages")
    parser.add_argument("--start-year", type=int, default=DEFAULT_YEAR_RANGE[0])
    parser.add_argument("--end-year", type=int, default=DEFAULT_YEAR_RANGE[1])
    parser.add_argument("--limit", type=int, default=None, help="crawl only first N games")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="re-download games whose manifest status is not ok",
    )
    args = parser.parse_args(argv)

    start_year = safe_int_or_none(args.start_year) or DEFAULT_YEAR_RANGE[0]
    end_year = safe_int_or_none(args.end_year) or DEFAULT_YEAR_RANGE[1]

    sys.stdout.write(f"crawling REVIEW pages for {start_year}-{end_year} games...\n")
    counters = crawl_reviews(
        start_year,
        end_year,
        limit=args.limit,
        retry_errors=args.retry_errors,
    )
    sys.stdout.write(f"done: ok={counters['ok']} skipped={counters['skipped']} error={counters['error']}\n")
    if counters["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
