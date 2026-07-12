from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.sources.relay import derive_bucket_id

logger = logging.getLogger(__name__)

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_CAPTURE_URL = "https://web.archive.org/web/{timestamp}id_/{original}"
USER_AGENT = "Mozilla/5.0 (compatible; KBORelayRecovery/1.0; +https://www.koreabaseball.com)"
WAYBACK_EXCEPTIONS = (
    HTTPError,
    URLError,
    TimeoutError,
    OSError,
    RuntimeError,
    ValueError,
    KeyError,
    json.JSONDecodeError,
)


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _selected_rows(
    input_path: Path,
    game_ids: set[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, str]]:
    rows = _load_rows(input_path)
    selected: list[dict[str, str]] = []
    for row in rows:
        game_id = str(row.get("game_id") or "").strip()
        if not game_id:
            continue
        if game_ids and game_id not in game_ids:
            continue
        selected.append(row)
        if limit and len(selected) >= limit:
            break
    return selected


def _candidate_targets(game_id: str) -> list[dict[str, str]]:
    game_date = game_id[:8]
    year = game_id[:4]
    base_gamecenter = (
        f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}"
    )
    return [
        {
            "url_kind": "gamecenter_relay",
            "search_url": f"{base_gamecenter}&section=RELAY",
            "match_type": "exact",
        },
        {
            "url_kind": "gamecenter_main_prefix",
            "search_url": base_gamecenter,
            "match_type": "prefix",
        },
        {
            "url_kind": "livetext",
            "search_url": (
                f"https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId={game_id}&gyear={year}"
            ),
            "match_type": "exact",
        },
    ]


def _fetch_json(url: str, timeout: float) -> Any:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_text(url: str, timeout: float) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _query_wayback(search_url: str, match_type: str, timeout: float) -> list[dict[str, str]]:
    params = {
        "url": search_url,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": "statuscode:200",
        "limit": "10",
    }
    if match_type != "exact":
        params["matchType"] = match_type
    payload = _fetch_json(f"{WAYBACK_CDX_URL}?{urlencode(params)}", timeout=timeout)
    if not isinstance(payload, list) or len(payload) <= 1:
        return []

    results: list[dict[str, str]] = []
    for row in payload[1:]:
        if not isinstance(row, list) or len(row) < 4:
            continue
        results.append(
            {
                "timestamp": str(row[0] or "").strip(),
                "original": str(row[1] or "").strip(),
                "statuscode": str(row[2] or "").strip(),
                "mimetype": str(row[3] or "").strip(),
            },
        )
    return results


def _pick_capture(url_kind: str, captures: list[dict[str, str]]) -> dict[str, str] | None:
    if not captures:
        return None
    if url_kind == "gamecenter_main_prefix":
        relay = [row for row in captures if "section=RELAY" in str(row.get("original") or "")]
        if relay:
            return relay[0]
        review = [row for row in captures if "section=REVIEW" in str(row.get("original") or "")]
        if review:
            return review[0]
    return captures[0]


def _download_capture(game_id: str, url_kind: str, capture: dict[str, str], download_dir: Path, timeout: float) -> Path:
    download_dir.mkdir(parents=True, exist_ok=True)
    archive_url = WAYBACK_CAPTURE_URL.format(
        timestamp=capture["timestamp"],
        original=capture["original"],
    )
    html = _fetch_text(archive_url, timeout=timeout)
    output_path = download_dir / f"{game_id}__{url_kind}__{capture['timestamp']}.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _process_discover_row(
    row: dict[str, str], *, timeout: float, download_dir: Path | None, sleep_seconds: float
) -> list[dict[str, str]]:
    game_id = str(row.get("game_id") or "").strip()
    bucket_id = str(row.get("bucket_id") or "").strip() or derive_bucket_id(game_id, row.get("league_type_name"))
    game_rows: list[dict[str, str]] = []
    found = False
    for target in _candidate_targets(game_id):
        notes = ""
        capture = None
        download_path = ""
        try:
            captures = _query_wayback(search_url=target["search_url"], match_type=target["match_type"], timeout=timeout)
            capture = _pick_capture(target["url_kind"], captures)
            if capture and download_dir is not None:
                saved = _download_capture(game_id, target["url_kind"], capture, download_dir, timeout)
                download_path = str(saved)
        except WAYBACK_EXCEPTIONS as exc:
            notes = f"{type(exc).__name__}: {exc}"
        if capture:
            found = True
            game_rows.append(
                {
                    "game_id": game_id,
                    "bucket_id": bucket_id,
                    "url_kind": target["url_kind"],
                    "search_url": target["search_url"],
                    "capture_found": "true",
                    "timestamp": capture["timestamp"],
                    "original": capture["original"],
                    "mimetype": capture["mimetype"],
                    "download_path": download_path,
                    "notes": notes,
                }
            )
            break
        game_rows.append(
            {
                "game_id": game_id,
                "bucket_id": bucket_id,
                "url_kind": target["url_kind"],
                "search_url": target["search_url"],
                "capture_found": "false",
                "timestamp": "",
                "original": "",
                "mimetype": "",
                "download_path": "",
                "notes": notes or "No capture found",
            }
        )
        time.sleep(sleep_seconds)
    if not found:
        time.sleep(sleep_seconds)
    return game_rows


def _write_discover_output(
    report_rows: list[dict[str, str]], output_path: Path, manifest_output_path: Path | None
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "game_id",
        "bucket_id",
        "url_kind",
        "search_url",
        "capture_found",
        "timestamp",
        "original",
        "mimetype",
        "download_path",
        "notes",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    if manifest_output_path is not None:
        _write_manifest_from_report(report_rows, manifest_output_path)


def discover_captures(
    *,
    input_path: Path,
    output_path: Path,
    game_ids: set[str] | None,
    limit: int | None,
    download_dir: Path | None,
    timeout: float,
    sleep_seconds: float,
    workers: int,
    manifest_output_path: Path | None = None,
) -> int:
    rows = _selected_rows(input_path, game_ids=game_ids, limit=limit)
    report_rows: list[dict[str, str]] = []
    total = len(rows)
    if workers <= 1:
        for index, row in enumerate(rows, start=1):
            game_id = str(row.get("game_id") or "").strip()
            logger.info("[WAYBACK] %s/%s: %s", index, total, game_id)
            report_rows.extend(
                _process_discover_row(row, timeout=timeout, download_dir=download_dir, sleep_seconds=sleep_seconds)
            )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(
                    _process_discover_row, row, timeout=timeout, download_dir=download_dir, sleep_seconds=sleep_seconds
                ): str(row.get("game_id") or "").strip()
                for row in rows
            }
            completed = 0
            for future in as_completed(future_map):
                completed += 1
                game_id = future_map[future]
                logger.info("[WAYBACK] %s/%s: %s", completed, total, game_id)
                report_rows.extend(future.result())
    _write_discover_output(report_rows, output_path, manifest_output_path)
    return len(report_rows)


def _write_manifest_from_report(report_rows: list[dict[str, str]], output_path: Path) -> int:
    manifest_rows = []
    for row in report_rows:
        if row.get("capture_found") != "true" or not row.get("download_path"):
            continue
        manifest_format = "kbo_html" if row.get("url_kind") == "livetext" else "relay_html"
        manifest_rows.append(
            {
                "game_id": row["game_id"],
                "source_type": "html_archive",
                "locator": row["download_path"],
                "format": manifest_format,
                "priority": 50,
                "notes": f"wayback:{row.get('timestamp')} {row.get('original')}",
            },
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_id", "source_type", "locator", "format", "priority", "notes"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)
    return len(manifest_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover archived KBO relay captures from the Internet Archive")
    parser.add_argument(
        "--input",
        default="data/recovery/relay_unresolved_completed_games_oci_20260415.csv",
        help="Unresolved relay backlog CSV",
    )
    parser.add_argument(
        "--output",
        default="data/recovery/wayback_capture_report_20260415.csv",
        help="Wayback discovery report CSV",
    )
    parser.add_argument("--game-ids", help="Optional comma separated game IDs to limit discovery")
    parser.add_argument("--limit", type=int, help="Optional max number of games to scan")
    parser.add_argument(
        "--download-dir",
        default="data/recovery/raw/html_archive",
        help="Directory to store downloaded archived HTML captures",
    )
    parser.add_argument("--manifest-output", help="Optional source_manifest CSV to write for downloaded captures")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--sleep-seconds", type=float, default=0.25, help="Delay between archive lookups")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent archive lookups")
    args = parser.parse_args()

    requested_ids = {token.strip() for token in str(args.game_ids or "").split(",") if token.strip()} or None

    written = discover_captures(
        input_path=Path(args.input),
        output_path=Path(args.output),
        game_ids=requested_ids,
        limit=args.limit,
        download_dir=Path(args.download_dir) if args.download_dir else None,
        manifest_output_path=Path(args.manifest_output) if args.manifest_output else None,
        timeout=args.timeout,
        sleep_seconds=args.sleep_seconds,
        workers=max(1, args.workers),
    )
    logger.info("Wrote %s wayback discovery rows to %s", written, args.output)


if __name__ == "__main__":
    main()
