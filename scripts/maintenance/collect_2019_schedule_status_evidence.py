#!/usr/bin/env python3
"""
Collect schedule-status evidence for unresolved 2019 games.

Output files:
  - data/game_status_schedule_evidence.csv
  - data/game_status_schedule_unmatched.csv

Evidence status mapping:
  - text contains "취소" -> CANCELLED
  - text contains "순연" or "연기" -> POSTPONED
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

SCHEDULE_API_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList"
DEFAULT_EVIDENCE_CSV = PROJECT_ROOT / "data/game_status_schedule_evidence.csv"
DEFAULT_UNMATCHED_CSV = PROJECT_ROOT / "data/game_status_schedule_unmatched.csv"
STATUS_CANCELLED = "CANCELLED"
STATUS_POSTPONED = "POSTPONED"

DATE_TOKEN_RE = re.compile(r"\b(\d{2}\.\d{2})")
GAME_ID_RE = re.compile(r"gameId=([0-9A-Z]+)", re.IGNORECASE)
GAME_ID_FALLBACK_RE = re.compile(r"\b(20\d{6}[A-Z]{4}\d)\b")


@dataclass
class ScheduleStatusRow:
    season: int
    month: int
    game_id: str
    resolved_status: str
    date_text: str
    status_text: str
    match_text: str
    evidence_source: str
    raw_row: str


def parse_months(months_arg: Optional[str]) -> List[int]:
    if not months_arg:
        return list(range(3, 11))

    months: List[int] = []
    for token in (x.strip() for x in months_arg.split(",") if x.strip()):
        if "-" in token:
            left, right = token.split("-", 1)
            try:
                start = int(left)
                end = int(right)
            except ValueError:
                continue
            months.extend(range(start, end + 1))
        else:
            try:
                months.append(int(token))
            except ValueError:
                continue
    return sorted({m for m in months if 1 <= m <= 12})


def _strip_html_text(value: Any) -> str:
    text_value = str(value or "")
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    text_value = html.unescape(text_value)
    return " ".join(text_value.split())


def _extract_game_id(raw_values: Sequence[str]) -> str:
    raw_blob = " ".join(raw_values)
    match = GAME_ID_RE.search(raw_blob)
    if match:
        return match.group(1).upper()
    fallback = GAME_ID_FALLBACK_RE.search(_strip_html_text(raw_blob))
    if fallback:
        return fallback.group(1).upper()
    return ""


def _detect_status(status_blob: str) -> str:
    if "취소" in status_blob:
        return STATUS_CANCELLED
    if "순연" in status_blob or "연기" in status_blob:
        return STATUS_POSTPONED
    return ""


def _extract_payload_rows(response_text: str) -> List[Any]:
    outer = json.loads(response_text)
    payload: Any = outer.get("d", outer)
    if isinstance(payload, str):
        payload = payload.strip()
        if payload:
            payload = json.loads(payload)
        else:
            payload = {}
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("Rows") or payload.get("data") or []
        if isinstance(rows, list):
            return rows
    if isinstance(payload, list):
        return payload
    return []


def _load_unresolved_games(year: int) -> Dict[str, str]:
    with SessionLocal() as session:
        rows = session.execute(
            text(
                """
                SELECT game_id, game_date
                FROM game
                WHERE substr(game_id, 1, 4) = :year
                  AND game_status = 'UNRESOLVED_MISSING'
                ORDER BY game_id
                """
            ),
            {"year": str(year)},
        ).fetchall()
    return {str(game_id): str(game_date) for game_id, game_date in rows}


def _fetch_month_rows(
    *,
    client: httpx.Client,
    year: int,
    month: int,
    league_id: str,
    series_ids: str,
) -> List[ScheduleStatusRow]:
    params = {
        "leId": str(league_id),
        "srIdList": str(series_ids),
        "season": str(year),
        "month": str(month),
    }
    response = client.get(SCHEDULE_API_URL, params=params, timeout=30.0)
    response.raise_for_status()
    rows = _extract_payload_rows(response.text)

    collected: List[ScheduleStatusRow] = []
    current_date_text = ""
    evidence_source = f"{SCHEDULE_API_URL}?leId={league_id}&srIdList={series_ids}&season={year}&month={month}"
    for item in rows:
        if isinstance(item, dict):
            raw_cells_obj = item.get("row") or item.get("Row") or item.get("data") or []
            raw_cells = [str(x or "") for x in raw_cells_obj] if isinstance(raw_cells_obj, list) else [str(item)]
        elif isinstance(item, list):
            raw_cells = [str(x or "") for x in item]
        else:
            raw_cells = [str(item)]

        clean_cells = [_strip_html_text(x) for x in raw_cells]
        joined = " ".join(x for x in clean_cells if x)
        if not joined:
            continue

        date_match = DATE_TOKEN_RE.search(joined)
        if date_match:
            current_date_text = date_match.group(1)

        resolved_status = _detect_status(joined)
        if not resolved_status:
            continue

        game_id = _extract_game_id(raw_cells + clean_cells)
        match_text = next((x for x in clean_cells if "vs" in x.lower()), "")
        status_text = joined
        collected.append(
            ScheduleStatusRow(
                season=year,
                month=month,
                game_id=game_id,
                resolved_status=resolved_status,
                date_text=current_date_text,
                status_text=status_text,
                match_text=match_text,
                evidence_source=evidence_source,
                raw_row=" | ".join(clean_cells),
            )
        )

    return collected


def _write_csv(path: Path, columns: Iterable[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns))
        writer.writeheader()
        writer.writerows(rows)


def collect_schedule_status_evidence(
    *,
    year: int = 2019,
    months: Sequence[int] = tuple(range(3, 11)),
    league_id: str = "1",
    series_ids: str = "0,9,6",
    evidence_csv: Path = DEFAULT_EVIDENCE_CSV,
    unmatched_csv: Path = DEFAULT_UNMATCHED_CSV,
) -> Dict[str, Any]:
    unresolved_games = _load_unresolved_games(year)
    unresolved_ids = set(unresolved_games.keys())

    all_rows: List[ScheduleStatusRow] = []
    unmatched_rows: List[Dict[str, Any]] = []
    error_months: List[str] = []
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    with httpx.Client(headers=headers) as client:
        for month in months:
            try:
                rows = _fetch_month_rows(
                    client=client,
                    year=year,
                    month=int(month),
                    league_id=league_id,
                    series_ids=series_ids,
                )
                all_rows.extend(rows)
            except Exception as exc:
                error_months.append(f"{year}-{int(month):02d}: {exc}")

    evidence_by_game_id: Dict[str, ScheduleStatusRow] = {}
    for row in all_rows:
        if not row.game_id:
            unmatched_rows.append(
                {
                    "expected_game_id": "",
                    "candidate_game_id": "",
                    "resolved_status": row.resolved_status,
                    "reason": "missing_game_id_in_schedule_row",
                    "evidence_source": row.evidence_source,
                    "season": row.season,
                    "month": row.month,
                    "date_text": row.date_text,
                    "status_text": row.status_text,
                    "match_text": row.match_text,
                    "raw_row": row.raw_row,
                }
            )
            continue

        if row.game_id not in unresolved_ids:
            unmatched_rows.append(
                {
                    "expected_game_id": "",
                    "candidate_game_id": row.game_id,
                    "resolved_status": row.resolved_status,
                    "reason": "game_id_not_in_unresolved_set",
                    "evidence_source": row.evidence_source,
                    "season": row.season,
                    "month": row.month,
                    "date_text": row.date_text,
                    "status_text": row.status_text,
                    "match_text": row.match_text,
                    "raw_row": row.raw_row,
                }
            )
            continue

        existing = evidence_by_game_id.get(row.game_id)
        if existing and existing.resolved_status != row.resolved_status:
            unmatched_rows.append(
                {
                    "expected_game_id": row.game_id,
                    "candidate_game_id": row.game_id,
                    "resolved_status": row.resolved_status,
                    "reason": f"conflicting_status_with_existing_{existing.resolved_status}",
                    "evidence_source": row.evidence_source,
                    "season": row.season,
                    "month": row.month,
                    "date_text": row.date_text,
                    "status_text": row.status_text,
                    "match_text": row.match_text,
                    "raw_row": row.raw_row,
                }
            )
            continue
        evidence_by_game_id[row.game_id] = row

    evidence_rows: List[Dict[str, Any]] = []
    for game_id in sorted(evidence_by_game_id.keys()):
        row = evidence_by_game_id[game_id]
        evidence_rows.append(
            {
                "game_id": row.game_id,
                "resolved_status": row.resolved_status,
                "reason": row.status_text,
                "evidence_source": row.evidence_source,
                "season": row.season,
                "month": row.month,
                "date_text": row.date_text,
                "match_text": row.match_text,
            }
        )

    for game_id in sorted(unresolved_ids - set(evidence_by_game_id.keys())):
        unmatched_rows.append(
            {
                "expected_game_id": game_id,
                "candidate_game_id": "",
                "resolved_status": "",
                "reason": "unresolved_game_not_found_in_schedule_rows",
                "evidence_source": "",
                "season": year,
                "month": "",
                "date_text": unresolved_games[game_id],
                "status_text": "",
                "match_text": "",
                "raw_row": "",
            }
        )

    for item in error_months:
        unmatched_rows.append(
            {
                "expected_game_id": "",
                "candidate_game_id": "",
                "resolved_status": "",
                "reason": f"month_fetch_error: {item}",
                "evidence_source": "",
                "season": year,
                "month": "",
                "date_text": "",
                "status_text": "",
                "match_text": "",
                "raw_row": "",
            }
        )

    _write_csv(
        evidence_csv,
        (
            "game_id",
            "resolved_status",
            "reason",
            "evidence_source",
            "season",
            "month",
            "date_text",
            "match_text",
        ),
        evidence_rows,
    )
    _write_csv(
        unmatched_csv,
        (
            "expected_game_id",
            "candidate_game_id",
            "resolved_status",
            "reason",
            "evidence_source",
            "season",
            "month",
            "date_text",
            "status_text",
            "match_text",
            "raw_row",
        ),
        unmatched_rows,
    )

    return {
        "year": year,
        "months": list(months),
        "unresolved_games": len(unresolved_ids),
        "evidence_rows": len(evidence_rows),
        "unmatched_rows": len(unmatched_rows),
        "evidence_csv": str(evidence_csv),
        "unmatched_csv": str(unmatched_csv),
        "error_months": error_months,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect 2019 schedule evidence for unresolved games")
    parser.add_argument("--year", type=int, default=2019, help="Target season year")
    parser.add_argument("--months", default="3-10", help="Month list/range (e.g., 3-10 or 3,4,5)")
    parser.add_argument("--league-id", default="1", help="KBO league id (leId)")
    parser.add_argument("--series-ids", default="0,9,6", help="Schedule series ids (srIdList)")
    parser.add_argument("--evidence-csv", default=str(DEFAULT_EVIDENCE_CSV), help="Output evidence CSV path")
    parser.add_argument("--unmatched-csv", default=str(DEFAULT_UNMATCHED_CSV), help="Output unmatched CSV path")
    args = parser.parse_args()

    result = collect_schedule_status_evidence(
        year=args.year,
        months=parse_months(args.months),
        league_id=args.league_id,
        series_ids=args.series_ids,
        evidence_csv=Path(args.evidence_csv),
        unmatched_csv=Path(args.unmatched_csv),
    )
    print("✅ Schedule status evidence collection completed")
    print(f"   unresolved_games: {result['unresolved_games']}")
    print(f"   evidence_rows: {result['evidence_rows']}")
    print(f"   unmatched_rows: {result['unmatched_rows']}")
    print(f"   evidence_csv: {result['evidence_csv']}")
    print(f"   unmatched_csv: {result['unmatched_csv']}")
    if result["error_months"]:
        print("   warnings:")
        for msg in result["error_months"]:
            print(f"   - {msg}")


if __name__ == "__main__":
    main()
