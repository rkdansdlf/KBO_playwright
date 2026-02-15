#!/usr/bin/env python3
"""
Ensure player_basic contains all override player IDs from player_id_overrides.csv.

For missing player IDs:
  1) Try KBO search page by player name
  2) Try KBO profile pages by player_id
  3) Fallback to minimal payload from overrides (name/team_code) when needed
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set
from urllib.parse import parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.parsers.player_profile_parser import parse_birth_date, tokenize_profile
from src.repositories.player_basic_repository import PlayerBasicRepository

DEFAULT_OVERRIDES_CSV = PROJECT_ROOT / "data/player_id_overrides.csv"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data"
SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx"
PROFILE_URLS = (
    "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={player_id}",
    "https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId={player_id}",
)


@dataclass
class OverrideGroup:
    player_id: int
    names: Set[str]
    team_codes: Set[str]
    source_tables: Set[str]


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_date(value: str) -> Optional[str]:
    text_value = normalize_text(value)
    if not text_value:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text_value):
        return text_value
    if "년" in text_value:
        return parse_birth_date(text_value)
    dotted = re.match(r"^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$", text_value)
    if dotted:
        yyyy, mm, dd = dotted.groups()
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
    return None


def parse_height_weight(value: str) -> tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None
    compact = value.replace(" ", "")
    match = re.search(r"(\d{2,3})cm[/,](\d{2,3})kg", compact, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(\d{2,3})\s*cm.*?(\d{2,3})\s*kg", value, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def extract_player_id_from_href(href: str) -> Optional[int]:
    try:
        query = parse_qs(urlparse(href).query)
        raw = normalize_text(query.get("playerId", [None])[0])
        if raw and raw.isdigit():
            return int(raw)
    except Exception:
        return None
    match = re.search(r"playerId=(\d+)", href)
    if match:
        return int(match.group(1))
    return None


def load_override_groups(path: Path) -> Dict[int, OverrideGroup]:
    groups: Dict[int, OverrideGroup] = {}
    if not path.exists():
        return groups

    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            pid_raw = normalize_text(row.get("resolved_player_id"))
            if not pid_raw or not pid_raw.isdigit():
                continue
            player_id = int(pid_raw)
            entry = groups.get(player_id)
            if not entry:
                entry = OverrideGroup(player_id=player_id, names=set(), team_codes=set(), source_tables=set())
                groups[player_id] = entry
            name = normalize_text(row.get("player_name"))
            team_code = normalize_text(row.get("team_code")).upper()
            source_table = normalize_text(row.get("source_table"))
            if name:
                entry.names.add(name)
            if team_code:
                entry.team_codes.add(team_code)
            if source_table:
                entry.source_tables.add(source_table)
    return groups


def fetch_existing_player_ids(player_ids: Sequence[int]) -> Set[int]:
    if not player_ids:
        return set()
    query = text("SELECT player_id FROM player_basic WHERE player_id IN :player_ids").bindparams(
        bindparam("player_ids", expanding=True)
    )
    with SessionLocal() as session:
        rows = session.execute(query, {"player_ids": list(player_ids)}).fetchall()
    return {int(row[0]) for row in rows}


def fetch_from_search(client: httpx.Client, player_name: str, player_id: int) -> Dict[str, Any]:
    if not player_name:
        return {}
    response = client.get(SEARCH_URL, params={"searchWord": player_name}, timeout=30.0)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    for tr in soup.select("table.tEx tbody tr"):
        tds = tr.select("td")
        if len(tds) < 7:
            continue
        link = tds[1].find("a", href=True)
        if not link:
            continue
        pid = extract_player_id_from_href(link["href"])
        if pid != player_id:
            continue
        uniform_no = normalize_text(tds[0].get_text(" ", strip=True))
        name = normalize_text(link.get_text(" ", strip=True))
        team = normalize_text(tds[2].get_text(" ", strip=True))
        position = normalize_text(tds[3].get_text(" ", strip=True))
        birth_date = normalize_date(tds[4].get_text(" ", strip=True))
        height_cm, weight_kg = parse_height_weight(tds[5].get_text(" ", strip=True))
        career = normalize_text(tds[6].get_text(" ", strip=True))
        return {
            "name": name or None,
            "uniform_no": uniform_no or None,
            "team": team or None,
            "position": position or None,
            "birth_date": birth_date,
            "height_cm": height_cm,
            "weight_kg": weight_kg,
            "career": career or None,
            "source": "kbo_search",
        }
    return {}


def fetch_from_profile(client: httpx.Client, player_id: int) -> Dict[str, Any]:
    for template in PROFILE_URLS:
        url = template.format(player_id=player_id)
        try:
            response = client.get(url, timeout=30.0)
            if response.status_code != 200:
                continue
        except Exception:
            continue
        soup = BeautifulSoup(response.text, "lxml")
        text_blob = " ".join(soup.stripped_strings)
        if "선수명" not in text_blob:
            continue
        tokens = tokenize_profile(text_blob)
        if not tokens:
            continue
        name = normalize_text(tokens.get("선수명"))
        uniform_no_match = re.search(r"(?:No\.\s*)?(\d+)", normalize_text(tokens.get("등번호")))
        uniform_no = uniform_no_match.group(1) if uniform_no_match else None
        position_raw = normalize_text(tokens.get("포지션"))
        if "(" in position_raw:
            position_raw = position_raw.split("(", 1)[0].strip()
        birth_date = normalize_date(normalize_text(tokens.get("생년월일")))
        height_cm, weight_kg = parse_height_weight(normalize_text(tokens.get("신장/체중")))
        career = normalize_text(tokens.get("경력") or tokens.get("출신교"))
        return {
            "name": name or None,
            "uniform_no": uniform_no,
            "team": None,
            "position": position_raw or None,
            "birth_date": birth_date,
            "height_cm": height_cm,
            "weight_kg": weight_kg,
            "career": career or None,
            "source": "kbo_profile",
        }
    return {}


def choose_best_player_payload(
    group: OverrideGroup,
    search_data: Dict[str, Any],
    profile_data: Dict[str, Any],
) -> Dict[str, Any]:
    preferred_name = sorted(group.names)[0] if group.names else ""
    preferred_team_code = sorted(group.team_codes)[0] if group.team_codes else ""
    source_table_hint = sorted(group.source_tables)[0] if group.source_tables else ""

    name = search_data.get("name") or profile_data.get("name") or preferred_name
    team = search_data.get("team") or profile_data.get("team") or preferred_team_code
    position = search_data.get("position") or profile_data.get("position")
    if not position:
        if source_table_hint == "game_pitching_stats":
            position = "투수"
        elif source_table_hint in {"game_batting_stats", "game_lineups"}:
            position = "야수"

    birth_date = search_data.get("birth_date") or profile_data.get("birth_date")
    birth_date_date = None
    if birth_date:
        try:
            birth_date_date = datetime.strptime(birth_date, "%Y-%m-%d").date()
        except ValueError:
            birth_date = None
            birth_date_date = None

    return {
        "player_id": group.player_id,
        "name": name,
        "uniform_no": search_data.get("uniform_no") or profile_data.get("uniform_no"),
        "team": team or None,
        "position": position or None,
        "birth_date": birth_date,
        "birth_date_date": birth_date_date,
        "height_cm": search_data.get("height_cm") or profile_data.get("height_cm"),
        "weight_kg": search_data.get("weight_kg") or profile_data.get("weight_kg"),
        "career": search_data.get("career") or profile_data.get("career"),
        "status": None,
        "staff_role": None,
        "status_source": "override_enrich",
    }


def _write_report(path: Path, rows: List[Dict[str, Any]]) -> None:
    columns = (
        "player_id",
        "name",
        "team",
        "position",
        "uniform_no",
        "status",
        "source_search",
        "source_profile",
        "notes",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns))
        writer.writeheader()
        writer.writerows(rows)


def enrich_missing_players(
    *,
    overrides_csv: Path = DEFAULT_OVERRIDES_CSV,
    report_dir: Path = DEFAULT_REPORT_DIR,
) -> Dict[str, Any]:
    groups = load_override_groups(overrides_csv)
    all_ids = sorted(groups.keys())
    existing_ids = fetch_existing_player_ids(all_ids)
    missing_ids = [pid for pid in all_ids if pid not in existing_ids]

    report_rows: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    with httpx.Client(headers=headers) as client:
        for player_id in missing_ids:
            group = groups[player_id]
            search_data: Dict[str, Any] = {}
            for candidate_name in sorted(group.names):
                try:
                    search_data = fetch_from_search(client, candidate_name, player_id)
                except Exception:
                    search_data = {}
                if search_data:
                    break

            try:
                profile_data = fetch_from_profile(client, player_id)
            except Exception:
                profile_data = {}

            payload = choose_best_player_payload(group, search_data, profile_data)
            if not payload.get("name"):
                report_rows.append(
                    {
                        "player_id": player_id,
                        "name": "",
                        "team": "",
                        "position": "",
                        "uniform_no": "",
                        "status": "skipped",
                        "source_search": int(bool(search_data)),
                        "source_profile": int(bool(profile_data)),
                        "notes": "missing_name_after_enrichment",
                    }
                )
                continue

            payloads.append(payload)
            report_rows.append(
                {
                    "player_id": player_id,
                    "name": payload.get("name") or "",
                    "team": payload.get("team") or "",
                    "position": payload.get("position") or "",
                    "uniform_no": payload.get("uniform_no") or "",
                    "status": "prepared",
                    "source_search": int(bool(search_data)),
                    "source_profile": int(bool(profile_data)),
                    "notes": "",
                }
            )

    upserted = PlayerBasicRepository().upsert_players(payloads) if payloads else 0
    existing_after = fetch_existing_player_ids(all_ids)
    remaining_missing = sorted(set(all_ids) - existing_after)

    report_path = report_dir / f"player_override_enrichment_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    _write_report(report_path, report_rows)

    return {
        "overrides_csv": str(overrides_csv),
        "total_override_ids": len(all_ids),
        "already_present": len(existing_ids),
        "missing_before": len(missing_ids),
        "prepared_payloads": len(payloads),
        "upserted": upserted,
        "remaining_missing": len(remaining_missing),
        "remaining_missing_ids": remaining_missing,
        "report_csv": str(report_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich missing player_basic rows for override IDs")
    parser.add_argument("--overrides-csv", default=str(DEFAULT_OVERRIDES_CSV), help="Path to player_id_overrides.csv")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="Directory for enrichment report CSV")
    args = parser.parse_args()

    result = enrich_missing_players(
        overrides_csv=Path(args.overrides_csv),
        report_dir=Path(args.report_dir),
    )
    print("✅ Player override enrichment completed")
    print(f"   total_override_ids: {result['total_override_ids']}")
    print(f"   already_present: {result['already_present']}")
    print(f"   missing_before: {result['missing_before']}")
    print(f"   prepared_payloads: {result['prepared_payloads']}")
    print(f"   upserted: {result['upserted']}")
    print(f"   remaining_missing: {result['remaining_missing']}")
    if result["remaining_missing_ids"]:
        print(f"   remaining_missing_ids: {','.join(str(x) for x in result['remaining_missing_ids'])}")
    print(f"   report_csv: {result['report_csv']}")


if __name__ == "__main__":
    main()
