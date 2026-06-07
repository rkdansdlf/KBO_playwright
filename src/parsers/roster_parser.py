"""
Parser for KBO mobile roster transaction pages (call-up / send-down).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _map_team_name(name: str) -> str | None:
    mapping = {
        "LG": "LG",
        "lg": "LG",
        "엘지": "LG",
        "HH": "HH",
        "한화": "HH",
        "SS": "SS",
        "삼성": "SS",
        "KT": "KT",
        "kt": "KT",
        "OB": "OB",
        "두산": "OB",
        "LT": "LT",
        "롯데": "LT",
        "HT": "HT",
        "KIA": "HT",
        "기아": "HT",
        "NC": "NC",
        "SK": "SK",
        "SSG": "SK",
        "WO": "WO",
        "키움": "WO",
    }
    return mapping.get(name)


def parse_mobile_roster(html: str, source_key: str, metadata: dict | None = None) -> list[dict[str, Any]]:
    target_date_str = (metadata or {}).get("fetched_at", "")
    target_date = date.today()
    try:
        target_date = datetime.fromisoformat(target_date_str).date() if target_date_str else date.today()
    except (ValueError, TypeError):
        logger.debug("Invalid target_date_str: %s", target_date_str)
    transactions = []

    registered_section = ""
    deregistered_section = ""

    reg_match = re.search(r"오늘자\s*선수\s*등록현황.*?(?=오늘자\s*선수\s*말소현황|\Z)", html, re.DOTALL)
    if reg_match:
        registered_section = reg_match.group(0)

    dereg_match = re.search(r"오늘자\s*선수\s*말소현황.*?(?=<div\s+(?:class|id)=|$)", html, re.DOTALL)
    if dereg_match:
        deregistered_section = dereg_match.group(0)

    if not registered_section and not deregistered_section:
        return _parse_alternate_mobile(html, target_date)

    for section_text, action in [(registered_section, "registered"), (deregistered_section, "deregistered")]:
        if not section_text:
            continue

        team_blocks = re.findall(
            r'<strong[^>]*class="team"[^>]*>([^<]+)</strong>\s*<ul[^>]*>(.*?)</ul>', section_text, re.DOTALL
        )
        for team_name_raw, list_html in team_blocks:
            team_code = _map_team_name(team_name_raw.strip())
            if not team_code:
                continue

            player_items = re.findall(
                r'<li[^>]*>(?:\s*<a[^>]*href="[^"]*playerId=(\d+)[^"]*"[^>]*>)?\s*([^<]+?)\s*(?:</a>)?\s*</li>',
                list_html,
            )
            for player_id_str, player_name in player_items:
                player_name = player_name.strip()
                if not player_name:
                    continue
                transactions.append(
                    {
                        "transaction_date": target_date,
                        "team_id": team_code,
                        "player_id": int(player_id_str) if player_id_str and player_id_str.isdigit() else None,
                        "player_name": player_name,
                        "action": action,
                        "roster_level": "first_team",
                        "inferred_to_level": "second_team" if action == "deregistered" else None,
                        "source_type": "kbo_today_page",
                        "confidence": "high",
                        "dedupe_key": f"{target_date}_{team_code}_{player_name}_{action}",
                    }
                )

    return transactions


def _parse_alternate_mobile(html: str, target_date: date) -> list[dict[str, Any]]:
    transactions = []
    current_team = None
    current_action = None

    for line in html.split("\n"):
        line = line.strip()
        team_match = re.search(r'class="team"[^>]*>\s*([^<]+)', line)
        if team_match:
            current_team = _map_team_name(team_match.group(1).strip())
            continue

        if "등록" in line and ("선수" in line or "현황" in line):
            current_action = "registered"
            continue
        if "말소" in line and ("선수" in line or "현황" in line):
            current_action = "deregistered"
            continue

        if current_team and current_action:
            player_match = re.search(r"playerId=(\d+)[^>]*>\s*([^<]+)", line)
            if player_match:
                pid, pname = int(player_match.group(1)), player_match.group(2).strip()
                transactions.append(
                    {
                        "transaction_date": target_date,
                        "team_id": current_team,
                        "player_id": pid,
                        "player_name": pname,
                        "action": current_action,
                        "roster_level": "first_team",
                        "inferred_to_level": "second_team" if current_action == "deregistered" else None,
                        "source_type": "kbo_today_page",
                        "confidence": "high",
                        "dedupe_key": f"{target_date}_{current_team}_{pname}_{current_action}",
                    }
                )

    return transactions
