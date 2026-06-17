from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

import contextlib

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page, TimeoutError, async_playwright
from playwright_stealth import Stealth
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.fa_contract import FAContract
from src.models.player import PlayerBasic, PlayerMovement
from src.models.team import Team
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.playwright_retry import LONG_TIMEOUT, NAV_TIMEOUT
from src.utils.team_codes import resolve_team_code

FA_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)
FA_IO_EXCEPTIONS = (OSError, json.JSONDecodeError, ValueError, TypeError)
FA_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def parse_amount_krw(amount_str: str | None) -> int | None:
    """
    Parse Korean amount string to 만원 (10,000 KRW) units.
    E.g. '75억원' -> 750000
         '6억 5천만원' -> 65000
         '5000만원' -> 5000
    """
    if not amount_str:
        return None
    amount_str = amount_str.replace(" ", "").replace(",", "")

    # Convert '천' to '000' (e.g. 5천 -> 5000)
    amount_str = amount_str.replace("천", "000")

    billion = 0
    ten_thousand = 0

    # Match X억
    match_billion = re.search(r"(\d+)억", amount_str)
    if match_billion:
        billion = int(match_billion.group(1))

    # Match Y만
    match_ten_thousand = re.search(r"(\d+)만", amount_str)
    if match_ten_thousand:
        ten_thousand = int(match_ten_thousand.group(1))

    total = (billion * 10000) + ten_thousand
    return total if total > 0 else None


def resolve_player_basic_id(session: Session, name: str, team_code: str) -> int | None:
    # Resolve team name from team_code
    team = session.query(Team).filter_by(team_id=team_code).first()
    team_names = [team_code]
    if team:
        if team.team_short_name:
            team_names.append(team.team_short_name)
        if team.team_name:
            team_names.append(team.team_name)

    # Search players matching the name and team
    from sqlalchemy import or_

    query = session.query(PlayerBasic).filter(PlayerBasic.name == name)
    filters = [PlayerBasic.team.contains(t) for t in team_names]
    players = query.filter(or_(*filters)).all()

    if len(players) == 1 or len(players) > 1:
        return players[0].player_id

    # If not found by team, fall back to matching name only if it's unique in the entire DB
    all_players_with_name = session.query(PlayerBasic).filter_by(name=name).all()
    if len(all_players_with_name) == 1:
        return all_players_with_name[0].player_id

    return None


class FACrawler:
    def __init__(self, headless: bool = False) -> None:
        self.url = "https://namu.wiki/w/KBO%20%EB%A6%AC%EA%B7%B8/%EC%97%AD%EB%8C%80%20FA"
        self.headless = headless

    async def crawl(self, target_years: list[int]) -> list[dict[str, Any]]:
        """
        Crawl FA data for specific years.
        If target_years is empty, crawls all available years (from 2017).
        """
        results = []
        async with async_playwright() as p:
            # Use stealth plugin with headless mode (or non-headless as recommended)
            browser = await p.chromium.launch(headless=self.headless)

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="ko-KR",
                timezone_id="Asia/Seoul",
            )
            await install_async_resource_blocking(context)

            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            try:
                logger.info("🌍 Navigating to %s...", self.url)
                await page.goto(self.url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT)

                # Wait for table or specific content wrapper
                try:
                    await page.wait_for_selector(".wiki-table-wrap table, #content .table, table", timeout=NAV_TIMEOUT)
                except TimeoutError:
                    logger.exception("⚠️ Timeout waiting for table selector. Exploring content...")

                # Extract data from 4 main sections
                sections = [
                    {"id": "s-2.1", "type": "RETAINED", "pos": "PITCHER"},
                    {"id": "s-2.2", "type": "RETAINED", "pos": "FIELDER"},
                    {"id": "s-3.1", "type": "TRANSFERRED", "pos": "PITCHER"},
                    {"id": "s-3.2", "type": "TRANSFERRED", "pos": "FIELDER"},
                ]

                for section in sections:
                    logger.info("🔍 Processing Section %s (%s - %s)...", section["id"], section["type"], section["pos"])
                    section_data = await self._extract_section_table(page, section["type"], section["pos"])

                    # Filter by year
                    if target_years:
                        filtered = [d for d in section_data if d["year"] in target_years]
                        logger.info("   => Found %s records for target years.", len(filtered))
                        results.extend(filtered)
                    else:
                        logger.info("   => Found %s records total.", len(section_data))
                        results.extend(section_data)

            except FA_CRAWL_EXCEPTIONS:
                logger.exception("❌ Error during crawling")
                import traceback

                traceback.print_exc()
            finally:
                await browser.close()

        return results

    async def _extract_section_table(self, page: Page, section_type: str, pos_type: str) -> list[dict[str, Any]]:
        """
        Extracts table data by scanning the headers of all tables on the page.
        Supports full colspan/rowspan grid expansion in javascript before parsing.
        """
        full_script = f"""
        () => {{
            const tables = Array.from(document.querySelectorAll('table'));
            const matchingTables = [];

            tables.forEach(t => {{
                const cells = Array.from(t.querySelectorAll('tr td, tr th'));
                const cellTexts = cells.map(c => c.innerText.trim());

                const hasOwnTeam = cellTexts.some(txt => txt.includes('소속팀'));
                const hasOldTeam = cellTexts.some(txt => txt.includes('원 소속팀'));
                const hasNewTeam = cellTexts.some(txt => txt.includes('이적팀'));
                const hasAmount = cellTexts.some(txt => txt.includes('총액'));
                const hasDuration = cellTexts.some(txt => txt.includes('계약기간'));

                if (hasAmount && hasDuration) {{
                    if (hasOldTeam && hasNewTeam) {{
                        matchingTables.push({{ type: 'TRANSFERRED', table: t }});
                    }} else if (hasOwnTeam) {{
                        matchingTables.push({{ type: 'RETAINED', table: t }});
                    }}
                }}
            }});

            const retainedTables = matchingTables.filter(m => m.type === 'RETAINED').map(m => m.table);
            const transferredTables = matchingTables.filter(m => m.type === 'TRANSFERRED').map(m => m.table);

            let table = null;
            if ("{section_type}" === "RETAINED") {{
                if ("{pos_type}" === "PITCHER") table = retainedTables[0];
                else table = retainedTables[1];
            }} else if ("{section_type}" === "TRANSFERRED") {{
                if ("{pos_type}" === "PITCHER") table = transferredTables[0];
                else table = transferredTables[1];
            }}

            if (!table) return null;

            // PARSE TABLE WITH ROWSPAN/COLSPAN EXPANSION
            const rows = Array.from(table.rows);
            const grid = [];
            rows.forEach((tr, r) => {{
                grid[r] = [];
            }});

            rows.forEach((tr, r) => {{
                let cIndex = 0;
                Array.from(tr.cells).forEach(cell => {{
                    while (grid[r][cIndex] !== undefined) {{
                        cIndex++;
                    }}

                    const text = cell.innerText.trim();
                    const rowSpan = cell.rowSpan || 1;
                    const colSpan = cell.colSpan || 1;

                    for (let rSpan = 0; rSpan < rowSpan; rSpan++) {{
                        for (let cSpan = 0; cSpan < colSpan; cSpan++) {{
                            if (grid[r + rSpan]) {{
                                grid[r + rSpan][cIndex + cSpan] = text;
                            }}
                        }}
                    }}
                    cIndex += colSpan;
                }});
            }});

            return grid;
        }}
        """

        raw_rows = await page.evaluate(full_script)

        if not raw_rows or len(raw_rows) < 2:
            logger.warning("   ⚠️ No table found matching section %s (%s)", section_type, pos_type)
            return []

        header = raw_rows[0]
        header = [h.strip() for h in header]

        # Helper to find column index by matching substring
        def find_index(candidates) -> int:
            for candidate in candidates:
                for idx, col in enumerate(header):
                    if candidate in col:
                        return idx
            return -1

        year_idx = find_index(["년도", "연도"])
        name_idx = find_index(["이름", "선수"])

        if section_type == "RETAINED":
            team_idx = find_index(["소속팀", "팀"])
            old_team_idx = -1
            new_team_idx = -1
        else:
            old_team_idx = find_index(["원 소속팀", "원소속", "이전 소속팀", "이전팀"])
            new_team_idx = find_index(["이적팀", "이적한 팀", "새 소속팀", "이적 구단"])
            if old_team_idx == -1:
                old_team_idx = find_index(["소속팀", "팀"])
            if new_team_idx == -1:
                new_team_idx = find_index(["소속팀", "팀"])
            team_idx = new_team_idx

        duration_idx = find_index(["계약기간", "기간"])
        amount_idx = find_index(["총액", "금액"])
        if amount_idx == -1:
            amount_idx = find_index(["계약조건", "조건"])

        remarks_idx = find_index(["비고", "기타", "상세", "옵션"])

        logger.info("   [Header Mapping] Section: %s (%s)", section_type, pos_type)
        logger.info("   => Header: %s", header)
        logger.info(
            "   => Indices - year: %s, name: %s, team: %s, old_team: %s, new_team: %s, duration: %s, amount: %s, remarks: %s",
            year_idx,
            name_idx,
            team_idx,
            old_team_idx,
            new_team_idx,
            duration_idx,
            amount_idx,
            remarks_idx,
        )

        if name_idx == -1 or amount_idx == -1:
            logger.warning("   ⚠️ Critical columns (이름, 총액) not found in header. Skipping table.")
            return []

        parsed_data = []
        for row in raw_rows[1:]:
            if len(row) < len(header):
                continue

            # Skip any duplicated header rows nested in table
            if "이름" in row or "총액" in row:
                continue

            # Parse Year
            year_val = None
            if year_idx != -1:
                year_str = row[year_idx]
                digits = re.sub(r"\D", "", year_str)
                if len(digits) == 4:
                    with contextlib.suppress(ValueError):
                        year_val = int(digits)

            if not year_val:
                continue

            name_val = row[name_idx].strip()
            if not name_val or name_val in ["이름", "선수명", "선수"]:
                continue

            item = {"year": year_val, "player_name": name_val, "fa_type": section_type.lower()}

            if duration_idx != -1:
                item["duration"] = row[duration_idx].strip()
            else:
                item["duration"] = ""

            item["amount"] = row[amount_idx].strip()

            remarks_parts = []
            if remarks_idx != -1:
                remarks_parts.append(row[remarks_idx].strip())

            # Additional column checks to collect details if they exist in separate columns
            def check_and_add_col(candidates, prefix) -> None:
                idx = find_index(candidates)
                if idx != -1 and idx not in [remarks_idx, amount_idx, name_idx, year_idx]:
                    val = row[idx].strip()
                    if val and val not in ["-", "0", "비공개"]:
                        remarks_parts.append(f"{prefix}: {val}")

            check_and_add_col(["옵션", "인센티브"], "옵션")
            check_and_add_col(["연봉"], "연봉")
            check_and_add_col(["계약금"], "계약금")

            item["remarks"] = " | ".join([p for p in remarks_parts if p])

            if section_type == "RETAINED":
                if team_idx != -1:
                    item["team"] = row[team_idx].strip()
                else:
                    item["team"] = ""
            else:  # TRANSFERRED
                if old_team_idx != -1:
                    item["old_team"] = row[old_team_idx].strip()
                else:
                    item["old_team"] = ""
                if new_team_idx != -1:
                    item["new_team"] = row[new_team_idx].strip()
                    item["team"] = row[new_team_idx].strip()
                else:
                    item["new_team"] = ""
                    item["team"] = ""

            for k, v in item.items():
                if isinstance(v, str):
                    item[k] = re.sub(r"\[[^\]]+\]", "", v).strip()

            parsed_data.append(item)

        return parsed_data

    def load_from_json(self, filepath: str) -> list[dict[str, Any]]:
        """Loads FA data from a JSON file."""
        logger.info("📂 Loading data from %s...", filepath)
        try:
            with Path(filepath).open(encoding="utf-8") as f:
                data = json.load(f)
        except FA_IO_EXCEPTIONS:
            logger.exception("❌ Error loading JSON")
            return []
        else:
            logger.info("   => Loaded %s records.", len(data))
            return data
            logger.exception("❌ Error loading JSON")
            return []

    def save_to_db(self, data: list[dict[str, Any]], session: Session, dry_run: bool = False) -> None:
        """
        Saves or updates FA data in the database.
        """
        new_records = 0
        updates = 0

        # New fa_contracts counts
        new_fa_contracts = 0
        updated_fa_contracts = 0

        logger.info("💾 processing %s records for Database...", len(data))

        for item in data:
            if not item.get("player_name"):
                continue

            name = item["player_name"]
            year = item["year"]
            team_raw = item.get("team")
            team_code = resolve_team_code(team_raw)

            if not team_code:
                if item.get("type") == "transfer" and not item.get("new_team"):
                    logger.warning(
                        "   ⚠️ Skipping record for %s (%s): No valid team (likely overseas split).",
                        name,
                        year,
                    )
                    continue

                logger.warning("   ⚠️ Could not resolve team code for '%s' (%s, %s). Skipping.", team_raw, name, year)
                continue

            # Construct Remarks string
            contract_info = f"FA계약: {item.get('contract_duration', item.get('duration', '?'))}, {item.get('total_amount', item.get('amount', '?'))}"
            remarks = item.get("remarks")
            if remarks and remarks != "-" and remarks != "비공개":
                contract_info += f", {remarks}"

            # Matching Logic for player_movements
            start_date = date(year - 1, 11, 1)
            end_date = date(year, 3, 31)

            existing = (
                session.query(PlayerMovement)
                .filter(
                    PlayerMovement.player_name == name,
                    PlayerMovement.team_code == team_code,
                    PlayerMovement.movement_date >= start_date,
                    PlayerMovement.movement_date <= end_date,
                )
                .first()
            )

            # FAContract Upsert Logic
            fa_type = "transferred"
            if item.get("fa_type"):
                fa_type = item["fa_type"]
            elif item.get("type") == "retained" or item.get("type") == "RETAINED" or not item.get("old_team"):
                fa_type = "retained"

            old_team_val = item.get("old_team")
            new_team_val = item.get("new_team", item.get("team"))
            duration_val = item.get("contract_duration", item.get("duration"))
            amount_val = item.get("total_amount", item.get("amount"))

            amount_krw = parse_amount_krw(amount_val)
            player_basic_id = resolve_player_basic_id(session, name, team_code)

            existing_contract = (
                session.query(FAContract)
                .filter(
                    FAContract.player_name == name,
                    FAContract.year == year,
                    FAContract.fa_type == fa_type,
                    FAContract.new_team == new_team_val,
                )
                .first()
            )

            if dry_run:
                logger.info(
                    "   [DRY RUN] player_movements: %s (%s): %s -> %s",
                    name,
                    year,
                    "MATCH FOUND" if existing else "NEW RECORD",
                    contract_info,
                )
                logger.info(
                    "   [DRY RUN] fa_contracts: %s (%s, %s): %s -> %s (%s만 원)",
                    name,
                    year,
                    fa_type,
                    "MATCH FOUND" if existing_contract else "NEW RECORD",
                    amount_val,
                    amount_krw,
                )
                continue

            # 1. Update player_movements
            if existing:
                if existing.remarks:
                    if "FA계약" not in existing.remarks:
                        existing.remarks = f"{existing.remarks} | {contract_info}"
                        updates += 1
                else:
                    existing.remarks = contract_info
                    updates += 1
            else:
                # Insert new record
                default_date = date(year, 1, 15)
                new_move = PlayerMovement(
                    movement_date=default_date,
                    section="FA",
                    team_code=team_code,
                    player_name=name,
                    remarks=contract_info,
                )
                session.add(new_move)
                new_records += 1

            # 2. Update fa_contracts
            if existing_contract:
                existing_contract.player_basic_id = player_basic_id
                existing_contract.old_team = old_team_val
                existing_contract.team_code = team_code
                existing_contract.contract_duration = duration_val
                existing_contract.total_amount = amount_val
                existing_contract.total_amount_krw = amount_krw
                existing_contract.remarks = remarks
                existing_contract.source_url = self.url
                updated_fa_contracts += 1
            else:
                new_contract = FAContract(
                    player_name=name,
                    player_basic_id=player_basic_id,
                    year=year,
                    fa_type=fa_type,
                    old_team=old_team_val,
                    new_team=new_team_val,
                    team_code=team_code,
                    contract_duration=duration_val,
                    total_amount=amount_val,
                    total_amount_krw=amount_krw,
                    remarks=remarks,
                    source_url=self.url,
                )
                session.add(new_contract)
                new_fa_contracts += 1

        if not dry_run:
            try:
                session.commit()
                logger.info("✅ player_movements Update Complete: %s Inserted, %s Updated.", new_records, updates)
                logger.info(
                    "✅ fa_contracts Update Complete: %s Inserted, %s Updated.",
                    new_fa_contracts,
                    updated_fa_contracts,
                )
            except FA_DB_EXCEPTIONS:
                session.rollback()
                logger.exception("❌ DB Error")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl KBO FA Contracts from Namu Wiki")
    parser.add_argument("--year", type=int, help="Specific year to crawl (e.g. 2024)")
    parser.add_argument("--save", action="store_true", help="Save changes to database")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without saving")
    parser.add_argument("--file", type=str, help="Load data from JSON file instead of crawling")

    args = parser.parse_args()

    crawler = FACrawler(
        headless=False,
    )  # Headless false helps bypass Namuwiki protection if run interactively, though we support it

    data = []
    if args.file:
        data = crawler.load_from_json(args.file)
        if args.year:
            data = [d for d in data if d.get("year") == args.year]
    else:
        target_years = [args.year] if args.year else []
        data = await crawler.crawl(target_years)

    if args.save or args.dry_run:
        session = SessionLocal()
        try:
            crawler.save_to_db(data, session, dry_run=args.dry_run or (not args.save))
        finally:
            session.close()
    else:
        logger.info("Fetched %s records.", len(data))
        for d in data[:5]:
            logger.info(d)


if __name__ == "__main__":
    asyncio.run(main())
