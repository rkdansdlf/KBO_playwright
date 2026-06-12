from __future__ import annotations

import argparse
import asyncio
import logging

from playwright.async_api import Page, async_playwright
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)
from src.repositories.broadcast_repository import BroadcastRepository
from src.urls import SCHEDULE
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.team_codes import build_kbo_game_id


class BroadcastCrawler:
    def __init__(self) -> None:
        self.url = SCHEDULE

    async def run(self, year: int = None, month: int = None, save: bool = False) -> None:
        from datetime import datetime

        year = year or datetime.now().year
        month = month or datetime.now().month

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await install_async_resource_blocking(context)
            page = await context.new_page()

            url = f"{self.url}?year={year}&month={month:02d}"
            logger.info("Loading %s...", url)
            await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT)
            await page.wait_for_timeout(2000)

            data = await self._extract_broadcast_data(page, year)
            logger.info("Found %s broadcast entries.", len(data))

            await browser.close()

            if save:
                self._save_to_db(data)
            else:
                for d in data[:10]:
                    logger.info(d)
                    logger.info("")

    async def _extract_broadcast_data(self, page: Page, year: int) -> list[dict]:
        script = """
        (args) => {
            const year = args.year;
            const results = [];
            const TEAM_MAP = {"LG":"LG","KT":"KT","NC":"NC","두산":"DB","롯데":"LT","삼성":"SS","키움":"KH","한화":"HH","KIA":"KIA","SSG":"SSG"};
            const BC_MAP = {"SPO":"SPOTV","SPO-2":"SPOTV2","SPO-2T":"SPOTV2","S-BS":"SBS Sports","SBS":"SBS Sports","K-BS":"KBS N Sports","MBC":"MBC SPORTS+","CPB":"CPBC TV"};
            const rows = document.querySelectorAll('#tblScheduleList tbody tr');
            rows.forEach(tr => {
                const cells = tr.querySelectorAll('td');
                if (cells.length < 9) return;
                const dayText = cells[0]?.innerText?.trim();
                if (!dayText || !dayText.match(/\\d+\\.\\d+\\(/)) return;
                const playText = cells[2]?.innerText?.trim() || '';
                const match = playText.match(/([A-Za-z가-힣]+)\\d*vs\\d*([A-Za-z가-힣]+)/);
                if (!match) return;
                const awayTeam = TEAM_MAP[match[1]] || null;
                const homeTeam = TEAM_MAP[match[2]] || null;
                if (!awayTeam || !homeTeam) return;
                const dateParts = dayText.match(/(\\d+)\\.(\\d+)/);
                if (!dateParts) return;
                const m = String(dateParts[1]).padStart(2, '0');
                const d = String(dateParts[2]).padStart(2, '0');
                const gameDate = String(year) + m + d;
                const tvText = cells[5]?.innerText?.trim() || '';
                const radioText = cells[6]?.innerText?.trim() || '';
                if (tvText && tvText !== '-') {
                    const norm = BC_MAP[tvText] || tvText;
                    results.push({game_date: gameDate, away_team_code: awayTeam, home_team_code: homeTeam, broadcaster: norm, channel_name: norm, source: 'KBO'});
                }
                if (radioText && radioText !== '-') {
                    results.push({game_date: gameDate, away_team_code: awayTeam, home_team_code: homeTeam, broadcaster: 'RADIO_' + radioText, channel_name: radioText + ' (라디오)', source: 'KBO'});
                }
            });
            return results;
        }
        """
        data = await page.evaluate(script, {"year": year})
        return self._normalize_game_ids(data, year)

    def _normalize_game_ids(self, data: list[dict], year: int) -> list[dict]:
        normalized = []
        for item in data:
            game_id = build_kbo_game_id(
                item.get("game_date"),
                item.get("away_team_code"),
                item.get("home_team_code"),
                season_year=year,
            )
            if not game_id:
                logger.warning("Skipping broadcast row with unresolved game_id: %s", item)
                continue
            normalized.append(
                {
                    "game_id": game_id,
                    "broadcaster": item["broadcaster"],
                    "channel_name": item["channel_name"],
                    "source": item.get("source") or "KBO",
                },
            )
        return normalized

    def _save_to_db(self, data: list[dict]) -> None:
        session = SessionLocal()
        repo = BroadcastRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_broadcast(item)
                    count += 1
                except SQLAlchemyError as ex:
                    logger.warning("Broadcast save failed for item: %s", ex)
            session.commit()
            logger.info("Saved %s broadcast records.", count)
        except SQLAlchemyError as e:
            session.rollback()
            logger.exception(f"Database error saving broadcasts: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    asyncio.run(BroadcastCrawler().run(year=args.year, month=args.month, save=args.save))
