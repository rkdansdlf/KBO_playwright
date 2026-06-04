import asyncio
import logging

from playwright.async_api import async_playwright
from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.franchise import Franchise
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)


class TeamInfoCrawler:
    """
    Crawls KBO Team Info page (https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx)
    Collects: CEO, Owner, Founded Date, Homepage, Phone, Address
    """

    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx"

    def __init__(self):
        self.browser = None
        self.page = None
        self.playwright = None
        self.context = None
        self._raw_pages: list[dict] = []

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        await install_async_resource_blocking(self.context)
        self.page = await self.context.new_page()

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self, save: bool = False):
        print("Crawling Team Info from %s", self.BASE_URL)
        if not self.page:
            await self.start()

        await self.page.goto(self.BASE_URL, wait_until="networkidle")

        html = await self.page.content()
        self._raw_pages.append({
            "source_key": "kbo_team_info",
            "url": self.BASE_URL,
            "html": html,
            "status_code": 200,
        })

        rows = await self.page.locator("table.tData tbody tr").all()
        print("Found %d team entries.", len(rows))

        teams_data = []

        for i in range(len(rows)):
            row = self.page.locator("table.tData tbody tr").nth(i)
            cols = await row.locator("td").all()
            if len(cols) < 4:
                continue

            team_name_full = await cols[0].inner_text()
            found_year_text = await cols[1].inner_text()
            hometown = await cols[2].inner_text()

            team_name = team_name_full.strip()
            print("Processing %s...", team_name)

            link = row.locator("td").nth(0).locator("a.showTg").first
            if await link.count() > 0:
                await link.click()

                modal = self.page.locator("div[id^='layerPop']:visible")
                try:
                    await modal.wait_for(state="visible", timeout=3000)

                    async def get_modal_field(label: str):
                        xpath = f".//th[normalize-space(text())='{label}']/following-sibling::td[1]"
                        el = modal.locator(f"xpath={xpath}")
                        if await el.count() > 0:
                            return (await el.inner_text()).strip()
                        return None

                    owner = await get_modal_field("구단주")
                    ceo = await get_modal_field("대표이사")
                    address = await get_modal_field("구단사무실")
                    phone = await get_modal_field("대표전화")
                    homepage = await get_modal_field("홈페이지")

                    await self.page.keyboard.press("Escape")

                    try:
                        if await modal.is_visible(timeout=1000):
                            close_btn = self.page.locator("a.btn_close, img[alt='닫기']").first
                            if await close_btn.count() > 0:
                                await close_btn.click()
                    except Exception:
                        pass

                    await self.page.locator("div[id^='layerPop']").wait_for(state="hidden", timeout=3000)

                except Exception:
                    logger.exception("Failed to parse modal for %s", team_name)
                    await self.page.keyboard.press("Escape")
                    owner, ceo, address, phone, homepage = None, None, None, None, None
            else:
                print("No link found for %s", team_name)
                owner, ceo, address, phone, homepage = None, None, None, None, None

            info = {
                "name": team_name,
                "found_year": found_year_text,
                "city": hometown,
                "owner": owner,
                "ceo": ceo,
                "address": address,
                "phone": phone,
                "homepage": homepage,
            }
            teams_data.append(info)
            await throttle.wait("koreabaseball.com")

        if save and self._raw_pages:
            self._save_raw_snapshots()

        return teams_data

    def _save_raw_snapshots(self) -> None:
        from src.repositories.source_registry_repository import save_raw_snapshots
        with SessionLocal() as session:
            count = save_raw_snapshots(session, self._raw_pages)
            print("Saved %d raw snapshots for team info.", count)

    async def save(self, data: list[dict]):
        print("Saving %d team profiles...", len(data))
        with SessionLocal() as session:
            for item in data:
                stmt = select(Franchise).where(Franchise.name.like(f"%{item['name']}%"))
                result = session.execute(stmt).scalars().first()
                if result:
                    meta = result.metadata_json or {}
                    meta.update(
                        {
                            "found_year": item["found_year"],
                            "owner": item["owner"],
                            "ceo": item["ceo"],
                            "address": item["address"],
                            "phone": item["phone"],
                        }
                    )
                    result.metadata_json = meta
                    result.web_url = item["homepage"]
                    session.add(result)
                    print("Updated %s", result.name)
                else:
                    print("Could not find franchise for %s", item["name"])
            session.commit()


if __name__ == "__main__":
    crawler = TeamInfoCrawler()
    asyncio.run(crawler.crawl())
