import asyncio
import contextlib
import logging

from playwright.async_api import async_playwright
from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.team_codes import resolve_team_code

logger = logging.getLogger(__name__)


class TeamHistoryCrawler:
    """
    Crawls KBO Team History page (https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx)
    Collects: Annual Team Names, Logos, Rankings, Season Info
    """

    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx"

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

    async def crawl(self):
        print(f"📜 Crawling Team History from {self.BASE_URL}")
        if not self.page:
            await self.start()

        await self.page.goto(self.BASE_URL, wait_until="networkidle")

        raw_html = await self.page.content()
        self._raw_pages.append({"url": self.BASE_URL, "html": raw_html, "source_key": "kbo_team_history"})

        # Selectors validated by Subagent
        # Table: table.tData.tbd02
        # Row: tbody tr
        # Year: th[scope='row']
        # Cells: td (12 columns)

        rows = await self.page.locator("table.tData.tbd02 tbody tr").all()
        print(f"Found {len(rows)} year entries.")

        history_data = []

        # State tracking: 12 slots for teams (KBO has max 10 active + history slots?)
        # Subagent said 12 columns.
        # We store {name: str, logo: str} for each column index.
        team_slots = [{"name": None, "logo": None} for _ in range(12)]

        for row in rows:
            # 1. Get Year
            # The year is in the 'th'.
            year_th = row.locator("th")
            if await year_th.count() == 0:
                continue

            year_text = await year_th.inner_text()
            try:
                year = int(year_text.strip())
            except Exception:
                logger.warning("Skipping invalid year: %s", year_text)
                continue

            # 2. Iterate Cells
            cells = await row.locator("td").all()

            # Subagent said 12 columns. Cells list should be length 12?
            # Or colspan might interfere? KBO history table usually fixed grid.

            for i, cell in enumerate(cells):
                if i >= 12:
                    break  # Safety

                # Check for content
                # Structure:
                # <a> <span class='nums'>Rank</span> <img alt='Name'> </a>
                # OR <a> <span class='nums'>Rank</span> <span>Name</span> </a>

                # Parse Rank
                rank_el = cell.locator("span.nums")
                rank = None
                if await rank_el.count() > 0:
                    with contextlib.suppress(Exception):
                        rank = int((await rank_el.inner_text()).strip())

                # Parse Name/Logo (Updates identity if present)
                # Look for img or name span
                img = cell.locator("img")
                name_span = cell.locator("span:not(.nums)")

                new_name = None
                new_logo = None

                if await img.count() > 0:
                    new_name = await img.get_attribute("alt")
                    new_logo = await img.get_attribute("src")
                elif await name_span.count() > 0:
                    new_name = (await name_span.inner_text()).strip()

                # Update State
                if new_name:
                    team_slots[i]["name"] = new_name
                if new_logo:
                    team_slots[i]["logo"] = new_logo

                # Create History Entry if there is a team in this slot (Rank is a good indicator of participation,
                # but sometimes teams participate without rank? No, KBO always ranks)
                # Or if we have a name in the slot state, it technically existed?
                # Usually only active teams have a cell in the row?
                # Wait, if the cell is empty, the team didn't play?
                # Subagent said "Team Ranks: 2, 1, 7, (Empty), 3..."
                # So if cell content implies participation.
                # If rank is present, definitely participated.
                # If rank is NOT present, did they participate?
                # In 2024, some columns are empty (Empty in subagent text).
                # So we only record if Rank is found? Or if name is explicitly shown?
                # Actually, "active teams" have ranks.

                if rank is not None:
                    # Identify Team Code from Name
                    # We need to map "Samsung Lions" -> "SS"
                    # We can resolve this during SAVE phase or here.
                    # Let's simple store the raw data.

                    current_name = team_slots[i]["name"]
                    current_logo = team_slots[i]["logo"]

                    if current_name:
                        history_data.append(
                            {
                                "season": year,
                                "team_name": current_name,
                                "logo_url": current_logo,
                                "ranking": rank,
                                "slot_index": i,  # Debug info
                            }
                        )

            print(f"Processed {year}: {len([h for h in history_data if h['season'] == year])} teams.")

        return history_data

    async def save(self, data: list[dict]):
        print(f"💾 Saving {len(data)} history entries...")
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)

                teams = session.execute(select(Team)).scalars().all()
                team_map = {t.team_id: t.franchise_id for t in teams}

                saved_count = 0
                for entry in data:
                    team_name = entry["team_name"]
                    season = entry["season"]

                    code = resolve_team_code(team_name)
                    if not code:
                        print(f"   ⚠️ Could not resolve code for '{team_name}' ({season})")
                        continue

                    franchise_id = team_map.get(code)
                    if not franchise_id:
                        print(f"   ⚠️ No franchise_id for code '{code}'")
                        continue

                    stmt = select(TeamHistory).where(
                        TeamHistory.season == season, TeamHistory.team_code == code
                    )
                    existing = session.execute(stmt).scalars().first()

                    if existing:
                        existing.team_name = team_name
                        existing.logo_url = entry["logo_url"]
                        existing.ranking = entry["ranking"]
                        existing.franchise_id = franchise_id
                    else:
                        session.add(
                            TeamHistory(
                                season=season,
                                team_code=code,
                                team_name=team_name,
                                logo_url=entry["logo_url"],
                                ranking=entry["ranking"],
                                franchise_id=franchise_id,
                            )
                        )
                    saved_count += 1

                session.commit()
                print(f"✅ Saved/Updated {saved_count} records ({saved_snaps} snapshots).")
            except Exception as e:
                session.rollback()
                logger.exception("Error saving team history")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    crawler = TeamHistoryCrawler()
    asyncio.run(crawler.crawl())
