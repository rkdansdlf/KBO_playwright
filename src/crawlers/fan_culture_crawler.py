import argparse
import asyncio
import re

from playwright.async_api import Page, async_playwright

from src.db.engine import SessionLocal
from src.repositories.fan_culture_repository import FanCultureRepository
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.safe_print import safe_print as print

TEAM_NAMUWIKI = {
    "KIA": "KIA_%ED%83%80%EC%9D%B4%EA%B1%B0%EC%A6%88",
    "SS": "%EC%82%BC%EC%84%B1_%EB%9D%BC%EC%9D%B4%EC%98%A8%EC%A6%88",
    "LG": "LG_%ED%8A%B8%EC%9C%88%EC%8A%A4",
    "DB": "%EB%91%90%EC%82%B0_%EB%B2%A0%EC%96%B4%EC%8A%A4",
    "KT": "KT_%EC%9C%84%EC%A6%88",
    "SSG": "SSG_%EB%9E%9C%EB%8D%94%EC%8A%A4",
    "NC": "NC_%EB%8B%A4%EC%9D%B4%EB%85%B8%EC%8A%A4",
    "HH": "%ED%95%9C%ED%99%94_%EC%9D%B4%EA%B8%80%EC%8A%A4",
    "LT": "%EB%A1%AF%EB%8D%B0_%EC%9E%90%EC%9D%B4%EC%96%B8%EC%B8%A0",
    "KH": "%ED%82%A4%EC%9B%80_%ED%9E%88%EC%96%B4%EB%A1%9C%EC%A6%88",
}


class FanCultureCrawler:
    def __init__(self):
        self.namuwiki_base = "https://namu.wiki/w/"

    async def run(self, save: bool = False):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await install_async_resource_blocking(context)
            page = await context.new_page()

            all_songs = []
            all_chants = []

            for team_code, wiki_path in TEAM_NAMUWIKI.items():
                url = self.namuwiki_base + wiki_path
                print(f"Crawling fan culture for {team_code} from {url}...")
                try:
                    songs, chants = await self._extract_fan_culture(page, team_code, url)
                    all_songs.extend(songs)
                    all_chants.extend(chants)
                    print(f"  > Found {len(songs)} songs, {len(chants)} chants")
                except Exception as e:
                    print(f"  > Error: {e}")
                await asyncio.sleep(1)

            await browser.close()

            if save:
                self._save_to_db(all_songs, all_chants)
            else:
                for s in all_songs[:5]:
                    print(f"[Song] {s}")
                for c in all_chants[:5]:
                    print(f"[Chant] {c}")

    async def _extract_fan_culture(self, page: Page, team_id: str, url: str) -> tuple:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        data = await page.evaluate("""
        () => {
            function findSectionHeading(headingText) {
                const headings = document.querySelectorAll('h2, h3, .wiki-heading');
                for (const h of headings) {
                    if (h.innerText.includes(headingText)) return h;
                }
                return null;
            }
            function getSectionContent(heading) {
                if (!heading) return '';
                let el = heading.nextElementSibling;
                let text = '';
                while (el && !el.matches('h2, h3, .wiki-heading')) {
                    text += el.innerText + '\\n';
                    el = el.nextElementSibling;
                }
                return text;
            }
            return {
                cheer: getSectionContent(findSectionHeading('응원가')),
                song: getSectionContent(findSectionHeading('응원')),
                chant: getSectionContent(findSectionHeading('구호')),
                fight: getSectionContent(findSectionHeading('응원')),
                culture: getSectionContent(findSectionHeading('문화')),
                rivalry: getSectionContent(findSectionHeading('라이벌')),
            };
        }
        """)

        songs = []
        combined_text = " ".join(v for v in data.values() if v)
        song_lines = re.findall(r"[가-힣\s!?~]+(?:[가-힣\s!?~]+)", combined_text)
        seen_songs = set()
        for line in song_lines:
            line = line.strip()
            if len(line) > 5 and line not in seen_songs:
                if any(kw in line for kw in ["응원", "노래", "가요", "♬", "♪"]):
                    seen_songs.add(line)
                    songs.append(
                        {
                            "team_id": team_id,
                            "song_name": line[:100],
                            "song_type": "TEAM",
                            "lyrics": line[:500],
                            "description": f"Extracted from Namuwiki {team_id} page",
                        }
                    )

        chants = []
        chant_lines = re.findall(r"(?:([가-힣]+(?:[\s,]+[가-힣]+)*)\s*(?:!|~))", combined_text)
        seen_chants = set()
        for line in chant_lines:
            line = line.strip()
            if len(line) > 3 and line not in seen_chants:
                if any(kw in line for kw in ["이기", "승리", "파이팅", "화이팅", "짝짝짝", "야"]):
                    seen_chants.add(line)
                    chants.append(
                        {
                            "team_id": team_id,
                            "chant_text": line[:200],
                            "situation": "GENERAL",
                            "description": f"Extracted from Namuwiki {team_id} page",
                        }
                    )

        return songs[:50], chants[:30]

    def _save_to_db(self, songs: list[dict], chants: list[dict]):
        session = SessionLocal()
        repo = FanCultureRepository(session)
        song_count = 0
        chant_count = 0
        try:
            for item in songs:
                try:
                    repo.save_cheer_song(item)
                    song_count += 1
                except Exception:
                    pass
            for item in chants:
                try:
                    repo.save_cheer_chant(item)
                    chant_count += 1
                except Exception:
                    pass
            session.commit()
            print(f"Saved {song_count} cheer songs and {chant_count} cheer chants to database.")
        except Exception as e:
            session.rollback()
            print(f"Error saving to DB: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl KBO fan culture data from Namuwiki")
    parser.add_argument("--save", action="store_true", help="Save to database")
    args = parser.parse_args()

    crawler = FanCultureCrawler()
    asyncio.run(crawler.run(save=args.save))
