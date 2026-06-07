"""
Crawler for static text sources: rulebooks (PDF) and baseball encyclopedias/wikis (Namuwiki).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

from bs4 import BeautifulSoup
from pypdf import PdfReader

from src.db.engine import SessionLocal
from src.utils.playwright_pool import AsyncPlaywrightPool


class StaticTextCrawler:
    """
    Crawls and extracts static text (rules, history, terminology)
    from local/remote PDFs and wikis.
    """

    def __init__(self, pool: AsyncPlaywrightPool | None = None):
        self.pool = pool
        self._raw_pages: list[dict] = []

    def parse_local_pdf(self, pdf_path: str) -> list[dict[str, Any]]:
        """
        Parses a local PDF rulebook using pypdf and extracts text by page.
        """
        logger.info(f"📄 Parsing local PDF: {pdf_path}")
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found at: {pdf_path}")

        chunks = []
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
        logger.info(f"   Total pages: {total_pages}")

        for page_idx in range(total_pages):
            page = reader.pages[page_idx]
            text = page.extract_text()
            if not text or not text.strip():
                continue

            chunks.append(
                {
                    "title": f"KBO 공식 야구 규칙서 - Page {page_idx + 1}",
                    "content": text,
                    "meta": {
                        "source": pdf_path,
                        "page_number": page_idx + 1,
                        "total_pages": total_pages,
                        "crawled_at": datetime.now().isoformat(),
                        "category": "rulebook",
                    },
                }
            )

        return chunks

    async def crawl_namuwiki(self, url: str, save: bool = False) -> dict[str, Any]:
        """
        Crawls a Namuwiki page using Playwright to bypass Cloudflare protection
        and extracts cleaned main content with BeautifulSoup.
        """
        logger.info(f"🌐 Crawling Namuwiki page: {url}")

        html_content = ""
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()

        try:
            page = await pool.acquire()
            try:
                # Go to the url and wait for it to load
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait a brief moment for dynamic rendering
                await page.wait_for_timeout(2000)
                html_content = await page.content()
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

        if not html_content:
            raise ValueError(f"Failed to fetch content from Namuwiki url: {url}")

        self._raw_pages.append(
            {
                "source_key": "namuwiki_kbo",
                "url": url,
                "html": html_content,
                "status_code": 200,
            }
        )

        if save:
            from src.repositories.source_registry_repository import save_raw_snapshots

            with SessionLocal() as session:
                save_raw_snapshots(session, self._raw_pages)

        # Parse and clean with BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove noisy tags: scripts, styles, ads, nav panels, sidebars
        for tag in ["script", "style", "noscript", "iframe", "header", "footer"]:
            for el in soup.find_all(tag):
                el.decompose()

        # Remove typical Namuwiki ads and sidebar selectors
        selectors_to_remove = [
            ".wiki-advertisement",
            ".wiki-aside",
            "#footer",
            ".ad-wrapper",
            "div[class*='advertisement']",
            "div[id*='ad']",
            "nav",
        ]
        for sel in selectors_to_remove:
            for el in soup.select(sel):
                el.decompose()

        # Find Namuwiki's article body wrapper
        # Often it is inside .wiki-heading-content or .wiki-content or article
        main_content = ""
        content_div = soup.select_one(".wiki-content") or soup.select_one("article")

        if content_div:
            # Clean anchors, edit buttons
            for edit_btn in content_div.select(".wiki-edit-section"):
                edit_btn.decompose()
            for fn in content_div.select(".wiki-fn-content"):
                fn.decompose()

            # Extract main text
            main_content = content_div.get_text(separator="\n").strip()
        else:
            # Fallback to general body text
            main_content = (
                soup.body.get_text(separator="\n").strip() if soup.body else soup.get_text(separator="\n").strip()
            )

        # Title extraction
        title_el = soup.select_one(".wiki-title") or soup.select_one("h1")
        title = title_el.get_text().strip() if title_el else "Namuwiki Document"
        # Strip potential garbage from Namuwiki titles (e.g. edit button text)
        if " [편집]" in title:
            title = title.split(" [편집]")[0]

        return {
            "title": title,
            "content": main_content,
            "meta": {"source": url, "crawled_at": datetime.now().isoformat(), "category": "namuwiki"},
        }
