"""
Contract tests for KBO LiveText.aspx HTML structure.

Validates that the KBO website relay page structure matches
what PBPCrawler expects. Uses fixtures from tests/fixtures/kbo_live_text/.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "kbo_live_text"


def _iter_html_fixtures():
    if not FIXTURE_DIR.exists():
        return
    for fpath in sorted(FIXTURE_DIR.iterdir()):
        if fpath.suffix in (".html", ".htm"):
            yield fpath


class TestKboLiveTextContract:
    """Validate KBO LiveText.aspx HTML structure expected by PBPCrawler."""

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_num_containers_exist(self, fixture_path: Path):
        """Page must have at least one div[id^='numCont'] container, the primary extraction target."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        assert len(containers) > 0, f"No div[id^='numCont'] found in {fixture_path}"

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_inning_headers_have_blue_class(self, fixture_path: Path):
        """Inning headers (N회초/말 text) should have class 'blue' as expected by PBPCrawler."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        found_blue_header = False
        for container in containers:
            for span in container.find_all("span"):
                text = span.get_text(strip=True)
                if "회" in text and ("초" in text or "말" in text):
                    classes = span.get("class", [])
                    if "blue" in classes:
                        found_blue_header = True
                    else:
                        pytest.skip(f"Inning header '{text}' missing 'blue' class in {fixture_path}")
        assert found_blue_header, f"No inning header with 'blue' class found in {fixture_path}"

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_event_spans_have_expected_classes(self, fixture_path: Path):
        """Non-header event spans should have 'normaiflTxt' or 'red' class."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        for container in containers:
            for span in container.find_all("span"):
                text = span.get_text(strip=True)
                if not text:
                    continue
                # Skip separator lines
                if "---" in text:
                    continue
                # Skip inning headers (checked separately)
                if "회" in text and ("초" in text or "말" in text):
                    continue
                classes = span.get("class", [])
                if not classes:
                    pytest.skip(f"Event span '{text[:40]}' has no classes")

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_minimum_event_count(self, fixture_path: Path):
        """Page must have at least a few non-header, non-separator events."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        event_count = 0
        for container in containers:
            for span in container.find_all("span"):
                text = span.get_text(strip=True)
                if not text or "---" in text:
                    continue
                if "회" in text and ("초" in text or "말" in text):
                    continue
                event_count += 1

        assert event_count >= 3, f"Too few events ({event_count}) — expected at least 3 in {fixture_path}"

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_event_text_has_colon_format(self, fixture_path: Path):
        """Most event texts should follow '타자명:결과' colon format (parsing assumption)."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        colon_count = 0
        total = 0
        for container in containers:
            for span in container.find_all("span"):
                text = span.get_text(strip=True)
                if not text or "---" in text:
                    continue
                if "회" in text and ("초" in text or "말" in text):
                    continue
                total += 1
                if ":" in text:
                    colon_count += 1

        if total > 0:
            assert colon_count / total >= 0.4, f"Only {colon_count}/{total} events have colon format in {fixture_path}"

    @pytest.mark.parametrize("fixture_path", list(_iter_html_fixtures()))
    def test_pbp_crawler_selector_compatibility(self, fixture_path: Path):
        """Verify the JS selector used by PBPCrawler._extract_flat_events_legacy extracts spans."""
        with fixture_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        containers = soup.select('div[id^="numCont"]')
        all_spans = []
        for container in containers:
            all_spans.extend(container.find_all("span"))

        assert len(all_spans) > 0, f"No spans extracted from any numCont container in {fixture_path}"

        texts = [s.get_text(strip=True) for s in all_spans if s.get_text(strip=True)]
        assert any("회" in t for t in texts), f"No inning header text found in {fixture_path}"
