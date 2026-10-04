"""A page that stopped being a KBO page is not a page with nothing on it.

The sweep visits seven standing pages under ``BusinessAndEvent`` -- an MVP hall,
a draft notice, a safety guide, a purchase guide. Most of them are not event
pages and have nothing to announce, so "no candidates" is the *correct* reading
of most of a healthy sweep. That is what made an empty result useless as a
verdict: it is the normal answer, and it is also what a redesign, a maintenance
interstitial and an error page all look like from the outside.

The distinction is the site's own frame. Every page in the section carries one
header, one navigation and one footer; if those are gone, the document is
whatever the site served instead. These tests pin that reading, using real
captured pages for the healthy case and the captured page with its frame removed
for the drift case, so neither depends on a hand-written approximation.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.kbo_event_crawler import has_kbo_event_frame, read_kbo_event_page
from src.crawlers.kbo_event_outcome import KboEventStatus, classify_page_failure

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "html"
BASE = "https://www.koreabaseball.com/Kbo/BusinessAndEvent"


def _html(name: str) -> str:
    return (FIXTURES / f"{name}.html").read_text(encoding="utf-8")


def _without_frame(html: str) -> str:
    """Return the same document with the site frame stripped.

    Built by removing the elements rather than by writing a new document, so the
    failure case differs from the healthy one in exactly one way.
    """
    soup = BeautifulSoup(html, "html.parser")
    for selector in ("header", "nav", "footer"):
        for element in soup.select(selector):
            element.decompose()
    return str(soup)


class TestARealPageIsReadable:
    def test_a_captured_guide_page_keeps_its_frame(self) -> None:
        assert has_kbo_event_frame(_html("kbo_business_event_safeguide")) is True

    def test_a_captured_guide_page_is_read_not_drifted(self) -> None:
        """The standing guide is exactly the case an empty-vocabulary would get wrong."""
        read = read_kbo_event_page(_html("kbo_business_event_safeguide"), f"{BASE}/SafeGuide.aspx")

        assert read.status is not KboEventStatus.SCHEMA_CHANGED
        assert read.reason is None

    def test_both_captured_pages_carry_the_frame(self) -> None:
        """Two unrelated pages agreeing is what makes the frame a site property."""
        for name in ("kbo_business_event_safeguide", "kbo_business_event_mvp"):
            assert has_kbo_event_frame(_html(name)) is True, name


class TestAMissingFrameIsDrift:
    def test_the_frame_check_sees_the_difference(self) -> None:
        html = _html("kbo_business_event_safeguide")

        assert has_kbo_event_frame(html) is True
        assert has_kbo_event_frame(_without_frame(html)) is False

    def test_a_page_without_the_frame_is_not_reported_as_empty(self) -> None:
        """The whole contract: unreadable and legitimately bare must not agree."""
        read = read_kbo_event_page(_without_frame(_html("kbo_business_event_safeguide")))

        assert read.status is KboEventStatus.SCHEMA_CHANGED
        assert read.events == []

    def test_drift_carries_a_reason_the_taxonomy_can_be_derived_from(self) -> None:
        read = read_kbo_event_page(_without_frame(_html("kbo_business_event_mvp")))

        assert read.reason is not None
        code, terminal = classify_page_failure(read.reason)
        assert code == FailureCode.PARSE_SELECTOR_MISSING.value
        assert terminal is True

    def test_a_maintenance_page_is_drift_too(self) -> None:
        """The other common substitution: a real 200 that is not the document."""
        read = read_kbo_event_page("<html><head><title>점검 중</title></head><body>곧 복구됩니다</body></html>")

        assert read.status is KboEventStatus.SCHEMA_CHANGED

    def test_the_frame_survives_a_page_with_nothing_to_announce(self) -> None:
        """A readable page with no candidate is empty, not drift.

        This is the distinction the whole module exists for, and the one that
        would quietly disappear if the frame check were dropped: both cases yield
        no events, so only the frame separates them.
        """
        html = "<html><head><title>메인 | KBO</title></head><body><header></header><nav></nav></body></html>"
        read = read_kbo_event_page(html)

        assert has_kbo_event_frame(html) is True
        assert read.status is KboEventStatus.EMPTY

    def test_a_page_title_alone_makes_an_event(self) -> None:
        """Why ``EMPTY`` is narrower than it sounds.

        ``extract_kbo_event_page`` treats a descriptive page title as a
        candidate, so most of the standing pages read as SUCCESS. That is the
        crawler's existing behaviour and not something the frame check changed;
        pinning it so a change to title handling is a deliberate act.
        """
        read = read_kbo_event_page(
            "<html><head><title>안전 가이드 | 주요 사업/행사 | KBO</title></head>"
            "<body><header></header><nav></nav></body></html>",
        )

        assert read.status is KboEventStatus.SUCCESS


class TestTheVocabularyIsTotal:
    def test_every_page_reason_maps_to_a_code(self) -> None:
        """An unmapped reason would reach the ledger as UNKNOWN and retry forever."""
        for reason in ("site_frame_missing", "page_fetch_failed"):
            code, _terminal = classify_page_failure(reason)
            assert code != FailureCode.UNKNOWN.value, reason

    def test_an_unknown_reason_is_reported_as_unknown_rather_than_guessed(self) -> None:
        code, terminal = classify_page_failure("something_new")

        assert code == FailureCode.UNKNOWN.value
        assert terminal is False

    def test_drift_is_terminal_and_a_fetch_failure_is_not(self) -> None:
        """Retrying drift returns the same document; retrying a timeout might not."""
        assert classify_page_failure("site_frame_missing")[1] is True
        assert classify_page_failure("page_fetch_failed")[1] is False

    def test_the_statuses_the_sweep_can_produce_are_exactly_these(self) -> None:
        assert {status.value for status in KboEventStatus} == {
            "success",
            "empty",
            "schema_changed",
            "fetch_failed",
        }


class TestThePageReadIsAValue:
    def test_it_carries_its_own_events(self) -> None:
        read = read_kbo_event_page(_html("kbo_business_event_safeguide"))

        assert read.status is KboEventStatus.SUCCESS
        assert read.events, "a captured guide page yielded candidates before the frame check"

    def test_a_read_with_no_events_is_empty_rather_than_success(self) -> None:
        html = "<html><head><title>메인 | KBO</title></head><body><header></header><nav></nav><footer></footer></body></html>"

        assert read_kbo_event_page(html).status is KboEventStatus.EMPTY

    def test_it_is_immutable(self) -> None:
        """A verdict that can be edited after the fact is not a verdict."""
        read = read_kbo_event_page(_html("kbo_business_event_mvp"))
        try:
            read.status = KboEventStatus.SUCCESS  # type: ignore[misc]
        except AttributeError:
            return
        raise AssertionError("KboEventPageRead should not be assignable")


def _unused_import_guard() -> Iterator[None]:  # pragma: no cover - import hygiene
    """Keep ``Iterator`` referenced so the TYPE_CHECKING import stays honest."""
    yield
