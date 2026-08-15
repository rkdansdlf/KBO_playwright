"""KBO Website Selector Drift Sentinel.

Validates HTML structure and CSS selectors against registered page contracts
to proactively detect KBO website UI and DOM mutations before crawler breakage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class PageContract:
    """Expected DOM structure contract for a specific KBO website page."""

    page_name: str
    required_selectors: Sequence[str] = field(default_factory=tuple)
    min_table_columns: Mapping[str, int] = field(default_factory=dict)
    expected_text_snippets: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class SelectorDriftReport:
    """Result of DOM contract validation against parsed HTML."""

    page_name: str
    is_healthy: bool
    drift_detected: bool
    missing_selectors: Sequence[str] = field(default_factory=tuple)
    mismatched_columns: Sequence[str] = field(default_factory=tuple)
    missing_snippets: Sequence[str] = field(default_factory=tuple)
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class SelectorDriftSentinel:
    """Monitors and inspects HTML responses against defined page contracts."""

    def __init__(self) -> None:
        """Initialize with an empty contract registry."""
        self._contracts: dict[str, PageContract] = {}

    def register_contract(self, contract: PageContract) -> None:
        """Register a page contract for monitoring."""
        self._contracts[contract.page_name] = contract

    def check_html(self, page_name: str, html_content: str) -> SelectorDriftReport:
        """Inspect HTML content against the registered contract for page_name."""
        contract = self._contracts.get(page_name)
        if not contract:
            msg = f"No contract registered for page: {page_name}"
            raise KeyError(msg)

        soup = BeautifulSoup(html_content, "html.parser")
        missing_selectors: list[str] = []
        mismatched_columns: list[str] = []

        # 1. Check required selectors
        for sel in contract.required_selectors:
            elements = soup.select(sel)
            if not elements:
                missing_selectors.append(sel)

        # 2. Check table column counts
        for table_sel, expected_min in contract.min_table_columns.items():
            table = soup.select_one(table_sel)
            if not table:
                mismatched_columns.append(f"Table '{table_sel}' not found")
                continue

            header_cells = table.select("th")
            if not header_cells:
                first_row = table.select_one("tr")
                header_cells = first_row.select("td") if first_row else []

            if len(header_cells) < expected_min:
                mismatched_columns.append(
                    f"Table '{table_sel}' has {len(header_cells)} columns, expected >= {expected_min}",
                )

        # 3. Check expected text snippets
        text_content = soup.get_text()
        missing_snippets = [snippet for snippet in contract.expected_text_snippets if snippet not in text_content]

        drift = bool(missing_selectors or mismatched_columns or missing_snippets)
        return SelectorDriftReport(
            page_name=page_name,
            is_healthy=not drift,
            drift_detected=drift,
            missing_selectors=tuple(missing_selectors),
            mismatched_columns=tuple(mismatched_columns),
            missing_snippets=tuple(missing_snippets),
        )


def create_default_kbo_sentinel() -> SelectorDriftSentinel:
    """Create Sentinel loaded with standard KBO core page contracts."""
    sentinel = SelectorDriftSentinel()

    # Game Detail Box Score Contract
    sentinel.register_contract(
        PageContract(
            page_name="game_detail",
            required_selectors=(
                ".box-score-area, .game-info, .score-board, .record-etc",
                "#tblAwayHitter1, #tblHomeHitter1, .tbl-type01",
                "#tblAwayPitcher, #tblHomePitcher, .tbl-type02",
            ),
            expected_text_snippets=("타자 기록", "투수 기록"),
        ),
    )

    # Schedule Page Contract
    sentinel.register_contract(
        PageContract(
            page_name="schedule",
            required_selectors=(
                ".tbl-type06",
                "table",
            ),
        ),
    )

    # Player Search Contract
    sentinel.register_contract(
        PageContract(
            page_name="player_search",
            required_selectors=(
                "input[id$='txtSearchPlayerName'], #txtSearchPlayerName",
                "table",
            ),
        ),
    )

    return sentinel


__all__ = [
    "PageContract",
    "SelectorDriftReport",
    "SelectorDriftSentinel",
    "create_default_kbo_sentinel",
]
