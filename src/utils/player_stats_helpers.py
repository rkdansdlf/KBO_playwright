"""유틸리티: player stats helpers."""

from __future__ import annotations

import logging

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page

logger = logging.getLogger(__name__)


def extract_rows_fast(
    page: Page,
    selector: str = "table",
    link_query: str = "td:nth-child(2) a",
) -> list[dict[str, object]] | None:
    """
    Extracts rows fast.

    Args:
        page: Playwright page object.
        selector: Selector.
        link_query: Link Query.

    Returns:
        The result of the operation.

    """
    try:
        payload = page.evaluate(
            """
            (args) => {
                const table = document.querySelector(args.selector);
                if (!table) return null;
                const body = table.tBodies && table.tBodies.length ? table.tBodies[0] : table;
                const rows = Array.from(body.querySelectorAll('tr'));
                return rows.map((row) => {
                    const cells = Array.from(row.querySelectorAll('td')).map(td => (td.textContent || '').trim());
                    const link = row.querySelector(args.linkQuery);
                    return {
                        cells,
                        linkText: link ? (link.textContent || '').trim() : null,
                        linkHref: link ? link.getAttribute('href') : null,
                    };
                });
            }
            """,
            {"selector": selector, "linkQuery": link_query},
        )
    except (PlaywrightError, RuntimeError, TypeError, ValueError):
        logger.exception("Failed to execute JS payload")
        return None
    else:
        return payload or []
