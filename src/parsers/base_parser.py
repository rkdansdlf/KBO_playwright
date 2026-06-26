"""
Base class for stadium-related HTML parsers.

Provides shared BeautifulSoup initialization and text extraction.
"""

from __future__ import annotations

import logging
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BaseStadiumParser:
    """
    Base class for stadium parsers (food, parking, seat, ticket).

    Subclasses override ``parse()`` with domain-specific logic.
    Backward-compatible module-level functions are provided in each parser module.
    """

    def __init__(self, html: str, source_key: str, metadata: dict[str, Any] | None = None) -> None:
        """Initializes a new instance."""
        self.source_key = source_key
        self.metadata = metadata or {}
        self.soup = BeautifulSoup(html, "html.parser")
        self.text = self.soup.get_text(separator=" ", strip=True)

    def parse(self) -> list[dict[str, Any]]:
        """
        Parses parse.

        Returns:
            List of results.

        """
        raise NotImplementedError
