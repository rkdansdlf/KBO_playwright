"""Base classes for KBO data parsers.

Provides generic, type-safe base abstractions for HTML and JSON data parsers,
including error handling, text extraction, table parsing, and metadata tracking.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from bs4 import BeautifulSoup

from src.parsers.dto import ParseResult

if TYPE_CHECKING:
    from bs4.element import Tag

T = TypeVar("T")
logger = logging.getLogger(__name__)


class BaseParser[T](ABC):
    """Generic abstract base class for all data parsers."""

    def __init__(
        self,
        raw_content: object,
        source_key: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize BaseParser.

        Args:
            raw_content: Raw input data (HTML string, JSON string, dict, etc.).
            source_key: Identifier for the data source.
            metadata: Contextual metadata (e.g. season, date, team_code).

        """
        self.raw_content = raw_content
        self.source_key = source_key
        self.metadata = metadata or {}
        self.logger = logging.getLogger(self.__class__.__module__)

    @abstractmethod
    def parse(self) -> T:
        """Parse raw content into structured output.

        Returns:
            Parsed structured data of type T.

        Raises:
            Exception: If parsing fails and cannot be recovered.

        """
        raise NotImplementedError

    def _get_default_data(self) -> Any:  # noqa: ANN401
        """Return a sensible default value for type T on parsing errors."""
        return []

    def parse_safe(self) -> ParseResult[T]:
        """Execute parse() with exception isolation, returning a structured ParseResult.

        Returns:
            ParseResult containing parsed data, success flag, and diagnostics.

        """
        try:
            data = self.parse()
            return ParseResult(data=data, success=True, metadata=self.metadata)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Parser %s failed for source_key=%s: %s", self.__class__.__name__, self.source_key, exc)
            return ParseResult(
                data=self._get_default_data(),
                success=False,
                errors=[f"{type(exc).__name__}: {exc}"],
                metadata=self.metadata,
            )


class BaseHtmlParser(BaseParser[T]):
    """Base parser for HTML content backed by BeautifulSoup."""

    def __init__(
        self,
        html: str,
        source_key: str = "",
        metadata: dict[str, Any] | None = None,
        *,
        parser_backend: str = "html.parser",
    ) -> None:
        """Initialize BaseHtmlParser.

        Args:
            html: HTML source string.
            source_key: Source identifier.
            metadata: Contextual metadata.
            parser_backend: BeautifulSoup parser engine (default: 'html.parser').

        """
        super().__init__(raw_content=html or "", source_key=source_key, metadata=metadata)
        self.html = html or ""
        self.soup = BeautifulSoup(self.html, parser_backend)
        self.text = self.soup.get_text(separator=" ", strip=True)

    @staticmethod
    def clean_text(text_or_tag: Tag | str | None) -> str:
        """Normalize and strip whitespace from text or a BeautifulSoup tag."""
        if text_or_tag is None:
            return ""
        raw = text_or_tag.get_text(separator=" ", strip=True) if hasattr(text_or_tag, "get_text") else str(text_or_tag)
        return " ".join(raw.split())

    def select_text(self, selector: str, default: str = "") -> str:
        """Find the first matching element by CSS selector and return its cleaned text."""
        elem = self.soup.select_one(selector)
        return self.clean_text(elem) if elem else default

    def select_all_text(self, selector: str) -> list[str]:
        """Find all matching elements by CSS selector and return a list of cleaned texts."""
        return [self.clean_text(elem) for elem in self.soup.select(selector)]

    def extract_table_rows(self, table_elem: Tag) -> list[dict[str, str]]:
        """Extract a <table> into a list of row dictionaries mapping header to cell text."""
        headers: list[str] = []
        header_row = table_elem.select_one("thead tr") or table_elem.select_one("tr")
        if header_row:
            headers = [self.clean_text(th) for th in header_row.select("th, td")]

        rows: list[dict[str, str]] = []
        body_rows = table_elem.select("tbody tr") or table_elem.select("tr")
        # If the first row was treated as header, skip it if body_rows selected all trs
        if not table_elem.select("tbody tr") and body_rows and header_row:
            body_rows = body_rows[1:]

        for row in body_rows:
            cells = [self.clean_text(td) for td in row.select("td, th")]
            if len(cells) == len(headers) and headers:
                rows.append(dict(zip(headers, cells, strict=False)))
            elif cells:
                rows.append({f"col_{i}": cell for i, cell in enumerate(cells)})
        return rows


class BaseStadiumParser(BaseHtmlParser[list[dict[str, Any]]]):
    """Base class for stadium-related HTML parsers (ticket, food, parking, seat).

    Subclasses override ``parse()`` with domain-specific logic.
    Provides 100% backward compatibility for all existing stadium parsers.
    """

    def __init__(self, html: str, source_key: str, metadata: dict[str, Any] | None = None) -> None:
        """Initialize BaseStadiumParser."""
        super().__init__(html=html, source_key=source_key, metadata=metadata)

    def parse(self) -> list[dict[str, Any]]:
        """Parse stadium data.

        Returns:
            List of parsed dictionary records.

        """
        raise NotImplementedError


class BaseJsonParser(BaseParser[T]):
    """Base parser for JSON API payloads."""

    def __init__(
        self,
        raw_json: str | dict[str, Any] | list[Any],
        source_key: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize BaseJsonParser.

        Args:
            raw_json: JSON string, dictionary, or list.
            source_key: Source identifier.
            metadata: Contextual metadata.

        """
        super().__init__(raw_content=raw_json, source_key=source_key, metadata=metadata)
        if isinstance(raw_json, str):
            try:
                self.payload: dict[str, Any] | list[Any] = json.loads(raw_json)
            except (json.JSONDecodeError, TypeError):
                self.payload = {}
        elif isinstance(raw_json, (dict, list)):
            self.payload = raw_json
        else:
            self.payload = {}
