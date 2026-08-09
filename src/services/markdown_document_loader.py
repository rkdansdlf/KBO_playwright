"""Load and classify local Markdown documents used by the KBO knowledge base."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.constants import KST

logger = logging.getLogger(__name__)

_CATEGORY_MAP: dict[str, str] = {
    "baseball_rules": "game_rules",
    "glossary": "glossary",
    "bylaws": "bylaws",
    "league_regulations": "league_regulations",
    "player_regulations": "player_regulations",
    "scoring_rules": "scoring_rules",
    "disciplinary_regulations": "disciplinary_regulations",
    "supplementary_regulations": "supplementary_regulations",
    "kbo_knowledge": "kbo_knowledge",
    "kbo_rulebook": "rulebook",
}

_REGULATION_PARTS = frozenset(
    {
        "baseball_rules",
        "bylaws",
        "disciplinary_regulations",
        "kbo_rulebook",
        "league_regulations",
        "player_regulations",
        "scoring_rules",
        "supplementary_regulations",
    },
)
_DEFINITION_PARTS = frozenset({"definitions", "glossary"})


def markdown_category(path_parts: list[str]) -> tuple[str, str | None]:
    """Return the display category and optional subcategory for a Markdown path."""
    category = "rulebook"
    subcategory = None
    for part in path_parts[:-1]:
        mapped = _CATEGORY_MAP.get(part)
        if mapped:
            if category == "rulebook":
                category = mapped
            else:
                subcategory = mapped
    return category, subcategory


def markdown_title(content: str, file: str) -> str:
    """Extract the first Markdown heading or derive a title from the filename."""
    first_line = content.lstrip().split("\n")[0]
    if first_line.startswith("#"):
        return first_line.lstrip("#").strip()
    return Path(file).stem.replace("_", " ").title()


def load_local_markdown_docs(rules_dir: str | Path = "Docs/baseball") -> list[dict[str, Any]]:
    """Load local Markdown files and attach stable source metadata."""
    root = Path(rules_dir)
    if not root.exists():
        return []

    logger.info("Scanning directory '%s' for static Markdown files", root)
    raw_docs: list[dict[str, Any]] = []
    for full_path in sorted(root.rglob("*.md")):
        try:
            content = full_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            logger.exception("Error reading local Markdown %s", full_path)
            continue

        relative_path = full_path.relative_to(root).as_posix()
        category, subcategory = markdown_category(relative_path.split("/"))
        raw_docs.append(
            {
                "title": markdown_title(content, full_path.name),
                "content": content,
                "meta": {
                    "source": str(full_path),
                    "source_file": full_path.name,
                    "source_path": relative_path,
                    "crawled_at": datetime.now(KST).isoformat(),
                    "category": category,
                    **({"subcategory": subcategory} if subcategory else {}),
                },
            },
        )
    return raw_docs


def markdown_source_table(doc: dict[str, Any]) -> str:
    """Map a local Markdown document to a logical RAG source table."""
    meta = doc.get("meta", {})
    relative_path = str(meta.get("source_path") or meta.get("source") or "")
    normalized = relative_path.replace("\\", "/").lower()
    parts = set(Path(normalized).parts)
    file_path = Path(normalized)
    file_name = file_path.name
    file_stem = file_path.stem

    if parts & _REGULATION_PARTS or "regulation" in file_name or "regulations" in file_name:
        return "kbo_regulations"
    if (
        parts & _DEFINITION_PARTS
        or file_stem in _DEFINITION_PARTS
        or file_name
        in {
            "kbo_metrics_explained.md",
            "rules_terms.md",
        }
    ):
        return "kbo_definitions"
    return "markdown_docs"
