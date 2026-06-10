"""
test_static_pipeline_smoke.py

Smoke tests for the Phase 1 static text pipeline.
Loads real markdown documents from Docs/baseball, runs the TextTransformer,
and verifies chunk count, metadata, quality thresholds.

These tests do NOT require a running database or network access.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.parsers.text_transformer import TextTransformer

# ── constants ────────────────────────────────────────────────────────────────

DOCS_DIR = PROJECT_ROOT / "Docs" / "baseball"

# Directory-to-category mapping (mirrors run_all_crawlers._CATEGORY_MAP)
CATEGORY_MAP: dict[str, str] = {
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

MIN_TOTAL_CHUNKS = 30  # sanity lower-bound
MIN_CHUNKS_PER_DOC = 2  # each .md should produce at least this many
MIN_CHUNK_LENGTH = 30  # chars — very short chunks are low-quality
MIN_AVG_LENGTH = 80  # chars


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_markdown_docs() -> list[dict[str, Any]]:
    """Recursively loads all .md files from DOCS_DIR with category metadata."""
    docs = []
    for md_path in sorted(DOCS_DIR.rglob("*.md")):
        try:
            content = md_path.read_text(encoding="utf-8")
        except OSError:
            continue

        # Derive category from directory parts
        rel = md_path.relative_to(DOCS_DIR)
        category = "rulebook"
        for part in rel.parts[:-1]:
            mapped = CATEGORY_MAP.get(part)
            if mapped:
                category = mapped
                break

        # Extract H1 title
        first_line = content.lstrip().split("\n")[0]
        doc_title = (
            first_line.lstrip("#").strip() if first_line.startswith("#") else md_path.stem.replace("_", " ").title()
        )

        docs.append(
            {
                "title": doc_title,
                "content": content,
                "meta": {
                    "source": str(md_path),
                    "source_file": md_path.name,
                    "category": category,
                },
            }
        )
    return docs


def _chunk_all(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    transformer = TextTransformer()
    all_chunks: list[dict[str, Any]] = []
    for doc in docs:
        chunks = transformer.chunk_document(doc)
        all_chunks.extend(chunks)
    return all_chunks


# ── fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def docs():
    found = _load_markdown_docs()
    assert found, f"No markdown files found under {DOCS_DIR}"
    return found


@pytest.fixture(scope="module")
def all_chunks(docs):
    return _chunk_all(docs)


# ── tests ────────────────────────────────────────────────────────────────────


def test_docs_dir_exists():
    """The Docs/baseball directory must exist and contain markdown files."""
    assert DOCS_DIR.exists(), f"Docs directory not found: {DOCS_DIR}"
    md_files = list(DOCS_DIR.rglob("*.md"))
    assert len(md_files) >= 5, f"Expected at least 5 markdown files under {DOCS_DIR}, found {len(md_files)}"


def test_markdown_docs_load(docs):
    """All markdown files should load successfully with title and content."""
    for doc in docs:
        assert doc["title"], f"Missing title for {doc['meta']['source_file']}"
        assert len(doc["content"]) > 0, f"Empty content for {doc['meta']['source_file']}"


def test_category_metadata_assigned(docs):
    """Every document should have a non-empty category assigned from CATEGORY_MAP."""
    for doc in docs:
        cat = doc["meta"]["category"]
        assert cat, f"Empty category for {doc['meta']['source_file']}"
        assert cat != "unknown", f"Unmapped category 'unknown' for {doc['meta']['source_file']}"


def test_total_chunk_count(all_chunks):
    """Total chunk count should meet the minimum sanity threshold."""
    assert len(all_chunks) >= MIN_TOTAL_CHUNKS, f"Expected ≥ {MIN_TOTAL_CHUNKS} chunks, got {len(all_chunks)}"


def test_no_empty_chunks(all_chunks):
    """No chunk should have empty content after cleaning."""
    empty = [c for c in all_chunks if not c["content"].strip()]
    assert len(empty) == 0, f"Found {len(empty)} empty chunks: {[c['title'] for c in empty[:5]]}"


def test_chunk_length_quality(all_chunks):
    """Average chunk length and minimum individual chunk length meet thresholds."""
    lengths = [len(c["content"]) for c in all_chunks]
    avg_len = sum(lengths) / len(lengths)

    assert avg_len >= MIN_AVG_LENGTH, f"Average chunk length {avg_len:.0f} chars is below threshold {MIN_AVG_LENGTH}"
    # Allow up to 10% stubs — real markdown docs have short heading-only sections
    stub_count = sum(1 for length in lengths if length < MIN_CHUNK_LENGTH)
    stub_rate = stub_count / len(lengths)
    assert stub_rate <= 0.10, (
        f"Too many stub chunks: {stub_count}/{len(lengths)} ({stub_rate * 100:.1f}%) "
        f"are shorter than {MIN_CHUNK_LENGTH} chars"
    )


def test_chunks_per_document(docs):
    """Each markdown document should produce at least MIN_CHUNKS_PER_DOC chunks.

    Note: Prose-style documents (e.g. narrative season reviews) may be a single
    section and produce only 1 chunk via overlap strategy. We allow up to 25% of
    documents to be single-section, but at least 75% must be properly chunked.
    """
    transformer = TextTransformer()
    under_chunked = []
    for doc in docs:
        chunks = transformer.chunk_document(doc)
        if len(chunks) < MIN_CHUNKS_PER_DOC:
            under_chunked.append((doc["meta"]["source_file"], len(chunks)))

    max_allowed = max(1, int(len(docs) * 0.25))  # at most 25% under-chunked
    assert len(under_chunked) <= max_allowed, (
        f"Too many under-chunked documents (< {MIN_CHUNKS_PER_DOC} chunks): "
        f"{len(under_chunked)}/{len(docs)} (max allowed: {max_allowed}). "
        f"Files: {under_chunked}"
    )


def test_chunk_metadata_integrity(all_chunks):
    """Every chunk must have required metadata fields populated."""
    required_fields = ["source", "category", "source_row_id", "chunk_index"]
    missing = [
        (chunk["title"], field) for chunk in all_chunks for field in required_fields if not chunk["meta"].get(field)
    ]

    assert len(missing) == 0, f"Chunks with missing metadata fields (showing up to 10): {missing[:10]}"


def test_no_duplicate_source_row_ids(all_chunks):
    """source_row_id should be unique across all chunks."""
    ids = [c["meta"]["source_row_id"] for c in all_chunks]
    unique_ids = set(ids)
    assert len(ids) == len(unique_ids), f"Found {len(ids) - len(unique_ids)} duplicate source_row_id(s)"


def test_rulebook_chunks_have_headings(all_chunks):
    """Rulebook/game_rules chunks should have a 'heading' metadata field."""
    rulebook_chunks = [c for c in all_chunks if c["meta"].get("category") in ("rulebook", "game_rules", "glossary")]
    assert len(rulebook_chunks) > 0, "No rulebook/game_rules chunks found"

    missing_heading = [c for c in rulebook_chunks if not c["meta"].get("heading")]
    # Allow up to 10% without heading (first section may not have a clear header)
    rate = len(missing_heading) / len(rulebook_chunks)
    assert rate <= 0.10, (
        f"{len(missing_heading)}/{len(rulebook_chunks)} rulebook chunks "
        f"are missing 'heading' metadata ({rate * 100:.1f}%)"
    )


def test_kbo_knowledge_chunks_present(all_chunks):
    """kbo_knowledge category documents should produce chunks."""
    kbo_chunks = [c for c in all_chunks if c["meta"].get("category") == "kbo_knowledge"]
    assert len(kbo_chunks) > 0, "No kbo_knowledge chunks found — check Docs/baseball/kbo_knowledge/"
