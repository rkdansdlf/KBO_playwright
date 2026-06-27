from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.cli.verify_chunk_quality import (
    MIN_CHUNK_LENGTH,
    _percentile,
    category_distribution,
    compute_length_stats,
    count_empty_chunks,
    count_stub_chunks,
    find_duplicates,
    keyword_coverage,
    chunks_per_source,
)


class TestPercentileViaComputeLengthStats:
    def test_percentile_empty_list(self):
        result = compute_length_stats([])
        assert result["count"] == 0
        assert result["avg"] == 0
        assert result["min"] == 0
        assert result["max"] == 0
        assert result["p50"] == 0
        assert result["p95"] == 0

    def test_percentile_single_value(self):
        result = compute_length_stats([{"content": "hello"}])
        assert result["count"] == 1
        assert result["avg"] == 5
        assert result["p50"] == 5
        assert result["p95"] == 5

    def test_percentile_p95(self):
        result = compute_length_stats([{"content": "x" * i} for i in range(1, 101)])
        assert result["p95"] >= 90

    def test_percentile_p50(self):
        result = compute_length_stats([{"content": "x" * i} for i in range(1, 101)])
        assert result["p50"] >= 45


class TestCountEmptyChunks:
    def test_no_empty(self):
        chunks = [{"content": "hello"}, {"content": "world"}]
        assert count_empty_chunks(chunks) == 0

    def test_all_empty(self):
        chunks = [{"content": ""}, {"content": ""}, {"content": ""}]
        assert count_empty_chunks(chunks) == 3

    def test_whitespace_only(self):
        chunks = [{"content": "   "}, {"content": "\t\n"}]
        assert count_empty_chunks(chunks) == 2

    def test_empty_list(self):
        assert count_empty_chunks([]) == 0


class TestCountStubChunks:
    def test_short_but_valid(self):
        chunks = [{"content": "x" * 30}]
        assert count_stub_chunks(chunks) == 1

    def test_exactly_min(self):
        chunks = [{"content": "x" * MIN_CHUNK_LENGTH}]
        assert count_stub_chunks(chunks) == 0

    def test_above_min(self):
        chunks = [{"content": "x" * 200}]
        assert count_stub_chunks(chunks) == 0

    def test_empty_is_not_stub(self):
        chunks = [{"content": ""}]
        assert count_stub_chunks(chunks) == 0

    def test_empty_list(self):
        assert count_stub_chunks([]) == 0


class TestFindDuplicates:
    def test_no_duplicates(self):
        chunks = [
            {"source_row_id": "1", "content": "a"},
            {"source_row_id": "2", "content": "b"},
        ]
        cnt, dupes = find_duplicates(chunks)
        assert cnt == 0
        assert dupes == []

    def test_one_duplicate(self):
        chunks = [
            {"source_row_id": "1", "content": "a"},
            {"source_row_id": "1", "content": "b"},
        ]
        cnt, dupes = find_duplicates(chunks)
        assert cnt == 1
        assert dupes == ["1"]

    def test_multiple_duplicates(self):
        chunks = [
            {"source_row_id": "1", "content": "a"},
            {"source_row_id": "1", "content": "a"},
            {"source_row_id": "2", "content": "b"},
            {"source_row_id": "2", "content": "b"},
        ]
        cnt, dupes = find_duplicates(chunks)
        assert cnt == 2
        assert set(dupes) == {"1", "2"}

    def test_empty_row_id_uses_content_hash(self):
        chunks = [
            {"source_row_id": "", "content": "a"},
            {"source_row_id": "", "content": "a"},
        ]
        cnt, dupes = find_duplicates(chunks)
        assert cnt == 1

    def test_empty_list(self):
        cnt, dupes = find_duplicates([])
        assert cnt == 0
        assert dupes == []


class TestKeywordCoverage:
    def test_all_have_keywords(self):
        chunks = [
            {"meta": {"keywords": ["a"]}},
            {"meta": {"keywords": ["b"]}},
        ]
        assert keyword_coverage(chunks) == 1.0

    def test_none_have_keywords(self):
        chunks = [
            {"meta": {}},
            {"meta": {"keywords": []}},
        ]
        assert keyword_coverage(chunks) == 0.0

    def test_partial(self):
        chunks = [
            {"meta": {"keywords": ["a"]}},
            {"meta": {}},
            {"meta": {}},
            {"meta": {}},
        ]
        assert keyword_coverage(chunks) == 0.25

    def test_empty(self):
        assert keyword_coverage([]) == 0.0


class TestCategoryDistribution:
    def test_basic(self):
        chunks = [
            {"meta": {"category": "rulebook"}},
            {"meta": {"category": "rulebook"}},
            {"meta": {"category": "game_rules"}},
        ]
        result = category_distribution(chunks)
        assert result["rulebook"] == 2
        assert result["game_rules"] == 1

    def test_unknown(self):
        chunks = [{"meta": {}}]
        result = category_distribution(chunks)
        assert result["unknown"] == 1

    def test_empty(self):
        result = category_distribution([])
        assert len(result) == 0


class TestChunksPerSource:
    def test_basic(self):
        chunks = [
            {"source_table": "rulebook", "meta": {}},
            {"source_table": "rulebook", "meta": {"source": "custom"}},
            {"source_table": "game_rules", "meta": {}},
        ]
        result = chunks_per_source(chunks)
        assert result == {"rulebook": 1, "custom": 1, "game_rules": 1}

    def test_fallback_to_source_table(self):
        chunks = [{"source_table": "rulebook", "meta": {}}]
        result = chunks_per_source(chunks)
        assert result == {"rulebook": 1}

    def test_no_source_table(self):
        chunks = [{"source_table": None, "meta": {}}]
        result = chunks_per_source(chunks)
        assert result == {"unknown": 1}
