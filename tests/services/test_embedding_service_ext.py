from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.embedding_service import EmbeddingService


class TestAdjustEmbeddingDimensionEdgeCases:
    def test_zero_vector_truncated_returns_zeros(self):
        svc = EmbeddingService()
        emb = [0.0, 0.0, 0.0, 0.0]
        result = svc.adjust_embedding_dimension(emb, 2)
        assert result == [0.0, 0.0]

    def test_negative_vector_truncated_and_normalized(self):
        svc = EmbeddingService()
        emb = [-3.0, 0.0, 4.0, 0.0]
        result = svc.adjust_embedding_dimension(emb, 2)
        assert abs(result[0] + 1.0) < 1e-6

    def test_custom_target_dimension(self):
        svc = EmbeddingService()
        emb = [0.1] * 10
        result = svc.adjust_embedding_dimension(emb, 5)
        assert len(result) == 5


class TestModelName:
    def test_openrouter_key(self):
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-test-key"
        assert svc._model_name() == "openai/text-embedding-3-small"

    def test_google_key(self):
        svc = EmbeddingService()
        svc.api_key = "AIza-test-key"
        assert svc._model_name() == "models/text-embedding-004"

    def test_custom_embedding_model_env(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_MODEL", "custom/model-v2")
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-test"
        assert svc._model_name() == "custom/model-v2"


class TestMissingEmbeddingInputs:
    def test_all_present(self):
        svc = EmbeddingService()
        texts = ["a", "b"]
        hashes = ["h1", "h2"]
        cached_map = {"h1": [0.1], "h2": [0.2]}
        indices, missing = svc._missing_embedding_inputs(texts, hashes, cached_map)
        assert indices == []
        assert missing == []

    def test_partial_miss(self):
        svc = EmbeddingService()
        texts = ["a", "b", "c"]
        hashes = ["h1", "h2", "h3"]
        cached_map = {"h1": [0.1]}
        indices, missing = svc._missing_embedding_inputs(texts, hashes, cached_map)
        assert indices == [1, 2]
        assert missing == ["b", "c"]


class TestMergeNewEmbeddings:
    def test_merges(self):
        svc = EmbeddingService()
        cached_map = {"h1": [0.1]}
        hashes = ["h1", "h2"]
        missing_indices = [1]
        new_embeddings = [[0.5, 0.6]]
        svc._merge_new_embeddings(cached_map, hashes, missing_indices, new_embeddings)
        assert "h2" in cached_map
        assert cached_map["h2"] == [0.5, 0.6]


class TestLoadCachedEmbeddings:
    def test_db_exception_returns_empty(self):
        svc = EmbeddingService()
        with patch("src.db.engine.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.side_effect = RuntimeError("db error")
            result = svc._load_cached_embeddings(["h1"], "model")
            assert result == {}

    def test_string_embedding_decoded(self):
        svc = EmbeddingService()
        mock_row = MagicMock()
        mock_row.text_hash = "h1"
        mock_row.embedding = "[0.1, 0.2, 0.3]"

        with patch("src.db.engine.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.scalars.return_value.all.return_value = [mock_row]
            result = svc._load_cached_embeddings(["h1"], "model")
            assert "h1" in result
            assert result["h1"] == [0.1, 0.2, 0.3]


class TestSaveCachedEmbeddings:
    def test_exception_handled(self):
        svc = EmbeddingService()
        with patch("src.db.engine.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.side_effect = RuntimeError("db error")
            svc._save_cached_embeddings(["h1"], [0], "model", [[0.1, 0.2]])

    def test_existing_entry_skipped(self):
        svc = EmbeddingService()
        mock_session = MagicMock()
        mock_session.get.return_value = MagicMock()

        with patch("src.db.engine.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.return_value = mock_session
            svc._save_cached_embeddings(["h1"], [0], "model", [[0.1, 0.2]])
            mock_session.add.assert_not_called()


class TestFetchMissingEmbeddings:
    def test_no_api_key_returns_zeros(self):
        svc = EmbeddingService()
        svc.api_key = None
        result = svc._fetch_missing_embeddings(["text1", "text2"])
        assert len(result) == 2
        assert all(v == 0.0 for emb in result for v in emb)

    def test_openrouter_route(self):
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-test"
        with patch.object(svc, "_fetch_openrouter_embeddings", return_value=[[0.1] * 256]):
            result = svc._fetch_missing_embeddings(["text"])
            assert len(result) == 1

    def test_google_route(self):
        svc = EmbeddingService()
        svc.api_key = "AIza-test"
        with patch.object(svc, "_fetch_google_embeddings", return_value=[[0.1] * 256]):
            result = svc._fetch_missing_embeddings(["text"])
            assert len(result) == 1


class TestGetEmbeddingsBatchWithCache:
    def test_all_cached(self):
        svc = EmbeddingService()
        svc.api_key = None
        with (
            patch.object(svc, "_load_cached_embeddings", return_value={"h1": [0.1] * 256}),
            patch.object(svc, "_compute_hash", return_value="h1"),
        ):
            result = svc.get_embeddings_batch(["text"])
            assert len(result) == 1

    def test_empty_batch(self):
        svc = EmbeddingService()
        assert svc.get_embeddings_batch([]) == []


class TestFetchOpenRouterException:
    def test_http_exception_returns_fallback(self):
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-test"
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.side_effect = httpx.ConnectError("fail")
            result = svc._fetch_openrouter_embeddings(["hello"])
            assert result == [[0.0] * 256]
