from unittest.mock import MagicMock, patch

from src.services.embedding_service import EmbeddingService


class TestAdjustEmbeddingDimension:
    def test_empty_embedding(self):
        svc = EmbeddingService()
        result = svc.adjust_embedding_dimension([], 256)
        assert result == [0.0] * 256

    def test_same_dimension(self):
        svc = EmbeddingService()
        emb = [0.1, 0.2, 0.3]
        result = svc.adjust_embedding_dimension(emb, 3)
        assert result == emb

    def test_truncates_and_normalizes(self):
        svc = EmbeddingService()
        emb = [3.0, 0.0, 0.0, 0.0]
        result = svc.adjust_embedding_dimension(emb, 2)
        assert abs(result[0] - 1.0) < 1e-6
        assert abs(result[1]) < 1e-6

    def test_pads_shorter_embedding(self):
        svc = EmbeddingService()
        emb = [0.5, 0.5]
        result = svc.adjust_embedding_dimension(emb, 5)
        assert result[:2] == [0.5, 0.5]
        assert result[2:] == [0.0, 0.0, 0.0]


class TestComputeHash:
    def test_hash_is_deterministic(self):
        svc = EmbeddingService()
        h1 = svc._compute_hash("hello world")
        h2 = svc._compute_hash("hello world")
        assert h1 == h2

    def test_hash_normalizes_whitespace(self):
        svc = EmbeddingService()
        h1 = svc._compute_hash("hello  world")
        h2 = svc._compute_hash("hello world")
        assert h1 == h2

    def test_hash_different_for_different_text(self):
        svc = EmbeddingService()
        h1 = svc._compute_hash("abc")
        h2 = svc._compute_hash("xyz")
        assert h1 != h2


class TestGetEmbedding:
    def test_empty_texts_returns_empty_list(self):
        svc = EmbeddingService()
        assert svc.get_embeddings_batch([]) == []

    def test_no_api_key_returns_zero_vectors(self):
        svc = EmbeddingService()
        svc.api_key = None
        result = svc.get_embedding("test")
        assert len(result) == 256
        assert all(v == 0.0 for v in result)

    def test_get_embedding_delegates_to_batch(self):
        svc = EmbeddingService()
        svc.api_key = None
        result = svc.get_embedding("single text")
        assert len(result) == 256


class TestFetchGoogleEmbeddings:
    def test_api_error_returns_fallback(self):
        svc = EmbeddingService()
        svc.api_key = "fake-key"
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.return_value.status_code = 500
            result = svc._fetch_google_embeddings(["hello"])
            assert len(result) == 1
            assert result[0] == [0.0] * 256

    def test_successful_response_parses_embeddings(self):
        svc = EmbeddingService()
        svc.api_key = "fake-key"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embeddings": [{"values": [0.1, 0.2]}]}
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.return_value = mock_response
            result = svc._fetch_google_embeddings(["hello"])
            assert len(result) == 1
            assert result[0] == [0.1, 0.2]

    def test_exception_during_request_returns_fallback(self):
        svc = EmbeddingService()
        svc.api_key = "fake-key"
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.side_effect = Exception("timeout")
            result = svc._fetch_google_embeddings(["hello"])
            assert result == [[0.0] * 256]


class TestFetchOpenRouterEmbeddings:
    def test_api_error_returns_fallback(self):
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-fake"
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.return_value.status_code = 401
            result = svc._fetch_openrouter_embeddings(["hello"])
            assert result == [[0.0] * 256]

    def test_successful_response_parses_in_order(self):
        svc = EmbeddingService()
        svc.api_key = "sk-or-v1-fake"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"index": 0, "embedding": [0.1]},
                {"index": 1, "embedding": [0.2]},
            ]
        }
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.return_value = mock_response
            result = svc._fetch_openrouter_embeddings(["a", "b"])
            assert result == [[0.1], [0.2]]
