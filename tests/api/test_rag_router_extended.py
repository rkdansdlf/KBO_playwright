"""Integration tests for RAG Router evaluation endpoint."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.app import app
from src.api.auth import get_api_key

client = TestClient(app)


def test_evaluate_retrieval_endpoint() -> None:
    app.dependency_overrides[get_api_key] = lambda: "test_key"

    payload = {
        "query": "최형우 1500타점 달성",
        "retrieved_chunk_ids": ["c1", "c2", "c3", "c4", "c5"],
        "golden_relevant_chunk_ids": ["c1", "c3"],
        "k": 5,
    }

    try:
        response = client.post("/api/rag/evaluate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "최형우 1500타점 달성"
        assert data["k"] == 5
        assert data["precision_at_k"] == 0.4
        assert data["recall_at_k"] == 1.0
        assert data["mrr"] == 1.0
        assert data["hit_rate"] == 1.0
    finally:
        app.dependency_overrides.clear()
