"""Tests for OpenAPI (Swagger) Documentation and X-API-Key Security Scheme."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.app import app


@pytest.fixture
def test_client() -> TestClient:
    """FastAPI TestClient fixture."""
    return TestClient(app)


def test_openapi_json_schema(test_client: TestClient) -> None:
    """Test GET /openapi.json returns valid OpenAPI 3.x schema with X-API-Key security scheme."""
    response = test_client.get("/openapi.json")
    assert response.status_code == 200

    schema = response.json()
    assert schema["openapi"].startswith("3.")
    assert "KBO Playwright Data & RAG Search Platform" in schema["info"]["title"]

    # Security Schemes
    components = schema.get("components", {})
    security_schemes = components.get("securitySchemes", {})
    assert "APIKeyHeader" in security_schemes
    assert security_schemes["APIKeyHeader"]["name"] == "X-API-Key"

    # Tags metadata
    tags = schema.get("tags", [])
    tag_names = [t["name"] for t in tags]
    assert "KBO Notices & Press Releases" in tag_names
    assert "KBO Player Milestones" in tag_names
    assert "KBO Futures League" in tag_names
    assert "KBO RAG & AI Hybrid Search" in tag_names

    # Paths check
    paths = schema.get("paths", {})
    assert "/api/v1/notices" in paths
    assert "/api/v1/milestones" in paths
    assert "/api/v1/futures/schedule" in paths
    assert "/api/v1/rag/hybrid-search" in paths
