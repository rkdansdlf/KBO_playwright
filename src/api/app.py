"""FastAPI application for KBO Playwright REST API & AI Knowledge Platform."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from src.api.routers import futures, games, health, milestones, notices, players, rag
from src.db.engine import init_db

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

TAGS_METADATA = [
    {
        "name": "KBO Notices & Press Releases",
        "description": "KBO 공식 행정 공시, 보도자료 및 유관 뉴스 조회 API",
    },
    {
        "name": "KBO Player Milestones",
        "description": "KBO 선수 통산 대기록 달성 현황 및 카운트다운 지표 조회 API",
    },
    {
        "name": "KBO Futures League",
        "description": "KBO 퓨처스 리그 2군 경기 일정 및 결과 조회 API",
    },
    {
        "name": "KBO RAG & AI Hybrid Search",
        "description": "Dense Vector + BM25 RRF 융합 지식 하이브리드 검색 REST API",
    },
    {
        "name": "Players & Teams",
        "description": "KBO 구단, 선수 프로필, 상황별/스플릿 통계 및 신인 드래프트 지명 이력 조회 API",
    },
    {
        "name": "Games & Schedules",
        "description": "1군 경기 일정, 하이라이트 및 전 경기 프리뷰 카드 조회 API",
    },
    {
        "name": "Health & System",
        "description": "FastAPI 서비스 헬스체크 및 시스템 상태 확인 API",
    },
]

DESCRIPTION = """
# ⚾ KBO Playwright Data & AI Search REST API Platform

Welcome to the **KBO Baseball Data & AI Knowledge Platform API**.
This REST API platform provides unified access to KBO 1st/2nd division schedules,
player profiles, milestones, press releases, situational split stats, and hybrid RAG
search capabilities.

---

### 🔑 Authentication Guide
When `REST_API_KEY` is configured in the server environment, all protected API endpoints require an API Key:
- **HTTP Header**: `X-API-Key: <your_api_key>`
- **Query Parameter**: `?api_key=<your_api_key>`

*Click the **Authorize 🔓** button at the top right of this page to set your API Key for interactive testing.*

---

### 🚀 Key Feature Categories
- 📰 **Notices & Press Releases**: Official KBO administrative announcements.
- 🏆 **Player Milestones**: Hit, HR, RBI, Wins milestone countdown tracking.
- ⚾ **Futures League**: 2nd Division schedule, scores, and status.
- 🤖 **Hybrid RAG Search**: Reciprocal Rank Fusion ($RRF$) search across 193+ KBO knowledge chunks.
- 📊 **Situational Splits & Drafts**: Scoring position stats, vs LHP/RHP, and rookie draft picks.
"""


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Initialize database tables on application startup."""
    try:
        init_db()
        logger.info("Database initialized successfully for FastAPI app.")
    except Exception:
        logger.exception("Failed to initialize database on startup")
    yield


app = FastAPI(
    title="KBO Playwright Data & RAG Search Platform",
    description=DESCRIPTION,
    version="1.5.0",
    openapi_tags=TAGS_METADATA,
    lifespan=lifespan,
)

# CORS configuration
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_openapi() -> dict[str, Any]:
    """Customize OpenAPI schema with X-API-Key security scheme."""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=TAGS_METADATA,
    )

    openapi_schema["components"] = openapi_schema.get("components", {})
    openapi_schema["components"]["securitySchemes"] = {
        "APIKeyHeader": {
            "type": "apiKey",
            "name": "X-API-Key",
            "in": "header",
            "description": "Enter your REST_API_KEY in the format: X-API-Key",
        }
    }

    # Apply security globally
    openapi_schema["security"] = [{"APIKeyHeader": []}]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore[method-assign]

# Include modular routers
app.include_router(health.router)
app.include_router(games.router)
app.include_router(players.router)
app.include_router(rag.router)
app.include_router(notices.router)
app.include_router(milestones.router)
app.include_router(futures.router)
