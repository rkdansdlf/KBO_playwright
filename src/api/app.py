"""FastAPI application for KBO Playwright REST API & AI Knowledge Platform."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from sqlalchemy.exc import SQLAlchemyError

from src.api.auth import API_KEY_NAME
from src.api.routers import (
    analytics,
    futures,
    games,
    health,
    milestones,
    notices,
    pipeline,
    players,
    rag,
    stadiums,
)
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
        "name": "KBO Sabermetrics & Matchup Analytics",
        "description": "KBO 리그 세이버메트릭스(wOBA, wRC+, FIP, WAR), BvP 상대전적 및 스플릿 조회 API",
    },
    {
        "name": "KBO Pipeline & Self-Healing",
        "description": "KBO 일일 데이터 파이프라인 관리, 결함 진단, 자가 치유 및 품질 허브 리포트 API",
    },
    {
        "name": "Players & Teams",
        "description": "KBO 구단, 선수 프로필, 상황별/스플릿 통계 및 신인 드래프트 지명 이력 조회 API",
    },
    {
        "name": "Games & Schedules",
        "description": "1군 경기 일정, 박스스코어, 상대 전적 및 실시간/당일 크롤러 트리거 API",
    },
    {
        "name": "Stadiums & Facilities",
        "description": "KBO 경기장 목록, 주차장, 식음료(F&B), 좌석 구역 및 예매 정보 조회 API",
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
boxscores, player profiles, sabermetrics, milestones, press releases, situational split stats,
stadium facilities, and hybrid RAG search capabilities.

## 🚀 Key Features
- 📊 **KBO Game Boxscore & Head-to-Head**: Full inning scoreboard, batting/pitching lines, and H2H analytics.
- 👤 **Player Stats & Sabermetrics**: Career season batting/pitching stats and advanced metrics (wOBA, wRC+, WAR, FIP).
- 🏟️ **Stadiums & Facilities**: Stadium locations, parking info, food vendors, and seating pricing.
- 🤖 **Hybrid RAG Search**: Reciprocal Rank Fusion ($RRF$) search across KBO knowledge chunks.
- ⚡ **High Performance Caching**: In-memory TTL caching for instant millisecond response times.
"""


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Startup and shutdown lifecycle handler."""
    logger.info("Initializing KBO Playwright API server...")
    try:
        init_db()
    except (SQLAlchemyError, RuntimeError, OSError, ValueError):
        logger.warning("Database bootstrap skipped or failed during startup", exc_info=True)
    yield
    logger.info("Shutting down KBO Playwright API server...")


app = FastAPI(
    title="KBO Playwright Data & RAG Search Platform",
    description=DESCRIPTION,
    version="1.5.0",
    openapi_tags=TAGS_METADATA,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

allowed_origins = [origin.strip() for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials="*" not in allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_openapi() -> dict[str, Any]:
    """Generate custom OpenAPI schema with API Key Header security."""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=TAGS_METADATA,
    )

    # Add security scheme definition
    openapi_schema["components"] = openapi_schema.get("components", {})
    openapi_schema["components"]["securitySchemes"] = {
        "APIKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": API_KEY_NAME,
            "description": f"Enter your REST_API_KEY in the {API_KEY_NAME} header.",
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
app.include_router(stadiums.router)
app.include_router(rag.router)
app.include_router(analytics.router)
app.include_router(pipeline.router)
app.include_router(notices.router)
app.include_router(milestones.router)
app.include_router(futures.router)
