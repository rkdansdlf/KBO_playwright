"""FastAPI application for KBO Playwright REST API server."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers import futures, games, health, milestones, notices, players, rag
from src.db.engine import init_db

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


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
    title="KBO Playwright Crawler & Data API",
    description="REST API to query KBO baseball data and control crawlers.",
    version="1.3.0",
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

# Include modular routers
app.include_router(health.router)
app.include_router(games.router)
app.include_router(players.router)
app.include_router(rag.router)
app.include_router(notices.router)
app.include_router(milestones.router)
app.include_router(futures.router)
