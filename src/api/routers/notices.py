"""FastAPI router for KBO press releases and administrative notices."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from src.api.auth import get_api_key
from src.db.engine import get_db_session
from src.models.kbo_press_release import KboPressRelease

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/notices", tags=["KBO Notices & Press Releases"])


@router.get("", dependencies=[Depends(get_api_key)])
def get_notices(
    category: Annotated[str | None, Query(description="카테고리 필터 (공시/공지, 뉴스 등)")] = None,
    keyword: Annotated[str | None, Query(description="제목 검색 키워드")] = None,
    page: Annotated[int, Query(ge=1, description="페이지 번호")] = 1,
    limit: Annotated[int, Query(ge=1, le=100, description="페이지 당 항목 수")] = 20,
) -> dict[str, Any]:
    """KBO 공식 공시 및 보도자료 목록을 조회합니다."""
    with get_db_session() as session:
        stmt = select(KboPressRelease)

        if category:
            stmt = stmt.where(KboPressRelease.category == category)
        if keyword:
            stmt = stmt.where(KboPressRelease.title.contains(keyword))

        offset = (page - 1) * limit
        stmt = stmt.order_by(KboPressRelease.id.desc()).offset(offset).limit(limit)

        records = list(session.execute(stmt).scalars().all())
        results = [
            {
                "id": r.id,
                "notice_id": r.notice_id,
                "category": r.category,
                "title": r.title,
                "published_date": r.published_date.isoformat() if r.published_date else None,
                "source_url": r.source_url,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in records
        ]

        return {
            "page": page,
            "limit": limit,
            "count": len(results),
            "notices": results,
        }
