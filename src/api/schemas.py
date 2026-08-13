"""Pydantic schemas and response models for FastAPI REST API documentation."""

from __future__ import annotations

from pydantic import BaseModel, Field


class NoticeItemSchema(BaseModel):
    """Schema for single KBO press release item."""

    id: int = Field(..., example=1)
    notice_id: str = Field(..., example="100")
    category: str | None = Field(None, example="공시/공지")
    title: str = Field(..., example="KBO 리그 경기일정 변경 안내")
    published_date: str | None = Field(None, example="2026-08-09")
    source_url: str | None = Field(None, example="https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100")
    created_at: str | None = Field(None, example="2026-08-09T14:00:00")


class NoticesListResponse(BaseModel):
    """Response schema for GET /api/v1/notices."""

    page: int = Field(..., example=1)
    limit: int = Field(..., example=20)
    count: int = Field(..., example=1)
    notices: list[NoticeItemSchema]


class MilestoneItemSchema(BaseModel):
    """Schema for single player milestone item."""

    id: int = Field(..., example=1)
    season: int = Field(..., example=2026)
    player_id: str = Field(..., example="78224")
    player_name: str = Field(..., example="최형우")
    team_code: str | None = Field(None, example="KIA")
    milestone_category: str = Field(..., example="1600타점")
    current_val: int = Field(..., example=1598)
    target_val: int = Field(..., example=1600)
    remaining_val: int = Field(..., example=2)
    is_achieved: bool = Field(..., example=False)
    achieved_date: str | None = Field(None, example=None)


class MilestonesListResponse(BaseModel):
    """Response schema for GET /api/v1/milestones."""

    season: int = Field(..., example=2026)
    count: int = Field(..., example=1)
    milestones: list[MilestoneItemSchema]


class FuturesScheduleItemSchema(BaseModel):
    """Schema for single Futures League schedule item."""

    game_id: str = Field(..., example="F20260809")
    season: int = Field(..., example=2026)
    game_date: str | None = Field(None, example="2026-08-09")
    away_team: str = Field(..., example="고양")
    home_team: str = Field(..., example="한화")
    away_score: int | None = Field(None, example=5)
    home_score: int | None = Field(None, example=3)
    stadium: str | None = Field(None, example="이천")
    game_status: str = Field(..., example="COMPLETED")
    cancel_reason: str | None = Field(None, example=None)


class FuturesScheduleResponse(BaseModel):
    """Response schema for GET /api/v1/futures/schedule."""

    season: int = Field(..., example=2026)
    count: int = Field(..., example=1)
    schedules: list[FuturesScheduleItemSchema]


class PlayerSplitItemSchema(BaseModel):
    """Schema for single player situational split item."""

    season: int = Field(..., example=2026)
    player_id: str = Field(..., example="78224")
    player_name: str = Field(..., example="김도영")
    team_code: str | None = Field(None, example="KIA")
    split_type: str = Field(..., example="scoring_position")
    split_key: str = Field(..., example="득점권시")
    ab: int | None = Field(None, example=80)
    hits: int | None = Field(None, example=30)
    hr: int | None = Field(None, example=8)
    rbi: int | None = Field(None, example=25)
    bb: int | None = Field(None, example=12)
    so: int | None = Field(None, example=15)
    avg: float | None = Field(None, example=0.375)
    obp: float | None = Field(None, example=0.450)
    slg: float | None = Field(None, example=0.650)
    ops: float | None = Field(None, example=1.100)


class PlayerSplitsResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/splits."""

    player_id: str = Field(..., example="78224")
    season: int = Field(..., example=2026)
    count: int = Field(..., example=1)
    splits: list[PlayerSplitItemSchema]


class HybridSearchResultItemSchema(BaseModel):
    """Schema for single hybrid search result chunk."""

    chunk_id: int = Field(..., example=12)
    title: str = Field(..., example="KBO 공시 - 올스타전 라인업 발표")
    content: str = Field(..., example="[2026-08-09] KBO 공식 공시: KBO 올스타전 라인업 발표...")
    score: float = Field(..., example=0.0328)
    vector_rank: int | None = Field(None, example=1)
    bm25_rank: int | None = Field(None, example=2)
    meta: dict | None = Field(None, example={"category": "press_release", "notice_id": "100"})


class HybridSearchResponse(BaseModel):
    """Response schema for POST /api/v1/rag/hybrid-search."""

    query: str = Field(..., example="올스타전 경기일정")
    total_results: int = Field(..., example=1)
    results: list[HybridSearchResultItemSchema]
