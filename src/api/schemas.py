"""Pydantic schemas and response models for FastAPI REST API documentation."""

from __future__ import annotations

from typing import Any

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

    chunk_id: str = Field(..., example="12")
    title: str | None = Field(None, example="KBO 공시 - 올스타전 라인업 발표")
    content: str = Field(..., example="[2026-08-09] KBO 공식 공시: KBO 올스타전 라인업 발표...")
    source_url: str | None = Field(None, example="https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100")
    category: str = Field("general", example="press_release")
    score: float = Field(..., example=0.0328)
    vector_rank: int | None = Field(None, example=1)
    bm25_rank: int | None = Field(None, example=2)
    meta: dict[str, Any] | None = Field(None, example={"category": "press_release", "notice_id": "100"})
    provenance: dict[str, Any] | None = Field(None)


class HybridSearchResponse(BaseModel):
    """Response schema for POST /api/v1/rag/hybrid-search."""

    query: str = Field(..., example="올스타전 경기일정")
    total_results: int = Field(..., example=1)
    results: list[HybridSearchResultItemSchema]
    retrieval: dict[str, Any] = Field(default_factory=dict)


class RagSourceSchema(BaseModel):
    """Schema for a source cited by the RAG Q&A endpoint."""

    title: str | None = Field(None, example="KBO 공시 - 올스타전 라인업 발표")
    source_url: str | None = Field(None, example="https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100")
    document_type: str = Field("general", example="press_release")
    snippet: str = Field(..., example="KBO 올스타전 라인업 발표...")
    meta: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, Any] | None = Field(None)


class RagAskResponse(BaseModel):
    """Response schema for POST /api/v1/rag/ask."""

    query: str
    answer: str
    sources: list[RagSourceSchema] = Field(default_factory=list)
    chunks: list[HybridSearchResultItemSchema] = Field(default_factory=list)
    chunk_count: int = 0
    analysis: dict[str, Any] = Field(default_factory=dict)
    retrieval: dict[str, Any] = Field(default_factory=dict)


# --- Stadiums & Facilities Schemas ---


class StadiumItemSchema(BaseModel):
    """Schema for single stadium basic metadata."""

    stadium_code: str = Field(..., example="잠실")
    stadium_name: str = Field(..., example="서울종합운동장 야구장 (잠실)")
    home_teams: list[str] = Field(..., example=["LG", "DB"])
    capacity: int | None = Field(None, example=23750)
    city: str | None = Field(None, example="서울")
    address: str | None = Field(None, example="서울특별시 송파구 올림픽로 25")


class StadiumParkingSchema(BaseModel):
    """Schema for stadium parking facility."""

    name: str = Field(..., example="잠실종합운동장 부설주차장")
    fee_type: str | None = Field(None, example="유료")
    capacity: int | None = Field(None, example=1200)
    tip: str | None = Field(None, example="경기 시작 2시간 전 만차 예상")
    address: str | None = Field(None, example="서울특별시 송파구")
    walking_minutes: int | None = Field(None, example=10)
    is_event_day_available: bool = Field(default=True, example=True)
    reservation_required: bool = Field(default=False, example=False)
    operating_hours: str | None = Field(None, example="08:00-23:00")


class StadiumFoodSchema(BaseModel):
    """Schema for stadium food vendor/menu."""

    vendor_name: str = Field(..., example="원샷치킨")
    location: str | None = Field(None, example="1루 내야 2층 복도")
    popular_menu: str | None = Field(None, example="커리원샷치킨 (12,000원)")
    category: str | None = Field(None, example="치킨/스낵")
    floor_level: str | None = Field(None, example="2F")
    base_side: str | None = Field(None, example="first_base")
    gate_info: str | None = Field(None, example="1루 게이트")
    order_method: str | None = Field(None, example="onsite")
    confidence: str | None = Field(None, example="high")


class StadiumSeatSectionSchema(BaseModel):
    """Schema for stadium seating section."""

    section_name: str = Field(..., example="1루 테이블석")
    section_code: str | None = Field(None, example="101B")
    seat_grade: str | None = Field(None, example="테이블석")
    weekday_price: int | None = Field(None, example=43000)
    weekend_price: int | None = Field(None, example=48000)
    description: str | None = Field(None, example="테이블 구비, 최적의 시야 제공")
    base_side: str | None = Field(None, example="first_base")
    floor_level: str | None = Field(None, example="2F")
    gate_info: str | None = Field(None, example="1루 게이트")
    seat_map_url: str | None = Field(None, example="https://example.com/seat-map.png")


class StadiumTicketPriceSchema(BaseModel):
    """Schema for a stadium ticket price."""

    team_id: str = Field(..., example="LG")
    season: int = Field(..., example=2026)
    seat_grade: str = Field(..., example="테이블석")
    day_type: str = Field(..., example="weekday")
    audience_type: str | None = Field(None, example="general")
    price: int = Field(..., example=43000)
    currency: str = Field("KRW", example="KRW")
    source_url: str | None = Field(None, example="https://ticket.example.com")


class StadiumTicketScheduleSchema(BaseModel):
    """Schema for a stadium ticket reservation schedule."""

    game_date: str = Field(..., example="2026-08-15")
    home_team: str = Field(..., example="LG")
    away_team: str = Field(..., example="KIA")
    stadium: str = Field(..., example="잠실")
    open_time: str = Field(..., example="2026-08-08T11:00:00")
    platform: str = Field(..., example="Ticketlink")
    url: str | None = Field(None, example="https://ticket.example.com")


class StadiumTicketOpenRuleSchema(BaseModel):
    """Schema for a team's recurring ticket opening rule."""

    team_id: str = Field(..., example="LG")
    platform: str = Field(..., example="Ticketlink")
    open_offset_days: int = Field(..., example=7)
    open_time: str = Field(..., example="11:00:00")
    sales_close_rule: str | None = Field(None, example="경기 시작 2시간 전")
    max_tickets_per_user: int | None = Field(None, example=4)
    fee_rule: str | None = Field(None, example="예매 수수료 별도")
    cancel_rule: str | None = Field(None, example="경기 전날까지 취소 가능")
    note: str | None = Field(None, example="구단 정책에 따라 변경될 수 있음")


class StadiumFacilitiesResponse(BaseModel):
    """Response schema for GET /api/v1/stadiums/{stadium_code}/facilities."""

    stadium_code: str = Field(..., example="잠실")
    stadium_name: str = Field(..., example="잠실야구장")
    home_teams: list[str] = Field(default_factory=list, example=["LG", "DB"])
    parkings: list[StadiumParkingSchema] = Field(default_factory=list)
    food_vendors: list[StadiumFoodSchema] = Field(default_factory=list)
    seat_sections: list[StadiumSeatSectionSchema] = Field(default_factory=list)
    ticket_prices: list[StadiumTicketPriceSchema] = Field(default_factory=list)
    ticket_schedules: list[StadiumTicketScheduleSchema] = Field(default_factory=list)
    ticket_open_rules: list[StadiumTicketOpenRuleSchema] = Field(default_factory=list)


# --- Game Boxscore & Head-to-Head Schemas ---


class HitterBoxscoreSchema(BaseModel):
    """Schema for single hitter line in a game boxscore."""

    order: int | None = Field(None, example=1)
    player_id: str | None = Field(None, example="78224")
    player_name: str = Field(..., example="김도영")
    position: str | None = Field(None, example="3루수")
    ab: int = Field(0, example=4)
    r: int = Field(0, example=2)
    h: int = Field(0, example=2)
    rbi: int = Field(0, example=3)
    bb: int = Field(0, example=1)
    so: int = Field(0, example=0)
    avg: float | None = Field(None, example=0.360)


class PitcherBoxscoreSchema(BaseModel):
    """Schema for single pitcher line in a game boxscore."""

    order: int | None = Field(None, example=1)
    player_id: str | None = Field(None, example="60181")
    player_name: str = Field(..., example="양현종")
    decision: str | None = Field(None, example="승")
    innings: str | None = Field(None, example="6.0")
    h: int = Field(0, example=5)
    r: int = Field(0, example=2)
    er: int = Field(0, example=2)
    bb: int = Field(0, example=2)
    so: int = Field(0, example=7)
    hr: int = Field(0, example=1)
    era: float | None = Field(None, example=3.15)


class InningScoreSchema(BaseModel):
    """Schema for single inning scoreboard line."""

    team: str = Field(..., example="KIA")
    scores: list[str | int] = Field(..., example=[0, 1, 0, 3, 0, 0, 2, 0, 0])
    r: int = Field(..., example=6)
    h: int = Field(..., example=10)
    e: int = Field(..., example=0)
    b: int = Field(..., example=4)


class GameLineupPlayerSchema(BaseModel):
    """Schema for a player in the game lineup snapshot."""

    order: int | None = Field(None, example=1)
    player_id: str | None = Field(None, example="78224")
    player_name: str = Field(..., example="김도영")
    position: str | None = Field(None, example="3루수")
    is_starter: bool = Field(default=False, example=True)


class GameHighlightItemSchema(BaseModel):
    """Schema for game highlight moment."""

    id: int = Field(..., example=1)
    game_id: str = Field(..., example="20260809LGKIA0")
    event_seq: int | None = Field(None, example=42)
    inning: int | None = Field(None, example=7)
    inning_half: str | None = Field(None, example="bottom")
    highlight_type: str = Field(..., example="LEAD_CHANGE")
    description: str = Field(..., example="김도영 역전 쓰리런 홈런")
    wpa: float | None = Field(None, example=0.385)
    importance_score: float = Field(0.0, example=0.95)
    tags: list[str] = Field(default_factory=list, example=["홈런", "역전", "결승타"])


class GameBoxscoreResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/boxscore."""

    game_id: str = Field(..., example="20260809LGKIA0")
    game_date: str = Field(..., example="2026-08-09")
    stadium: str = Field(..., example="광주")
    home_team: str = Field(..., example="KIA")
    away_team: str = Field(..., example="LG")
    home_score: int = Field(..., example=6)
    away_score: int = Field(..., example=3)
    game_status: str = Field(..., example="FINAL")
    scoreboard: list[InningScoreSchema] = Field(default_factory=list)
    away_lineup: list[GameLineupPlayerSchema] = Field(default_factory=list)
    home_lineup: list[GameLineupPlayerSchema] = Field(default_factory=list)
    away_batters: list[HitterBoxscoreSchema] = Field(default_factory=list)
    home_batters: list[HitterBoxscoreSchema] = Field(default_factory=list)
    away_pitchers: list[PitcherBoxscoreSchema] = Field(default_factory=list)
    home_pitchers: list[PitcherBoxscoreSchema] = Field(default_factory=list)
    highlights: list[GameHighlightItemSchema] = Field(default_factory=list)


class HeadToHeadGameItemSchema(BaseModel):
    """Schema for past head-to-head game item."""

    game_id: str = Field(..., example="20260809LGKIA0")
    game_date: str = Field(..., example="2026-08-09")
    home_team: str = Field(..., example="KIA")
    away_team: str = Field(..., example="LG")
    home_score: int = Field(..., example=6)
    away_score: int = Field(..., example=3)
    winner: str | None = Field(None, example="KIA")


class HeadToHeadResponse(BaseModel):
    """Response schema for GET /api/v1/games/head-to-head."""

    team1: str = Field(..., example="KIA")
    team2: str = Field(..., example="LG")
    season: int | None = Field(None, example=2026)
    team1_wins: int = Field(0, example=8)
    team2_wins: int = Field(0, example=5)
    draws: int = Field(0, example=1)
    total_games: int = Field(0, example=14)
    team1_avg_runs: float = Field(0.0, example=5.4)
    team2_avg_runs: float = Field(0.0, example=4.1)
    recent_games: list[HeadToHeadGameItemSchema] = Field(default_factory=list)


# --- Player Season Stats & Sabermetrics Schemas ---


class PlayerBattingSeasonSchema(BaseModel):
    """Schema for player batting season statistics."""

    season: int = Field(..., example=2026)
    team_code: str | None = Field(None, example="KIA")
    g: int = Field(0, example=110)
    pa: int = Field(0, example=480)
    ab: int = Field(0, example=420)
    r: int = Field(0, example=88)
    h: int = Field(0, example=145)
    two_b: int = Field(0, example=28)
    three_b: int = Field(0, example=3)
    hr: int = Field(0, example=25)
    rbi: int = Field(0, example=85)
    sb: int = Field(0, example=30)
    cs: int = Field(0, example=4)
    bb: int = Field(0, example=52)
    so: int = Field(0, example=70)
    avg: float | None = Field(None, example=0.345)
    obp: float | None = Field(None, example=0.420)
    slg: float | None = Field(None, example=0.605)
    ops: float | None = Field(None, example=1.025)


class PlayerPitchingSeasonSchema(BaseModel):
    """Schema for player pitching season statistics."""

    season: int = Field(..., example=2026)
    team_code: str | None = Field(None, example="KIA")
    g: int = Field(0, example=25)
    w: int = Field(0, example=12)
    losses: int = Field(0, example=4)
    sv: int = Field(0, example=0)
    hld: int = Field(0, example=0)
    ip: float | None = Field(None, example=150.0)
    h: int = Field(0, example=135)
    r: int = Field(0, example=58)
    er: int = Field(0, example=52)
    bb: int = Field(0, example=40)
    so: int = Field(0, example=140)
    hr: int = Field(0, example=10)
    era: float | None = Field(None, example=3.12)
    whip: float | None = Field(None, example=1.17)


class PlayerSeasonStatResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/stats."""

    player_id: str = Field(..., example="78224")
    player_name: str = Field(..., example="김도영")
    position: str | None = Field(None, example="내야수")
    team: str | None = Field(None, example="KIA")
    batting_seasons: list[PlayerBattingSeasonSchema] = Field(default_factory=list)
    pitching_seasons: list[PlayerPitchingSeasonSchema] = Field(default_factory=list)


class PlayerSabermetricsResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/sabermetrics."""

    player_id: str = Field(..., example="78224")
    player_name: str = Field(..., example="김도영")
    season: int = Field(..., example=2026)
    woba: float | None = Field(None, example=0.435)
    wraa: float | None = Field(None, example=42.5)
    wrc_plus: float | None = Field(None, example=165.2)
    ops_plus: int | None = Field(None, example=158)
    fip: float | None = Field(None, example=None)
    lob_pct: float | None = Field(None, example=0.76)
    batting_war: float | None = Field(None, example=6.12)
    pitching_war: float | None = Field(None, example=None)
    war: float | None = Field(None, example=6.85)
    babip: float | None = Field(None, example=0.362)
    isop: float | None = Field(None, example=0.260)


# --- WPA & Game Momentum Schemas ---


class WpaTimelineItemSchema(BaseModel):
    """Schema for a single play in Win Expectancy time-series."""

    event_seq: int = Field(..., example=1)
    inning: int = Field(..., example=1)
    inning_half: str = Field(..., example="top")
    batter_name: str | None = Field(None, example="박찬호")
    pitcher_name: str | None = Field(None, example="켈리")
    description: str = Field(..., example="좌전 안타")
    home_win_prob: float = Field(..., example=0.485)
    wpa: float = Field(..., example=0.032)
    home_score: int = Field(..., example=0)
    away_score: int = Field(..., example=0)


class WpaTurningPointSchema(BaseModel):
    """Schema for key momentum shifting play."""

    event_seq: int = Field(..., example=42)
    inning: int = Field(..., example=7)
    inning_half: str = Field(..., example="bottom")
    description: str = Field(..., example="김도영 역전 3점 홈런")
    batter_name: str | None = Field(None, example="김도영")
    wpa: float = Field(..., example=0.385)
    importance_score: float = Field(..., example=0.385)
    impact_type: str = Field(..., example="GAME_CHANGER")


class WpaChartResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/wpa."""

    game_id: str = Field(..., example="20260809LGKIA0")
    game_date: str = Field(..., example="2026-08-09")
    stadium: str = Field(..., example="광주")
    home_team: str = Field(..., example="KIA")
    away_team: str = Field(..., example="LG")
    home_score: int = Field(..., example=6)
    away_score: int = Field(..., example=3)
    game_status: str = Field(..., example="FINAL")
    timeline: list[WpaTimelineItemSchema] = Field(default_factory=list)
    turning_points: list[WpaTurningPointSchema] = Field(default_factory=list)
    home_total_wpa: float = Field(0.0, example=0.452)
    away_total_wpa: float = Field(0.0, example=-0.452)


class GameHighlightsResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/highlights."""

    game_id: str = Field(..., example="20260809LGKIA0")
    count: int = Field(..., example=1)
    highlights: list[GameHighlightItemSchema] = Field(default_factory=list)
