"""Pydantic schemas and response models for FastAPI REST API documentation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class NoticeItemSchema(BaseModel):
    """Schema for single KBO press release item."""

    id: int = Field(..., examples=[1])
    notice_id: str = Field(..., examples=["100"])
    category: str | None = Field(None, examples=["공시/공지"])
    title: str = Field(..., examples=["KBO 리그 경기일정 변경 안내"])
    published_date: str | None = Field(None, examples=["2026-08-09"])
    source_url: str | None = Field(None, examples=["https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100"])
    created_at: str | None = Field(None, examples=["2026-08-09T14:00:00"])


class NoticesListResponse(BaseModel):
    """Response schema for GET /api/v1/notices."""

    page: int = Field(..., examples=[1])
    limit: int = Field(..., examples=[20])
    count: int = Field(..., examples=[1])
    notices: list[NoticeItemSchema]


class MilestoneItemSchema(BaseModel):
    """Schema for single player milestone item."""

    id: int = Field(..., examples=[1])
    season: int = Field(..., examples=[2026])
    player_id: str = Field(..., examples=["78224"])
    player_name: str = Field(..., examples=["최형우"])
    team_code: str | None = Field(None, examples=["KIA"])
    milestone_category: str = Field(..., examples=["1600타점"])
    current_val: int = Field(..., examples=[1598])
    target_val: int = Field(..., examples=[1600])
    remaining_val: int = Field(..., examples=[2])
    is_achieved: bool = Field(..., examples=[False])
    achieved_date: str | None = Field(None, examples=[None])


class MilestonesListResponse(BaseModel):
    """Response schema for GET /api/v1/milestones."""

    season: int = Field(..., examples=[2026])
    count: int = Field(..., examples=[1])
    milestones: list[MilestoneItemSchema]


class FuturesScheduleItemSchema(BaseModel):
    """Schema for single Futures League schedule item."""

    game_id: str = Field(..., examples=["F20260809"])
    season: int = Field(..., examples=[2026])
    game_date: str | None = Field(None, examples=["2026-08-09"])
    away_team: str = Field(..., examples=["고양"])
    home_team: str = Field(..., examples=["한화"])
    away_score: int | None = Field(None, examples=[5])
    home_score: int | None = Field(None, examples=[3])
    stadium: str | None = Field(None, examples=["이천"])
    game_status: str = Field(..., examples=["COMPLETED"])
    cancel_reason: str | None = Field(None, examples=[None])


class FuturesScheduleResponse(BaseModel):
    """Response schema for GET /api/v1/futures/schedule."""

    season: int = Field(..., examples=[2026])
    count: int = Field(..., examples=[1])
    schedules: list[FuturesScheduleItemSchema]


class PlayerSplitItemSchema(BaseModel):
    """Schema for single player situational split item."""

    season: int = Field(..., examples=[2026])
    player_id: str = Field(..., examples=["78224"])
    player_name: str = Field(..., examples=["김도영"])
    team_code: str | None = Field(None, examples=["KIA"])
    split_type: str = Field(..., examples=["scoring_position"])
    split_key: str = Field(..., examples=["득점권시"])
    ab: int | None = Field(None, examples=[80])
    hits: int | None = Field(None, examples=[30])
    hr: int | None = Field(None, examples=[8])
    rbi: int | None = Field(None, examples=[25])
    bb: int | None = Field(None, examples=[12])
    so: int | None = Field(None, examples=[15])
    avg: float | None = Field(None, examples=[0.375])
    obp: float | None = Field(None, examples=[0.450])
    slg: float | None = Field(None, examples=[0.650])
    ops: float | None = Field(None, examples=[1.100])


class PlayerSplitsResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/splits."""

    player_id: str = Field(..., examples=["78224"])
    season: int = Field(..., examples=[2026])
    count: int = Field(..., examples=[1])
    splits: list[PlayerSplitItemSchema]


class HybridSearchResultItemSchema(BaseModel):
    """Schema for single hybrid search result chunk."""

    chunk_id: str = Field(..., examples=["12"])
    title: str | None = Field(None, examples=["KBO 공시 - 올스타전 라인업 발표"])
    content: str = Field(..., examples=["[2026-08-09] KBO 공식 공시: KBO 올스타전 라인업 발표..."])
    source_url: str | None = Field(None, examples=["https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100"])
    category: str = Field("general", examples=["press_release"])
    score: float = Field(..., examples=[0.0328])
    vector_rank: int | None = Field(None, examples=[1])
    bm25_rank: int | None = Field(None, examples=[2])
    meta: dict[str, Any] | None = Field(None, examples=[{"category": "press_release", "notice_id": "100"}])
    provenance: dict[str, Any] | None = Field(None)


class HybridSearchResponse(BaseModel):
    """Response schema for POST /api/v1/rag/hybrid-search."""

    query: str = Field(..., examples=["올스타전 경기일정"])
    total_results: int = Field(..., examples=[1])
    results: list[HybridSearchResultItemSchema]
    retrieval: dict[str, Any] = Field(default_factory=dict)


class RagSourceSchema(BaseModel):
    """Schema for a source cited by the RAG Q&A endpoint."""

    title: str | None = Field(None, examples=["KBO 공시 - 올스타전 라인업 발표"])
    source_url: str | None = Field(None, examples=["https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=100"])
    document_type: str = Field("general", examples=["press_release"])
    snippet: str = Field(..., examples=["KBO 올스타전 라인업 발표..."])
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

    stadium_code: str = Field(..., examples=["잠실"])
    stadium_name: str = Field(..., examples=["서울종합운동장 야구장 (잠실)"])
    home_teams: list[str] = Field(..., examples=[["LG", "DB"]])
    capacity: int | None = Field(None, examples=[23750])
    city: str | None = Field(None, examples=["서울"])
    address: str | None = Field(None, examples=["서울특별시 송파구 올림픽로 25"])


class StadiumParkingSchema(BaseModel):
    """Schema for stadium parking facility."""

    name: str = Field(..., examples=["잠실종합운동장 부설주차장"])
    fee_type: str | None = Field(None, examples=["유료"])
    capacity: int | None = Field(None, examples=[1200])
    tip: str | None = Field(None, examples=["경기 시작 2시간 전 만차 예상"])
    address: str | None = Field(None, examples=["서울특별시 송파구"])
    walking_minutes: int | None = Field(None, examples=[10])
    is_event_day_available: bool = Field(default=True, examples=[True])
    reservation_required: bool = Field(default=False, examples=[False])
    operating_hours: str | None = Field(None, examples=["08:00-23:00"])


class StadiumFoodSchema(BaseModel):
    """Schema for stadium food vendor/menu."""

    vendor_name: str = Field(..., examples=["원샷치킨"])
    location: str | None = Field(None, examples=["1루 내야 2층 복도"])
    popular_menu: str | None = Field(None, examples=["커리원샷치킨 (12,000원)"])
    category: str | None = Field(None, examples=["치킨/스낵"])
    floor_level: str | None = Field(None, examples=["2F"])
    base_side: str | None = Field(None, examples=["first_base"])
    gate_info: str | None = Field(None, examples=["1루 게이트"])
    order_method: str | None = Field(None, examples=["onsite"])
    confidence: str | None = Field(None, examples=["high"])


class StadiumSeatSectionSchema(BaseModel):
    """Schema for stadium seating section."""

    section_name: str = Field(..., examples=["1루 테이블석"])
    section_code: str | None = Field(None, examples=["101B"])
    seat_grade: str | None = Field(None, examples=["테이블석"])
    weekday_price: int | None = Field(None, examples=[43000])
    weekend_price: int | None = Field(None, examples=[48000])
    description: str | None = Field(None, examples=["테이블 구비, 최적의 시야 제공"])
    base_side: str | None = Field(None, examples=["first_base"])
    floor_level: str | None = Field(None, examples=["2F"])
    gate_info: str | None = Field(None, examples=["1루 게이트"])
    seat_map_url: str | None = Field(None, examples=["https://example.com/seat-map.png"])


class StadiumTicketPriceSchema(BaseModel):
    """Schema for a stadium ticket price."""

    team_id: str = Field(..., examples=["LG"])
    season: int = Field(..., examples=[2026])
    seat_grade: str = Field(..., examples=["테이블석"])
    day_type: str = Field(..., examples=["weekday"])
    audience_type: str | None = Field(None, examples=["general"])
    price: int = Field(..., examples=[43000])
    currency: str = Field("KRW", examples=["KRW"])
    source_url: str | None = Field(None, examples=["https://ticket.example.com"])


class StadiumTicketScheduleSchema(BaseModel):
    """Schema for a stadium ticket reservation schedule."""

    game_date: str = Field(..., examples=["2026-08-15"])
    home_team: str = Field(..., examples=["LG"])
    away_team: str = Field(..., examples=["KIA"])
    stadium: str = Field(..., examples=["잠실"])
    open_time: str = Field(..., examples=["2026-08-08T11:00:00"])
    platform: str = Field(..., examples=["Ticketlink"])
    url: str | None = Field(None, examples=["https://ticket.example.com"])


class StadiumTicketOpenRuleSchema(BaseModel):
    """Schema for a team's recurring ticket opening rule."""

    team_id: str = Field(..., examples=["LG"])
    platform: str = Field(..., examples=["Ticketlink"])
    open_offset_days: int = Field(..., examples=[7])
    open_time: str = Field(..., examples=["11:00:00"])
    sales_close_rule: str | None = Field(None, examples=["경기 시작 2시간 전"])
    max_tickets_per_user: int | None = Field(None, examples=[4])
    fee_rule: str | None = Field(None, examples=["예매 수수료 별도"])
    cancel_rule: str | None = Field(None, examples=["경기 전날까지 취소 가능"])
    note: str | None = Field(None, examples=["구단 정책에 따라 변경될 수 있음"])


class StadiumFacilitiesResponse(BaseModel):
    """Response schema for GET /api/v1/stadiums/{stadium_code}/facilities."""

    stadium_code: str = Field(..., examples=["잠실"])
    stadium_name: str = Field(..., examples=["잠실야구장"])
    home_teams: list[str] = Field(default_factory=list, examples=[["LG", "DB"]])
    parkings: list[StadiumParkingSchema] = Field(default_factory=list)
    food_vendors: list[StadiumFoodSchema] = Field(default_factory=list)
    seat_sections: list[StadiumSeatSectionSchema] = Field(default_factory=list)
    ticket_prices: list[StadiumTicketPriceSchema] = Field(default_factory=list)
    ticket_schedules: list[StadiumTicketScheduleSchema] = Field(default_factory=list)
    ticket_open_rules: list[StadiumTicketOpenRuleSchema] = Field(default_factory=list)


# --- Game Boxscore & Head-to-Head Schemas ---


class HitterBoxscoreSchema(BaseModel):
    """Schema for single hitter line in a game boxscore."""

    order: int | None = Field(None, examples=[1])
    player_id: str | None = Field(None, examples=["78224"])
    player_name: str = Field(..., examples=["김도영"])
    position: str | None = Field(None, examples=["3루수"])
    ab: int = Field(0, examples=[4])
    r: int = Field(0, examples=[2])
    h: int = Field(0, examples=[2])
    rbi: int = Field(0, examples=[3])
    bb: int = Field(0, examples=[1])
    so: int = Field(0, examples=[0])
    avg: float | None = Field(None, examples=[0.360])


class PitcherBoxscoreSchema(BaseModel):
    """Schema for single pitcher line in a game boxscore."""

    order: int | None = Field(None, examples=[1])
    player_id: str | None = Field(None, examples=["60181"])
    player_name: str = Field(..., examples=["양현종"])
    decision: str | None = Field(None, examples=["승"])
    innings: str | None = Field(None, examples=["6.0"])
    h: int = Field(0, examples=[5])
    r: int = Field(0, examples=[2])
    er: int = Field(0, examples=[2])
    bb: int = Field(0, examples=[2])
    so: int = Field(0, examples=[7])
    hr: int = Field(0, examples=[1])
    era: float | None = Field(None, examples=[3.15])


class InningScoreSchema(BaseModel):
    """Schema for single inning scoreboard line."""

    team: str = Field(..., examples=["KIA"])
    scores: list[str | int] = Field(..., examples=[[0, 1, 0, 3, 0, 0, 2, 0, 0]])
    r: int = Field(..., examples=[6])
    h: int = Field(..., examples=[10])
    e: int = Field(..., examples=[0])
    b: int = Field(..., examples=[4])


class GameLineupPlayerSchema(BaseModel):
    """Schema for a player in the game lineup snapshot."""

    order: int | None = Field(None, examples=[1])
    player_id: str | None = Field(None, examples=["78224"])
    player_name: str = Field(..., examples=["김도영"])
    position: str | None = Field(None, examples=["3루수"])
    is_starter: bool = Field(default=False, examples=[True])


class GameHighlightItemSchema(BaseModel):
    """Schema for game highlight moment."""

    id: int = Field(..., examples=[1])
    game_id: str = Field(..., examples=["20260809LGKIA0"])
    event_seq: int | None = Field(None, examples=[42])
    inning: int | None = Field(None, examples=[7])
    inning_half: str | None = Field(None, examples=["bottom"])
    highlight_type: str = Field(..., examples=["LEAD_CHANGE"])
    description: str = Field(..., examples=["김도영 역전 쓰리런 홈런"])
    wpa: float | None = Field(None, examples=[0.385])
    importance_score: float = Field(0.0, examples=[0.95])
    tags: list[str] = Field(default_factory=list, examples=[["홈런", "역전", "결승타"]])


class GameBoxscoreResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/boxscore."""

    game_id: str = Field(..., examples=["20260809LGKIA0"])
    game_date: str = Field(..., examples=["2026-08-09"])
    stadium: str = Field(..., examples=["광주"])
    home_team: str = Field(..., examples=["KIA"])
    away_team: str = Field(..., examples=["LG"])
    home_score: int = Field(..., examples=[6])
    away_score: int = Field(..., examples=[3])
    game_status: str = Field(..., examples=["FINAL"])
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

    game_id: str = Field(..., examples=["20260809LGKIA0"])
    game_date: str = Field(..., examples=["2026-08-09"])
    home_team: str = Field(..., examples=["KIA"])
    away_team: str = Field(..., examples=["LG"])
    home_score: int = Field(..., examples=[6])
    away_score: int = Field(..., examples=[3])
    winner: str | None = Field(None, examples=["KIA"])


class HeadToHeadResponse(BaseModel):
    """Response schema for GET /api/v1/games/head-to-head."""

    team1: str = Field(..., examples=["KIA"])
    team2: str = Field(..., examples=["LG"])
    season: int | None = Field(None, examples=[2026])
    team1_wins: int = Field(0, examples=[8])
    team2_wins: int = Field(0, examples=[5])
    draws: int = Field(0, examples=[1])
    total_games: int = Field(0, examples=[14])
    team1_avg_runs: float = Field(0.0, examples=[5.4])
    team2_avg_runs: float = Field(0.0, examples=[4.1])
    recent_games: list[HeadToHeadGameItemSchema] = Field(default_factory=list)


# --- Player Season Stats & Sabermetrics Schemas ---


class PlayerBattingSeasonSchema(BaseModel):
    """Schema for player batting season statistics."""

    season: int = Field(..., examples=[2026])
    team_code: str | None = Field(None, examples=["KIA"])
    g: int = Field(0, examples=[110])
    pa: int = Field(0, examples=[480])
    ab: int = Field(0, examples=[420])
    r: int = Field(0, examples=[88])
    h: int = Field(0, examples=[145])
    two_b: int = Field(0, examples=[28])
    three_b: int = Field(0, examples=[3])
    hr: int = Field(0, examples=[25])
    rbi: int = Field(0, examples=[85])
    sb: int = Field(0, examples=[30])
    cs: int = Field(0, examples=[4])
    bb: int = Field(0, examples=[52])
    so: int = Field(0, examples=[70])
    avg: float | None = Field(None, examples=[0.345])
    obp: float | None = Field(None, examples=[0.420])
    slg: float | None = Field(None, examples=[0.605])
    ops: float | None = Field(None, examples=[1.025])


class PlayerPitchingSeasonSchema(BaseModel):
    """Schema for player pitching season statistics."""

    season: int = Field(..., examples=[2026])
    team_code: str | None = Field(None, examples=["KIA"])
    g: int = Field(0, examples=[25])
    w: int = Field(0, examples=[12])
    losses: int = Field(0, examples=[4])
    sv: int = Field(0, examples=[0])
    hld: int = Field(0, examples=[0])
    ip: float | None = Field(None, examples=[150.0])
    h: int = Field(0, examples=[135])
    r: int = Field(0, examples=[58])
    er: int = Field(0, examples=[52])
    bb: int = Field(0, examples=[40])
    so: int = Field(0, examples=[140])
    hr: int = Field(0, examples=[10])
    era: float | None = Field(None, examples=[3.12])
    whip: float | None = Field(None, examples=[1.17])


class PlayerSeasonStatResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/stats."""

    player_id: str = Field(..., examples=["78224"])
    player_name: str = Field(..., examples=["김도영"])
    position: str | None = Field(None, examples=["내야수"])
    team: str | None = Field(None, examples=["KIA"])
    batting_seasons: list[PlayerBattingSeasonSchema] = Field(default_factory=list)
    pitching_seasons: list[PlayerPitchingSeasonSchema] = Field(default_factory=list)


class PlayerSabermetricsResponse(BaseModel):
    """Response schema for GET /api/v1/players/{player_id}/sabermetrics."""

    player_id: str = Field(..., examples=["78224"])
    player_name: str = Field(..., examples=["김도영"])
    season: int = Field(..., examples=[2026])
    woba: float | None = Field(None, examples=[0.435])
    wraa: float | None = Field(None, examples=[42.5])
    wrc_plus: float | None = Field(None, examples=[165.2])
    ops_plus: int | None = Field(None, examples=[158])
    fip: float | None = Field(None, examples=[None])
    lob_pct: float | None = Field(None, examples=[0.76])
    batting_war: float | None = Field(None, examples=[6.12])
    pitching_war: float | None = Field(None, examples=[None])
    war: float | None = Field(None, examples=[6.85])
    babip: float | None = Field(None, examples=[0.362])
    isop: float | None = Field(None, examples=[0.260])


# --- WPA & Game Momentum Schemas ---


class WpaTimelineItemSchema(BaseModel):
    """Schema for a single play in Win Expectancy time-series."""

    event_seq: int = Field(..., examples=[1])
    inning: int = Field(..., examples=[1])
    inning_half: str = Field(..., examples=["top"])
    batter_name: str | None = Field(None, examples=["박찬호"])
    pitcher_name: str | None = Field(None, examples=["켈리"])
    description: str = Field(..., examples=["좌전 안타"])
    home_win_prob: float = Field(..., examples=[0.485])
    wpa: float = Field(..., examples=[0.032])
    home_score: int = Field(..., examples=[0])
    away_score: int = Field(..., examples=[0])


class WpaTurningPointSchema(BaseModel):
    """Schema for key momentum shifting play."""

    event_seq: int = Field(..., examples=[42])
    inning: int = Field(..., examples=[7])
    inning_half: str = Field(..., examples=["bottom"])
    description: str = Field(..., examples=["김도영 역전 3점 홈런"])
    batter_name: str | None = Field(None, examples=["김도영"])
    wpa: float = Field(..., examples=[0.385])
    importance_score: float = Field(..., examples=[0.385])
    impact_type: str = Field(..., examples=["GAME_CHANGER"])


class WpaChartResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/wpa."""

    game_id: str = Field(..., examples=["20260809LGKIA0"])
    game_date: str = Field(..., examples=["2026-08-09"])
    stadium: str = Field(..., examples=["광주"])
    home_team: str = Field(..., examples=["KIA"])
    away_team: str = Field(..., examples=["LG"])
    home_score: int = Field(..., examples=[6])
    away_score: int = Field(..., examples=[3])
    game_status: str = Field(..., examples=["FINAL"])
    timeline: list[WpaTimelineItemSchema] = Field(default_factory=list)
    turning_points: list[WpaTurningPointSchema] = Field(default_factory=list)
    home_total_wpa: float = Field(0.0, examples=[0.452])
    away_total_wpa: float = Field(0.0, examples=[-0.452])


class GameHighlightsResponse(BaseModel):
    """Response schema for GET /api/v1/games/{game_id}/highlights."""

    game_id: str = Field(..., examples=["20260809LGKIA0"])
    count: int = Field(..., examples=[1])
    highlights: list[GameHighlightItemSchema] = Field(default_factory=list)


# --- Analytics & Sabermetrics Schemas ---


class LeagueConstantsResponse(BaseModel):
    """Response schema for GET /api/analytics/constants."""

    year: int = Field(..., examples=[2025])
    level: str = Field("KBO1", examples=["KBO1"])
    woba_scale: float = Field(..., examples=[1.25])
    w_bb: float = Field(..., examples=[0.69])
    w_hbp: float = Field(..., examples=[0.72])
    w_1b: float = Field(..., examples=[0.89])
    w_2b: float = Field(..., examples=[1.27])
    w_3b: float = Field(..., examples=[1.62])
    w_hr: float = Field(..., examples=[2.10])
    league_woba: float = Field(..., examples=[0.330])
    league_era: float = Field(..., examples=[4.20])
    fip_constant: float = Field(..., examples=[3.80])
    runs_per_win: float = Field(..., examples=[10.0])


class BattingSabermetricsResponse(BaseModel):
    """Response schema for GET /api/analytics/batting."""

    player_id: int = Field(..., examples=[78224])
    season: int = Field(..., examples=[2025])
    plate_appearances: int = Field(..., examples=[500])
    at_bats: int = Field(..., examples=[420])
    hits: int = Field(..., examples=[130])
    woba: float = Field(..., examples=[0.395])
    wraa: float = Field(..., examples=[22.5])
    wrc: float = Field(..., examples=[82.5])
    wrc_plus: float = Field(..., examples=[138.4])
    babip: float = Field(..., examples=[0.320])
    iso: float = Field(..., examples=[0.210])
    ops_plus: float = Field(..., examples=[138.4])
    war: float = Field(..., examples=[4.85])
    bb_pct: float = Field(..., examples=[12.5])
    k_pct: float = Field(..., examples=[15.0])


class PitchingSabermetricsResponse(BaseModel):
    """Response schema for GET /api/analytics/pitching."""

    player_id: int = Field(..., examples=[61234])
    season: int = Field(..., examples=[2025])
    innings_pitched: float = Field(..., examples=[165.1])
    earned_runs: int = Field(..., examples=[55])
    era: float = Field(..., examples=[3.0])
    fip: float = Field(..., examples=[3.25])
    kfip: float = Field(..., examples=[3.30])
    whip: float = Field(..., examples=[1.15])
    era_plus: float = Field(..., examples=[140.0])
    fip_minus: float = Field(..., examples=[77.4])
    babip: float = Field(..., examples=[0.285])
    k_per_9: float = Field(..., examples=[8.5])
    bb_per_9: float = Field(..., examples=[2.1])
    hr_per_9: float = Field(..., examples=[0.6])
    war: float = Field(..., examples=[4.20])


class MatchupBvpResponse(BaseModel):
    """Response schema for GET /api/analytics/matchup/bvp."""

    batter_id: int = Field(..., examples=[78224])
    pitcher_id: int = Field(..., examples=[61234])
    plate_appearances: int = Field(..., examples=[15])
    at_bats: int = Field(..., examples=[12])
    hits: int = Field(..., examples=[4])
    doubles: int = Field(..., examples=[1])
    triples: int = Field(..., examples=[0])
    home_runs: int = Field(..., examples=[1])
    walks: int = Field(..., examples=[3])
    strikeouts: int = Field(..., examples=[2])
    hbp: int = Field(..., examples=[0])
    avg: float = Field(..., examples=[0.333])
    obp: float = Field(..., examples=[0.467])
    slg: float = Field(..., examples=[0.667])
    ops: float = Field(..., examples=[1.134])


class SplitMetricsResponse(BaseModel):
    """Response schema for GET /api/analytics/splits."""

    category: str = Field(..., examples=["risp"])
    entity_id: int = Field(..., examples=[78224])
    season: int = Field(..., examples=[2025])
    split_key: str = Field(..., examples=["RISP"])
    sample_size: int = Field(..., examples=[110])
    stats: dict[str, Any] = Field(default_factory=dict)


# --- Pipeline & Quality Schemas ---


class PipelineDefectItemSchema(BaseModel):
    """Schema for individual pipeline defect item."""

    game_id: str = Field(..., examples=["20250615LGSS0"])
    defect_type: str = Field(..., examples=["STUCK_SCHEDULED"])
    severity: str = Field(..., examples=["ERROR"])
    description: str = Field(..., examples=["Game is stuck in SCHEDULED status"])
    details: dict[str, Any] = Field(default_factory=dict)


class PipelineDefectReportResponse(BaseModel):
    """Response schema for GET /api/pipeline/defects."""

    target_date: str = Field(..., examples=["2025-06-15"])
    total_defects: int = Field(..., examples=[2])
    summary_by_type: dict[str, int] = Field(default_factory=dict)
    timestamp: str = Field(..., examples=["2025-06-16T04:00:00"])
    defects: list[PipelineDefectItemSchema] = Field(default_factory=list)


class PipelineHealingActionResponse(BaseModel):
    """Response schema for POST /api/pipeline/heal."""

    game_id: str = Field(..., examples=["20250615LGSS0"])
    action_taken: str = Field(..., examples=["update_status_to_COMPLETED"])
    status: str = Field(..., examples=["SUCCESS"])
    error_message: str | None = Field(None, examples=[None])
    elapsed_seconds: float = Field(..., examples=[0.05])
    details: dict[str, Any] = Field(default_factory=dict)


class PipelineStageResultSchema(BaseModel):
    """Schema for single stage result in a pipeline run."""

    stage_name: str = Field(..., examples=["Stage 1: Finalize"])
    status: str = Field(..., examples=["SUCCESS"])
    duration_seconds: float = Field(..., examples=[1.5])
    metrics: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)


class PipelineRunResponse(BaseModel):
    """Response schema for POST /api/pipeline/run."""

    run_id: str = Field(..., examples=["pipeline_20250615_abc123"])
    target_date: str = Field(..., examples=["2025-06-15"])
    overall_status: str = Field(..., examples=["SUCCESS"])
    total_duration_seconds: float = Field(..., examples=[4.2])
    timestamp: str = Field(..., examples=["2025-06-16T04:00:00"])
    stages: list[PipelineStageResultSchema] = Field(default_factory=list)
    healed_defects: list[PipelineHealingActionResponse] = Field(default_factory=list)


class QualityHubSummaryResponse(BaseModel):
    """Response schema for GET /api/pipeline/quality."""

    timestamp: str = Field(..., examples=["2025-06-16T04:00:00"])
    overall_status: str = Field(..., examples=["PASS"])
    quality_score: int = Field(..., examples=[98])
    remediation_hints: list[str] = Field(default_factory=list)
    pa_formula: dict[str, Any] | None = None
    team_stats: dict[str, Any] | None = None
    freshness: dict[str, Any] | None = None
    gaps: dict[str, Any] | None = None
    invariants: dict[str, Any] | None = None


# --- RAG Evaluation Schemas ---


class RagEvaluateRequest(BaseModel):
    """Request schema for POST /api/rag/evaluate."""

    query: str = Field(..., examples=["최형우 1500타점 달성"])
    retrieved_chunk_ids: list[str] = Field(..., examples=[["chunk_1", "chunk_2", "chunk_3"]])
    golden_relevant_chunk_ids: list[str] = Field(..., examples=[["chunk_1"]])
    k: int = Field(5, ge=1, le=50, examples=[5])


class RagEvaluateResponse(BaseModel):
    """Response schema for POST /api/rag/evaluate."""

    query: str = Field(..., examples=["최형우 1500타점 달성"])
    k: int = Field(5, examples=[5])
    precision_at_k: float = Field(..., examples=[0.2])
    recall_at_k: float = Field(..., examples=[1.0])
    mrr: float = Field(..., examples=[1.0])
    ndcg: float = Field(..., examples=[1.0])
    hit_rate: float = Field(..., examples=[1.0])
