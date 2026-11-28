"""Structured KBO retrieval for facts that should not depend on text similarity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, or_, select

from src.models.award import Award
from src.models.franchise import Franchise
from src.models.game import Game
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.standings import TeamStandingsDaily
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.services.query_router import STRUCTURED_SOURCE_TABLES, QueryIntent, QueryPlan

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.services.hybrid_retriever import HybridSearchResult


@dataclass(frozen=True)
class StructuredSearchResult:
    """Represent one authoritative row returned from a structured table."""

    chunk_id: str
    title: str
    content: str
    source_table: str
    source_row_id: str
    category: str
    score: float
    meta: dict[str, Any]

    def to_hybrid_result(self) -> HybridSearchResult:
        """Adapt a structured row to the existing API result contract."""
        from src.services.hybrid_retriever import HybridSearchResult

        return HybridSearchResult(
            chunk_id=self.chunk_id,
            title=self.title,
            content=self.content,
            source_url=None,
            category=self.category,
            vector_rank=None,
            bm25_rank=None,
            rrf_score=self.score,
            meta={"structured": True, "source_table": self.source_table, **self.meta},
            provenance={
                "type": "structured_db",
                "table": self.source_table,
                "record_key": f"{self.source_table}:{self.source_row_id}",
                **self.meta,
            },
        )


_BATTING_METRICS = {
    "avg": ("타율", "avg"),
    "obp": ("출루율", "obp"),
    "slg": ("장타율", "slg"),
    "ops": ("OPS", "ops"),
    "home_runs": ("홈런", "home_runs"),
    "hits": ("안타", "hits"),
    "rbi": ("타점", "rbi"),
    "stolen_bases": ("도루", "stolen_bases"),
}
_PITCHING_METRICS = {
    "era": ("방어율", "era"),
    "wins": ("승", "wins"),
    "saves": ("세이브", "saves"),
    "holds": ("홀드", "holds"),
    "innings_pitched": ("이닝", "innings_pitched"),
}
#: Longer names first so `올스타전MVP` is not read as plain `MVP`.
_AWARD_TYPES = (
    ("올스타전mvp", "올스타전MVP"),
    ("한국시리즈mvp", "한국시리즈MVP"),
    ("골든글러브", "골든글러브"),
    ("신인상", "신인상"),
    ("수비상", "수비상"),
    ("mvp", "MVP"),
)
_AWARD_CATEGORIES = (
    "투수",
    "포수",
    "1루수",
    "2루수",
    "3루수",
    "유격수",
    "좌익수",
    "중견수",
    "우익수",
    "지명타자",
)


class StructuredRetriever:
    """Retrieve player facts and standings directly from relational tables."""

    def __init__(self, session: Session) -> None:
        """Initialize the structured retriever with a database session."""
        self.session = session

    def retrieve(self, plan: QueryPlan, top_k: int = 5) -> list[StructuredSearchResult]:
        """Retrieve rows matching the structured portion of a query plan."""
        source_table = plan.filters.get("source_table")
        if source_table in STRUCTURED_SOURCE_TABLES:
            return self._by_source_table(plan, source_table, top_k)
        if plan.stat_type == "rank" and plan.entities.extracted.season_year:
            return self._standings(plan, top_k)
        if plan.entities.player_id and plan.intent in {
            QueryIntent.STAT_QUERY,
            QueryIntent.ENTITY_LOOKUP,
            QueryIntent.MIXED_QUERY,
        }:
            if plan.stat_type in _PITCHING_METRICS:
                return self._pitching(plan, top_k)
            if plan.intent is QueryIntent.ENTITY_LOOKUP and not plan.stat_type:
                return self._player_profile(plan)
            return self._batting(plan, top_k)
        return []

    def _by_source_table(self, plan: QueryPlan, source_table: str, top_k: int) -> list[StructuredSearchResult]:
        """Dispatch an explicit source filter to its relational table."""
        if source_table == "teams":
            return self._teams(plan, top_k)
        if source_table == "awards":
            return self._awards(plan, top_k)
        if source_table == "team_history":
            return self._team_history(plan, top_k)
        if source_table == "game":
            return self._game(plan, top_k)
        return []

    def _team_codes(self, plan: QueryPlan) -> list[str]:
        """Resolve team codes from entities, team names, and franchise names in the query."""
        codes: list[str] = []
        explicit = plan.entities.extracted.team_id or plan.filters.get("team_id")
        if explicit:
            codes.append(str(explicit))
        lowered = plan.query.lower()
        codes.extend(
            team.team_id
            for team in self.session.execute(select(Team)).scalars()
            if any(
                candidate and str(candidate).lower() in lowered
                for candidate in (team.team_id, team.team_name, team.team_short_name)
            )
        )
        for franchise in self.session.execute(select(Franchise)).scalars():
            if franchise.name and franchise.name.lower() in lowered:
                codes.extend((franchise.current_code, franchise.original_code))
        return list(dict.fromkeys(codes))

    def _teams(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return canonical team rows for a ballpark or hometown question."""
        codes = self._team_codes(plan)
        if not codes:
            return []
        rows = list(self.session.execute(select(Team).where(Team.team_id.in_(codes))).scalars())[:top_k]
        return [
            StructuredSearchResult(
                chunk_id=f"structured:teams:{team.team_id}",
                title=f"{team.team_name} 팀 정보",
                content=(
                    f"{team.team_name} ({team.team_id}) 구조화 팀 정보: "
                    f"연고지 {team.city}, 홈구장 {team.stadium_name or '미상'}"
                ),
                source_table="teams",
                source_row_id=team.team_id,
                category="structured_entity",
                score=1.0,
                meta={"team_id": team.team_id, "city": team.city, "stadium": team.stadium_name},
            )
            for team in rows
        ]

    def _awards(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return award rows resolved by year, type, position, and winner."""
        year = plan.entities.extracted.season_year or plan.filters.get("year") or plan.filters.get("season_year")
        if year is None:
            return []
        stmt = select(Award).where(Award.year == int(year))
        lowered = plan.query.lower()
        award_type = next((canonical for token, canonical in _AWARD_TYPES if token in lowered), None)
        if award_type:
            stmt = stmt.where(Award.award_type == award_type)
        category = next((value for value in _AWARD_CATEGORIES if value in plan.query), None)
        if category:
            stmt = stmt.where(Award.category == category)
        # The entity resolver reads the question word "수상자" as a player name
        # (measured 2026-11-28 on both award golden queries). An award lookup
        # must not inherit that guess; only a caller-supplied filter is trusted.
        player_name = plan.filters.get("player_name")
        if player_name:
            stmt = stmt.where(Award.player_name.ilike(f"%{player_name}%"))
        rows = list(self.session.execute(stmt.order_by(Award.id)).scalars())[:top_k]
        return [
            StructuredSearchResult(
                chunk_id=f"structured:awards:{award.id}",
                title=f"{award.year}년 {award.award_type} 수상자",
                content=(
                    f"{award.year}년 {award.award_type}"
                    f"{f' {award.category}' if award.category else ''} 수상자: "
                    f"{award.player_name} ({award.team_name})"
                ),
                source_table="awards",
                source_row_id=str(award.id),
                category="structured_entity",
                score=1.0,
                meta={
                    "year": award.year,
                    "award_type": award.award_type,
                    "category": award.category,
                    "player_name": award.player_name,
                    "team_name": award.team_name,
                },
            )
            for award in rows
        ]

    def _team_history(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return the season's team identity row for a historical question."""
        season = plan.entities.extracted.season_year
        if season is None:
            return []
        codes = self._team_codes(plan)
        lowered = plan.query.lower()
        rows = list(self.session.execute(select(TeamHistory).where(TeamHistory.season == season)).scalars())
        rows = [row for row in rows if row.team_code in codes or (row.team_name and row.team_name.lower() in lowered)][
            :top_k
        ]
        return [
            StructuredSearchResult(
                chunk_id=f"structured:team_history:{row.id}",
                title=f"{row.season}년 {row.team_name} 팀 이력",
                content=(
                    f"{row.season}년 {row.team_name} 구조화 팀 이력: "
                    f"순위 {row.ranking if row.ranking is not None else '-'}위, "
                    f"구장 {row.stadium or '-'}, 연고지 {row.city or '-'}"
                ),
                source_table="team_history",
                source_row_id=str(row.id),
                category="structured_entity",
                score=1.0,
                meta={
                    "season_year": row.season,
                    "team_id": row.team_code,
                    "ranking": row.ranking,
                    "stadium": row.stadium,
                },
            )
            for row in rows
        ]

    def _game(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return the final game row for a dated matchup question."""
        game_date = plan.entities.extracted.game_date or plan.filters.get("game_date")
        if not game_date:
            return []
        try:
            target_date = date.fromisoformat(str(game_date)[:10])
        except ValueError:
            return []
        stmt = select(Game).where(Game.game_date == target_date, Game.game_status.in_(("COMPLETED", "DRAW")))
        codes = self._team_codes(plan)
        if len(codes) > 1:
            stmt = stmt.where(
                or_(
                    and_(Game.home_team == codes[0], Game.away_team == codes[1]),
                    and_(Game.home_team == codes[1], Game.away_team == codes[0]),
                )
            )
        elif codes:
            stmt = stmt.where(or_(Game.home_team.in_(codes), Game.away_team.in_(codes)))
        rows = list(self.session.execute(stmt.order_by(Game.game_id)).scalars())[:top_k]
        return [
            StructuredSearchResult(
                chunk_id=f"structured:game:{game.game_id}",
                title=f"{game.game_date} {game.away_team} vs {game.home_team} 경기 결과",
                content=(
                    f"{game.game_date} {game.away_team} vs {game.home_team} 경기 결과: "
                    f"{game.away_score if game.away_score is not None else '-'}-"
                    f"{game.home_score if game.home_score is not None else '-'} ({game.game_status})"
                ),
                source_table="game",
                source_row_id=game.game_id,
                category="structured_stat",
                score=1.0,
                meta={
                    "game_id": game.game_id,
                    "game_date": str(game.game_date),
                    "away_team": game.away_team,
                    "home_team": game.home_team,
                    "away_score": game.away_score,
                    "home_score": game.home_score,
                },
            )
            for game in rows
        ]

    def _batting(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return season batting rows for a resolved player."""
        season = plan.entities.extracted.season_year
        if season is None or plan.entities.player_id is None:
            return []
        stmt = (
            select(PlayerSeasonBatting, PlayerBasic)
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .where(
                PlayerSeasonBatting.player_id == int(plan.entities.player_id),
                PlayerSeasonBatting.season == season,
                PlayerSeasonBatting.league == "REGULAR",
                PlayerSeasonBatting.level == "KBO1",
            )
        )
        team_id = plan.entities.extracted.team_id
        if team_id:
            stmt = stmt.where(
                or_(
                    PlayerSeasonBatting.team_code == team_id,
                    PlayerSeasonBatting.canonical_team_code == team_id,
                )
            )
        rows = list(self.session.execute(stmt).all())[:top_k]
        results: list[StructuredSearchResult] = []
        for batting, player in rows:
            content = self._batting_content(player, batting, plan.stat_type)
            results.append(
                StructuredSearchResult(
                    chunk_id=f"structured:player_season_batting:{batting.id}",
                    title=f"{batting.season}년 {player.name} 타격 기록",
                    content=content,
                    source_table="player_season_batting",
                    source_row_id=str(batting.id),
                    category="structured_stat",
                    score=1.0,
                    meta={
                        "player_id": str(batting.player_id),
                        "player_name": player.name,
                        "season_year": batting.season,
                        "team_id": batting.canonical_team_code or batting.team_code,
                        "stat_type": plan.stat_type,
                    },
                )
            )
        return results

    @staticmethod
    def _batting_content(player: PlayerBasic, batting: PlayerSeasonBatting, stat_type: str | None) -> str:
        """Format a batting row with the requested metric and supporting fields."""
        team = batting.canonical_team_code or batting.team_code or "팀 미상"
        values: list[str] = []
        if stat_type in _BATTING_METRICS:
            label, field_name = _BATTING_METRICS[stat_type]
            value = getattr(batting, field_name)
            values.append(f"{label}: {value if value is not None else '-'}")
        else:
            values.extend(
                f"{label}: {getattr(batting, field_name) if getattr(batting, field_name) is not None else '-'}"
                for label, field_name in _BATTING_METRICS.values()
            )
        return f"{batting.season}년 {player.name} ({team}) 구조화 타격 기록: " + ", ".join(values)

    def _pitching(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return season pitching rows for a resolved player."""
        season = plan.entities.extracted.season_year
        if season is None or plan.entities.player_id is None:
            return []
        stmt = (
            select(PlayerSeasonPitching, PlayerBasic)
            .join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id)
            .where(
                PlayerSeasonPitching.player_id == int(plan.entities.player_id),
                PlayerSeasonPitching.season == season,
                PlayerSeasonPitching.league == "REGULAR",
                PlayerSeasonPitching.level == "KBO1",
            )
        )
        team_id = plan.entities.extracted.team_id
        if team_id:
            stmt = stmt.where(
                or_(
                    PlayerSeasonPitching.team_code == team_id,
                    PlayerSeasonPitching.canonical_team_code == team_id,
                )
            )
        rows = list(self.session.execute(stmt).all())[:top_k]
        results: list[StructuredSearchResult] = []
        for pitching, player in rows:
            label, field_name = _PITCHING_METRICS.get(plan.stat_type or "era", ("방어율", "era"))
            value = getattr(pitching, field_name)
            content = (
                f"{pitching.season}년 {player.name} 구조화 투구 기록: {label} {value if value is not None else '-'}, "
                f"승 {pitching.wins or 0}, 패 {pitching.losses or 0}, 삼진 {pitching.strikeouts or 0}"
            )
            results.append(
                StructuredSearchResult(
                    chunk_id=f"structured:player_season_pitching:{pitching.id}",
                    title=f"{pitching.season}년 {player.name} 투구 기록",
                    content=content,
                    source_table="player_season_pitching",
                    source_row_id=str(pitching.id),
                    category="structured_stat",
                    score=1.0,
                    meta={
                        "player_id": str(pitching.player_id),
                        "player_name": player.name,
                        "season_year": pitching.season,
                        "stat_type": plan.stat_type,
                    },
                )
            )
        return results

    def _standings(self, plan: QueryPlan, top_k: int) -> list[StructuredSearchResult]:
        """Return the latest available standings snapshot for a season."""
        season = plan.entities.extracted.season_year
        if season is None:
            return []
        stmt = select(TeamStandingsDaily).where(
            TeamStandingsDaily.standings_date >= date(season, 1, 1),
            TeamStandingsDaily.standings_date <= date(season, 12, 31),
        )
        team_id = plan.entities.extracted.team_id
        if team_id:
            stmt = stmt.where(TeamStandingsDaily.team_code == team_id)
        rows = list(
            self.session.execute(
                stmt.order_by(TeamStandingsDaily.standings_date.desc(), TeamStandingsDaily.rank.asc())
            ).scalars()
        )
        if not rows:
            return []
        latest_date = rows[0].standings_date
        rows = [row for row in rows if row.standings_date == latest_date][:top_k]
        return [
            StructuredSearchResult(
                chunk_id=f"structured:team_standings_daily:{row.id}",
                title=f"{row.standings_date} {row.team_code} 최종 순위",
                content=(
                    f"{row.standings_date} 기준 {row.team_code} 구조화 순위: {row.rank}위, "
                    f"{row.wins}승 {row.losses}패 {row.draws}무, 승률 {row.win_pct:.3f}"
                ),
                source_table="team_standings_daily",
                source_row_id=str(row.id),
                category="structured_standings",
                score=1.0,
                meta={
                    "season_year": season,
                    "team_id": row.team_code,
                    "rank": row.rank,
                    "standings_date": str(row.standings_date),
                },
            )
            for row in rows
        ]

    def _player_profile(self, plan: QueryPlan) -> list[StructuredSearchResult]:
        """Return a canonical player profile row."""
        if plan.entities.player_id is None:
            return []
        player = self.session.get(PlayerBasic, int(plan.entities.player_id))
        if player is None:
            return []
        content = f"선수 프로필: {player.name}, 팀: {player.team or '미상'}, 포지션: {player.position or '미상'}"
        return [
            StructuredSearchResult(
                chunk_id=f"structured:player_basic:{player.player_id}",
                title=f"{player.name} 선수 프로필",
                content=content,
                source_table="player_basic",
                source_row_id=str(player.player_id),
                category="structured_entity",
                score=1.0,
                meta={"player_id": str(player.player_id), "player_name": player.name, "team_id": player.team},
            )
        ]
