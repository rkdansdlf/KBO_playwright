"""KBO 전체 데이터를 청킹하여 pgvector RAG 인덱스를 구축합니다.

SQLite 메인 DB에서 데이터를 읽어 임베딩을 생성하고 pgvector DB에 저장합니다.
챗봇이 KBO 관련 모든 질문에 답할 수 있도록 포괄적인 소스를 커버합니다.

지원 소스:
  players     — 선수 프로필 (player_basic)
  batting     — 타자 시즌 기록 (player_season_batting)
  pitching    — 투수 시즌 기록 (player_season_pitching)
  games       — 경기 결과 (game)
  standings   — 팀 순위 (team_standings_daily, 전체 날짜)
  events      — 구단 이벤트/뉴스 (team_events)
  movements   — 선수 이동/트레이드 (player_movements)
  awards      — KBO 시상 (awards)
  lineups     — 경기별 라인업 (game_lineups)
  rankings    — 시즌별 통계 순위 (stat_rankings)
  teams       — 구단 정보 (teams)
   team_history — 구단 변천사 (team_history)
   highlights  — 경기 하이라이트/명장면 (game_highlights)
   markdown_docs — 일반 로컬 Markdown 문서 (Docs/baseball)
   kbo_definitions — KBO 용어/지표 정의 Markdown 문서
   kbo_regulations — KBO 규정/규칙 Markdown 문서
   all         — 위 소스 전체

사용법:
    python -m src.cli.build_rag_index --source all
    python -m src.cli.build_rag_index --source players --season 2025
    python -m src.cli.build_rag_index --source games --limit 500 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.parsers.text_transformer import TextTransformer
from src.services.markdown_document_loader import load_local_markdown_docs, markdown_source_table

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# 임베딩 배치 크기: API 호출 당 처리할 청크 수 (OpenRouter embeddings는 입력 개수 제한이 넉넉하므로 200으로 증가)
_BATCH_SIZE = 200
# DB 커밋 간격
_COMMIT_EVERY = 100

_VALID_SOURCES = (
    "players",
    "batting",
    "pitching",
    "games",
    "standings",
    "events",
    "movements",
    "awards",
    "lineups",
    "rankings",
    "teams",
    "team_history",
    "highlights",
    "pbp",
    "markdown_docs",
    "kbo_definitions",
    "kbo_regulations",
    "all",
)

# 정규 시즌 리그 코드 (kbo_seasons.league_type_code 0 = 정규시즌)
_REGULAR_SEASON_CODE = 0
# 전 소스 공통 언어
_DEFAULT_LANGUAGE = "ko"
# 순위 청크당 상위 N위까지만 포함
_RANKING_TOP_N = 20


# ─── 청크 생성 함수 ────────────────────────────────────────────────────────────


def _iter_player_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_basic 테이블에서 선수 프로파일 청크를 생성합니다."""
    from src.models.player import PlayerBasic

    query = session.query(PlayerBasic).filter(PlayerBasic.name.is_not(None))
    if limit:
        query = query.limit(limit)

    for player in query.yield_per(200):
        parts = [f"선수: {player.name}"]
        details = [
            ("팀", player.team),
            ("포지션", player.position),
            ("생년월일", player.birth_date),
            ("신장", f"{player.height_cm}cm" if player.height_cm else None),
            ("체중", f"{player.weight_kg}kg" if player.weight_kg else None),
            ("타석", player.bats),
            ("투구", player.throws),
            ("데뷔", f"{player.debut_year}년" if player.debut_year else None),
            ("출신교", player.career),
            ("상태", player.status),
            ("드래프트", player.draft_info),
            ("연봉", player.salary_original),
        ]
        parts.extend(f"{label}: {value}" for label, value in details if value)

        content = ", ".join(parts)
        yield {
            "source_table": "player_basic",
            "source_row_id": str(player.player_id),
            "title": f"{player.name} ({player.team or '무소속'})",
            "content": content,
            "team_id": player.team,
            "player_id": str(player.player_id),
            "season_year": None,
            "document_type": "player_profile",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": "ko",
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"player_id": player.player_id, "position": player.position, "status": player.status},
        }


def _iter_batting_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_season_batting에서 타자 시즌 기록 청크를 생성합니다."""
    from src.models.player import PlayerBasic, PlayerSeasonBatting

    query = session.query(PlayerSeasonBatting, PlayerBasic).join(
        PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id, isouter=True
    )
    if season:
        query = query.filter(PlayerSeasonBatting.season == season)
    query = query.filter(PlayerSeasonBatting.level == "KBO1")
    if limit:
        query = query.limit(limit)

    for batting, player in query.yield_per(200):
        name = player.name if player else f"ID:{batting.player_id}"

        def _fmt(val: float | None, decimals: int = 3) -> str:
            return f"{val:.{decimals}f}" if val is not None else "-"

        parts = [
            f"선수: {name}",
            f"시즌: {batting.season}년",
            f"팀: {batting.team_code or '-'}",
            f"경기: {batting.games or 0}",
            f"타율: {_fmt(batting.avg)}",
            f"출루율: {_fmt(batting.obp)}",
            f"장타율: {_fmt(batting.slg)}",
            f"OPS: {_fmt(batting.ops)}",
            f"안타: {batting.hits or 0}",
            f"홈런: {batting.home_runs or 0}",
            f"타점: {batting.rbi or 0}",
            f"득점: {batting.runs or 0}",
            f"도루: {batting.stolen_bases or 0}",
            f"삼진: {batting.strikeouts or 0}",
            f"볼넷: {batting.walks or 0}",
        ]
        content = f"{name} {batting.season}시즌 타격기록: " + ", ".join(parts)
        yield {
            "source_table": "player_season_batting",
            "source_row_id": f"{batting.player_id}_{batting.season}_{batting.team_code or 'NA'}",
            "title": f"{name} {batting.season}시즌 타격기록",
            "content": content,
            "team_id": batting.team_code,
            "player_id": str(batting.player_id),
            "season_year": batting.season,
            "document_type": "season_batting",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "avg": batting.avg,
                "hr": batting.home_runs,
                "rbi": batting.rbi,
                "ops": batting.ops,
            },
        }


def _iter_pitching_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_season_pitching에서 투수 시즌 기록 청크를 생성합니다."""
    from src.models.player import PlayerBasic, PlayerSeasonPitching

    query = session.query(PlayerSeasonPitching, PlayerBasic).join(
        PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id, isouter=True
    )
    if season:
        query = query.filter(PlayerSeasonPitching.season == season)
    query = query.filter(PlayerSeasonPitching.level == "KBO1")
    if limit:
        query = query.limit(limit)

    for pitching, player in query.yield_per(200):
        name = player.name if player else f"ID:{pitching.player_id}"

        def _fmt(val: float | None, decimals: int = 2) -> str:
            return f"{val:.{decimals}f}" if val is not None else "-"

        parts = [
            f"선수: {name}",
            f"시즌: {pitching.season}년",
            f"팀: {pitching.team_code or '-'}",
            f"경기: {pitching.games or 0}",
            f"선발: {pitching.games_started or 0}",
            f"승: {pitching.wins or 0}",
            f"패: {pitching.losses or 0}",
            f"세이브: {pitching.saves or 0}",
            f"홀드: {pitching.holds or 0}",
            f"ERA: {_fmt(pitching.era)}",
            f"이닝: {_fmt(pitching.innings_pitched)}",
            f"WHIP: {_fmt(pitching.whip)}",
            f"삼진: {pitching.strikeouts or 0}",
            f"볼넷: {pitching.walks_allowed or 0}",
        ]
        content = f"{name} {pitching.season}시즌 투구기록: " + ", ".join(parts)
        yield {
            "source_table": "player_season_pitching",
            "source_row_id": f"{pitching.player_id}_{pitching.season}_{pitching.team_code or 'NA'}",
            "title": f"{name} {pitching.season}시즌 투구기록",
            "content": content,
            "team_id": pitching.team_code,
            "player_id": str(pitching.player_id),
            "season_year": pitching.season,
            "document_type": "season_pitching",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "era": pitching.era,
                "wins": pitching.wins,
                "losses": pitching.losses,
                "saves": pitching.saves,
            },
        }


def _iter_game_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Game 테이블에서 경기 결과 청크를 생성합니다."""
    from src.models.game import Game
    from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

    query = session.query(Game).filter(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
    if season:
        query = query.filter(Game.season_id == season)
    query = query.order_by(Game.game_date.desc())
    if limit:
        query = query.limit(limit)

    for game in query.yield_per(500):
        date_str = str(game.game_date) if game.game_date else "날짜불명"
        home = game.home_team or "?"
        away = game.away_team or "?"
        home_score = game.home_score if game.home_score is not None else "?"
        away_score = game.away_score if game.away_score is not None else "?"
        winner = game.winning_team or "무승부"
        stadium = game.stadium or "-"

        content = f"{date_str} KBO 경기: {away} {away_score} - {home} {home_score} (구장: {stadium}, 결과: {winner} 승)"
        yield {
            "source_table": "game",
            "source_row_id": game.game_id,
            "title": f"{date_str} {away} vs {home}",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": game.season_id,
            "document_type": "game_result",
            "game_date": game.game_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "home_team": home,
                "away_team": away,
                "home_score": game.home_score,
                "away_score": game.away_score,
                "winner": winner,
                "stadium": stadium,
            },
        }


def _iter_standings_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_standings_daily에서 전체 날짜의 팀 순위 청크를 생성합니다."""
    from sqlalchemy import extract, func

    from src.models.standings import TeamStandingsDaily

    sub = session.query(func.max(TeamStandingsDaily.standings_date)).scalar()
    if not sub:
        return

    query = session.query(TeamStandingsDaily)
    if season:
        query = query.filter(extract("year", TeamStandingsDaily.standings_date) == season)
    query = query.order_by(TeamStandingsDaily.standings_date.desc(), TeamStandingsDaily.rank)
    if limit:
        query = query.limit(limit)

    for row in query.all():
        if row.current_streak > 0:
            streak_str = f"{row.current_streak}연승"
        elif row.current_streak < 0:
            streak_str = f"{abs(row.current_streak)}연패"
        else:
            streak_str = "연속없음"
        content = (
            f"{row.standings_date} 기준 {row.team_code} 팀: "
            f"{row.rank}위, {row.games_played}경기 {row.wins}승 {row.losses}패 {row.draws}무, "
            f"승률 {row.win_pct:.3f}, 게차 {row.games_behind}, 현재 {streak_str}, "
            f"득점 {row.runs_scored} 실점 {row.runs_allowed}"
        )
        yield {
            "source_table": "team_standings_daily",
            "source_row_id": f"{row.standings_date}_{row.team_code}",
            "title": f"{row.standings_date} {row.team_code} 순위",
            "content": content,
            "team_id": row.team_code,
            "player_id": None,
            "season_year": row.standings_date.year if row.standings_date else None,
            "document_type": "standings",
            "game_date": row.standings_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "rank": row.rank,
                "wins": row.wins,
                "losses": row.losses,
                "win_pct": row.win_pct,
                "games_behind": row.games_behind,
            },
        }


def _iter_event_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_events에서 구단 이벤트/뉴스 청크를 생성합니다."""
    from src.models.team_event import TeamEvent

    query = session.query(TeamEvent).filter(TeamEvent.title.is_not(None))
    if limit:
        query = query.limit(limit)

    for event in query.yield_per(200):
        parts = [f"이벤트: {event.title}"]
        if event.event_type:
            parts.append(f"유형: {event.event_type}")
        if event.team_id:
            parts.append(f"팀: {event.team_id}")
        if event.description:
            parts.append(f"내용: {event.description[:200]}")
        if event.benefit_text:
            parts.append(f"혜택: {event.benefit_text[:100]}")
        if event.target_audience:
            parts.append(f"대상: {event.target_audience}")
        if event.event_start_at:
            parts.append(f"시작일: {event.event_start_at.strftime('%Y-%m-%d')}")
        if event.status:
            parts.append(f"상태: {event.status}")

        content = " | ".join(parts)
        yield {
            "source_table": "team_events",
            "source_row_id": str(event.id),
            "title": event.title,
            "content": content,
            "team_id": event.team_id,
            "player_id": None,
            "season_year": event.event_start_at.year if event.event_start_at else None,
            "document_type": "team_event",
            "game_date": event.event_start_at.date() if event.event_start_at else None,
            "published_at": event.published_at,
            "source_url": event.source_url,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"event_type": event.event_type, "status": event.status},
        }


def _iter_movement_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_movements에서 선수 이동/트레이드 청크를 생성합니다."""
    from src.models.player import PlayerMovement

    query = session.query(PlayerMovement)
    if season:
        from sqlalchemy import extract

        query = query.filter(extract("year", PlayerMovement.movement_date) == season)
    query = query.order_by(PlayerMovement.movement_date.desc())
    if limit:
        query = query.limit(limit)

    for mv in query.yield_per(200):
        date_str = str(mv.movement_date) if mv.movement_date else "날짜불명"
        content = f"{date_str}: {mv.player_name} 선수 {mv.section} (팀: {mv.team_code})"
        if mv.remarks:
            content += f" — {mv.remarks[:150]}"

        yield {
            "source_table": "player_movements",
            "source_row_id": str(mv.id),
            "title": f"{mv.player_name} {mv.section}",
            "content": content,
            "team_id": mv.canonical_team_id or mv.team_code,
            "player_id": str(mv.player_basic_id) if mv.player_basic_id else None,
            "season_year": mv.movement_date.year if mv.movement_date else None,
            "document_type": "player_movement",
            "game_date": mv.movement_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"section": mv.section, "team_code": mv.team_code},
        }


def _iter_lineup_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_lineups에서 경기 라인업 청크를 생성합니다 (게임+팀별 1청크)."""
    from itertools import groupby

    from src.models.game import Game, GameLineup

    query = (
        session.query(GameLineup, Game)
        .join(Game, GameLineup.game_id == Game.game_id)
        .order_by(
            Game.game_date,
            GameLineup.game_id,
            GameLineup.team_side,
            GameLineup.batting_order,
            GameLineup.appearance_seq,
        )
    )
    if season:
        query = query.filter(Game.season_id == season)

    for count, (key, group) in enumerate(
        groupby(query.yield_per(2000), key=lambda row: (row[0].game_id, row[0].team_side))
    ):
        game_id, team_side = key
        lines: list[str] = []
        game_date = None
        season_id = None
        team_code = None
        for lineup, game in group:
            game_date = game.game_date
            season_id = game.season_id
            team_code = lineup.team_code or game.home_team if lineup.team_side == "home" else game.away_team
            pos = lineup.standard_position or lineup.position or ""
            lines.append(
                f"{lineup.batting_order}번 {lineup.player_name} ({pos})" + (" [교체]" if not lineup.is_starter else "")
            )
        content = (
            f"경기일: {game_date}, 경기 ID: {game_id}, "
            f"{'홈' if team_side == 'home' else '원정'}팀 {team_code} 라인업: " + ", ".join(lines)
        )
        yield {
            "source_table": "game_lineups",
            "source_row_id": f"{game_id}_{team_side}",
            "title": f"[{game_date}] {team_code} 라인업 ({game_id})",
            "content": content,
            "team_id": team_code,
            "player_id": None,
            "season_year": season_id,
            "document_type": "lineup",
            "game_date": str(game_date) if game_date else None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"game_id": game_id, "team_side": team_side, "team_code": team_code},
        }
        count += 1
        if limit and count >= limit:
            break


def _iter_rankings_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """stat_rankings에서 시즌별/지표별 상위 순위 청크를 생성합니다."""
    from itertools import groupby

    from src.models.rankings import StatRanking

    query = session.query(StatRanking)
    if season:
        query = query.filter(StatRanking.season == season)
    query = query.order_by(StatRanking.season.desc(), StatRanking.metric, StatRanking.rank.asc())
    if limit:
        query = query.limit(limit)

    for count, (r_season, metric, group) in enumerate(
        (
            (r_season, metric, tuple(rows))
            for (r_season, metric), rows in groupby(query.yield_per(1000), key=lambda r: (r.season, r.metric))
        )
    ):
        top = [
            f"{row.rank}위 {row.entity_label}" + (f" ({row.team_id})" if row.team_id else "") + f" {row.value}"
            for row in group
            if row.rank <= _RANKING_TOP_N
        ]
        if not top:
            continue
        content = f"{r_season}시즌 KBO {metric} 순위: " + ", ".join(top)
        yield {
            "source_table": "stat_rankings",
            "source_row_id": f"{r_season}_{metric}",
            "title": f"{r_season}시즌 {metric} 순위",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": r_season,
            "document_type": "ranking",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"metric": metric, "season": r_season},
        }
        count += 1
        if limit and count >= limit:
            break


def _clean_aliases(aliases: object) -> list[str] | None:
    """teams.aliases는 시드 이슈로 깨진 JSON('["{", "}"]')일 수 있어 실제 별칭만 추출합니다."""
    if isinstance(aliases, str):
        try:
            import json

            parsed = json.loads(aliases)
        except (ValueError, TypeError):
            return None
    else:
        parsed = aliases
    if not isinstance(parsed, list):
        return None
    clean = [a for a in parsed if isinstance(a, str) and len(a) > 1 and a not in ("{", "}", ",")]
    return clean or None


def _iter_team_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """teams에서 구단 정보 청크를 생성합니다."""
    from src.models.team import Team

    query = session.query(Team)
    if limit:
        query = query.limit(limit)

    for team in query.yield_per(100):
        aliases = _clean_aliases(team.aliases)
        aliases_str = f" (별칭: {', '.join(aliases)})" if aliases else ""
        status_str = "" if team.is_active else " (해체/휴면 구단)"
        content = (
            f"구단: {team.team_name} ({team.team_short_name}){status_str} — "
            f"연고: {team.city}, 창단: {team.founded_year}년"
            + (f", 홈구장: {team.stadium_name}" if team.stadium_name else "")
            + aliases_str
        )
        yield {
            "source_table": "teams",
            "source_row_id": team.team_id,
            "title": f"구단 정보: {team.team_name}",
            "content": content,
            "team_id": team.team_id,
            "player_id": None,
            "season_year": None,
            "document_type": "team_info",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "team_id": team.team_id,
                "team_name": team.team_name,
                "city": team.city,
                "is_active": team.is_active,
            },
        }


def _iter_team_history_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_history에서 구단 변천사 청크를 생성합니다 (연도별 1청크)."""
    from src.models.team_history import TeamHistory

    query = session.query(TeamHistory)
    if season:
        query = query.filter(TeamHistory.season == season)
    query = query.order_by(TeamHistory.season.desc())
    if limit:
        query = query.limit(limit)

    for row in query.yield_per(200):
        ranking_str = f"{row.ranking}위" if row.ranking else "순위불명"
        content = (
            f"{row.season}년 {row.team_name} ({row.team_code}): {ranking_str}, "
            f"구장: {row.stadium or '불명'}, 도시: {row.city or '불명'}"
        )
        yield {
            "source_table": "team_history",
            "source_row_id": str(row.id),
            "title": f"{row.season} {row.team_name}",
            "content": content,
            "team_id": row.team_code,
            "player_id": None,
            "season_year": row.season,
            "document_type": "team_history",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "franchise_id": row.franchise_id,
                "team_name": row.team_name,
                "ranking": row.ranking,
            },
        }


def _iter_highlight_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_highlights에서 경기 명장면 청크를 생성합니다."""
    from src.models.game import Game, GameHighlight

    query = (
        session.query(GameHighlight, Game)
        .join(Game, GameHighlight.game_id == Game.game_id)
        .order_by(Game.game_date.desc(), GameHighlight.wpa.desc())
    )
    if season:
        query = query.filter(Game.season_id == season)
    if limit:
        query = query.limit(limit)

    for highlight, game in query.yield_per(200):
        tags_str = f" [태그: {', '.join(highlight.tags)}]" if highlight.tags else ""
        wpa_str = f" (WPA {highlight.wpa:+.3f})" if highlight.wpa is not None else ""
        content = (
            f"{game.game_date} {game.away_team} vs {game.home_team} "
            f"{highlight.inning}회{highlight.inning_half or ''} "
            f"{highlight.highlight_type}: {highlight.description}{tags_str}{wpa_str}"
        )
        yield {
            "source_table": "game_highlights",
            "source_row_id": str(highlight.id),
            "title": f"[{game.game_date}] {game.away_team} vs {game.home_team} 하이라이트",
            "content": content,
            "team_id": game.home_team,
            "player_id": None,
            "season_year": game.season_id,
            "document_type": "highlight",
            "game_date": str(game.game_date),
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "game_id": game.game_id,
                "highlight_type": highlight.highlight_type,
                "inning": highlight.inning,
                "tags": highlight.tags,
            },
        }


def _iter_award_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """awards에서 KBO 시상 내역 청크를 생성합니다."""
    from src.models.award import Award

    query = session.query(Award)
    if season:
        query = query.filter(Award.year == season)
    query = query.order_by(Award.year.desc())
    if limit:
        query = query.limit(limit)

    for award in query.yield_per(200):
        category_str = f" ({award.category})" if award.category else ""
        content = f"{award.year}시즌 KBO {award.award_type}{category_str}: {award.player_name} ({award.team_name})"
        yield {
            "source_table": "awards",
            "source_row_id": str(award.id),
            "title": f"{award.year} {award.award_type}{category_str}",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": award.year,
            "document_type": "award",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "award_type": award.award_type,
                "category": award.category,
                "player_name": award.player_name,
                "team_name": award.team_name,
            },
        }


def _iter_pbp_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_play_by_play에서 주요 승부처 및 득점 이벤트 청크를 생성합니다."""
    from src.models.game import Game, GamePlayByPlay

    query = (
        session.query(GamePlayByPlay, Game)
        .join(Game, GamePlayByPlay.game_id == Game.game_id)
        .filter(GamePlayByPlay.play_description.is_not(None))
    )
    if season:
        query = query.filter(Game.season_id == season)

    keywords = [
        "홈런",
        "적시타",
        "안타",
        "삼진",
        "병살타",
        "도루",
        "결승타",
        "타점",
        "끝내기",
        "만루홈런",
        "역전타",
        "득점권",
    ]
    count = 0
    for pbp, game in query.yield_per(500):
        desc = pbp.play_description or ""
        if not any(kw in desc for kw in keywords):
            continue

        content = (
            f"경기일: {game.game_date}, 경기: {game.away_team} vs {game.home_team}, "
            f"이닝: {pbp.inning}회{pbp.inning_half or ''}, "
            f"투수: {pbp.pitcher_name or '미상'}, 타자: {pbp.batter_name or '미상'}, "
            f"내용: {desc}"
        )
        yield {
            "source_table": "game_play_by_play",
            "source_row_id": str(pbp.id),
            "title": (
                f"[{game.game_date}] {game.away_team} vs {game.home_team} "
                f"{pbp.inning}회{pbp.inning_half or ''} {pbp.batter_name or ''}"
            ),
            "content": content,
            "team_id": game.home_team,
            "player_id": str(pbp.player_id) if pbp.player_id else None,
            "season_year": game.season_id,
            "document_type": "game_play_by_play",
            "game_date": str(game.game_date),
            "published_at": None,
            "source_url": None,
            "language": "ko",
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "game_id": game.game_id,
                "inning": pbp.inning,
                "event_type": pbp.event_type,
            },
        }
        count += 1
        if limit and count >= limit:
            break


_MARKDOWN_SOURCE_TABLES = ("markdown_docs", "kbo_definitions", "kbo_regulations")


def _markdown_docs_root() -> Path:
    """Return the local Markdown root, allowing Docker deployments to override it."""
    default_root = Path(__file__).resolve().parents[2] / "Docs" / "baseball"
    return Path(os.getenv("KBO_MARKDOWN_DOCS_DIR", str(default_root)))


def _iter_local_markdown_chunks(
    source_table: str,
    limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks in the shape expected by the vector repository."""
    if source_table not in _MARKDOWN_SOURCE_TABLES:
        return

    transformer = TextTransformer()
    yielded = 0
    for doc in load_local_markdown_docs(_markdown_docs_root()):
        if markdown_source_table(doc) != source_table:
            continue

        for chunk in transformer.chunk_document(doc):
            meta = dict(chunk.get("meta", {}))
            source_row_id = str(meta.get("source_row_id", ""))
            if not source_row_id:
                continue
            meta["source_table"] = source_table
            meta["document_type"] = "markdown_doc"
            yield {
                "source_table": source_table,
                "source_row_id": source_row_id,
                "title": chunk.get("title"),
                "content": chunk["content"],
                "team_id": None,
                "player_id": None,
                "season_year": None,
                "document_type": "markdown_doc",
                "game_date": None,
                "published_at": None,
                "source_url": None,
                "language": _DEFAULT_LANGUAGE,
                "league_type_code": None,
                "meta": meta,
            }
            yielded += 1
            if limit and yielded >= limit:
                return


def _iter_markdown_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield general local Markdown knowledge chunks."""
    yield from _iter_local_markdown_chunks("markdown_docs", limit)


def _iter_definition_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks containing KBO definitions."""
    yield from _iter_local_markdown_chunks("kbo_definitions", limit)


def _iter_regulation_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks containing KBO regulations."""
    yield from _iter_local_markdown_chunks("kbo_regulations", limit)


# ─── 소스 매핑 ────────────────────────────────────────────────────────────────

_SOURCE_MAP = {
    "players": _iter_player_chunks,
    "batting": _iter_batting_chunks,
    "pitching": _iter_pitching_chunks,
    "games": _iter_game_chunks,
    "standings": _iter_standings_chunks,
    "events": _iter_event_chunks,
    "movements": _iter_movement_chunks,
    "awards": _iter_award_chunks,
    "lineups": _iter_lineup_chunks,
    "rankings": _iter_rankings_chunks,
    "teams": _iter_team_chunks,
    "team_history": _iter_team_history_chunks,
    "highlights": _iter_highlight_chunks,
    "pbp": _iter_pbp_chunks,
    "markdown_docs": _iter_markdown_chunks,
    "kbo_definitions": _iter_definition_chunks,
    "kbo_regulations": _iter_regulation_chunks,
}


# ─── 임베딩 + 저장 ─────────────────────────────────────────────────────────────


def _dedupe_batch(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """배치 내 동일 키(source_table + source_row_id) 중복을 제거합니다 (마지막 항목 유지)."""
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for chunk in batch:
        deduped[(chunk["source_table"], chunk["source_row_id"])] = chunk
    return list(deduped.values())


def _process_source(
    source_name: str,
    chunk_iter: Iterator[dict[str, Any]],
    embedding_service: object,
    vector_repo: object,
    *,
    dry_run: bool,
) -> int:
    """단일 소스의 청크들을 임베딩하여 pgvector DB에 저장합니다."""
    from src.db.vector_engine import get_vector_session

    total = 0
    batch: list[dict[str, Any]] = []

    def _flush_batch(batch: list[dict[str, Any]]) -> None:
        nonlocal total
        if not batch:
            return
        batch = _dedupe_batch(batch)
        # 임베딩 배치 생성
        texts = [c["content"] for c in batch]
        embeddings = embedding_service.get_embeddings_batch(texts)  # type: ignore[attr-defined]
        for chunk, emb in zip(batch, embeddings, strict=False):
            chunk["embedding"] = emb

        if not dry_run:
            with get_vector_session() as vsession:
                for chunk in batch:
                    vector_repo.upsert_chunk(vsession, chunk)  # type: ignore[attr-defined]
                    total += 1
                    if total % _COMMIT_EVERY == 0:
                        vsession.commit()
                        logger.info("  [%s] %d 청크 저장 완료...", source_name, total)
        else:
            total += len(batch)

    for chunk in chunk_iter:
        batch.append(chunk)
        if len(batch) >= _BATCH_SIZE:
            _flush_batch(batch)
            batch = []

    if batch:
        _flush_batch(batch)

    return total


# ─── 메인 CLI ─────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    """KBO RAG 인덱스를 구축합니다."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="KBO 데이터를 pgvector RAG 인덱스에 저장")
    parser.add_argument(
        "--source",
        choices=_VALID_SOURCES,
        default="all",
        help="임베딩할 데이터 소스 (기본값: all)",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="특정 시즌만 처리 (예: 2025)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="소스당 최대 처리 행 수 (테스트용)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="임베딩만 생성하고 DB에 저장하지 않음",
    )
    args = parser.parse_args(argv)

    # pgvector 연결 확인
    from src.db.vector_engine import init_vector_db, is_pgvector_available

    if not is_pgvector_available():
        logger.error(
            "pgvector DB에 연결할 수 없습니다. "
            "docker-compose up pgvector -d 후 run_pgvector_migration을 먼저 실행하세요."
        )
        sys.exit(1)

    if not args.dry_run:
        logger.info("pgvector 테이블 초기화 확인 중...")
        init_vector_db()

    # 서비스 초기화
    from src.repositories.vector_search_repository import VectorSearchRepository
    from src.services.embedding_service import EmbeddingService

    embedding_service = EmbeddingService()
    vector_repo = VectorSearchRepository()

    # 소스 선택
    sources = list(_SOURCE_MAP.keys()) if args.source == "all" else [args.source]

    logger.info(
        "RAG 인덱스 빌드 시작 | 소스: %s | 시즌: %s | 제한: %s | dry-run: %s",
        args.source,
        args.season or "전체",
        args.limit or "없음",
        args.dry_run,
    )

    # 메인 SQLite 세션에서 데이터 읽기
    from src.db.engine import get_db_session

    grand_total = 0
    with get_db_session() as session:
        for source_name in sources:
            logger.info("▶ [%s] 처리 시작...", source_name)
            chunk_fn = _SOURCE_MAP[source_name]
            chunk_iter = chunk_fn(session, args.season, args.limit)
            count = _process_source(
                source_name,
                chunk_iter,
                embedding_service,
                vector_repo,
                dry_run=args.dry_run,
            )
            logger.info("✅ [%s] %d 청크 완료", source_name, count)
            grand_total += count

    logger.info(
        "🎉 RAG 인덱스 빌드 완료 — 총 %d 청크 처리%s",
        grand_total,
        " (dry-run, 저장 없음)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
