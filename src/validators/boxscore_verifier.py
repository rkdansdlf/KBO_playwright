"""Boxscore sum verification and game count reconciliation logic.

Validates:
1. Individual players' stats sums (AB, H, R, IP) vs team totals (표 하단 합계 행)
   and scoreboard totals.
2. DB game counts vs actual scheduled/played games on a target date.
3. Cross-team consistency (pitcher runs allowed vs opponent runs scored).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.models.game import Game, GameBattingStat, GameInningScore, GamePitchingStat
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

MIN_OFFICIAL_GAME_OUTS = 15


def _verify_team_batting_sums(
    side: str,
    team_info: dict[str, Any],
    player_hitters: list[dict[str, Any]],
    total_row: dict[str, Any],
    discrepancies: list[str],
) -> None:
    """Verify one team's batting sums against total row and scoreboard."""
    sum_ab = sum(int(p.get("stats", {}).get("at_bats") or 0) for p in player_hitters)
    sum_hits = sum(int(p.get("stats", {}).get("hits") or 0) for p in player_hitters)
    sum_runs = sum(int(p.get("stats", {}).get("runs") or 0) for p in player_hitters)

    if total_row.get("at_bats") is not None and sum_ab != int(total_row["at_bats"]):
        discrepancies.append(f"{side.upper()} 타수 불일치: 선수합({sum_ab}) != 팀합계행({total_row['at_bats']})")
    if total_row.get("hits") is not None and sum_hits != int(total_row["hits"]):
        discrepancies.append(f"{side.upper()} 안타 불일치: 선수합({sum_hits}) != 팀합계행({total_row['hits']})")
    if total_row.get("runs") is not None and sum_runs != int(total_row["runs"]):
        discrepancies.append(f"{side.upper()} 득점 불일치: 선수합({sum_runs}) != 팀합계행({total_row['runs']})")

    sb_score = team_info.get("score")
    if sb_score is not None and sum_runs != int(sb_score):
        discrepancies.append(f"{side.upper()} 득점-스코어 불일치: 선수합({sum_runs}) != 스코어보드({sb_score})")
    sb_hits = team_info.get("hits")
    if sb_hits is not None and sum_hits != int(sb_hits):
        discrepancies.append(f"{side.upper()} 안타-스코어 불일치: 선수합({sum_hits}) != 스코어보드({sb_hits})")


def _verify_pitching_cross_checks(
    teams: dict[str, Any],
    pitchers: dict[str, Any],
    discrepancies: list[str],
) -> None:
    """Verify pitcher runs allowed against opponent score and minimum outs."""
    away_score = teams.get("away", {}).get("score")
    home_score = teams.get("home", {}).get("score")
    away_pitchers = pitchers.get("away") or []
    home_pitchers = pitchers.get("home") or []

    if home_score is not None and away_pitchers:
        away_runs = sum(int(p.get("stats", {}).get("runs_allowed") or 0) for p in away_pitchers)
        if away_runs != int(home_score):
            discrepancies.append(f"원정 투수 실점합({away_runs}) != 홈팀 점수({home_score})")

    if away_score is not None and home_pitchers:
        home_runs = sum(int(p.get("stats", {}).get("runs_allowed") or 0) for p in home_pitchers)
        if home_runs != int(away_score):
            discrepancies.append(f"홈 투수 실점합({home_runs}) != 원정팀 점수({away_score})")

    for side, plist in [("away", away_pitchers), ("home", home_pitchers)]:
        total_outs = sum(int(p.get("stats", {}).get("innings_outs") or 0) for p in plist)
        if plist and total_outs < MIN_OFFICIAL_GAME_OUTS:
            discrepancies.append(
                f"{side.upper()} 투구 이닝 부족: {total_outs}아웃 (최소 {MIN_OFFICIAL_GAME_OUTS}아웃 필요)"
            )


def verify_boxscore_sums(
    game_data: dict[str, Any],
    team_totals: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, list[str]]:
    """Compare collected player stats sums against team totals row and scoreboard.

    Args:
        game_data: Full game detail payload including teams, hitters, pitchers.
        team_totals: Optional extracted team total rows {"away": {...}, "home": {...}}.

    Returns:
        (is_valid, list_of_discrepancies)

    """
    discrepancies: list[str] = []
    game_id = str(game_data.get("game_id", "UNKNOWN"))
    teams = game_data.get("teams") or {}
    hitters = game_data.get("hitters") or {}
    pitchers = game_data.get("pitchers") or {}
    team_totals = team_totals or {}

    for side in ("away", "home"):
        _verify_team_batting_sums(
            side=side,
            team_info=teams.get(side) or {},
            player_hitters=hitters.get(side) or [],
            total_row=team_totals.get(side) or {},
            discrepancies=discrepancies,
        )

    _verify_pitching_cross_checks(teams, pitchers, discrepancies)

    is_valid = len(discrepancies) == 0
    if not is_valid:
        logger.warning(
            "⚠️ [검산 경고] 게임 %s 박스스코어 합계 검증 실패 (%s건 불일치):\n - %s",
            game_id,
            len(discrepancies),
            "\n - ".join(discrepancies),
        )

    return is_valid, discrepancies


def verify_game_db_boxscore_integrity(
    session: Session,
    game_id: str,
) -> tuple[bool, list[str]]:
    """Verify saved game data in DB for consistency between player sums and team totals.

    Args:
        session: SQLAlchemy DB session.
        game_id: Target game_id.

    Returns:
        (is_valid, list_of_errors)

    """
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return False, [f"Game not found in DB: {game_id}"]

    if game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
        return True, []

    errors: list[str] = []
    batting_rows = session.query(GameBattingStat).filter(GameBattingStat.game_id == game_id).all()
    away_hitters = [r for r in batting_rows if r.team_side == "away"]
    home_hitters = [r for r in batting_rows if r.team_side == "home"]

    away_runs = sum(r.runs or 0 for r in away_hitters)
    home_runs = sum(r.runs or 0 for r in home_hitters)

    if game.away_score is not None and away_runs != game.away_score:
        errors.append(f"원정 타자 득점 합계({away_runs}) != 경기 원정 점수({game.away_score})")

    if game.home_score is not None and home_runs != game.home_score:
        errors.append(f"홈 타자 득점 합계({home_runs}) != 경기 홈 점수({game.home_score})")

    pitching_rows = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == game_id).all()
    away_pitchers = [r for r in pitching_rows if r.team_side == "away"]
    home_pitchers = [r for r in pitching_rows if r.team_side == "home"]

    away_p_runs = sum(r.runs_allowed or 0 for r in away_pitchers)
    home_p_runs = sum(r.runs_allowed or 0 for r in home_pitchers)

    if game.home_score is not None and away_pitchers and away_p_runs != game.home_score:
        errors.append(f"원정 투수 실점 합계({away_p_runs}) != 홈팀 점수({game.home_score})")

    if game.away_score is not None and home_pitchers and home_p_runs != game.away_score:
        errors.append(f"홈 투수 실점 합계({home_p_runs}) != 원정팀 점수({game.away_score})")

    innings = session.query(GameInningScore).filter(GameInningScore.game_id == game_id).all()
    away_inning_runs = sum(i.runs or 0 for i in innings if i.team_side == "away")
    home_inning_runs = sum(i.runs or 0 for i in innings if i.team_side == "home")

    if innings and game.away_score is not None and away_inning_runs != game.away_score:
        errors.append(f"원정 이닝스코어 합계({away_inning_runs}) != 경기 원정 점수({game.away_score})")

    if innings and game.home_score is not None and home_inning_runs != game.home_score:
        errors.append(f"홈 이닝스코어 합계({home_inning_runs}) != 경기 홈 점수({game.home_score})")

    return len(errors) == 0, errors


def verify_daily_game_count(
    target_date: str | date,
    session: Session,
    expected_count: int | None = None,
) -> tuple[bool, int, int | None, list[str]]:
    """Compare the number of games stored in DB for a date with expected/actual schedule count.

    Args:
        target_date: Date as string (YYYY-MM-DD, YYYYMMDD) or date object.
        session: SQLAlchemy DB session.
        expected_count: Authoritative game count from internet schedule/official KBO source.

    Returns:
        (is_match, db_game_count, expected_count, list_of_notes)

    """
    if isinstance(target_date, str):
        cleaned_date = target_date.replace("-", "").strip()
        parsed_date = parse_date_str(cleaned_date)
    else:
        parsed_date = target_date

    games = session.query(Game).filter(Game.game_date == parsed_date).all()
    db_count = len(games)
    notes: list[str] = []

    if expected_count is not None:
        if db_count != expected_count:
            notes.append(f"날짜({parsed_date}) 경기 수 불일치: DB({db_count}개) != 실제({expected_count}개)")
            return False, db_count, expected_count, notes
        return True, db_count, expected_count, notes

    non_terminal = [g for g in games if g.game_status not in COMPLETED_LIKE_GAME_STATUSES]
    if non_terminal:
        notes.append(f"날짜({parsed_date}) 미완료 경기 {len(non_terminal)}건 존재")

    return True, db_count, None, notes
