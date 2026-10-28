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

REGULATION_INNINGS = 9
TIEBREAKER_MIN_INNINGS = 10
MIN_OFFICIAL_GAME_OUTS = 15
OUTS_PER_INNING = 3
OUTS_ROUNDING_OFFSET = 2


def _is_2011_exhibition_tiebreaker(
    game_id: str,
    game_date: str | date | None = None,
    num_innings: int = REGULATION_INNINGS,
) -> bool:
    """Detect if a game falls under the 2011 KBO exhibition experimental 10th-inning tiebreaker rule (승부치기).

    In March 2011 exhibition games tied after 9 innings, KBO tested an experimental 10th-inning
    tiebreaker (무사 1,2루 승부치기). Official boxscores on KBO website recorded individual
    batter/pitcher statistics strictly for regulation 9 innings (experimental tiebreaker runs
    were excluded from player totals, but recorded on the scoreboard and inning scores).
    """
    if num_innings < TIEBREAKER_MIN_INNINGS:
        return False
    date_str = str(game_date or "")
    return game_id.startswith("201103") or "2011-03" in date_str or "201103" in date_str


def _verify_team_batting_sums(
    side: str,
    player_hitters: list[dict[str, Any]],
    expected: dict[str, Any],
    discrepancies: list[str],
) -> None:
    """Verify one team's batting sums against total row and scoreboard."""
    total_row = expected.get("total_row") or {}
    sb_score = expected.get("score")
    sb_hits = expected.get("hits")
    extra_runs = int(expected.get("extra_runs") or 0)

    sum_ab = sum(int(p.get("stats", {}).get("at_bats") or 0) for p in player_hitters)
    sum_hits = sum(int(p.get("stats", {}).get("hits") or 0) for p in player_hitters)
    sum_runs = sum(int(p.get("stats", {}).get("runs") or 0) for p in player_hitters)

    if total_row.get("at_bats") is not None and sum_ab != int(total_row["at_bats"]):
        discrepancies.append(f"{side.upper()} 타수 불일치: 선수합({sum_ab}) != 팀합계행({total_row['at_bats']})")
    if total_row.get("hits") is not None and sum_hits != int(total_row["hits"]):
        discrepancies.append(f"{side.upper()} 안타 불일치: 선수합({sum_hits}) != 팀합계행({total_row['hits']})")
    if total_row.get("runs") is not None and sum_runs != int(total_row["runs"]):
        discrepancies.append(f"{side.upper()} 득점 불일치: 선수합({sum_runs}) != 팀합계행({total_row['runs']})")

    if sb_score is not None:
        if extra_runs > 0 and sum_runs + extra_runs == int(sb_score):
            pass
        elif sum_runs != int(sb_score):
            discrepancies.append(f"{side.upper()} 득점-스코어 불일치: 선수합({sum_runs}) != 스코어보드({sb_score})")
    if sb_hits is not None and sum_hits != int(sb_hits):
        discrepancies.append(f"{side.upper()} 안타-스코어 불일치: 선수합({sum_hits}) != 스코어보드({sb_hits})")


def _extract_pitching_total_outs(total_row: dict[str, Any]) -> int | None:
    """Extract total pitching outs from a team totals row if present."""
    if not total_row:
        return None
    if total_row.get("innings_outs") is not None:
        try:
            return int(total_row["innings_outs"])
        except ValueError, TypeError:
            pass
    if total_row.get("innings_pitched") is not None:
        from src.utils.type_helpers import parse_innings_to_outs

        return parse_innings_to_outs(total_row["innings_pitched"])
    return None


def _check_pitching_innings_outs_plausibility(
    away_outs: int,
    home_outs: int,
    away_score: int,
    home_score: int,
    num_innings: int,
) -> list[str]:
    """Check outs plausibility handling 9th inning bottom unplayed and walk-offs."""
    errors: list[str] = []
    if away_outs < MIN_OFFICIAL_GAME_OUTS:
        errors.append(f"원정 투수 총 이닝 부족: {away_outs}아웃 (최소 {MIN_OFFICIAL_GAME_OUTS}아웃 필요)")
    if home_outs < MIN_OFFICIAL_GAME_OUTS:
        errors.append(f"홈 투수 총 이닝 부족: {home_outs}아웃 (최소 {MIN_OFFICIAL_GAME_OUTS}아웃 필요)")

    if home_score > away_score:
        min_away_outs = (num_innings - 1) * OUTS_PER_INNING
        max_away_outs = num_innings * OUTS_PER_INNING
        if not (min_away_outs <= away_outs <= max_away_outs):
            errors.append(
                f"홈 승리 시 원정 투수 아웃카운트 비정상: {away_outs}아웃 "
                f"(정상 범위: {min_away_outs}~{max_away_outs}아웃, 9회말 미진행 또는 끝내기)"
            )
    elif away_score > home_score and num_innings * OUTS_PER_INNING > away_outs >= MIN_OFFICIAL_GAME_OUTS:
        errors.append(f"원정 투수 아웃카운트 부족: {away_outs}아웃 (예상: {num_innings * OUTS_PER_INNING}아웃)")
    return errors


def _verify_pitching_team_totals(
    pitching_totals: dict[str, Any],
    away_pitchers: list[dict[str, Any]],
    home_pitchers: list[dict[str, Any]],
    discrepancies: list[str],
) -> bool:
    verified = False
    for side, plist in [("away", away_pitchers), ("home", home_pitchers)]:
        total_outs = sum(int(p.get("stats", {}).get("innings_outs") or 0) for p in plist)
        side_total_row = pitching_totals.get(side) or {}
        exp_outs = _extract_pitching_total_outs(side_total_row)
        if exp_outs is not None:
            verified = True
            if total_outs != exp_outs:
                discrepancies.append(
                    f"{side.upper()} 투수 이닝 불일치: 선수합({total_outs}아웃) != 팀합계행({exp_outs}아웃)"
                )
        elif plist and total_outs < MIN_OFFICIAL_GAME_OUTS:
            discrepancies.append(
                f"{side.upper()} 투구 이닝 부족: {total_outs}아웃 (최소 {MIN_OFFICIAL_GAME_OUTS}아웃 필요)"
            )
    return verified


def _verify_pitching_cross_checks(
    teams: dict[str, Any],
    pitchers: dict[str, Any],
    context: dict[str, Any],
    discrepancies: list[str],
) -> None:
    """Verify pitcher runs allowed against opponent score and innings outs plausibility."""
    away_score = teams.get("away", {}).get("score")
    home_score = teams.get("home", {}).get("score")
    away_pitchers = pitchers.get("away") or []
    home_pitchers = pitchers.get("home") or []

    team_totals = context.get("team_totals") or {}
    num_innings = int(context.get("num_innings") or REGULATION_INNINGS)
    away_extra = int(context.get("away_extra") or 0)
    home_extra = int(context.get("home_extra") or 0)
    is_tb = bool(context.get("is_tiebreaker"))

    if home_score is not None and away_pitchers:
        away_runs = sum(int(p.get("stats", {}).get("runs_allowed") or 0) for p in away_pitchers)
        if not (home_extra > 0 and away_runs + home_extra == int(home_score)) and away_runs != int(home_score):
            discrepancies.append(f"원정 투수 실점합({away_runs}) != 홈팀 점수({home_score})")

    if away_score is not None and home_pitchers:
        home_runs = sum(int(p.get("stats", {}).get("runs_allowed") or 0) for p in home_pitchers)
        if not (away_extra > 0 and home_runs + away_extra == int(away_score)) and home_runs != int(away_score):
            discrepancies.append(f"홈 투수 실점합({home_runs}) != 원정팀 점수({away_score})")

    pitching_totals = team_totals.get("pitching") if isinstance(team_totals.get("pitching"), dict) else team_totals
    verified_with_team_totals = _verify_pitching_team_totals(
        pitching_totals, away_pitchers, home_pitchers, discrepancies
    )

    has_score = away_score is not None and home_score is not None
    if not verified_with_team_totals and has_score and away_pitchers and home_pitchers:
        away_outs = sum(int(p.get("stats", {}).get("innings_outs") or 0) for p in away_pitchers)
        home_outs = sum(int(p.get("stats", {}).get("innings_outs") or 0) for p in home_pitchers)
        pitcher_inferred = max(
            (home_outs + OUTS_ROUNDING_OFFSET) // OUTS_PER_INNING,
            (away_outs + OUTS_ROUNDING_OFFSET) // OUTS_PER_INNING,
        )
        effective_num_innings = REGULATION_INNINGS if is_tb else max(num_innings, pitcher_inferred)
        errors = _check_pitching_innings_outs_plausibility(
            away_outs=away_outs,
            home_outs=home_outs,
            away_score=int(away_score),
            home_score=int(home_score),
            num_innings=effective_num_innings,
        )
        discrepancies.extend(errors)


def _extract_boxscore_extra_runs(
    innings_list: list[Any],
    away_line: list[Any],
    home_line: list[Any],
) -> tuple[int, int]:
    """Extract extra-inning runs (inning 10+) from innings list or line score."""
    away_extra = 0
    home_extra = 0
    for r in innings_list:
        if isinstance(r, dict) and int(r.get("inning") or 0) >= TIEBREAKER_MIN_INNINGS:
            runs = int(r.get("runs") or 0)
            side = str(r.get("team_side") or "").lower()
            if side == "away":
                away_extra += runs
            elif side == "home":
                home_extra += runs
    if away_extra == 0 and len(away_line) >= TIEBREAKER_MIN_INNINGS:
        away_extra = sum(int(x) for x in away_line[9:] if x is not None)
    if home_extra == 0 and len(home_line) >= TIEBREAKER_MIN_INNINGS:
        home_extra = sum(int(x) for x in home_line[9:] if x is not None)
    return away_extra, home_extra


def verify_boxscore_sums(
    game_data: dict[str, Any],
    team_totals: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, list[str]]:
    """Compare collected player stats sums against team totals row and scoreboard."""
    discrepancies: list[str] = []
    game_id = str(game_data.get("game_id", "UNKNOWN"))
    teams = game_data.get("teams") or {}
    hitters = game_data.get("hitters") or {}
    pitchers = game_data.get("pitchers") or {}
    totals = team_totals or {}

    scoreboard = game_data.get("scoreboard") or {}
    innings_list = scoreboard.get("innings") or game_data.get("innings") or []
    away_line = (teams.get("away") or {}).get("line_score") or []
    home_line = (teams.get("home") or {}).get("line_score") or []

    line_innings = max(
        max((idx for idx, runs in enumerate(away_line, start=1) if runs is not None), default=0),
        max((idx for idx, runs in enumerate(home_line, start=1) if runs is not None), default=0),
    )
    if isinstance(innings_list, list) and innings_list:
        if all(isinstance(r, dict) and "inning" in r for r in innings_list):
            innings_from_list = max((int(r["inning"]) for r in innings_list if r.get("runs") is not None), default=0)
        else:
            innings_from_list = len(innings_list)
    else:
        innings_from_list = 0

    num_innings = max(line_innings, innings_from_list, REGULATION_INNINGS)
    is_2011_tb = _is_2011_exhibition_tiebreaker(game_id, game_data.get("game_date"), num_innings)
    away_extra, home_extra = _extract_boxscore_extra_runs(innings_list, away_line, home_line) if is_2011_tb else (0, 0)

    for side in ("away", "home"):
        extra = away_extra if side == "away" else home_extra
        expected = {
            "score": (teams.get(side) or {}).get("score"),
            "hits": (teams.get(side) or {}).get("hits"),
            "total_row": totals.get(side) or {},
            "extra_runs": extra,
        }
        _verify_team_batting_sums(side, hitters.get(side) or [], expected, discrepancies)

    context = {
        "team_totals": totals,
        "num_innings": num_innings,
        "away_extra": away_extra,
        "home_extra": home_extra,
        "is_tiebreaker": is_2011_tb,
    }
    _verify_pitching_cross_checks(teams, pitchers, context, discrepancies)

    is_valid = len(discrepancies) == 0
    if not is_valid:
        logger.warning(
            "⚠️ [검산 경고] 게임 %s 박스스코어 합계 검증 실패 (%s건 불일치):\n - %s",
            game_id,
            len(discrepancies),
            "\n - ".join(discrepancies),
        )

    return is_valid, discrepancies


def _verify_db_batting(
    game: Game,
    batting_rows: list[GameBattingStat],
    extra_runs: tuple[int, int],
    errors: list[str],
) -> None:
    away_extra, home_extra = extra_runs
    away_runs = sum(r.runs or 0 for r in batting_rows if r.team_side == "away")
    home_runs = sum(r.runs or 0 for r in batting_rows if r.team_side == "home")

    if game.away_score is not None:
        if away_extra > 0 and away_runs + away_extra == game.away_score:
            pass
        elif away_runs != game.away_score:
            errors.append(f"원정 타자 득점 합계({away_runs}) != 경기 원정 점수({game.away_score})")

    if game.home_score is not None:
        if home_extra > 0 and home_runs + home_extra == game.home_score:
            pass
        elif home_runs != game.home_score:
            errors.append(f"홈 타자 득점 합계({home_runs}) != 경기 홈 점수({game.home_score})")


def _verify_db_pitching(
    game: Game,
    pitching_rows: list[GamePitchingStat],
    extra_runs: tuple[int, int],
    errors: list[str],
) -> None:
    away_extra, home_extra = extra_runs
    away_pitchers = [r for r in pitching_rows if r.team_side == "away"]
    home_pitchers = [r for r in pitching_rows if r.team_side == "home"]

    if game.home_score is not None and away_pitchers:
        away_p_runs = sum(r.runs_allowed or 0 for r in away_pitchers)
        if home_extra > 0 and away_p_runs + home_extra == game.home_score:
            pass
        elif away_p_runs != game.home_score:
            errors.append(f"원정 투수 실점 합계({away_p_runs}) != 홈팀 점수({game.home_score})")

    if game.away_score is not None and home_pitchers:
        home_p_runs = sum(r.runs_allowed or 0 for r in home_pitchers)
        if away_extra > 0 and home_p_runs + away_extra == game.away_score:
            pass
        elif home_p_runs != game.away_score:
            errors.append(f"홈 투수 실점 합계({home_p_runs}) != 원정팀 점수({game.away_score})")


def _verify_db_innings(
    game: Game,
    innings: list[GameInningScore],
    errors: list[str],
) -> None:
    if not innings:
        return
    away_inning_runs = sum(i.runs or 0 for i in innings if i.team_side == "away")
    home_inning_runs = sum(i.runs or 0 for i in innings if i.team_side == "home")

    if game.away_score is not None and away_inning_runs != game.away_score:
        errors.append(f"원정 이닝스코어 합계({away_inning_runs}) != 경기 원정 점수({game.away_score})")

    if game.home_score is not None and home_inning_runs != game.home_score:
        errors.append(f"홈 이닝스코어 합계({home_inning_runs}) != 경기 홈 점수({game.home_score})")


def _verify_db_pitching_outs(
    game: Game,
    pitching_rows: list[GamePitchingStat],
    num_innings: int,
    errors: list[str],
) -> None:
    away_pitchers = [r for r in pitching_rows if r.team_side == "away"]
    home_pitchers = [r for r in pitching_rows if r.team_side == "home"]
    if not (game.home_score is not None and game.away_score is not None and away_pitchers and home_pitchers):
        return

    away_outs = sum(r.innings_outs or 0 for r in away_pitchers)
    home_outs = sum(r.innings_outs or 0 for r in home_pitchers)
    pitcher_inferred = max(
        (home_outs + OUTS_ROUNDING_OFFSET) // OUTS_PER_INNING,
        (away_outs + OUTS_ROUNDING_OFFSET) // OUTS_PER_INNING,
    )
    effective_num_innings = max(num_innings, pitcher_inferred)
    errors.extend(
        _check_pitching_innings_outs_plausibility(
            away_outs=away_outs,
            home_outs=home_outs,
            away_score=game.away_score,
            home_score=game.home_score,
            num_innings=effective_num_innings,
        )
    )


def verify_game_db_boxscore_integrity(
    session: Session,
    game_id: str,
) -> tuple[bool, list[str]]:
    """Verify saved game data in DB for consistency between player sums and team totals."""
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return False, [f"Game not found in DB: {game_id}"]

    if game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
        return True, []

    errors: list[str] = []
    batting_rows = session.query(GameBattingStat).filter(GameBattingStat.game_id == game_id).all()
    pitching_rows = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == game_id).all()
    innings = session.query(GameInningScore).filter(GameInningScore.game_id == game_id).all()

    num_innings = max((i.inning for i in innings), default=REGULATION_INNINGS) if innings else REGULATION_INNINGS
    is_2011_tb = _is_2011_exhibition_tiebreaker(game.game_id, game.game_date, num_innings)

    away_extra = (
        sum(i.runs or 0 for i in innings if i.team_side == "away" and i.inning >= TIEBREAKER_MIN_INNINGS)
        if is_2011_tb
        else 0
    )
    home_extra = (
        sum(i.runs or 0 for i in innings if i.team_side == "home" and i.inning >= TIEBREAKER_MIN_INNINGS)
        if is_2011_tb
        else 0
    )
    extra_runs = (away_extra, home_extra)

    _verify_db_batting(game, batting_rows, extra_runs, errors)
    _verify_db_pitching(game, pitching_rows, extra_runs, errors)
    _verify_db_innings(game, innings, errors)

    effective_innings_for_outs = REGULATION_INNINGS if is_2011_tb else num_innings
    _verify_db_pitching_outs(game, pitching_rows, effective_innings_for_outs, errors)

    return len(errors) == 0, errors


def verify_daily_game_count(
    target_date: str | date,
    session: Session,
    expected_count: int | None = None,
) -> tuple[bool, int, int | None, list[str]]:
    """Compare the number of games stored in DB for a date with expected/actual schedule count."""
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
