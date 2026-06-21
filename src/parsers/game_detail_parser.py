"""Parse KBO GameCenter REVIEW HTML into structured box scores."""


# ruff: noqa: PLR2004from __future__ import annotations

import re
from io import StringIO
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.constants import GAME_ID_FULL_LEN, GAME_ID_MIN_LEN, GAME_ID_YEAR_LEN
from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment
from src.utils.type_helpers import parse_innings_to_outs, safe_float_or_none, safe_int_or_none


def parse_game_detail_html(
    html: str,
    game_id: str,
    game_date: str,
    db_session: Session | None = None,
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    dataframes = pd.read_html(StringIO(html))

    scoreboard_df = _extract_scoreboard(dataframes)
    hitter_tables = _extract_hitter_tables(dataframes)
    pitcher_tables = _extract_pitcher_tables(dataframes)

    season_year = _season_year_from_game(game_date)
    teams = _build_team_info(scoreboard_df, game_id, season_year)
    hitters = _build_hitter_payload(hitter_tables, teams, db_session)
    pitchers = _build_pitcher_payload(pitcher_tables, teams, db_session)

    metadata = _parse_metadata(soup)

    return {
        "game_id": game_id,
        "game_date": game_date,
        "metadata": metadata,
        "teams": teams,
        "home_team_code": teams["home"]["code"],
        "away_team_code": teams["away"]["code"],
        "hitters": hitters,
        "pitchers": pitchers,
    }


def _extract_scoreboard(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for df in tables:
        headers = [str(col) for col in df.columns]
        if {"팀", "R", "H", "E"}.issubset(headers):
            return df
    return None


def _extract_hitter_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    hitters = []
    for df in tables:
        headers = [str(col) for col in df.columns]
        if any("타수" in h for h in headers):
            hitters.append(df)
    return hitters


def _extract_pitcher_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    pitchers = []
    for df in tables:
        headers = [str(col) for col in df.columns]
        if any(h in ("이닝", "IP") for h in headers) and "삼진" in headers:
            pitchers.append(df)
    return pitchers


def _build_team_info(
    scoreboard: pd.DataFrame | None,
    game_id: str,
    season_year: int | None,
) -> dict[str, dict[str, Any]]:
    away_info = {
        "name": None,
        "code": team_code_from_game_id_segment(game_id[8:10] if len(game_id) >= GAME_ID_MIN_LEN else None, season_year),
        "score": None,
        "hits": None,
        "errors": None,
        "line_score": [],
    }
    home_info = {
        "name": None,
        "code": team_code_from_game_id_segment(
            game_id[10:12] if len(game_id) >= GAME_ID_FULL_LEN else None, season_year
        ),
        "score": None,
        "hits": None,
        "errors": None,
        "line_score": [],
    }

    if scoreboard is not None and not scoreboard.empty:
        df = scoreboard.fillna(0)
        away_row = df.iloc[0]
        home_row = df.iloc[1] if len(df) > 1 else None

        def parse_row(row: pd.Series, info: dict[str, Any]) -> None:
            name = str(row.get("팀", "")).strip()
            if name:
                info["name"] = name
                resolved = resolve_team_code(name)
                if resolved:
                    info["code"] = resolved
            info["score"] = safe_int_or_none(row.get("R"))
            info["hits"] = safe_int_or_none(row.get("H"))
            info["errors"] = safe_int_or_none(row.get("E"))
            inning_cols = [col for col in row.index if re.fullmatch(r"\d+", str(col))]
            info["line_score"] = [safe_int_or_none(row[col]) for col in inning_cols]

        parse_row(away_row, away_info)
        if home_row is not None:
            parse_row(home_row, home_info)

    return {"away": away_info, "home": home_info}


def _build_hitter_payload(
    tables: list[pd.DataFrame],
    teams: dict[str, dict[str, Any]],
    db_session: Session | None = None,
) -> dict[str, list[dict[str, Any]]]:
    results = {"away": [], "home": []}
    team_cycle = ["away", "home"]
    team_index = 0

    for raw_df in tables:
        df = raw_df.fillna(0)
        team_side = team_cycle[team_index % 2]
        team_index += 1

        for _, row in df.iterrows():
            name = str(row.get("선수", "") or row.get("선수명", "")).strip()
            if not name or name in {"팀합계", "합계"}:
                continue

            p_id = _safe_player_id(row.get("선수ID") or row.get("playerId"))
            team_code = teams[team_side]["code"]
            if p_id is None and db_session:
                p_id = _resolve_missing_player_id(db_session, name, team_code)

            entry = {
                "player_id": p_id,
                "player_name": name,
                "team_code": team_code,
                "team_side": team_side,
                "batting_order": safe_int_or_none(row.get("타순")),
                "position": str(row.get("POS", "") or row.get("포지션", "")).strip() or None,
                "is_starter": safe_int_or_none(row.get("타순")) is not None and safe_int_or_none(row.get("타순")) <= 9,
                "stats": {
                    "plate_appearances": safe_int_or_none(row.get("타석")),
                    "at_bats": safe_int_or_none(row.get("타수")),
                    "runs": safe_int_or_none(row.get("득점")),
                    "hits": safe_int_or_none(row.get("안타")),
                    "doubles": safe_int_or_none(row.get("2루타")),
                    "triples": safe_int_or_none(row.get("3루타")),
                    "home_runs": safe_int_or_none(row.get("홈런")),
                    "rbi": safe_int_or_none(row.get("타점")),
                    "walks": safe_int_or_none(row.get("볼넷")),
                    "intentional_walks": safe_int_or_none(row.get("고의4구")),
                    "hbp": safe_int_or_none(row.get("사구")),
                    "strikeouts": safe_int_or_none(row.get("삼진")),
                    "stolen_bases": safe_int_or_none(row.get("도루")),
                    "caught_stealing": safe_int_or_none(row.get("도실")),
                    "sacrifice_hits": safe_int_or_none(row.get("희타")),
                    "sacrifice_flies": safe_int_or_none(row.get("희비")),
                    "gdp": safe_int_or_none(row.get("병살")),
                    "avg": safe_float_or_none(row.get("타율")),
                    "obp": safe_float_or_none(row.get("출루율")),
                    "slg": safe_float_or_none(row.get("장타율")),
                    "ops": safe_float_or_none(row.get("OPS")),
                    "iso": safe_float_or_none(row.get("ISO")),
                    "babip": safe_float_or_none(row.get("BABIP")),
                },
            }
            results[team_side].append(entry)

    return results


def _build_pitcher_payload(
    tables: list[pd.DataFrame],
    teams: dict[str, dict[str, Any]],
    db_session: Session | None = None,
) -> dict[str, list[dict[str, Any]]]:
    results = {"away": [], "home": []}
    team_cycle = ["away", "home"]
    team_index = 0

    for raw_df in tables:
        df = raw_df.fillna(0)
        team_side = team_cycle[team_index % 2]
        team_index += 1

        for _, row in df.iterrows():
            name = str(row.get("선수", "") or row.get("선수명", "")).strip()
            if not name or name in {"팀합계", "합계"}:
                continue

            p_id = _safe_player_id(row.get("선수ID") or row.get("playerId"))
            team_code = teams[team_side]["code"]
            if p_id is None and db_session:
                p_id = _resolve_missing_player_id(db_session, name, team_code)

            innings_text = str(row.get("이닝", "") or row.get("IP", "")).strip()
            entry = {
                "player_id": p_id,
                "player_name": name,
                "team_code": team_code,
                "team_side": team_side,
                "is_starting": len(results[team_side]) == 0,
                "stats": {
                    "innings_outs": parse_innings_to_outs(innings_text),
                    "batters_faced": safe_int_or_none(row.get("타자")),
                    "pitches": safe_int_or_none(row.get("투구수")),
                    "hits_allowed": safe_int_or_none(row.get("피안타")),
                    "runs_allowed": safe_int_or_none(row.get("실점")),
                    "earned_runs": safe_int_or_none(row.get("자책")),
                    "home_runs_allowed": safe_int_or_none(row.get("피홈런")),
                    "walks_allowed": safe_int_or_none(row.get("볼넷")),
                    "strikeouts": safe_int_or_none(row.get("삼진")),
                    "hit_batters": safe_int_or_none(row.get("사구")),
                    "wild_pitches": safe_int_or_none(row.get("폭투")),
                    "balks": safe_int_or_none(row.get("보크")),
                    "wins": safe_int_or_none(row.get("승")),
                    "losses": safe_int_or_none(row.get("패")),
                    "saves": safe_int_or_none(row.get("세")),
                    "holds": safe_int_or_none(row.get("홀드")),
                    "era": safe_float_or_none(row.get("ERA")),
                    "whip": safe_float_or_none(row.get("WHIP")),
                    "k_per_nine": safe_float_or_none(row.get("K/9")),
                    "bb_per_nine": safe_float_or_none(row.get("BB/9")),
                    "kbb": safe_float_or_none(row.get("K/BB")),
                },
            }
            decision = _parse_decision(row.get("결과"))
            if decision:
                entry["stats"]["decision"] = decision

            results[team_side].append(entry)

    return results


def _parse_metadata(soup: BeautifulSoup) -> dict[str, Any]:
    info_text = ""
    info_area = soup.select_one(".box-score-area, .game-info, .score-board")
    if info_area:
        info_text = info_area.get_text(" ", strip=True)

    metadata = {
        "stadium": None,
        "attendance": None,
        "start_time": None,
        "end_time": None,
        "game_time": None,
        "duration_minutes": None,
    }

    if info_text:
        stadium_match = re.search(r"구장\s*[:：]\s*([^\s]+)", info_text)
        if stadium_match:
            metadata["stadium"] = stadium_match.group(1)

        attendance_match = re.search(r"관중\s*[:：]\s*([\d,]+)", info_text)
        if attendance_match:
            metadata["attendance"] = safe_int_or_none(attendance_match.group(1))

        time_match = re.search(r"개시\s*[:：]\s*([\d:]+)", info_text)
        if time_match:
            metadata["start_time"] = time_match.group(1)

        end_match = re.search(r"종료\s*[:：]\s*([\d:]+)", info_text)
        if end_match:
            metadata["end_time"] = end_match.group(1)

        duration_match = re.search(r"경기시간\s*[:：]\s*([\d:]+)", info_text)
        if duration_match:
            metadata["game_time"] = duration_match.group(1)
            metadata["duration_minutes"] = _parse_duration_minutes(metadata["game_time"])

    return metadata


def _season_year_from_game(game_date: str) -> int | None:
    digits = "".join(ch for ch in str(game_date) if ch.isdigit())
    if len(digits) >= GAME_ID_YEAR_LEN:
        try:
            return int(digits[:4])
        except ValueError:
            return None
    return None


def _parse_decision(text: object) -> str | None:
    if not text:
        return None
    text = str(text)
    if "승" in text:
        return "W"
    if "패" in text:
        return "L"
    if "세" in text:
        return "S"
    if "홀드" in text or "H" in text:
        return "H"
    return None


def _safe_player_id(value: object) -> int | None:
    if value is None:
        return None
    value = str(value).strip()
    if not value.isdigit():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_duration_minutes(duration: str | None) -> int | None:
    if not duration:
        return None
    parts = duration.split(":")
    if len(parts) != 2:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        return hours * 60 + minutes
    except ValueError:
        return None


def _resolve_missing_player_id(db_session: Session, player_name: str, team_code: str) -> int | None:
    """
    Fallback resolution of player_id via name and team search.
    Useful for exhibition games where IDs are missing from the HTML.
    """
    if not db_session or not player_name:
        return None

    # Try name + team match (team name in player_basic is often Korean)
    # Mapping team_code to Korean name fragment might help but let's try direct first
    text("""
        SELECT player_id FROM player_basic
        WHERE (name = :name OR name = :name_space)
          AND (team LIKE :team OR team IS NULL OR :team_empty = 1)
        LIMIT 2
    """)
    # Handle space variations (e.g. '김 태연' vs '김태연')
    name_no_space = player_name.replace(" ", "")
    name_with_space = (
        player_name
        if " " in player_name
        else f"{player_name[0]} {player_name[1:]}"
        if len(player_name) > 2
        else player_name
    )

    try:
        # We don't have a reliable code -> Korean name mapping here easily
        # but let's try searching by name first and if multiple, filter by team
        rows = db_session.execute(
            text("SELECT player_id, team FROM player_basic WHERE name = :n1 OR name = :n2"),
            {"n1": name_no_space, "n2": name_with_space},
        ).fetchall()

        if not rows:
            return None
        if len(rows) == 1:
            return rows[0][0]

        # If multiple, try to find a team match
        # This is a heuristic - KBO team names in player_basic vary (e.g. '한화 이글스')
        for r_id, r_team in rows:
            if not r_team:
                continue
            # Check if team_code (e.g. 'HH') is represented in Korean team name (e.g. '한화')
            # This requires some knowledge of team mapping, but 'LIKE' is a start
            from src.utils.team_codes import STANDARD_TEAM_CODES

            team_meta = STANDARD_TEAM_CODES.get(team_code, {})
            k_name = team_meta.get("name", "")
            if k_name and k_name in r_team:
                return r_id

        # Last resort: just return first if we have to, but better to be safe
        return rows[0][0]
    except SQLAlchemyError:
        return None


__all__ = ["parse_game_detail_html"]
