"""Parse KBO GameCenter REVIEW HTML into structured box scores."""
from __future__ import annotations

import re
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment


def parse_game_detail_html(html: str, game_id: str, game_date: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    dataframes = pd.read_html(StringIO(html))

    scoreboard_df = _extract_scoreboard(dataframes)
    hitter_tables = _extract_hitter_tables(dataframes)
    pitcher_tables = _extract_pitcher_tables(dataframes)

    season_year = _season_year_from_game(game_date)
    teams = _build_team_info(scoreboard_df, game_id, season_year)
    hitters = _build_hitter_payload(hitter_tables, teams)
    pitchers = _build_pitcher_payload(pitcher_tables, teams)

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


def _extract_scoreboard(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    for df in tables:
        headers = [str(col) for col in df.columns]
        if {"팀", "R", "H", "E"}.issubset(headers):
            return df
    return None


def _extract_hitter_tables(tables: List[pd.DataFrame]) -> List[pd.DataFrame]:
    hitters = []
    for df in tables:
        headers = [str(col) for col in df.columns]
        if any("타수" in h for h in headers):
            hitters.append(df)
    return hitters


def _extract_pitcher_tables(tables: List[pd.DataFrame]) -> List[pd.DataFrame]:
    pitchers = []
    for df in tables:
        headers = [str(col) for col in df.columns]
        if any(h in ("이닝", "IP") for h in headers) and "삼진" in headers:
            pitchers.append(df)
    return pitchers


def _build_team_info(scoreboard: Optional[pd.DataFrame], game_id: str, season_year: Optional[int]) -> Dict[str, Dict[str, Any]]:
    away_info = {
        "name": None,
        "code": team_code_from_game_id_segment(game_id[8:10] if len(game_id) >= 10 else None, season_year),
        "score": None,
        "hits": None,
        "errors": None,
        "line_score": [],
    }
    home_info = {
        "name": None,
        "code": team_code_from_game_id_segment(game_id[10:12] if len(game_id) >= 12 else None, season_year),
        "score": None,
        "hits": None,
        "errors": None,
        "line_score": [],
    }

    if scoreboard is not None and not scoreboard.empty:
        df = scoreboard.fillna(0)
        away_row = df.iloc[0]
        home_row = df.iloc[1] if len(df) > 1 else None

        def parse_row(row: pd.Series, info: Dict[str, Any]) -> None:
            name = str(row.get("팀", "")).strip()
            if name:
                info["name"] = name
                resolved = resolve_team_code(name)
                if resolved:
                    info["code"] = resolved
            info["score"] = _safe_int(row.get("R"))
            info["hits"] = _safe_int(row.get("H"))
            info["errors"] = _safe_int(row.get("E"))
            inning_cols = [col for col in row.index if re.fullmatch(r"\d+", str(col))]
            info["line_score"] = [_safe_int(row[col]) for col in inning_cols]

        parse_row(away_row, away_info)
        if home_row is not None:
            parse_row(home_row, home_info)

    return {"away": away_info, "home": home_info}


def _build_hitter_payload(tables: List[pd.DataFrame], teams: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    results = {"away": [], "home": []}
    team_cycle = ["away", "home"]
    team_index = 0

    for df in tables:
        df = df.fillna(0)
        team_side = team_cycle[team_index % 2]
        team_index += 1

        for _, row in df.iterrows():
            name = str(row.get("선수", "") or row.get("선수명", "")).strip()
            if not name or name in {"팀합계", "합계"}:
                continue
            entry = {
                "player_id": _safe_player_id(row.get("선수ID") or row.get("playerId")),
                "player_name": name,
                "team_code": teams[team_side]["code"],
                "team_side": team_side,
                "batting_order": _safe_int(row.get("타순")),
                "position": str(row.get("POS", "") or row.get("포지션", "")).strip() or None,
                "is_starter": _safe_int(row.get("타순")) is not None and _safe_int(row.get("타순")) <= 9,
                "stats": {
                    "plate_appearances": _safe_int(row.get("타석")),
                    "at_bats": _safe_int(row.get("타수")),
                    "runs": _safe_int(row.get("득점")),
                    "hits": _safe_int(row.get("안타")),
                    "doubles": _safe_int(row.get("2루타")),
                    "triples": _safe_int(row.get("3루타")),
                    "home_runs": _safe_int(row.get("홈런")),
                    "rbi": _safe_int(row.get("타점")),
                    "walks": _safe_int(row.get("볼넷")),
                    "intentional_walks": _safe_int(row.get("고의4구")),
                    "hbp": _safe_int(row.get("사구")),
                    "strikeouts": _safe_int(row.get("삼진")),
                    "stolen_bases": _safe_int(row.get("도루")),
                    "caught_stealing": _safe_int(row.get("도실")),
                    "sacrifice_hits": _safe_int(row.get("희타")),
                    "sacrifice_flies": _safe_int(row.get("희비")),
                    "gdp": _safe_int(row.get("병살")),
                    "avg": _safe_float(row.get("타율")),
                    "obp": _safe_float(row.get("출루율")),
                    "slg": _safe_float(row.get("장타율")),
                    "ops": _safe_float(row.get("OPS")),
                    "iso": _safe_float(row.get("ISO")),
                    "babip": _safe_float(row.get("BABIP")),
                },
            }
            results[team_side].append(entry)

    return results


def _build_pitcher_payload(tables: List[pd.DataFrame], teams: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    results = {"away": [], "home": []}
    team_cycle = ["away", "home"]
    team_index = 0

    for df in tables:
        df = df.fillna(0)
        team_side = team_cycle[team_index % 2]
        team_index += 1

        for _, row in df.iterrows():
            name = str(row.get("선수", "") or row.get("선수명", "")).strip()
            if not name or name in {"팀합계", "합계"}:
                continue

            innings_text = str(row.get("이닝", "") or row.get("IP", "")).strip()
            entry = {
                "player_id": _safe_player_id(row.get("선수ID") or row.get("playerId")),
                "player_name": name,
                "team_code": teams[team_side]["code"],
                "team_side": team_side,
                "is_starting": len(results[team_side]) == 0,
                "stats": {
                    "innings_outs": _parse_innings_to_outs(innings_text),
                    "batters_faced": _safe_int(row.get("타자")),
                    "pitches": _safe_int(row.get("투구수")),
                    "hits_allowed": _safe_int(row.get("피안타")),
                    "runs_allowed": _safe_int(row.get("실점")),
                    "earned_runs": _safe_int(row.get("자책")),
                    "home_runs_allowed": _safe_int(row.get("피홈런")),
                    "walks_allowed": _safe_int(row.get("볼넷")),
                    "strikeouts": _safe_int(row.get("삼진")),
                    "hit_batters": _safe_int(row.get("사구")),
                    "wild_pitches": _safe_int(row.get("폭투")),
                    "balks": _safe_int(row.get("보크")),
                    "wins": _safe_int(row.get("승")),
                    "losses": _safe_int(row.get("패")),
                    "saves": _safe_int(row.get("세")),
                    "holds": _safe_int(row.get("홀드")),
                    "era": _safe_float(row.get("ERA")),
                    "whip": _safe_float(row.get("WHIP")),
                    "k_per_nine": _safe_float(row.get("K/9")),
                    "bb_per_nine": _safe_float(row.get("BB/9")),
                    "kbb": _safe_float(row.get("K/BB")),
                },
            }
            decision = _parse_decision(row.get("결과"))
            if decision:
                entry["stats"]["decision"] = decision

            results[team_side].append(entry)

    return results


def _parse_metadata(soup: BeautifulSoup) -> Dict[str, Any]:
    info_text = ""
    info_area = soup.select_one('.box-score-area, .game-info, .score-board')
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
        stadium_match = re.search(r'구장\s*[:：]\s*([^\s]+)', info_text)
        if stadium_match:
            metadata['stadium'] = stadium_match.group(1)

        attendance_match = re.search(r'관중\s*[:：]\s*([\d,]+)', info_text)
        if attendance_match:
            metadata['attendance'] = _safe_int(attendance_match.group(1))

        time_match = re.search(r'개시\s*[:：]\s*([\d:]+)', info_text)
        if time_match:
            metadata['start_time'] = time_match.group(1)

        end_match = re.search(r'종료\s*[:：]\s*([\d:]+)', info_text)
        if end_match:
            metadata['end_time'] = end_match.group(1)

        duration_match = re.search(r'경기시간\s*[:：]\s*([\d:]+)', info_text)
        if duration_match:
            metadata['game_time'] = duration_match.group(1)
            metadata['duration_minutes'] = _parse_duration_minutes(metadata['game_time'])

    return metadata


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, "", "-", "null"):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "null"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _season_year_from_game(game_date: str) -> Optional[int]:
    digits = ''.join(ch for ch in str(game_date) if ch.isdigit())
    if len(digits) >= 4:
        try:
            return int(digits[:4])
        except ValueError:
            return None
    return None


def _parse_innings_to_outs(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    cleaned = str(text).strip()
    if cleaned in ("", "-"):
        return None
    cleaned = cleaned.replace('⅓', '.1').replace('⅔', '.2')
    match = re.match(r'^(\d+)(?:\.(\d))?$', cleaned)
    if match:
        whole = int(match.group(1))
        frac = int(match.group(2)) if match.group(2) else 0
        return whole * 3 + frac
    try:
        value = float(cleaned)
        return int(round(value * 3))
    except ValueError:
        return None


def _parse_decision(text: Any) -> Optional[str]:
    if not text:
        return None
    text = str(text)
    if '승' in text:
        return 'W'
    if '패' in text:
        return 'L'
    if '세' in text:
        return 'S'
    if '홀드' in text or 'H' in text:
        return 'H'
    return None


def _safe_player_id(value: Any) -> Optional[int]:
    if value is None:
        return None
    value = str(value).strip()
    if not value.isdigit():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_duration_minutes(duration: Optional[str]) -> Optional[int]:
    if not duration:
        return None
    parts = duration.split(':')
    if len(parts) != 2:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        return hours * 60 + minutes
    except ValueError:
        return None


__all__ = ["parse_game_detail_html"]
