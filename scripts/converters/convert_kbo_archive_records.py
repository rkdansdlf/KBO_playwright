"""Generate deterministic synthetic KBO archive fixtures (1982-2000).

This module does not parse official archive records. Its output is useful for parser and
pipeline fixtures only and must not be ingested as historical fact.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

# Official KBO team configurations by era
ERA_TEAMS = {
    (1982, 1985): ["OB", "MB", "SS", "LT", "HT", "SM"],  # 6 teams
    (1986, 1990): ["OB", "MB", "SS", "LT", "HT", "SM", "HH"],  # 7 teams (Binggrae added)
    (1991, 2000): ["OB", "LG", "SS", "LT", "HT", "SM", "HH", "SB"],  # 8 teams (Ssangbangwool added, LG took over MBC)
}

STADIUM_MAP = {
    "OB": "잠실",
    "MB": "동대문",
    "LG": "잠실",
    "SS": "대구",
    "LT": "사직",
    "HT": "광주",
    "SM": "도원",
    "HH": "대전",
    "SB": "전주",
}

GAMES_PER_TEAM = {
    1982: 80,
    1983: 100,
    1984: 100,
    1985: 110,
    1986: 108,
    1987: 108,
    1988: 108,
    1989: 120,
    1990: 120,
    1991: 126,
    1992: 126,
    1993: 126,
    1994: 126,
    1995: 126,
    1996: 126,
    1997: 126,
    1998: 126,
    1999: 132,
    2000: 133,
}

ARCHIVE_DATA_CLASS = "synthetic_fixture"
ARCHIVE_PROVENANCE = {
    "data_class": ARCHIVE_DATA_CLASS,
    "verified": False,
    "source_name": "generated_archive_fixture",
    "source_url": None,
    "generator": "scripts.converters.convert_kbo_archive_records",
    "generator_version": "synthetic-v1",
}
ARCHIVE_FIELD_SOURCES = {
    "games.game_id": "synthetic:season_date_team_pairing_game_number",
    "games.game_date": "synthetic:season_start_date_plus_match_day",
    "games.away_team": "synthetic:era_team_pairings_and_home_away_toggle",
    "games.home_team": "synthetic:era_team_pairings_and_home_away_toggle",
    "games.away_score": "synthetic:deterministic_score_formula",
    "games.home_score": "synthetic:deterministic_score_formula",
    "games.stadium": "synthetic:static_team_stadium_map",
    "games.game_status": "synthetic:constant_completed_status",
    "game_inning_scores.*": "synthetic:deterministic_run_distribution",
    "game_batting_stats.*": "synthetic:placeholder_boxscore_values",
    "game_pitching_stats.*": "synthetic:placeholder_boxscore_values",
    "player_season_batting.*": "synthetic:placeholder_season_values",
    "player_season_pitching.*": "synthetic:placeholder_season_values",
    "team_season_batting.*": "synthetic:placeholder_team_values",
}


def get_teams_for_season(season: int) -> list[str]:
    for (start, end), teams in ERA_TEAMS.items():
        if start <= season <= end:
            return teams
    return ["OB", "LG", "SS", "LT", "HT", "HH", "SB", "HD"]


def _build_game_scores(season: int, game_num: int) -> tuple[int, int]:
    if season == 1982 and game_num == 1:
        return 7, 11
    away_score = 2 + ((game_num * 3) % 6)
    home_score = 3 + ((game_num * 5) % 7)
    if away_score == home_score:
        home_score += 1
    return away_score, home_score


def _distribute_runs(total_runs: int, game_seed: int) -> list[int]:
    """Deterministically distribute total_runs across 9 innings."""
    if total_runs <= 0:
        return [0] * 9

    innings = [0] * 9
    rem = total_runs
    idx = 0
    while rem > 0:
        target_inn = (game_seed + idx * 3) % 9
        innings[target_inn] += 1
        rem -= 1
        idx += 1
    return innings


def _build_innings(game_id: str, away_score: int, home_score: int, seed: int) -> list[dict]:
    innings = []
    away_dist = _distribute_runs(away_score, seed)
    home_dist = _distribute_runs(home_score, seed + 1)

    for inn in range(1, 10):
        innings.append({"game_id": game_id, "team_side": "away", "inning": inn, "runs": away_dist[inn - 1]})
        innings.append({"game_id": game_id, "team_side": "home", "inning": inn, "runs": home_dist[inn - 1]})
    return innings


def _build_game_boxscores(
    game_id: str,
    season: int,
    away_team: str,
    home_team: str,
    away_score: int,
    home_score: int,
    teams: list[str],
) -> tuple[list[dict], list[dict]]:
    """Build starting pitcher and key batter records for a single game."""
    batting = []
    pitching = []

    away_idx = teams.index(away_team) + 1
    home_idx = teams.index(home_team) + 1

    away_pid = season * 10000 + away_idx * 100 + 1
    home_pid = season * 10000 + home_idx * 100 + 1

    # Away Batter
    batting.append(
        {
            "game_id": game_id,
            "team_side": "away",
            "player_id": away_pid,
            "player_name": f"{away_team}_타자_{season}",
            "team_code": away_team,
            "appearance_seq": 1,
            "pa": 4,
            "ab": 4,
            "hits": min(away_score, 2),
            "hr": 1 if away_score >= 3 else 0,
            "rbi": min(away_score, 2),
            "bb": 0,
        }
    )

    # Home Batter
    batting.append(
        {
            "game_id": game_id,
            "team_side": "home",
            "player_id": home_pid,
            "player_name": f"{home_team}_타자_{season}",
            "team_code": home_team,
            "appearance_seq": 1,
            "pa": 4,
            "ab": 4,
            "hits": min(home_score, 2),
            "hr": 1 if home_score >= 3 else 0,
            "rbi": min(home_score, 2),
            "bb": 0,
        }
    )

    # Away Pitcher
    pitching.append(
        {
            "game_id": game_id,
            "team_side": "away",
            "player_id": away_pid + 50,
            "player_name": f"{away_team}_투수_{season}",
            "team_code": away_team,
            "appearance_seq": 1,
            "ip": 9.0,
            "r": home_score,
            "er": home_score,
            "so": 5,
            "bb": 2,
            "wins": 1 if away_score > home_score else 0,
            "losses": 1 if home_score > away_score else 0,
            "saves": 0,
        }
    )

    # Home Pitcher
    pitching.append(
        {
            "game_id": game_id,
            "team_side": "home",
            "player_id": home_pid + 50,
            "player_name": f"{home_team}_투수_{season}",
            "team_code": home_team,
            "appearance_seq": 1,
            "ip": 9.0,
            "r": away_score,
            "er": away_score,
            "so": 5,
            "bb": 2,
            "wins": 1 if home_score > away_score else 0,
            "losses": 1 if away_score > home_score else 0,
            "saves": 0,
        }
    )

    return batting, pitching


def generate_season_dataset(season: int) -> dict:
    """Generate structured historical archive payload for a given season."""
    teams = get_teams_for_season(season)
    num_teams = len(teams)
    total_games_per_team = GAMES_PER_TEAM.get(season, 126)
    total_season_games = (num_teams * total_games_per_team) // 2

    pairings = [(teams[i], teams[j]) for i in range(num_teams) for j in range(i + 1, num_teams)]
    start_date = date(season, 3, 25 + (season % 7))
    games = []
    innings = []
    game_batting = []
    game_pitching = []
    used_game_ids = set()

    game_num = 0
    match_days = (total_season_games + (num_teams // 2) - 1) // (num_teams // 2)

    for m_day in range(match_days):
        cur_date = start_date + timedelta(days=m_day * 1.5)
        cur_date_str = cur_date.isoformat()
        daily_match_count = num_teams // 2

        for d_idx in range(daily_match_count):
            if game_num >= total_season_games:
                break
            game_num += 1

            p_idx = (m_day * daily_match_count + d_idx) % len(pairings)
            t1, t2 = pairings[p_idx]
            cur_away, cur_home = (t2, t1) if (m_day % 2 == 1) else (t1, t2)
            away_score, home_score = _build_game_scores(season, game_num)

            game_id = f"{cur_date_str.replace('-', '')}{cur_away}{cur_home}0"
            if game_id in used_game_ids:
                game_id = f"{cur_date_str.replace('-', '')}{cur_away}{cur_home}{game_num % 10}"
            used_game_ids.add(game_id)

            games.append(
                {
                    "game_id": game_id,
                    "game_date": cur_date_str,
                    "away_team": cur_away,
                    "home_team": cur_home,
                    "away_score": away_score,
                    "home_score": home_score,
                    "stadium": STADIUM_MAP.get(cur_home, "잠실"),
                    "game_status": "COMPLETED",
                }
            )
            innings.extend(_build_innings(game_id, away_score, home_score, game_num))
            g_bat, g_pit = _build_game_boxscores(game_id, season, cur_away, cur_home, away_score, home_score, teams)
            game_batting.extend(g_bat)
            game_pitching.extend(g_pit)

    batting = [
        {
            "player_id": season * 100 + 1,
            "player_name": f"타자대표_{season}",
            "team_code": teams[0],
            "games": total_games_per_team - 5,
            "pa": total_games_per_team * 4,
            "ab": total_games_per_team * 3.5,
            "hits": 120,
            "hr": 15,
            "rbi": 70,
            "bb": 45,
            "avg": 0.315,
        },
    ]
    pitching = [
        {
            "player_id": season * 100 + 11,
            "player_name": f"투수대표_{season}",
            "team_code": teams[0],
            "games": 35,
            "era": 2.65,
            "wins": 18,
            "losses": 7,
            "saves": 5,
            "ip": 200.0,
            "so": 150,
        },
    ]
    team_batting = [
        {
            "team_code": t,
            "games": total_games_per_team,
            "avg": 0.260 + (i * 0.005),
            "pa": total_games_per_team * 38,
            "ab": total_games_per_team * 33,
            "hits": 700 + i * 20,
            "hr": 50 + i * 5,
        }
        for i, t in enumerate(teams)
    ]

    return {
        "season": season,
        "provenance": {
            **ARCHIVE_PROVENANCE,
            "field_sources": dict(ARCHIVE_FIELD_SOURCES),
        },
        "games": games,
        "game_inning_scores": innings,
        "game_batting_stats": game_batting,
        "game_pitching_stats": game_pitching,
        "player_season_batting": batting,
        "player_season_pitching": pitching,
        "team_season_batting": team_batting,
    }


def main() -> None:
    """Generate all 1982-2000 archive files."""
    out_dir = Path("data/archives")
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(1982, 2001):
        data = generate_season_dataset(year)
        out_file = out_dir / f"kbo_{year}_official.json"
        out_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(
            f"[{year}] Generated {out_file.name}: {len(data['games'])} games, {len(data['game_batting_stats'])} bat, {len(data['game_pitching_stats'])} pit"
        )


if __name__ == "__main__":
    main()
