"""Synthetic KBO data generator for creating mathematically invariant-compliant datasets."""

from __future__ import annotations

import logging
import random
import time
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.testing.dto import (
    SyntheticGenerationResult,
    SyntheticSeasonConfig,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

BATTERS_PER_LINEUP = 9
RUN_PROBABILITY_THRESHOLD = 0.7

STADIUM_MAP = {
    "LG": "잠실",
    "OB": "잠실",
    "SSG": "문학",
    "KT": "수원",
    "KIA": "광주",
    "NC": "창원",
    "SS": "대구",
    "LT": "사직",
    "HH": "대전",
    "WO": "고척",
}


class SyntheticKBOGenerator:
    """Generates synthetic KBO match datasets that strictly satisfy all quality gate formulas."""

    def __init__(self, seed: int = 42) -> None:
        """Initialize the synthetic generator with a deterministic seed."""
        self.random = random.Random(seed)

    def generate_players(self, team_codes: list[str], count_per_team: int = 15) -> list[PlayerBasic]:
        """Generate synthetic player profiles for the given teams."""
        players: list[PlayerBasic] = []
        base_id = 900_000

        for t_idx, team in enumerate(team_codes):
            for p_idx in range(count_per_team):
                p_id = base_id + (t_idx * 100) + p_idx
                is_pitcher = p_idx >= BATTERS_PER_LINEUP
                pos = "투수" if is_pitcher else ("내야수" if p_idx % 2 == 0 else "외야수")
                name = f"{team}_투수_{p_idx - 8}" if is_pitcher else f"{team}_타자_{p_idx + 1}"

                player = PlayerBasic(
                    player_id=p_id,
                    name=name,
                    team=team,
                    position=pos,
                    uniform_no=str(p_idx + 1),
                    birth_date=f"199{p_idx % 10}-0{max(1, p_idx % 9 + 1)}-15",
                )
                players.append(player)
        return players

    def generate_game(
        self,
        game_date: date,
        home_team: str,
        away_team: str,
        game_idx: int = 0,
        *,
        players_by_team: dict[str, list[PlayerBasic]] | None = None,
        include_pbp: bool = True,
    ) -> dict[str, Any]:
        """Generate a single full game graph with mathematically consistent boxscores and innings."""
        date_compact = game_date.strftime("%Y%m%d")
        game_id = f"{date_compact}{away_team}{home_team}{game_idx}"
        year = game_date.year

        # Generate inning runs ensuring deterministic positive scores
        away_innings_runs = [self.random.choice([0, 0, 1, 0, 2, 0, 1, 0, 0]) for _ in range(9)]
        home_innings_runs = [self.random.choice([0, 1, 0, 0, 1, 2, 0, 0, 1]) for _ in range(9)]
        away_score = sum(away_innings_runs)
        home_score = sum(home_innings_runs)
        if home_score == away_score:
            home_score += 1
            home_innings_runs[8] += 1

        game = Game(
            game_id=game_id,
            game_date=game_date,
            stadium=STADIUM_MAP.get(home_team, "잠실"),
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            season_id=year,
            game_status="COMPLETED",
            is_primary=True,
        )

        metadata = GameMetadata(
            game_id=game_id,
            stadium_name=STADIUM_MAP.get(home_team, "잠실"),
            start_time="18:30",
            end_time="21:45",
            attendance=self.random.randint(10000, 23750),
        )

        innings: list[GameInningScore] = []
        for i in range(1, 10):
            innings.append(
                GameInningScore(
                    game_id=game_id,
                    team_side="away",
                    team_code=away_team,
                    inning=i,
                    runs=away_innings_runs[i - 1],
                )
            )
            innings.append(
                GameInningScore(
                    game_id=game_id,
                    team_side="home",
                    team_code=home_team,
                    inning=i,
                    runs=home_innings_runs[i - 1],
                )
            )

        lineups: list[GameLineup] = []
        batting_stats: list[GameBattingStat] = []
        pitching_stats: list[GamePitchingStat] = []

        home_players = (players_by_team or {}).get(home_team, [])
        away_players = (players_by_team or {}).get(away_team, [])

        # Build Lineups & Stats for Home & Away
        for side, team_code, team_players, opp_score in [
            ("away", away_team, away_players, home_score),
            ("home", home_team, home_players, away_score),
        ]:
            for order in range(1, 10):
                p = team_players[order - 1] if len(team_players) >= order else None
                p_id = p.player_id if p else 990000 + order
                p_name = p.name if p else f"{team_code}_타자_{order}"

                lineups.append(
                    GameLineup(
                        game_id=game_id,
                        team_side=side,
                        team_code=team_code,
                        batting_order=order,
                        position=p.position if p else "외야수",
                        player_id=p_id,
                        player_name=p_name,
                        is_starter=True,
                        appearance_seq=order,
                    )
                )

                # Invariant: PA = AB + BB + HBP + SH + SF, H = 1B + 2B + 3B + HR
                d1 = self.random.choice([0, 1, 1, 0, 2])
                d2 = self.random.choice([0, 0, 1, 0])
                d3 = 0
                hr = self.random.choice([0, 0, 0, 1])
                hits = d1 + d2 + d3 + hr
                outs = self.random.choice([2, 3, 2])
                ab = hits + outs
                bb = self.random.choice([0, 1, 0])
                hbp = 0
                sh = 0
                sf = 0
                pa = ab + bb + hbp + sh + sf
                runs = 1 if hr > 0 or self.random.random() > RUN_PROBABILITY_THRESHOLD else 0
                rbi = hr * 2 if hr > 0 else (1 if hits > 1 else 0)

                batting_stats.append(
                    GameBattingStat(
                        game_id=game_id,
                        team_side=side,
                        team_code=team_code,
                        player_id=p_id,
                        player_name=p_name,
                        batting_order=order,
                        is_starter=True,
                        appearance_seq=order,
                        plate_appearances=pa,
                        at_bats=ab,
                        hits=hits,
                        doubles=d2,
                        triples=d3,
                        home_runs=hr,
                        runs=runs,
                        rbi=rbi,
                        walks=bb,
                        hbp=hbp,
                        sacrifice_hits=sh,
                        sacrifice_flies=sf,
                        strikeouts=self.random.choice([0, 1, 2]),
                    )
                )

            # Starting Pitcher Stat
            starter = team_players[BATTERS_PER_LINEUP] if len(team_players) > BATTERS_PER_LINEUP else None
            sp_id = starter.player_id if starter else 999001
            sp_name = starter.name if starter else f"{team_code}_선발투수"

            lineups.append(
                GameLineup(
                    game_id=game_id,
                    team_side=side,
                    team_code=team_code,
                    batting_order=10,
                    position="투수",
                    player_id=sp_id,
                    player_name=sp_name,
                    is_starter=True,
                    appearance_seq=10,
                )
            )

            pitching_stats.append(
                GamePitchingStat(
                    game_id=game_id,
                    team_side=side,
                    team_code=team_code,
                    player_id=sp_id,
                    player_name=sp_name,
                    is_starting=True,
                    appearance_seq=1,
                    innings_pitched=6.0,
                    hits_allowed=self.random.randint(3, 7),
                    runs_allowed=opp_score,
                    earned_runs=opp_score,
                    walks_allowed=self.random.randint(1, 4),
                    strikeouts=self.random.randint(3, 8),
                    batters_faced=25,
                    pitches=self.random.randint(85, 105),
                )
            )

        pbp_events: list[GamePlayByPlay] = []
        if include_pbp:
            for i in range(1, 10):
                pbp_events.append(
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=i,
                        inning_half="초",
                        batter_name=f"{away_team}_타자_{(i - 1) % 9 + 1}",
                        pitcher_name=f"{home_team}_선발투수",
                        play_description=f"{i}회초 {away_team} 공격: {away_innings_runs[i - 1]}점 득점",
                        event_type="HIT" if away_innings_runs[i - 1] > 0 else "OUT",
                    )
                )
                pbp_events.append(
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=i,
                        inning_half="말",
                        batter_name=f"{home_team}_타자_{(i - 1) % 9 + 1}",
                        pitcher_name=f"{away_team}_선발투수",
                        play_description=f"{i}회말 {home_team} 공격: {home_innings_runs[i - 1]}점 득점",
                        event_type="HIT" if home_innings_runs[i - 1] > 0 else "OUT",
                    )
                )

        summary = GameSummary(
            game_id=game_id,
            summary_type="경기총평",
            detail_text=(
                f"{home_team}이(가) {away_team}을(를) 상대로 {home_score}:{away_score}로 승리를 거두었습니다."
            ),
        )

        return {
            "game": game,
            "metadata": metadata,
            "innings": innings,
            "lineups": lineups,
            "batting_stats": batting_stats,
            "pitching_stats": pitching_stats,
            "pbp_events": pbp_events,
            "summary": summary,
        }

    def generate_season(self, config: SyntheticSeasonConfig) -> dict[str, Any]:
        """Generate a complete synthetic season dataset matching the provided configuration."""
        players = self.generate_players(config.team_codes, config.players_per_team)
        players_by_team: dict[str, list[PlayerBasic]] = {}
        for p in players:
            players_by_team.setdefault(p.team, []).append(p)

        all_games: list[Game] = []
        all_metadata: list[GameMetadata] = []
        all_innings: list[GameInningScore] = []
        all_lineups: list[GameLineup] = []
        all_batting: list[GameBattingStat] = []
        all_pitching: list[GamePitchingStat] = []
        all_pbp: list[GamePlayByPlay] = []
        all_summaries: list[GameSummary] = []

        start_date = date(config.season_year, 4, 1)
        teams = config.team_codes
        game_counter = 0

        for g_idx in range(config.games_per_team):
            g_date = start_date + timedelta(days=g_idx)
            for i in range(0, len(teams) - 1, 2):
                home = teams[i]
                away = teams[i + 1]
                game_graph = self.generate_game(
                    game_date=g_date,
                    home_team=home,
                    away_team=away,
                    game_idx=0,
                    players_by_team=players_by_team,
                    include_pbp=config.include_pbp,
                )
                all_games.append(game_graph["game"])
                all_metadata.append(game_graph["metadata"])
                all_innings.extend(game_graph["innings"])
                all_lineups.extend(game_graph["lineups"])
                all_batting.extend(game_graph["batting_stats"])
                all_pitching.extend(game_graph["pitching_stats"])
                all_pbp.extend(game_graph["pbp_events"])
                all_summaries.append(game_graph["summary"])
                game_counter += 1

        return {
            "players": players,
            "games": all_games,
            "metadata": all_metadata,
            "innings": all_innings,
            "lineups": all_lineups,
            "batting_stats": all_batting,
            "pitching_stats": all_pitching,
            "pbp_events": all_pbp,
            "summaries": all_summaries,
        }

    def seed_to_database(self, session: Session, dataset: dict[str, Any]) -> SyntheticGenerationResult:
        """Persist generated synthetic entities into the target database session."""
        start_mono = time.monotonic()

        for player in dataset.get("players", []):
            session.merge(player)

        for game in dataset.get("games", []):
            session.merge(game)

        for meta in dataset.get("metadata", []):
            session.merge(meta)

        for inning in dataset.get("innings", []):
            session.merge(inning)

        for lineup in dataset.get("lineups", []):
            session.merge(lineup)

        for b_stat in dataset.get("batting_stats", []):
            session.merge(b_stat)

        for p_stat in dataset.get("pitching_stats", []):
            session.merge(p_stat)

        for pbp in dataset.get("pbp_events", []):
            session.merge(pbp)

        for summary in dataset.get("summaries", []):
            session.merge(summary)

        session.flush()
        elapsed = time.monotonic() - start_mono

        return SyntheticGenerationResult(
            total_games=len(dataset.get("games", [])),
            total_players=len(dataset.get("players", [])),
            total_lineups=len(dataset.get("lineups", [])),
            total_pbp_events=len(dataset.get("pbp_events", [])),
            elapsed_seconds=elapsed,
        )
