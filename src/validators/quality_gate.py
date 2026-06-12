"""Statistical quality gate for KBO data."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

INVALID_TEAM_CODES = ("", "합계", "TOTAL", "ALL", "-")


class QualityGate:
    """Validate consistency between cumulative and game-by-game records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def _get_regular_season_ids(self, year: int) -> list[int]:
        """Fetch season_ids that correspond to Regular Season (league_type_code=0)."""
        stmt = text("SELECT season_id FROM kbo_seasons WHERE season_year = :year AND league_type_code = 0")
        result = self.session.execute(stmt, {"year": year}).scalars().all()
        return [int(r) for r in result]

    def _result(
        self,
        *,
        season: int,
        league: str,
        checked_players: int = 0,
        mismatches: list[dict[str, Any]] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        mismatches = mismatches or []
        return {
            "season": season,
            "league": league,
            "checked_players": checked_players,
            "mismatches": mismatches,
            "ok": not error and len(mismatches) == 0,
            "error": error,
        }

    @staticmethod
    def _valid_team_code_filters(model: Any) -> tuple[Any, ...]:
        from sqlalchemy import or_

        team_expr = func.coalesce(model.canonical_team_code, model.team_code)
        return (
            team_expr.isnot(None),
            or_(model.team_code.is_(None), model.team_code.not_in(INVALID_TEAM_CODES)),
        )

    def validate_season_batting(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """
        Compare PlayerSeasonBatting (cumulative) with GameBattingStat sum (transactional).
        """
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        # 1. Get cumulative totals per player
        cumulative_stmt = select(
            PlayerSeasonBatting.player_id,
            PlayerSeasonBatting.plate_appearances,
            PlayerSeasonBatting.hits,
            PlayerSeasonBatting.runs,
            PlayerSeasonBatting.home_runs,
        ).where(
            PlayerSeasonBatting.season == season,
            PlayerSeasonBatting.league == league,
        )
        cumulative_data = self.session.execute(cumulative_stmt).all()
        cumulative_map = {row.player_id: row for row in cumulative_data}

        # 2. Get transactional totals per player
        transactional_stmt = (
            select(
                GameBattingStat.player_id,
                func.sum(GameBattingStat.plate_appearances).label("pa"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.runs).label("runs"),
                func.sum(GameBattingStat.home_runs).label("hr"),
            )
            .join(
                Game,
                Game.game_id == GameBattingStat.game_id,
            )
            .where(
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.season_id.in_(reg_season_ids),
            )
            .group_by(GameBattingStat.player_id)
        )

        transactional_data = self.session.execute(transactional_stmt).all()
        mismatches = []
        for row in transactional_data:
            pid = row.player_id
            if pid is None:
                continue
            if pid not in cumulative_map:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Missing cumulative record",
                        "transactional": {"pa": row.pa, "hits": row.hits},
                    },
                )
                continue

            cum = cumulative_map[pid]
            # Allow 0.5% tolerance or small absolute diff (1-2 units)
            # because KBO site sometimes has sync delay between summary and detail
            diff = (row.pa or 0) - (cum.plate_appearances or 0)
            if diff > 2 and diff / (cum.plate_appearances or 1) > 0.005:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Transactional PA > Cumulative PA",
                        "cumulative": cum.plate_appearances,
                        "transactional": row.pa,
                    },
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(transactional_data),
            mismatches=mismatches,
        )

    @staticmethod
    def _resolve_pitching_cumulative_outs(cumulative_row: Any) -> int | None:
        cum_outs = cumulative_row.innings_outs
        if cum_outs is not None:
            return cum_outs
        if cumulative_row.extra_stats and "innings_outs" in cumulative_row.extra_stats:
            return int(cumulative_row.extra_stats["innings_outs"])
        if cumulative_row.innings_pitched is None:
            return None
        ip = float(cumulative_row.innings_pitched)
        whole = int(ip)
        frac = round((ip - whole) * 100)
        if frac == 33:
            return whole * 3 + 1
        if frac == 66:
            return whole * 3 + 2
        return whole * 3

    @staticmethod
    def _pitching_outs_mismatch(row: Any, cumulative_outs: int | None) -> dict[str, Any] | None:
        diff = (row.outs or 0) - (cumulative_outs or 0)
        if diff <= 3 or (cumulative_outs is not None and diff / (cumulative_outs or 1) <= 0.01):
            return None
        return {
            "player_id": row.player_id,
            "issue": "Transactional Outs > Cumulative Outs",
            "cumulative": cumulative_outs,
            "transactional": row.outs,
        }

    @staticmethod
    def _missing_pitching_cumulative_record(row: Any) -> dict[str, Any]:
        return {
            "player_id": row.player_id,
            "issue": "Missing cumulative record",
            "transactional": {"outs": row.outs, "wins": row.wins},
        }

    def validate_season_pitching(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """
        Compare PlayerSeasonPitching (cumulative) with GamePitchingStat sum (transactional).
        """
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        cumulative_stmt = select(
            PlayerSeasonPitching.player_id,
            PlayerSeasonPitching.innings_outs,
            PlayerSeasonPitching.innings_pitched,
            PlayerSeasonPitching.extra_stats,
            PlayerSeasonPitching.wins,
            PlayerSeasonPitching.strikeouts,
        ).where(
            PlayerSeasonPitching.season == season,
            PlayerSeasonPitching.league == league,
        )
        cumulative_data = self.session.execute(cumulative_stmt).all()
        cumulative_map = {row.player_id: row for row in cumulative_data}

        transactional_stmt = (
            select(
                GamePitchingStat.player_id,
                func.sum(GamePitchingStat.innings_outs).label("outs"),
                func.sum(GamePitchingStat.wins).label("wins"),
                func.sum(GamePitchingStat.strikeouts).label("so"),
            )
            .join(
                Game,
                Game.game_id == GamePitchingStat.game_id,
            )
            .where(
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.season_id.in_(reg_season_ids),
            )
            .group_by(GamePitchingStat.player_id)
        )

        transactional_data = self.session.execute(transactional_stmt).all()
        mismatches = []
        for row in transactional_data:
            pid = row.player_id
            if pid is None:
                continue
            if pid not in cumulative_map:
                mismatches.append(self._missing_pitching_cumulative_record(row))
                continue

            cum = cumulative_map[pid]
            cum_outs = self._resolve_pitching_cumulative_outs(cum)
            mismatch = self._pitching_outs_mismatch(row, cum_outs)
            if mismatch:
                mismatches.append(mismatch)

        return self._result(
            season=season,
            league=league,
            checked_players=len(transactional_data),
            mismatches=mismatches,
        )

    def validate_season_pa_formula(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """
        Validate PA = AB + BB + HBP + SH + SF consistency for game batting stats.
        """
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        # Get transactional totals per player from game_batting_stats
        transactional_stmt = (
            select(
                GameBattingStat.player_id,
                func.sum(GameBattingStat.plate_appearances).label("pa"),
                func.sum(GameBattingStat.at_bats).label("ab"),
                func.sum(GameBattingStat.walks).label("bb"),
                func.sum(GameBattingStat.hbp).label("hbp"),
                func.sum(GameBattingStat.sacrifice_hits).label("sh"),
                func.sum(GameBattingStat.sacrifice_flies).label("sf"),
            )
            .join(
                Game,
                Game.game_id == GameBattingStat.game_id,
            )
            .where(
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.season_id.in_(reg_season_ids),
            )
            .group_by(GameBattingStat.player_id)
        )

        transactional_data = self.session.execute(transactional_stmt).all()
        mismatches = []
        for row in transactional_data:
            pid = row.player_id
            if pid is None:
                continue

            # Calculate expected PA from components
            expected_pa = (row.ab or 0) + (row.bb or 0) + (row.hbp or 0) + (row.sh or 0) + (row.sf or 0)
            actual_pa = row.pa or 0

            if actual_pa != expected_pa:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "PA formula mismatch",
                        "expected_pa": expected_pa,
                        "actual_pa": actual_pa,
                        "difference": actual_pa - expected_pa,
                    },
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(transactional_data),
            mismatches=mismatches,
        )

    def validate_season_team_batting(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """Compare TeamSeasonBatting with PlayerSeasonBatting sum grouped by team."""
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        # 1. Get team-level totals from team_season_batting
        team_stmt = select(
            TeamSeasonBatting.team_id,
            TeamSeasonBatting.games,
            TeamSeasonBatting.plate_appearances,
            TeamSeasonBatting.at_bats,
            TeamSeasonBatting.runs,
            TeamSeasonBatting.hits,
            TeamSeasonBatting.doubles,
            TeamSeasonBatting.triples,
            TeamSeasonBatting.home_runs,
            TeamSeasonBatting.rbi,
            TeamSeasonBatting.stolen_bases,
            TeamSeasonBatting.caught_stealing,
            TeamSeasonBatting.walks,
            TeamSeasonBatting.strikeouts,
            TeamSeasonBatting.intentional_walks,
            TeamSeasonBatting.hbp,
            TeamSeasonBatting.sacrifice_hits,
            TeamSeasonBatting.sacrifice_flies,
            TeamSeasonBatting.gdp,
        ).where(
            TeamSeasonBatting.season == season,
            TeamSeasonBatting.league == league,
        )
        team_data = self.session.execute(team_stmt).all()
        team_map = {r.team_id: r for r in team_data}

        if not team_map:
            return self._result(season=season, league=league)

        # 2. Get player-level aggregates per team
        player_agg = (
            select(
                func.coalesce(PlayerSeasonBatting.canonical_team_code, PlayerSeasonBatting.team_code).label(
                    "team_code",
                ),
                func.sum(PlayerSeasonBatting.games).label("games"),
                func.sum(PlayerSeasonBatting.plate_appearances).label("plate_appearances"),
                func.sum(PlayerSeasonBatting.at_bats).label("at_bats"),
                func.sum(PlayerSeasonBatting.runs).label("runs"),
                func.sum(PlayerSeasonBatting.hits).label("hits"),
                func.sum(PlayerSeasonBatting.doubles).label("doubles"),
                func.sum(PlayerSeasonBatting.triples).label("triples"),
                func.sum(PlayerSeasonBatting.home_runs).label("home_runs"),
                func.sum(PlayerSeasonBatting.rbi).label("rbi"),
                func.sum(PlayerSeasonBatting.stolen_bases).label("stolen_bases"),
                func.sum(PlayerSeasonBatting.caught_stealing).label("caught_stealing"),
                func.sum(PlayerSeasonBatting.walks).label("walks"),
                func.sum(PlayerSeasonBatting.strikeouts).label("strikeouts"),
                func.sum(PlayerSeasonBatting.intentional_walks).label("intentional_walks"),
                func.sum(PlayerSeasonBatting.hbp).label("hbp"),
                func.sum(PlayerSeasonBatting.sacrifice_hits).label("sacrifice_hits"),
                func.sum(PlayerSeasonBatting.sacrifice_flies).label("sacrifice_flies"),
                func.sum(PlayerSeasonBatting.gdp).label("gdp"),
            )
            .where(
                PlayerSeasonBatting.season == season,
                PlayerSeasonBatting.league == league,
                *self._valid_team_code_filters(PlayerSeasonBatting),
            )
            .group_by(func.coalesce(PlayerSeasonBatting.canonical_team_code, PlayerSeasonBatting.team_code))
        )
        player_data = self.session.execute(player_agg).all()
        player_map = {r.team_code: r for r in player_data if r.team_code}

        mismatches = []
        for team_id, team_r in team_map.items():
            player_r = player_map.get(team_id)
            if player_r is None:
                mismatches.append(
                    {
                        "team_id": team_id,
                        "issue": "No player season batting records for this team",
                    },
                )
                continue

            stat_fields = [
                "plate_appearances",
                "at_bats",
                "runs",
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "rbi",
                "stolen_bases",
                "caught_stealing",
                "walks",
                "strikeouts",
                "intentional_walks",
                "hbp",
                "sacrifice_hits",
                "sacrifice_flies",
                "gdp",
            ]
            diffs = []
            for field in stat_fields:
                t_val = getattr(team_r, field) or 0
                p_val = getattr(player_r, field) or 0
                diff = abs(t_val - p_val)
                if diff > 5:
                    diffs.append(f"{field}: team={t_val} player_sum={p_val} diff={diff}")

            if diffs:
                mismatches.append(
                    {
                        "team_id": team_id,
                        "issue": "Team batting stats mismatch with player sum",
                        "diffs": diffs[:10],
                    },
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(team_map),
            mismatches=mismatches,
        )

    def validate_season_team_pitching(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """Compare TeamSeasonPitching with PlayerSeasonPitching sum grouped by team."""
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        # 1. Get team-level totals from team_season_pitching
        team_stmt = select(
            TeamSeasonPitching.team_id,
            TeamSeasonPitching.games,
            TeamSeasonPitching.wins,
            TeamSeasonPitching.losses,
            TeamSeasonPitching.saves,
            TeamSeasonPitching.holds,
            TeamSeasonPitching.innings_pitched,
            TeamSeasonPitching.runs_allowed,
            TeamSeasonPitching.earned_runs,
            TeamSeasonPitching.hits_allowed,
            TeamSeasonPitching.home_runs_allowed,
            TeamSeasonPitching.walks_allowed,
            TeamSeasonPitching.strikeouts,
            TeamSeasonPitching.innings_outs,
            TeamSeasonPitching.intentional_walks,
            TeamSeasonPitching.hit_batters,
            TeamSeasonPitching.tbf,
            TeamSeasonPitching.complete_games,
            TeamSeasonPitching.shutouts,
            TeamSeasonPitching.wild_pitches,
            TeamSeasonPitching.balks,
            TeamSeasonPitching.sacrifices_allowed,
            TeamSeasonPitching.sacrifice_flies_allowed,
        ).where(
            TeamSeasonPitching.season == season,
            TeamSeasonPitching.league == league,
        )
        team_data = self.session.execute(team_stmt).all()
        team_map = {r.team_id: r for r in team_data}

        if not team_map:
            return self._result(season=season, league=league)

        # 2. Get player-level aggregates per team
        player_agg = (
            select(
                func.coalesce(PlayerSeasonPitching.canonical_team_code, PlayerSeasonPitching.team_code).label(
                    "team_code",
                ),
                func.sum(PlayerSeasonPitching.games).label("games"),
                func.sum(PlayerSeasonPitching.wins).label("wins"),
                func.sum(PlayerSeasonPitching.losses).label("losses"),
                func.sum(PlayerSeasonPitching.saves).label("saves"),
                func.sum(PlayerSeasonPitching.holds).label("holds"),
                func.sum(PlayerSeasonPitching.innings_outs).label("innings_outs"),
                func.sum(PlayerSeasonPitching.runs_allowed).label("runs_allowed"),
                func.sum(PlayerSeasonPitching.earned_runs).label("earned_runs"),
                func.sum(PlayerSeasonPitching.hits_allowed).label("hits_allowed"),
                func.sum(PlayerSeasonPitching.home_runs_allowed).label("home_runs_allowed"),
                func.sum(PlayerSeasonPitching.walks_allowed).label("walks_allowed"),
                func.sum(PlayerSeasonPitching.strikeouts).label("strikeouts"),
                func.sum(PlayerSeasonPitching.intentional_walks).label("intentional_walks"),
                func.sum(PlayerSeasonPitching.hit_batters).label("hit_batters"),
                func.sum(PlayerSeasonPitching.tbf).label("tbf"),
                func.sum(PlayerSeasonPitching.complete_games).label("complete_games"),
                func.sum(PlayerSeasonPitching.shutouts).label("shutouts"),
                func.sum(PlayerSeasonPitching.wild_pitches).label("wild_pitches"),
                func.sum(PlayerSeasonPitching.balks).label("balks"),
                func.sum(PlayerSeasonPitching.sacrifices_allowed).label("sacrifices_allowed"),
                func.sum(PlayerSeasonPitching.sacrifice_flies_allowed).label("sacrifice_flies_allowed"),
            )
            .where(
                PlayerSeasonPitching.season == season,
                PlayerSeasonPitching.league == league,
                *self._valid_team_code_filters(PlayerSeasonPitching),
            )
            .group_by(func.coalesce(PlayerSeasonPitching.canonical_team_code, PlayerSeasonPitching.team_code))
        )
        player_data = self.session.execute(player_agg).all()
        player_map = {r.team_code: r for r in player_data if r.team_code}

        mismatches = []
        for team_id, team_r in team_map.items():
            player_r = player_map.get(team_id)
            if player_r is None:
                mismatches.append(
                    {
                        "team_id": team_id,
                        "issue": "No player season pitching records for this team",
                    },
                )
                continue

            stat_fields = [
                "wins",
                "losses",
                "saves",
                "holds",
                "runs_allowed",
                "earned_runs",
                "hits_allowed",
                "home_runs_allowed",
                "walks_allowed",
                "strikeouts",
                "innings_outs",
                "intentional_walks",
                "hit_batters",
                "tbf",
                "complete_games",
                "shutouts",
                "wild_pitches",
                "balks",
                "sacrifices_allowed",
                "sacrifice_flies_allowed",
            ]
            diffs = []
            for field in stat_fields:
                t_val = getattr(team_r, field) or 0
                p_val = getattr(player_r, field) or 0
                diff = abs(t_val - p_val)
                if diff > 5:
                    diffs.append(f"{field}: team={t_val} player_sum={p_val} diff={diff}")

            # Compare innings_pitched (special: float, not integer)
            team_ip = team_r.innings_pitched or 0.0
            player_outs = player_r.innings_outs or 0
            expected_outs = int(team_ip * 3 + 0.5) if team_ip else 0
            if abs(player_outs - expected_outs) > 3:
                diffs.append(f"innings_pitched: team_ip={team_ip} ({expected_outs} outs) player_outs={player_outs}")

            if diffs:
                mismatches.append(
                    {
                        "team_id": team_id,
                        "issue": "Team pitching stats mismatch with player sum",
                        "diffs": diffs[:10],
                    },
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(team_map),
            mismatches=mismatches,
        )


def run_quality_gate(session: Session, season: int) -> dict[str, Any]:
    gate = QualityGate(session)
    batting_result = gate.validate_season_batting(season)
    pitching_result = gate.validate_season_pitching(season)
    pa_formula_result = gate.validate_season_pa_formula(season)
    team_batting_result = gate.validate_season_team_batting(season)
    team_pitching_result = gate.validate_season_team_pitching(season)

    return {
        "batting": batting_result,
        "pitching": pitching_result,
        "pa_formula": pa_formula_result,
        "team_batting": team_batting_result,
        "team_pitching": team_pitching_result,
        "ok": batting_result.get("ok", False)
        and pitching_result.get("ok", False)
        and pa_formula_result.get("ok", False)
        and team_batting_result.get("ok", False)
        and team_pitching_result.get("ok", False),
    }
