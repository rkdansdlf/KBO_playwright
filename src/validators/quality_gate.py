"""Statistical quality gate for KBO data."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy import func, or_, select, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only

from src.constants import IP_FRAC_THIRD, IP_FRAC_TWO_THIRDS, MAX_OUTS
from src.models.base import Base
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.team_codes import STANDARD_TEAM_CODES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

AGGREGATE_TEAM_CODES = ("", "합계", "TOTAL", "ALL", "-")
INVALID_TEAM_CODES = (*AGGREGATE_TEAM_CODES, "EA", "WE")
BATTING_PA_ABSOLUTE_TOLERANCE = 2
BATTING_PA_RELATIVE_TOLERANCE = 0.005
PITCHING_OUTS_RELATIVE_TOLERANCE = 0.01
TEAM_STAT_ABSOLUTE_TOLERANCE = 5
FUTURES_BATTING_TOLERANCE = 0.005
FUTURES_PITCHING_TOLERANCE = 0.01
FUTURES_FIP_TOLERANCE = 0.02


class PitchingCumulativeRow(Protocol):
    """Minimal row shape needed to resolve cumulative pitching outs."""

    innings_outs: int | None
    extra_stats: dict[str, Any] | None
    innings_pitched: float | str | None


def _batting_pa_mismatch(diff: int, cumulative_pa: int) -> bool:
    """Return True when transactional PA exceeds cumulative PA beyond tolerance."""
    return diff > BATTING_PA_ABSOLUTE_TOLERANCE and diff / (cumulative_pa or 1) > BATTING_PA_RELATIVE_TOLERANCE


def _team_stat_mismatch(diff: int, threshold: int = TEAM_STAT_ABSOLUTE_TOLERANCE) -> bool:
    """Return True when absolute team/player stat difference exceeds threshold."""
    return abs(diff) > threshold


def _pa_formula_expected(
    ab: int | None,
    bb: int | None,
    hbp: int | None,
    sh: int | None,
    sf: int | None,
) -> int:
    """Return expected PA formula: AB + BB + HBP + SH + SF."""
    return (ab or 0) + (bb or 0) + (hbp or 0) + (sh or 0) + (sf or 0)


def _ip_to_outs_float(team_ip: float | None) -> int:
    """Convert team innings_pitched (float) to outs (int), round-half-up."""
    if not team_ip:
        return 0
    return int(team_ip * 3 + 0.5)


def _round_stat_half_up(value: float) -> float:
    """Round a displayed statistical value using conventional half-up rounding."""
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


class QualityGate:
    """Validate consistency between cumulative and game-by-game records."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def _get_regular_season_ids(self, year: int) -> list[int]:
        """Fetch season_ids that correspond to Regular Season (league_type_code=0).

        Args:
            year: Season year.
            year: Season year.

        """
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

    def _team_code_expression(self, model: type[Base]) -> ColumnElement[object]:
        """Use canonical team code only when the target schema has that column."""
        try:
            bind = self.session.get_bind()
            columns = {str(column["name"]).lower() for column in sa_inspect(bind).get_columns(model.__tablename__)}
            if "canonical_team_code" not in columns:
                return model.team_code  # type: ignore[attr-defined]
        except (SQLAlchemyError, AttributeError, TypeError):
            pass
        return func.coalesce(
            model.canonical_team_code,  # type: ignore[attr-defined]
            model.team_code,  # type: ignore[attr-defined]
        )

    def _is_oracle(self) -> bool:
        """Return whether the active quality-gate connection uses Oracle."""
        try:
            return self.session.get_bind().dialect.name == "oracle"
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def _valid_team_code_filters(
        model: type[Base],
        team_expr: ColumnElement[object] | None = None,
        *,
        exclude_empty_string: bool = False,
    ) -> tuple[Any, ...]:
        if team_expr is None:
            team_expr = func.coalesce(
                model.canonical_team_code,  # type: ignore[attr-defined]
                model.team_code,  # type: ignore[attr-defined]
            )
        invalid_codes = tuple(code for code in INVALID_TEAM_CODES if not (exclude_empty_string and code == ""))
        aggregate_codes = tuple(code for code in AGGREGATE_TEAM_CODES if not (exclude_empty_string and code == ""))
        return (
            team_expr.isnot(None),
            team_expr.not_in(invalid_codes),
            or_(
                model.team_code.is_(None),  # type: ignore[attr-defined]
                model.team_code.not_in(aggregate_codes),  # type: ignore[attr-defined]
            ),
        )

    def validate_season_batting(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """Compare PlayerSeasonBatting (cumulative) with GameBattingStat sum (transactional).

        Args:
            season: Season year.
            league: League.
            season: Season year.
            league: League.

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
            if _batting_pa_mismatch(diff, cum.plate_appearances or 0):
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
    def _resolve_pitching_cumulative_outs(cumulative_row: PitchingCumulativeRow) -> int | None:
        cum_outs = cumulative_row.innings_outs
        if cum_outs is not None:
            return int(cum_outs)
        if cumulative_row.extra_stats and "innings_outs" in cumulative_row.extra_stats:
            return int(cumulative_row.extra_stats["innings_outs"])
        if cumulative_row.innings_pitched is None:
            return None
        ip = float(cumulative_row.innings_pitched)
        whole = int(ip)
        frac = round((ip - whole) * 100)
        if frac == IP_FRAC_THIRD:
            return whole * 3 + 1
        if frac == IP_FRAC_TWO_THIRDS:
            return whole * 3 + 2
        return whole * 3

    @staticmethod
    def _pitching_outs_mismatch(row: object, cumulative_outs: int | None) -> dict[str, Any] | None:
        diff = (row.outs or 0) - (cumulative_outs or 0)  # type: ignore[attr-defined]
        if diff <= MAX_OUTS or (
            cumulative_outs is not None and diff / (cumulative_outs or 1) <= PITCHING_OUTS_RELATIVE_TOLERANCE
        ):
            return None
        return {
            "player_id": row.player_id,  # type: ignore[attr-defined]
            "issue": "Transactional Outs > Cumulative Outs",
            "cumulative": cumulative_outs,
            "transactional": row.outs,  # type: ignore[attr-defined]
        }

    @staticmethod
    def _missing_pitching_cumulative_record(row: object) -> dict[str, Any]:
        return {
            "player_id": row.player_id,  # type: ignore[attr-defined]
            "issue": "Missing cumulative record",
            "transactional": {"outs": row.outs, "wins": row.wins},  # type: ignore[attr-defined]
        }

    def validate_season_pitching(self, season: int, league: str = "REGULAR") -> dict[str, Any]:
        """Compare PlayerSeasonPitching (cumulative) with GamePitchingStat sum (transactional).

        Args:
            season: Season year.
            league: League.
            season: Season year.
            league: League.

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
        """Validate PA = AB + BB + HBP + SH + SF consistency for game batting stats.

        Args:
            season: Season year.
            league: League.
            season: Season year.
            league: League.

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
        """Compare TeamSeasonBatting with PlayerSeasonBatting sum grouped by team.

        Args:
            season: Season year.
            league: League.
            season: Season year.
            league: League.

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
        team_map = {r.team_id: r for r in team_data if r.team_id in STANDARD_TEAM_CODES}

        if not team_map:
            return self._result(season=season, league=league)

        # 2. Get player-level aggregates per team
        team_code_expr = self._team_code_expression(PlayerSeasonBatting)
        player_agg = (
            select(
                team_code_expr.label("team_code"),
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
                *self._valid_team_code_filters(
                    PlayerSeasonBatting,
                    team_code_expr,
                    exclude_empty_string=self._is_oracle(),
                ),
            )
            .group_by(team_code_expr)
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
                if _team_stat_mismatch(diff):
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
        """Compare TeamSeasonPitching with PlayerSeasonPitching sum grouped by team.

        Args:
            season: Season year.
            league: League.
            season: Season year.
            league: League.

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
        team_map = {r.team_id: r for r in team_data if r.team_id in STANDARD_TEAM_CODES}

        if not team_map:
            return self._result(season=season, league=league)

        # 2. Get player-level aggregates per team
        team_code_expr = self._team_code_expression(PlayerSeasonPitching)
        player_agg = (
            select(
                team_code_expr.label("team_code"),
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
                *self._valid_team_code_filters(
                    PlayerSeasonPitching,
                    team_code_expr,
                    exclude_empty_string=self._is_oracle(),
                ),
            )
            .group_by(team_code_expr)
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
                if _team_stat_mismatch(diff):
                    diffs.append(f"{field}: team={t_val} player_sum={p_val} diff={diff}")

            # Compare innings_pitched (special: float, not integer)
            team_ip = team_r.innings_pitched or 0.0
            player_outs = player_r.innings_outs or 0
            expected_outs = int(team_ip * 3 + 0.5) if team_ip else 0
            if abs(player_outs - expected_outs) > MAX_OUTS:
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

    def _check_futures_batting_impossible(self, player: PlayerSeasonBatting) -> str | None:
        pa = player.plate_appearances or 0
        ab = player.at_bats or 0
        hits = player.hits or 0
        doubles = player.doubles or 0
        triples = player.triples or 0
        hr = player.home_runs or 0
        so = player.strikeouts or 0
        walks = player.walks or 0

        if ab > pa:
            return f"AB ({ab}) > PA ({pa})"
        if hits > ab:
            return f"Hits ({hits}) > AB ({ab})"
        if doubles + triples + hr > hits:
            return f"Extra-base hits ({doubles + triples + hr}) > Hits ({hits})"
        if so > pa:
            return f"Strikeouts ({so}) > PA ({pa})"
        if walks > pa:
            return f"Walks ({walks}) > PA ({pa})"
        return None

    def _check_futures_batting_rates(self, player: PlayerSeasonBatting) -> list[str]:
        ab = player.at_bats or 0
        hits = player.hits or 0
        doubles = player.doubles or 0
        triples = player.triples or 0
        hr = player.home_runs or 0
        walks = player.walks or 0
        hbp = player.hbp or 0
        sf = player.sacrifice_flies or 0
        avg = player.avg
        obp = player.obp
        slg = player.slg

        diffs = []
        if ab > 0 and avg is not None:
            expected_avg = round(hits / ab, 3)
            if abs(avg - expected_avg) > FUTURES_BATTING_TOLERANCE:
                diffs.append(f"AVG mismatch: recorded={avg}, expected={expected_avg}")

        obp_denom = ab + walks + hbp + sf
        if player.sacrifice_flies is not None and obp_denom > 0 and obp is not None:
            expected_obp = round((hits + walks + hbp) / obp_denom, 3)
            if abs(obp - expected_obp) > FUTURES_BATTING_TOLERANCE:
                diffs.append(f"OBP mismatch: recorded={obp}, expected={expected_obp}")

        if ab > 0 and slg is not None:
            singles = hits - doubles - triples - hr
            tb = singles + 2 * doubles + 3 * triples + 4 * hr
            expected_slg = round(tb / ab, 3)
            if abs(slg - expected_slg) > FUTURES_BATTING_TOLERANCE:
                diffs.append(f"SLG mismatch: recorded={slg}, expected={expected_slg}")
        return diffs

    def validate_futures_batting(self, season: int) -> dict[str, Any]:
        """Validate Futures batting stats for impossible values and wOBA consistency.

        Args:
            season: Season year.

        """
        import contextlib

        from sqlalchemy.exc import SQLAlchemyError

        from src.aggregators.sabermetrics_calculator import SabermetricsCalculator

        players = (
            self.session.query(PlayerSeasonBatting)
            .options(
                load_only(
                    PlayerSeasonBatting.player_id,
                    PlayerSeasonBatting.plate_appearances,
                    PlayerSeasonBatting.at_bats,
                    PlayerSeasonBatting.hits,
                    PlayerSeasonBatting.doubles,
                    PlayerSeasonBatting.triples,
                    PlayerSeasonBatting.home_runs,
                    PlayerSeasonBatting.strikeouts,
                    PlayerSeasonBatting.walks,
                    PlayerSeasonBatting.intentional_walks,
                    PlayerSeasonBatting.hbp,
                    PlayerSeasonBatting.sacrifice_flies,
                    PlayerSeasonBatting.avg,
                    PlayerSeasonBatting.obp,
                    PlayerSeasonBatting.slg,
                    PlayerSeasonBatting.extra_stats,
                ),
            )
            .filter(
                PlayerSeasonBatting.season == season,
                PlayerSeasonBatting.league == "FUTURES",
            )
            .all()
        )

        if not players:
            return self._result(season=season, league="FUTURES")

        lg_constants = None
        with contextlib.suppress(SQLAlchemyError, ValueError):
            lg_constants = SabermetricsCalculator.get_league_constants(self.session, season, level="KBO2")

        mismatches = []
        for player in players:
            pid = player.player_id

            impossible_issue = self._check_futures_batting_impossible(player)
            if impossible_issue:
                mismatches.append({"player_id": pid, "issue": f"Impossible batting stats: {impossible_issue}"})
                continue

            rate_diffs = self._check_futures_batting_rates(player)
            mismatches.extend({"player_id": pid, "issue": diff} for diff in rate_diffs)

            if lg_constants and player.extra_stats and "woba" in player.extra_stats:
                woba = player.extra_stats["woba"]
                metrics = SabermetricsCalculator.calculate_batting_metrics(player, lg_constants)
                expected_woba = metrics["woba"]
                if abs(woba - expected_woba) > FUTURES_BATTING_TOLERANCE:
                    mismatches.append(
                        {
                            "player_id": pid,
                            "issue": f"wOBA mismatch: recorded={woba}, calculated={expected_woba}",
                        }
                    )

        return self._result(
            season=season,
            league="FUTURES",
            checked_players=len(players),
            mismatches=mismatches,
        )

    def _check_futures_pitching_impossible(self, player: PlayerSeasonPitching) -> str | None:
        games = player.games or 0
        wins = player.wins or 0
        losses = player.losses or 0
        saves = player.saves or 0
        holds = player.holds or 0
        outs = player.innings_outs or 0
        er = player.earned_runs or 0
        r_allowed = player.runs_allowed or 0
        walks_allowed = player.walks_allowed or 0
        so = player.strikeouts or 0

        if er > r_allowed:
            return f"Earned Runs ({er}) > Runs Allowed ({r_allowed})"
        if wins + losses + saves + holds > games:
            return f"W+L+S+H ({wins + losses + saves + holds}) > Games ({games})"
        if outs < 0 or walks_allowed < 0 or so < 0:
            return f"Negative values found (outs={outs}, bb={walks_allowed}, so={so})"
        return None

    def _check_futures_pitching_rates(self, player: PlayerSeasonPitching) -> list[str]:
        outs = player.innings_outs or 0
        er = player.earned_runs or 0
        hits_allowed = player.hits_allowed or 0
        walks_allowed = player.walks_allowed or 0
        era = player.era
        whip = player.whip

        diffs = []
        if outs > 0 and era is not None:
            expected_era = _round_stat_half_up((er * 27) / outs)
            if abs(era - expected_era) > FUTURES_PITCHING_TOLERANCE:
                diffs.append(f"ERA mismatch: recorded={era}, expected={expected_era}")

        if outs > 0 and whip is not None:
            expected_whip = round(((walks_allowed + hits_allowed) * 3) / outs, 2)
            if abs(whip - expected_whip) > FUTURES_PITCHING_TOLERANCE:
                diffs.append(f"WHIP mismatch: recorded={whip}, expected={expected_whip}")
        return diffs

    def validate_futures_pitching(self, season: int) -> dict[str, Any]:
        """Validate Futures pitching stats for impossible values and FIP consistency.

        Args:
            season: Season year.

        """
        import contextlib

        from sqlalchemy.exc import SQLAlchemyError

        from src.aggregators.sabermetrics_calculator import SabermetricsCalculator

        players = (
            self.session.query(PlayerSeasonPitching)
            .options(
                load_only(
                    PlayerSeasonPitching.player_id,
                    PlayerSeasonPitching.games,
                    PlayerSeasonPitching.innings_outs,
                    PlayerSeasonPitching.innings_pitched,
                    PlayerSeasonPitching.wins,
                    PlayerSeasonPitching.losses,
                    PlayerSeasonPitching.saves,
                    PlayerSeasonPitching.holds,
                    PlayerSeasonPitching.runs_allowed,
                    PlayerSeasonPitching.earned_runs,
                    PlayerSeasonPitching.hits_allowed,
                    PlayerSeasonPitching.home_runs_allowed,
                    PlayerSeasonPitching.walks_allowed,
                    PlayerSeasonPitching.strikeouts,
                    PlayerSeasonPitching.hit_batters,
                    PlayerSeasonPitching.era,
                    PlayerSeasonPitching.whip,
                    PlayerSeasonPitching.fip,
                    PlayerSeasonPitching.extra_stats,
                ),
            )
            .filter(
                PlayerSeasonPitching.season == season,
                PlayerSeasonPitching.league == "FUTURES",
            )
            .all()
        )

        if not players:
            return self._result(season=season, league="FUTURES")

        lg_constants = None
        with contextlib.suppress(SQLAlchemyError, ValueError):
            lg_constants = SabermetricsCalculator.get_league_constants(self.session, season, level="KBO2")

        mismatches = []
        for player in players:
            pid = player.player_id

            impossible_issue = self._check_futures_pitching_impossible(player)
            if impossible_issue:
                mismatches.append({"player_id": pid, "issue": f"Impossible pitching stats: {impossible_issue}"})
                continue

            rate_diffs = self._check_futures_pitching_rates(player)
            mismatches.extend({"player_id": pid, "issue": diff} for diff in rate_diffs)

            if lg_constants and player.fip is not None:
                fip = player.fip
                metrics = SabermetricsCalculator.calculate_pitching_metrics(player, lg_constants)
                expected_fip = metrics["fip_adj"]
                if abs(fip - expected_fip) > FUTURES_FIP_TOLERANCE:
                    mismatches.append(
                        {
                            "player_id": pid,
                            "issue": f"FIP mismatch: recorded={fip}, calculated={expected_fip}",
                        }
                    )

        return self._result(
            season=season,
            league="FUTURES",
            checked_players=len(players),
            mismatches=mismatches,
        )


def run_quality_gate(session: Session, season: int) -> dict[str, Any]:
    """Run quality gate.

    Args:
        session: Session.
        season: Season year.

    Returns:
        Dictionary result.

    """
    gate = QualityGate(session)

    batting_result = gate.validate_season_batting(season)
    pitching_result = gate.validate_season_pitching(season)
    pa_formula_result = gate.validate_season_pa_formula(season)
    team_batting_result = gate.validate_season_team_batting(season)
    team_pitching_result = gate.validate_season_team_pitching(season)
    futures_batting_result = gate.validate_futures_batting(season)
    futures_pitching_result = gate.validate_futures_pitching(season)

    return {
        "batting": batting_result,
        "pitching": pitching_result,
        "pa_formula": pa_formula_result,
        "team_batting": team_batting_result,
        "team_pitching": team_pitching_result,
        "futures_batting": futures_batting_result,
        "futures_pitching": futures_pitching_result,
        "ok": batting_result.get("ok", False)
        and pitching_result.get("ok", False)
        and pa_formula_result.get("ok", False)
        and team_batting_result.get("ok", False)
        and team_pitching_result.get("ok", False)
        and futures_batting_result.get("ok", False)
        and futures_pitching_result.get("ok", False),
    }
