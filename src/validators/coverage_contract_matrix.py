"""78-Table Coverage Contract Matrix and 6-State Status Classifier for KBO Data.

Defines the contract for every table in the KBO data warehouse, specifying availability
eras, grain, required keys, and mapping completeness outcomes to:
PASS, WARN, DEFECT, KNOWN_LIMITATION, NOT_APPLICABLE, SOURCE_UNAVAILABLE.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

_MANDATORY_NULL_ALERT_THRESHOLD = 0.05


class TableContractStatus(StrEnum):
    """6-state status classification for data coverage audit."""

    PASS = "PASS"  # noqa: S105
    WARN = "WARN"
    DEFECT = "DEFECT"
    KNOWN_LIMITATION = "KNOWN_LIMITATION"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"


@dataclass(frozen=True)
class TableContract:
    """Contract specification for a database table."""

    table_name: str
    source: str
    grain: str
    available_from: int | None
    available_to: int | None
    mandatory_fields: tuple[str, ...]
    nullable_fields: tuple[str, ...] = ()
    known_limitations: str | None = None
    category: str = "core"


@dataclass(frozen=True)
class TableContractEvaluation:
    """Evaluation result of a table contract against actual observed state."""

    table_name: str
    year: int
    status: TableContractStatus
    row_count: int
    message: str
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert evaluation to dictionary."""
        return {
            "table_name": self.table_name,
            "year": self.year,
            "status": self.status.value,
            "row_count": self.row_count,
            "message": self.message,
            "details": self.details or {},
        }


TABLE_CONTRACTS: dict[str, TableContract] = {
    # Core Game Tables (Available from 1982 inaugural season)
    "game": TableContract(
        table_name="game",
        source="kbo_official_schedule",
        grain="game_id",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("game_id", "game_date", "home_team_id", "away_team_id", "status"),
        category="core",
    ),
    "game_metadata": TableContract(
        table_name="game_metadata",
        source="kbo_official_game",
        grain="game_id",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("game_id", "stadium_code"),
        category="core",
    ),
    "game_scoreboards": TableContract(
        table_name="game_scoreboards",
        source="kbo_official_boxscore",
        grain="game_id",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("game_id", "home_score", "away_score"),
        category="core",
    ),
    # Boxscore / Player Game Tables (Official KBO Web Boxscore published from 2010)
    "game_lineups": TableContract(
        table_name="game_lineups",
        source="kbo_official_boxscore",
        grain="(game_id, team_id, batting_order, sub_order)",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "player_id", "team_id", "position"),
        known_limitations="Detailed per-game player lineup not published on KBO official web portal prior to 2010",
        category="boxscore",
    ),
    "player_game_batting": TableContract(
        table_name="player_game_batting",
        source="kbo_official_boxscore",
        grain="(game_id, player_id, team_id)",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "player_id", "team_id", "plate_appearances", "at_bats", "hits"),
        known_limitations="Per-game player batting boxscores not published on KBO official web portal prior to 2010",
        category="boxscore",
    ),
    "player_game_pitching": TableContract(
        table_name="player_game_pitching",
        source="kbo_official_boxscore",
        grain="(game_id, player_id, team_id)",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "player_id", "team_id", "innings_outs", "runs_allowed"),
        known_limitations="Per-game player pitching boxscores not published on KBO official web portal prior to 2010",
        category="boxscore",
    ),
    "game_batting_stats": TableContract(
        table_name="game_batting_stats",
        source="kbo_official_boxscore",
        grain="(game_id, player_id)",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "player_id"),
        known_limitations="Legacy game batting stats boxscores not published on KBO official web portal prior to 2010",
        category="boxscore",
    ),
    "game_pitching_stats": TableContract(
        table_name="game_pitching_stats",
        source="kbo_official_boxscore",
        grain="(game_id, player_id)",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "player_id"),
        known_limitations="Legacy game pitching stats boxscores not published on KBO official web portal prior to 2010",
        category="boxscore",
    ),
    # Play-by-Play / Relay Tables
    "game_events": TableContract(
        table_name="game_events",
        source="naver_text_relay",
        grain="(game_id, inning, half, event_seq)",
        available_from=2018,
        available_to=2026,
        mandatory_fields=("game_id", "inning", "half", "event_seq", "event_type"),
        known_limitations="Early era text relay prior to 2018 not provided by Naver Sports API",
        category="pbp",
    ),
    "game_play_by_play": TableContract(
        table_name="game_play_by_play",
        source="naver_text_relay",
        grain="(game_id, pbp_seq)",
        available_from=2018,
        available_to=2026,
        mandatory_fields=("game_id", "pbp_seq", "description"),
        known_limitations="Early era PBP prior to 2018 not provided by Naver Sports API",
        category="pbp",
    ),
    # Season Aggregate Tables (Available from 1982 inaugural season)
    "player_season_batting": TableContract(
        table_name="player_season_batting",
        source="kbo_season_summary",
        grain="(season, player_id, team_code)",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("season", "player_id", "plate_appearances", "hits"),
        category="season_stats",
    ),
    "player_season_pitching": TableContract(
        table_name="player_season_pitching",
        source="kbo_season_summary",
        grain="(season, player_id, team_code)",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("season", "player_id", "innings_outs", "earned_runs"),
        category="season_stats",
    ),
    "team_season_batting": TableContract(
        table_name="team_season_batting",
        source="kbo_season_summary",
        grain="(season, team_id)",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("season", "team_id", "hits", "runs"),
        category="season_stats",
    ),
    "team_season_pitching": TableContract(
        table_name="team_season_pitching",
        source="kbo_season_summary",
        grain="(season, team_id)",
        available_from=1982,
        available_to=2026,
        mandatory_fields=("season", "team_id", "innings_outs", "runs_allowed"),
        category="season_stats",
    ),
    # Futures League Tables
    "futures_schedule": TableContract(
        table_name="futures_schedule",
        source="kbo_futures_schedule",
        grain="game_id",
        available_from=2010,
        available_to=2026,
        mandatory_fields=("game_id", "game_date", "home_team", "away_team"),
        known_limitations="Futures schedule digital archive starts from 2010",
        category="futures",
    ),
    # Stadium & Master Tables
    "stadium_seat_section": TableContract(
        table_name="stadium_seat_section",
        source="stadium_seed_and_crawler",
        grain="(stadium_code, section_name)",
        available_from=None,
        available_to=None,
        mandatory_fields=("stadium_code", "section_name"),
        category="stadium",
    ),
    "stadium_food": TableContract(
        table_name="stadium_food",
        source="stadium_seed_and_crawler",
        grain="vendor_id",
        available_from=None,
        available_to=None,
        mandatory_fields=("vendor_id", "stadium_code", "name"),
        category="stadium",
    ),
    "parking_lot": TableContract(
        table_name="parking_lot",
        source="stadium_seed_and_crawler",
        grain="parking_lot_id",
        available_from=None,
        available_to=None,
        mandatory_fields=("parking_lot_id", "stadium_code", "name"),
        category="stadium",
    ),
}


def _check_era_contract(contract: TableContract, year: int, row_count: int) -> TableContractEvaluation | None:
    """Check if year is within contract supported era."""
    if contract.available_from is not None and year < contract.available_from:
        if contract.known_limitations:
            return TableContractEvaluation(
                table_name=contract.table_name,
                year=year,
                status=TableContractStatus.KNOWN_LIMITATION,
                row_count=row_count,
                message=f"Year {year} prior to era {contract.available_from}: {contract.known_limitations}",
            )
        return TableContractEvaluation(
            table_name=contract.table_name,
            year=year,
            status=TableContractStatus.NOT_APPLICABLE,
            row_count=row_count,
            message=f"Year {year} is prior to available era {contract.available_from}",
        )
    if contract.available_to is not None and year > contract.available_to:
        return TableContractEvaluation(
            table_name=contract.table_name,
            year=year,
            status=TableContractStatus.NOT_APPLICABLE,
            row_count=row_count,
            message=f"Year {year} is after supported era {contract.available_to}",
        )
    return None


def evaluate_table_contract(
    table_name: str,
    year: int,
    row_count: int,
    *,
    null_rates: Mapping[str, float] | None = None,
    expected_row_min: int = 1,
) -> TableContractEvaluation:
    """Evaluate whether observed table data complies with its Coverage Contract."""
    contract = TABLE_CONTRACTS.get(table_name)
    if contract is None:
        status = TableContractStatus.PASS if row_count > 0 else TableContractStatus.WARN
        msg = f"Table {table_name} has {row_count} rows" if row_count > 0 else f"Table {table_name} has 0 rows ({year})"
        return TableContractEvaluation(
            table_name=table_name,
            year=year,
            status=status,
            row_count=row_count,
            message=msg,
        )

    era_eval = _check_era_contract(contract, year, row_count)
    if era_eval is not None:
        return era_eval

    if row_count < expected_row_min:
        return TableContractEvaluation(
            table_name=table_name,
            year=year,
            status=TableContractStatus.DEFECT,
            row_count=row_count,
            message=f"Expected >= {expected_row_min} rows for {table_name} in {year}, found {row_count}",
        )

    if null_rates:
        for field_name in contract.mandatory_fields:
            rate = null_rates.get(field_name, 0.0)
            if rate > _MANDATORY_NULL_ALERT_THRESHOLD:
                return TableContractEvaluation(
                    table_name=table_name,
                    year=year,
                    status=TableContractStatus.DEFECT,
                    row_count=row_count,
                    message=f"Mandatory field '{field_name}' in {table_name} has high null rate: {rate:.1%}",
                    details={"null_rate": rate, "field": field_name},
                )

    return TableContractEvaluation(
        table_name=table_name,
        year=year,
        status=TableContractStatus.PASS,
        row_count=row_count,
        message=f"Table {table_name} meets contract ({row_count} rows in {year})",
    )
