"""Stage official season statistics without writing to a database."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.crawlers.player_batting_all_series_crawler import BattingSeriesCrawlRequest, crawl_series_batting_stats
from src.crawlers.player_pitching_all_series_crawler import PitchingSeriesCrawlRequest, crawl_pitcher_series
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.utils.team_mapping import get_team_mapping_for_year
from src.utils.team_stats_helpers import has_complete_team_stats
from src.utils.type_helpers import parse_innings_to_outs

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

CORE_BATTING_FIELDS = (
    "plate_appearances",
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
)
CORE_PITCHING_FIELDS = (
    "innings_outs",
    "hits_allowed",
    "runs_allowed",
    "earned_runs",
    "home_runs_allowed",
    "walks_allowed",
    "strikeouts",
)
MAX_REASONABLE_ERA = 30.0
OFFICIAL_STAGE_REQUIRED = "Official team statistics could not be staged without fallback data"


@dataclass(frozen=True)
class StageRows:
    """Official rows collected for one season-stage run."""

    team_batting: list[dict[str, Any]]
    team_pitching: list[dict[str, Any]]
    player_batting: list[dict[str, Any]]
    player_pitching: list[object]
    expected_team_ids: set[str]


def _value(row: object, field: str) -> object:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _as_number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: object) -> int:
    return int(_as_number(value))


def _team_id(row: object) -> str | None:
    value = _value(row, "team_id") or _value(row, "team_code")
    return str(value) if value else None


def _pitching_value(row: object, field: str) -> object:
    value = _value(row, field)
    if field == "innings_outs" and value is None:
        innings_pitched = _value(row, "innings_pitched")
        if innings_pitched is not None:
            return parse_innings_to_outs(str(innings_pitched))
    return value


def _sum_fields(
    rows: Iterable[object],
    fields: tuple[str, ...],
    *,
    value_getter: Callable[[object, str], object] = _value,
) -> dict[str, int]:
    totals = dict.fromkeys(fields, 0)
    for row in rows:
        for field in fields:
            totals[field] += _as_int(value_getter(row, field))
    return totals


def _group_by_team(rows: Iterable[object]) -> dict[str, list[object]]:
    grouped: dict[str, list[object]] = {}
    for row in rows:
        team_id = _team_id(row)
        if team_id:
            grouped.setdefault(team_id, []).append(row)
    return grouped


def _diffs(
    expected: dict[str, int],
    actual: dict[str, int],
    fields: tuple[str, ...],
) -> dict[str, int]:
    return {
        field: actual.get(field, 0) - expected.get(field, 0)
        for field in fields
        if actual.get(field, 0) != expected.get(field, 0)
    }


def _source_comparison(
    team_rows: list[object],
    player_rows: list[object],
    fields: tuple[str, ...],
    *,
    value_getter: Callable[[object, str], object] = _value,
) -> dict[str, object]:
    team_totals = _sum_fields(team_rows, fields, value_getter=value_getter)
    player_totals = _sum_fields(player_rows, fields, value_getter=value_getter)
    return {
        "team_totals": team_totals,
        "player_totals": player_totals,
        "diff": _diffs(team_totals, player_totals, fields),
        "ok": not _diffs(team_totals, player_totals, fields),
    }


def _team_comparison(
    team_rows: list[object],
    player_rows: list[object],
    fields: tuple[str, ...],
    *,
    value_getter: Callable[[object, str], object] = _value,
) -> dict[str, object]:
    team_by_id = _group_by_team(team_rows)
    player_by_id = _group_by_team(player_rows)
    comparisons: dict[str, object] = {}
    for team_id in sorted(team_by_id):
        team_totals = _sum_fields(team_by_id[team_id], fields, value_getter=value_getter)
        player_totals = _sum_fields(player_by_id.get(team_id, []), fields, value_getter=value_getter)
        diff = _diffs(team_totals, player_totals, fields)
        comparisons[team_id] = {"diff": diff, "ok": not diff}
    return {
        "teams": comparisons,
        "ok": all(item["ok"] for item in comparisons.values() if isinstance(item, dict)),
    }


def _invalid_era_rows(rows: list[object]) -> list[dict[str, object]]:
    invalid: list[dict[str, object]] = []
    for row in rows:
        era = _as_number(_value(row, "era"))
        innings_outs = _as_number(_value(row, "innings_outs"))
        innings_pitched = _as_number(_value(row, "innings_pitched"))
        if era < 0 or (era > MAX_REASONABLE_ERA and innings_outs <= 0 and innings_pitched <= 0):
            invalid.append(
                {
                    "player_id": _value(row, "player_id"),
                    "team_code": _value(row, "team_code"),
                    "era": _value(row, "era"),
                    "innings_outs": _value(row, "innings_outs"),
                    "innings_pitched": _value(row, "innings_pitched"),
                },
            )
    return invalid


def build_stage_report(
    year: int,
    rows: StageRows,
    *,
    current_year: int | None = None,
) -> dict[str, object]:
    """Build a no-write reconciliation report from staged official rows."""
    batting_source = _source_comparison(rows.team_batting, rows.player_batting, CORE_BATTING_FIELDS)
    pitching_source = _source_comparison(
        rows.team_pitching,
        rows.player_pitching,
        CORE_PITCHING_FIELDS,
        value_getter=_pitching_value,
    )
    batting_teams = _team_comparison(rows.team_batting, rows.player_batting, CORE_BATTING_FIELDS)
    pitching_teams = _team_comparison(
        rows.team_pitching,
        rows.player_pitching,
        CORE_PITCHING_FIELDS,
        value_getter=_pitching_value,
    )
    batting_complete = has_complete_team_stats(
        rows.team_batting,
        expected_team_ids=rows.expected_team_ids,
        season=year,
        current_year=current_year,
    )
    pitching_complete = has_complete_team_stats(
        rows.team_pitching,
        expected_team_ids=rows.expected_team_ids,
        season=year,
        current_year=current_year,
    )
    invalid_era = _invalid_era_rows(rows.player_pitching)
    return {
        "year": year,
        "read_only": True,
        "team_rows": {"batting": len(rows.team_batting), "pitching": len(rows.team_pitching)},
        "player_rows": {"batting": len(rows.player_batting), "pitching": len(rows.player_pitching)},
        "team_coverage": {"batting": batting_complete, "pitching": pitching_complete},
        "batting": {"global": batting_source, "by_team": batting_teams},
        "pitching": {"global": pitching_source, "by_team": pitching_teams},
        "invalid_era_rows": invalid_era,
        "ready_for_sync": (
            batting_complete
            and pitching_complete
            and batting_source["ok"]
            and pitching_source["ok"]
            and not invalid_era
        ),
    }


def collect_stage_report(year: int, *, headless: bool = True) -> dict[str, object]:
    """Collect official rows in memory and return a reconciliation report."""
    team_mapping = get_team_mapping_for_year(year)
    expected_team_ids = set(team_mapping.values())
    team_batting = TeamBattingStatsCrawler()._collect_from_site(  # noqa: SLF001
        year,
        team_mapping,
        headless=headless,
    )
    team_pitching = TeamPitchingStatsCrawler()._collect_from_site(  # noqa: SLF001
        year,
        team_mapping,
        headless=headless,
    )
    if not team_batting or not team_pitching:
        raise RuntimeError(OFFICIAL_STAGE_REQUIRED)
    player_batting = crawl_series_batting_stats(
        BattingSeriesCrawlRequest(year=year, series_key="regular", save_to_db=False, headless=headless),
    )
    player_pitching = crawl_pitcher_series(
        PitchingSeriesCrawlRequest(year=year, series_key="regular", save_to_db=False, headless=headless, by_team=True),
    )
    return build_stage_report(
        year,
        StageRows(
            team_batting=team_batting,
            team_pitching=team_pitching,
            player_batting=player_batting,
            player_pitching=player_pitching,
            expected_team_ids=expected_team_ids,
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run official season-stat staging without database writes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = collect_stage_report(args.year)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        report = {"year": args.year, "read_only": True, "ready_for_sync": False, "error": str(exc)}
    rendered = json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        sys.stdout.write(f"Official season staging report: {args.output}\n")
    else:
        sys.stdout.write(rendered)
    return 0 if report.get("ready_for_sync") else 1


if __name__ == "__main__":
    raise SystemExit(main())
