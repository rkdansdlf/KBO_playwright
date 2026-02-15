#!/usr/bin/env python3
"""
Conservative NULL player_id resolver.

Resolution policy (fixed):
  1) Override exact group match (source_table, year, team_code, player_name)
  2) Apply name alias
  3) Build season+team candidates
  4) Apply role filter (batting/lineups -> batting profile, pitching -> pitching profile)
  5) Apply uniform_no filter
  6) Update only when exactly one candidate remains
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

TABLES = ("game_batting_stats", "game_pitching_stats", "game_lineups")
ALIASES_PATH = PROJECT_ROOT / "data/player_name_aliases.csv"
DEFAULT_OVERRIDES_CSV = PROJECT_ROOT / "data/player_id_overrides.csv"

TEAM_VARIANTS_MAP: Dict[str, Sequence[str]] = {
    "DB": ("DB", "OB", "DO"),
    "OB": ("OB", "DB", "DO"),
    "DO": ("DO", "OB", "DB"),
    "KIA": ("KIA", "HT", "KI"),
    "HT": ("HT", "KIA", "KI"),
    "KI": ("KI", "KIA", "HT"),
    "SSG": ("SSG", "SK"),
    "SK": ("SK", "SSG"),
    "KH": ("KH", "WO", "NX"),
    "WO": ("WO", "NX", "KH"),
    "NX": ("NX", "WO", "KH"),
}

ROLE_TABLE_MAP = {
    "game_batting_stats": "player_season_batting",
    "game_lineups": "player_season_batting",
    "game_pitching_stats": "player_season_pitching",
}


@dataclass(frozen=True)
class OverrideEntry:
    source_table: str
    year: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str
    evidence_source: str


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_team_code(value: Any) -> str:
    return normalize_text(value).upper()


def normalize_uniform_no(value: Any) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    if raw.isdigit():
        return str(int(raw))
    return raw.upper()


def load_alias_map(path: Path = ALIASES_PATH) -> Dict[str, str]:
    if not path.exists():
        return {}
    aliases: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            old_name = normalize_text(row.get("old_name"))
            new_name = normalize_text(row.get("new_name"))
            if old_name and new_name and old_name != new_name:
                aliases[old_name] = new_name
    return aliases


def load_overrides(path: Path) -> Dict[Tuple[str, int, str, str], OverrideEntry]:
    overrides: Dict[Tuple[str, int, str, str], OverrideEntry] = {}
    if not path.exists():
        return overrides

    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            source_table = normalize_text(row.get("source_table"))
            if source_table not in TABLES:
                continue
            year_raw = normalize_text(row.get("year"))
            player_name = normalize_text(row.get("player_name"))
            team_code = normalize_team_code(row.get("team_code"))
            resolved_player_id_raw = normalize_text(row.get("resolved_player_id"))
            if not year_raw or not player_name or not resolved_player_id_raw:
                continue
            try:
                year = int(year_raw)
                resolved_player_id = int(resolved_player_id_raw)
            except ValueError:
                continue

            key = (source_table, year, team_code, player_name)
            overrides[key] = OverrideEntry(
                source_table=source_table,
                year=year,
                team_code=team_code,
                player_name=player_name,
                resolved_player_id=resolved_player_id,
                reason=normalize_text(row.get("reason")),
                evidence_source=normalize_text(row.get("evidence_source")),
            )
    return overrides


def team_code_variants(team_code: str | None) -> List[str]:
    code = normalize_team_code(team_code)
    if not code:
        return []
    variants = list(TEAM_VARIANTS_MAP.get(code, (code,)))
    seen = set()
    result: List[str] = []
    for item in variants:
        norm = normalize_team_code(item)
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def is_group_resolvable(candidate_ids: Sequence[int]) -> bool:
    return len(candidate_ids) == 1


def player_exists(session, player_id: int) -> bool:
    found = session.execute(
        text("SELECT 1 FROM player_basic WHERE player_id = :player_id LIMIT 1"),
        {"player_id": int(player_id)},
    ).fetchone()
    return found is not None


def fetch_group_uniform_nos(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
) -> List[str]:
    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT COALESCE(uniform_no, '') AS uniform_no
            FROM {table_name}
            WHERE player_id IS NULL
              AND substr(game_id, 1, 4) = :year
              AND COALESCE(team_code, '') = :team_code
              AND player_name = :player_name
            """
        ),
        {
            "year": str(year),
            "team_code": normalize_text(team_code),
            "player_name": player_name,
        },
    ).fetchall()
    uniforms: List[str] = []
    for row in rows:
        normalized = normalize_uniform_no(row[0])
        if normalized:
            uniforms.append(normalized)
    return sorted(set(uniforms))


def resolve_candidate_ids(
    session,
    *,
    season: int,
    team_code: str | None,
    player_name: str,
) -> List[int]:
    teams = team_code_variants(team_code)
    if not teams:
        return []

    candidate_sql = (
        text(
            """
            SELECT DISTINCT pb.player_id
            FROM player_basic pb
            JOIN player_season_batting psb ON psb.player_id = pb.player_id
            WHERE pb.name = :player_name
              AND psb.season = :season
              AND psb.team_code IN :team_codes
            UNION
            SELECT DISTINCT pb.player_id
            FROM player_basic pb
            JOIN player_season_pitching psp ON psp.player_id = pb.player_id
            WHERE pb.name = :player_name
              AND psp.season = :season
              AND psp.team_code IN :team_codes
            ORDER BY 1
            """
        )
        .bindparams(bindparam("team_codes", expanding=True))
    )

    rows = session.execute(
        candidate_sql,
        {"player_name": player_name, "season": int(season), "team_codes": teams},
    ).fetchall()
    return [int(row[0]) for row in rows]


def filter_candidates_by_role(
    session,
    *,
    table_name: str,
    season: int,
    team_codes: Sequence[str],
    candidate_ids: Sequence[int],
) -> List[int]:
    if not candidate_ids:
        return []
    role_table = ROLE_TABLE_MAP.get(table_name)
    if not role_table:
        return list(candidate_ids)

    role_sql = (
        text(
            f"""
            SELECT DISTINCT player_id
            FROM {role_table}
            WHERE season = :season
              AND team_code IN :team_codes
              AND player_id IN :candidate_ids
            """
        )
        .bindparams(bindparam("team_codes", expanding=True))
        .bindparams(bindparam("candidate_ids", expanding=True))
    )
    rows = session.execute(
        role_sql,
        {"season": int(season), "team_codes": list(team_codes), "candidate_ids": list(candidate_ids)},
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


def filter_candidates_by_uniform(
    session,
    *,
    candidate_ids: Sequence[int],
    uniform_nos: Sequence[str],
) -> List[int]:
    if not candidate_ids:
        return []
    normalized_uniforms = {normalize_uniform_no(u) for u in uniform_nos if normalize_uniform_no(u)}
    if not normalized_uniforms:
        return list(candidate_ids)

    uniform_sql = text(
        """
        SELECT player_id, COALESCE(uniform_no, '') AS uniform_no
        FROM player_basic
        WHERE player_id IN :candidate_ids
        """
    ).bindparams(bindparam("candidate_ids", expanding=True))
    rows = session.execute(uniform_sql, {"candidate_ids": list(candidate_ids)}).fetchall()

    matched: List[int] = []
    for player_id, uniform_no in rows:
        if normalize_uniform_no(uniform_no) in normalized_uniforms:
            matched.append(int(player_id))
    return sorted(set(matched))


def choose_candidate_ids(
    session,
    *,
    table_name: str,
    season: int,
    team_code: str,
    player_name: str,
    uniform_nos: Sequence[str],
    alias_map: Dict[str, str],
    overrides: Dict[Tuple[str, int, str, str], OverrideEntry],
) -> Dict[str, Any]:
    key = (table_name, int(season), normalize_team_code(team_code), player_name)
    override = overrides.get(key)
    if override:
        if player_exists(session, override.resolved_player_id):
            return {
                "candidate_ids": [override.resolved_player_id],
                "resolution_method": "override_exact_group",
                "resolution_reason": "override_applied",
                "resolved_name": player_name,
                "override_reason": override.reason,
                "override_evidence_source": override.evidence_source,
            }
        return {
            "candidate_ids": [],
            "resolution_method": "override_exact_group",
            "resolution_reason": "override_player_id_not_found_in_player_basic",
            "resolved_name": player_name,
            "override_reason": override.reason,
            "override_evidence_source": override.evidence_source,
        }

    resolved_name = alias_map.get(player_name, player_name)
    teams = team_code_variants(team_code)
    candidates = resolve_candidate_ids(
        session,
        season=int(season),
        team_code=team_code,
        player_name=resolved_name,
    )
    if not candidates:
        return {
            "candidate_ids": [],
            "resolution_method": "season_team_candidate",
            "resolution_reason": "no_candidates",
            "resolved_name": resolved_name,
            "override_reason": "",
            "override_evidence_source": "",
        }

    role_filtered = filter_candidates_by_role(
        session,
        table_name=table_name,
        season=int(season),
        team_codes=teams,
        candidate_ids=candidates,
    )
    if not role_filtered:
        return {
            "candidate_ids": [],
            "resolution_method": "role_filter",
            "resolution_reason": "filtered_to_zero_by_role",
            "resolved_name": resolved_name,
            "override_reason": "",
            "override_evidence_source": "",
        }

    uniform_filtered = filter_candidates_by_uniform(
        session,
        candidate_ids=role_filtered,
        uniform_nos=uniform_nos,
    )
    if uniform_nos:
        if not uniform_filtered:
            return {
                "candidate_ids": [],
                "resolution_method": "uniform_filter",
                "resolution_reason": "filtered_to_zero_by_uniform",
                "resolved_name": resolved_name,
                "override_reason": "",
                "override_evidence_source": "",
            }
        return {
            "candidate_ids": uniform_filtered,
            "resolution_method": "uniform_filter",
            "resolution_reason": "season_team_role_uniform",
            "resolved_name": resolved_name,
            "override_reason": "",
            "override_evidence_source": "",
        }

    return {
        "candidate_ids": role_filtered,
        "resolution_method": "role_filter",
        "resolution_reason": "season_team_role",
        "resolved_name": resolved_name,
        "override_reason": "",
        "override_evidence_source": "",
    }


def update_null_player_ids_for_group(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str | None,
    player_name: str,
    player_id: int,
    dry_run: bool = False,
) -> int:
    update_sql = text(
        f"""
        UPDATE {table_name}
        SET player_id = :player_id
        WHERE player_id IS NULL
          AND substr(game_id, 1, 4) = :year
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
        """
    )
    result = session.execute(
        update_sql,
        {
            "player_id": int(player_id),
            "year": str(year),
            "team_code": normalize_text(team_code),
            "player_name": player_name,
        },
    )
    if dry_run:
        session.rollback()
    return int(result.rowcount or 0)


def process_table(
    session,
    table_name: str,
    alias_map: Dict[str, str],
    overrides: Dict[Tuple[str, int, str, str], OverrideEntry],
    dry_run: bool = False,
):
    group_sql = text(
        f"""
        SELECT
            CAST(substr(game_id, 1, 4) AS INTEGER) AS season,
            COALESCE(team_code, '') AS team_code,
            player_name,
            COUNT(*) AS unresolved_rows
        FROM {table_name}
        WHERE player_id IS NULL
          AND player_name IS NOT NULL
        GROUP BY CAST(substr(game_id, 1, 4) AS INTEGER), COALESCE(team_code, ''), player_name
        ORDER BY season, team_code, player_name
        """
    )
    groups = session.execute(group_sql).fetchall()
    applied_rows: List[Dict[str, Any]] = []
    unresolved_rows: List[Dict[str, Any]] = []

    for season, team_code, player_name, unresolved_count in groups:
        season_int = int(season)
        team_code_norm = normalize_team_code(team_code)
        uniforms = fetch_group_uniform_nos(
            session,
            table_name=table_name,
            year=season_int,
            team_code=team_code_norm,
            player_name=player_name,
        )
        selected = choose_candidate_ids(
            session,
            table_name=table_name,
            season=season_int,
            team_code=team_code_norm,
            player_name=player_name,
            uniform_nos=uniforms,
            alias_map=alias_map,
            overrides=overrides,
        )
        candidates = [int(x) for x in selected["candidate_ids"]]
        candidate_ids_str = ",".join(str(x) for x in candidates)
        uniforms_str = ",".join(uniforms)
        alias_applied = alias_map.get(player_name, player_name) != player_name

        base = {
            "source_table": table_name,
            "year": season_int,
            "team_code": team_code_norm,
            "player_name": player_name,
            "unresolved_rows": int(unresolved_count),
            "uniform_nos": uniforms_str,
            "candidate_count": len(candidates),
            "candidate_ids": candidate_ids_str,
            "resolved_name": selected["resolved_name"],
            "alias_applied": int(alias_applied),
            "resolution_method": selected["resolution_method"],
            "resolution_reason": selected["resolution_reason"],
            "override_reason": selected["override_reason"],
            "override_evidence_source": selected["override_evidence_source"],
        }

        if is_group_resolvable(candidates):
            resolved_player_id = candidates[0]
            updated = update_null_player_ids_for_group(
                session,
                table_name=table_name,
                year=season_int,
                team_code=team_code_norm,
                player_name=player_name,
                player_id=resolved_player_id,
                dry_run=dry_run,
            )
            applied_rows.append(
                {
                    **base,
                    "resolved_player_id": resolved_player_id,
                    "updated_rows": updated,
                }
            )
        else:
            unresolved_rows.append(base)

    if dry_run:
        session.rollback()
    else:
        session.commit()

    return applied_rows, unresolved_rows


def _write_csv(path: Path, columns: Iterable[str], rows: List[Dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns))
        writer.writeheader()
        writer.writerows(rows)


def run_conservative_resolution(
    *,
    dry_run: bool = False,
    output_dir: str = "data",
    overrides_csv: str = str(DEFAULT_OVERRIDES_CSV),
) -> Dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    alias_map = load_alias_map()
    overrides = load_overrides(Path(overrides_csv))

    all_applied: List[Dict[str, Any]] = []
    all_unresolved: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        for table_name in TABLES:
            applied, unresolved = process_table(
                session,
                table_name,
                alias_map,
                overrides,
                dry_run=dry_run,
            )
            all_applied.extend(applied)
            all_unresolved.extend(unresolved)

    applied_csv = out_dir / f"null_player_id_conservative_applied_{stamp}.csv"
    unresolved_csv = out_dir / f"null_player_id_conservative_unresolved_{stamp}.csv"

    _write_csv(
        applied_csv,
        (
            "source_table",
            "year",
            "team_code",
            "player_name",
            "unresolved_rows",
            "uniform_nos",
            "candidate_count",
            "candidate_ids",
            "resolved_name",
            "alias_applied",
            "resolution_method",
            "resolution_reason",
            "override_reason",
            "override_evidence_source",
            "resolved_player_id",
            "updated_rows",
        ),
        all_applied,
    )
    _write_csv(
        unresolved_csv,
        (
            "source_table",
            "year",
            "team_code",
            "player_name",
            "unresolved_rows",
            "uniform_nos",
            "candidate_count",
            "candidate_ids",
            "resolved_name",
            "alias_applied",
            "resolution_method",
            "resolution_reason",
            "override_reason",
            "override_evidence_source",
        ),
        all_unresolved,
    )

    return {
        "dry_run": dry_run,
        "applied_csv": str(applied_csv),
        "unresolved_csv": str(unresolved_csv),
        "applied_groups": len(all_applied),
        "unresolved_groups": len(all_unresolved),
        "updated_rows": sum(int(row.get("updated_rows", 0)) for row in all_applied),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Conservative resolver for NULL player_id rows")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates")
    parser.add_argument("--output-dir", default="data", help="Directory to write output CSV files")
    parser.add_argument(
        "--overrides-csv",
        default=str(DEFAULT_OVERRIDES_CSV),
        help="Path to player_id_overrides.csv",
    )
    args = parser.parse_args()

    result = run_conservative_resolution(
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        overrides_csv=args.overrides_csv,
    )
    print("✅ Conservative NULL player_id resolution completed")
    print(f"   dry_run: {result['dry_run']}")
    print(f"   applied_groups: {result['applied_groups']}")
    print(f"   unresolved_groups: {result['unresolved_groups']}")
    print(f"   updated_rows: {result['updated_rows']}")
    print(f"   applied_csv: {result['applied_csv']}")
    print(f"   unresolved_csv: {result['unresolved_csv']}")


if __name__ == "__main__":
    main()
