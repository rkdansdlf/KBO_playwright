#!/usr/bin/env python3
"""Resolve remaining unresolved_player entries in player_movements table."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal  # noqa: E402


def split_label(raw: str) -> tuple[str, str | None]:
    raw = str(raw or "").strip()
    match = re.search(r"\(([^)]*)\)\s*$", raw)
    position = match.group(1).strip() if match else None
    player_name = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
    return player_name, position


def run_resolution(apply: bool = False):
    with SessionLocal() as session:
        # Get unresolved movements
        query = text("""
            SELECT id, movement_date, section, team_code, player_name, remarks, canonical_team_id
            FROM player_movements
            WHERE resolution_status IN ('unresolved', 'unresolved_player') AND player_basic_id IS NULL
        """)
        movements = session.execute(query).fetchall()
        print(f"🔍 Found {len(movements)} unresolved player movements.")

        resolved_count = 0
        for m in movements:
            m_id, m_date, section, team_code, raw_name, remarks, canonical_team_id = m

            # 1. Parse name and position
            name, pos = split_label(raw_name)
            year = m_date.year if hasattr(m_date, "year") else int(str(m_date)[:4])

            # Get candidate player_ids for this name from player_basic
            cand_rows_raw = session.execute(
                text("SELECT player_id, team, position, status FROM player_basic WHERE name = :name"), {"name": name}
            ).fetchall()

            cand_rows = []
            for r in cand_rows_raw:
                p_id, p_team, p_pos, p_status = r
                if not p_pos:
                    r_pos = session.execute(
                        text(
                            "SELECT position FROM team_daily_roster WHERE player_basic_id = :pid AND position IS NOT NULL LIMIT 1"
                        ),
                        {"pid": p_id},
                    ).scalar()
                    if r_pos:
                        p_pos = r_pos
                cand_rows.append((p_id, p_team, p_pos, p_status))
            candidate_ids = [r[0] for r in cand_rows]

            if not candidate_ids:
                continue

            # Try to resolve
            resolved_id = None
            evidence = ""

            # Strategy A: Use team_daily_roster for this season
            if canonical_team_id:
                roster_rows = session.execute(
                    text("""
                        SELECT DISTINCT player_basic_id
                        FROM team_daily_roster
                        WHERE team_code = :team_code
                          AND player_name = :name
                          AND roster_date >= :start_date
                          AND roster_date < :end_date
                          AND player_basic_id IS NOT NULL
                    """),
                    {
                        "team_code": canonical_team_id,
                        "name": name,
                        "start_date": f"{year}-01-01",
                        "end_date": f"{year + 1}-01-01",
                    },
                ).fetchall()
                roster_ids = {r[0] for r in roster_rows if r[0] in candidate_ids}
                if len(roster_ids) == 1:
                    resolved_id = next(iter(roster_ids))
                    evidence = f"roster_match_season_{year}"

            # Strategy B: Use season stats (batting/pitching) for this season & team
            if not resolved_id and canonical_team_id:
                season_ids = set()
                # If pos is '투수', check pitching stats first
                tables = []
                if pos == "투수":
                    tables = ["player_season_pitching", "player_season_batting"]
                else:
                    tables = ["player_season_batting", "player_season_pitching"]

                cand_ids_str = ",".join(str(cid) for cid in candidate_ids)
                for tbl in tables:
                    stat_rows = session.execute(
                        text(f"""
                            SELECT DISTINCT player_id
                            FROM {tbl}
                            WHERE player_id IN ({cand_ids_str})
                              AND season = :year
                              AND team_code = :team_code
                        """),
                        {"year": year, "team_code": canonical_team_id},
                    ).fetchall()
                    season_ids.update(r[0] for r in stat_rows)

                if len(season_ids) == 1:
                    resolved_id = next(iter(season_ids))
                    evidence = f"season_stat_match_{year}_{canonical_team_id}"

            # Strategy C: If pos is unique in the candidate pool
            if not resolved_id and pos:
                # Map KBO position label to standard
                # Try exact match or contains match in player_basic
                pos_matches = []
                for p_id, _p_team, p_pos, _p_status in cand_rows:
                    if p_pos and (pos in p_pos or p_pos in pos):
                        pos_matches.append(p_id)
                if len(pos_matches) == 1:
                    resolved_id = pos_matches[0]
                    evidence = f"unique_position_match_{pos}"

            # Strategy D: Use players table matching birth date / info in remarks if any
            # e.g. Remarks: "생년월일:93.5.12"
            if not resolved_id and remarks and "생년월일" in remarks:
                match = re.search(r"생년월일:\s*(\d{2})\.(\d{1,2})\.(\d{1,2})", remarks)
                if match:
                    y_part, m_part, d_part = match.groups()
                    # construct year
                    birth_year = 1900 + int(y_part) if int(y_part) > 30 else 2000 + int(y_part)
                    birth_str = f"{birth_year}-{int(m_part):02d}-{int(d_part):02d}"

                    cand_ids_str = ",".join(f"'{cid}'" for cid in candidate_ids)
                    player_rows = session.execute(
                        text(f"""
                            SELECT kbo_person_id
                            FROM players
                            WHERE kbo_person_id IN ({cand_ids_str})
                              AND birth_date = :birth_date
                        """),
                        {"birth_date": birth_str},
                    ).fetchall()

                    if len(player_rows) == 1:
                        resolved_id = int(player_rows[0][0])
                        evidence = f"birth_date_remarks_match_{birth_str}"

            # Strategy F: Match team transfer remarks with player career / team history
            if not resolved_id and remarks:
                found_teams = []
                for t_name, t_code in [
                    ("롯데", "LT"),
                    ("두산", "DB"),
                    ("삼성", "SS"),
                    ("SK", "SK"),
                    ("SSG", "SSG"),
                    ("LG", "LG"),
                    ("NC", "NC"),
                    ("KT", "KT"),
                    ("KIA", "KIA"),
                    ("HT", "HT"),
                    ("한화", "HH"),
                    ("키움", "KH"),
                    ("넥센", "NX"),
                ]:
                    if t_name in remarks or t_code in remarks:
                        found_teams.append(t_code)

                if found_teams:
                    team_matches = []
                    for p_id in candidate_ids:
                        p_row = next((r for r in cand_rows if r[0] == p_id), None)
                        if p_row:
                            p_team = p_row[1]
                            if p_team in found_teams:
                                team_matches.append(p_id)
                                continue

                        has_history = False
                        for tbl in ["player_season_batting", "player_season_pitching"]:
                            # Safe formatting for IN clause
                            teams_in_str = ",".join(f"'{t}'" for t in found_teams)
                            hist_rows = session.execute(
                                text(
                                    f"SELECT 1 FROM {tbl} WHERE player_id = :pid AND team_code IN ({teams_in_str}) LIMIT 1"
                                ),
                                {"pid": p_id},
                            ).fetchall()
                            if hist_rows:
                                has_history = True
                                break
                        if has_history:
                            team_matches.append(p_id)

                    if len(team_matches) == 1:
                        resolved_id = team_matches[0]
                        evidence = f"remarks_team_transfer_match_{'-'.join(found_teams)}"

            # Strategy E: Fallback unique candidate
            if not resolved_id and len(candidate_ids) == 1:
                resolved_id = candidate_ids[0]
                evidence = "unique_player_name"

            if resolved_id:
                resolved_count += 1
                if apply:
                    session.execute(
                        text("""
                            UPDATE player_movements
                            SET player_basic_id = :pid, resolution_status = 'resolved'
                            WHERE id = :id
                        """),
                        {"pid": resolved_id, "id": m_id},
                    )
                    print(f"  [APPLY] Resolved {raw_name} ({team_code}) -> {resolved_id} ({evidence})")
                else:
                    print(f"  [DRY-RUN] Would resolve {raw_name} ({team_code}) -> {resolved_id} ({evidence})")

        if apply and resolved_count > 0:
            session.commit()
            print(f"🎉 Successfully resolved and committed {resolved_count} movements.")
        else:
            print(f"🔍 Dry-run completed. Resolvable: {resolved_count} / {len(movements)}")


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    run_resolution(apply=apply)
