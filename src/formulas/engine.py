"""Evaluation, Reproducibility Verification, and Audit Engine for Sabermetrics Formulas."""

from __future__ import annotations

import hashlib
import subprocess
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import Engine, get_db_session
from src.formulas.constants import LeagueConstantsEngine
from src.formulas.models import (
    FormulaAuditReport,
    MetricCategory,
    MetricEvaluationResult,
)
from src.formulas.registry import FormulaRegistry

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine import Engine as SQLAlchemyEngine

_TOLERANCE_MAP: dict[str, float] = {
    "AVG": 0.002,
    "OBP": 0.035,
    "SLG": 0.002,
    "OPS": 0.035,
    "ISO": 0.002,
    "wOBA": 0.005,
    "wRC_PLUS": 1.0,
    "OPS_PLUS": 1.0,
    "ERA": 0.02,
    "WHIP": 0.02,
    "FIP": 0.05,
    "ERA_PLUS": 1.0,
}


def _get_git_commit_sha() -> str:
    """Retrieve current Git commit SHA safely."""
    try:
        res = subprocess.run(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode == 0:
            return res.stdout.strip()
    except (subprocess.SubprocessError, OSError):
        pass
    return "unknown_sha"


class FormulaEngine:
    """Master engine for mathematical evaluation, invariant enforcement, and reproducibility certification."""

    def __init__(self, engine: SQLAlchemyEngine | None = None) -> None:
        """Initialize engine with database engine instance."""
        self._engine = engine
        self._constants_cache: dict[int, dict[str, float]] = {}

    def _resolve_engine(self) -> SQLAlchemyEngine:
        """Resolve database engine instance."""
        if self._engine is not None:
            return self._engine
        try:
            with get_db_session() as session:
                session.execute(text("SELECT 1"))
                bind = session.get_bind()
                if bind is not None:
                    return bind  # type: ignore[return-value]
        except (SQLAlchemyError, OSError, RuntimeError):
            return create_engine("sqlite:///./data/kbo_dev.db")
        return Engine

    def get_season_constants(self, season: int) -> dict[str, float]:
        """Retrieve and cache league environment constants for a specific season."""
        if season in self._constants_cache:
            return self._constants_cache[season]

        engine = self._resolve_engine()
        from sqlalchemy.orm import sessionmaker

        session_local = sessionmaker(bind=engine)
        try:
            with session_local() as session:
                constants = LeagueConstantsEngine.compute_league_constants(session, season)
        except (SQLAlchemyError, OSError, RuntimeError, ValueError):
            constants = LeagueConstantsEngine.get_baseline_constants(season)

        self._constants_cache[season] = constants
        return constants

    def _resolve_player_id_and_name(self, conn: Connection, player: str | int) -> tuple[int, str]:
        """Resolve player ID and canonical name."""
        if isinstance(player, int) or (isinstance(player, str) and player.isdigit()):
            pid = int(player)
            row = conn.execute(
                text("SELECT name FROM player_basic WHERE player_id = :pid LIMIT 1"),
                {"pid": pid},
            ).fetchone()
            pname = str(row[0]) if row else f"Player_{pid}"
            return pid, pname

        p_name = str(player).strip()
        row = conn.execute(
            text("SELECT player_id, name FROM player_basic WHERE name = :name LIMIT 1"),
            {"name": p_name},
        ).fetchone()
        if row:
            return int(row[0]), str(row[1])

        # Fallback to game batting stats
        row_stat = conn.execute(
            text("SELECT player_id, player_name FROM game_batting_stats WHERE player_name = :name LIMIT 1"),
            {"name": p_name},
        ).fetchone()
        if row_stat and row_stat[0] is not None:
            return int(row_stat[0]), str(row_stat[1])

        err = f"Player '{player}' not found in registry."
        raise ValueError(err)

    def evaluate_player_metric(
        self,
        player: str | int,
        season: int,
        metric_id: str,
        constants: dict[str, float] | None = None,
    ) -> MetricEvaluationResult:
        """Evaluate a specific registered metric for a player season and compare against stored value."""
        start_t = time.perf_counter()
        engine = self._resolve_engine()
        metric_def = FormulaRegistry.get(metric_id)

        with engine.connect() as conn:
            pid, _ = self._resolve_player_id_and_name(conn, player)

            # Determine table
            is_pitching = metric_def.category == MetricCategory.PITCHING
            stat_table = "player_season_pitching" if is_pitching else "player_season_batting"
            order_col = "innings_outs" if is_pitching else "plate_appearances"
            row_season = (
                conn.execute(
                    text(f"""
                    SELECT * FROM {stat_table}
                    WHERE player_id = :pid AND season = :season
                    ORDER BY CASE WHEN (level = '1군' OR level = 'KBO1' OR level IS NULL) THEN 0 ELSE 1 END,
                             CASE WHEN source = 'AGGREGATED' THEN 0 WHEN source = 'ROLLUP' THEN 1 ELSE 2 END,
                             COALESCE({order_col}, 0) DESC
                    LIMIT 1
                """),  # noqa: S608
                    {"pid": pid, "season": season},
                )
                .mappings()
                .fetchone()
            )

            # Retrieve cached league constants
            c_dict = constants if constants is not None else self.get_season_constants(season)

            inputs: dict[str, Any] = {}
            if row_season:
                inputs.update(dict(row_season))
            else:
                # Fallback: aggregate from game boxscores
                game_stat_tbl = "game_pitching_stats" if is_pitching else "game_batting_stats"
                boxscore_rows = (
                    conn.execute(
                        text(f"""
                        SELECT * FROM {game_stat_tbl}
                        WHERE player_id = :pid AND game_id LIKE :season || '%'
                    """),  # noqa: S608
                        {"pid": pid, "season": str(season)},
                    )
                    .mappings()
                    .fetchall()
                )

                if boxscore_rows:
                    inputs["hits"] = sum(r.get("hits", 0) or 0 for r in boxscore_rows)
                    inputs["at_bats"] = sum(r.get("at_bats", 0) or 0 for r in boxscore_rows)
                    inputs["doubles"] = sum(r.get("doubles", 0) or 0 for r in boxscore_rows)
                    inputs["triples"] = sum(r.get("triples", 0) or 0 for r in boxscore_rows)
                    inputs["home_runs"] = sum(r.get("home_runs", 0) or 0 for r in boxscore_rows)
                    inputs["walks"] = sum(r.get("walks", 0) or 0 for r in boxscore_rows)
                    inputs["hbp"] = sum(r.get("hbp", 0) or 0 for r in boxscore_rows)
                    inputs["strikeouts"] = sum(r.get("strikeouts", 0) or 0 for r in boxscore_rows)
                    sf_sum = sum(r.get("sacrifice_flies", 0) or 0 for r in boxscore_rows)
                    inputs["plate_appearances"] = inputs["at_bats"] + inputs["walks"] + inputs["hbp"] + sf_sum

            # Evaluate pure formula
            calculated = metric_def.evaluate(inputs, c_dict)

            # Validate mathematical invariants
            val_errors = [
                f"Invariant '{rule.name}' failed: {rule.error_message}"
                for rule in metric_def.validation_rules
                if not rule.validate(calculated, inputs)
            ]
            invariants_passed = len(val_errors) == 0

            # Comparison with stored value
            stored_val = None
            db_field_map = {
                "AVG": "avg",
                "OBP": "obp",
                "SLG": "slg",
                "OPS": "ops",
                "ISO": "iso",
                "BABIP_BAT": "babip",
                "wOBA": "woba",
                "wRC_PLUS": "wrc_plus",
                "ERA": "era",
                "WHIP": "whip",
            }
            db_col = db_field_map.get(metric_def.metric_id)
            if db_col and row_season and db_col in row_season and row_season[db_col] is not None:
                try:
                    stored_val = float(row_season[db_col])
                except (ValueError, TypeError):
                    stored_val = None

            delta = 0.0
            is_reproducible = True
            if stored_val is not None and isinstance(calculated, (int, float)):
                delta = round(abs(float(calculated) - stored_val), 4)
                tol = _TOLERANCE_MAP.get(metric_def.metric_id, 0.01)
                is_reproducible = delta <= tol

            exec_us = (time.perf_counter() - start_t) * 1_000_000.0

            return MetricEvaluationResult(
                metric_id=metric_def.metric_id,
                entity_id=pid,
                season=season,
                calculated_value=calculated,
                stored_value=stored_val,
                inputs_used=inputs,
                constants_used=c_dict,
                delta=delta,
                is_reproducible=is_reproducible,
                invariants_passed=invariants_passed,
                validation_errors=val_errors,
                execution_time_us=exec_us,
            )

    def audit_reproducibility(
        self,
        season: int | None = None,
        category: MetricCategory | None = None,
        sample: int | None = None,
    ) -> FormulaAuditReport:
        """Audit mathematical reproducibility across database player season records."""
        start_t = time.perf_counter()
        engine = self._resolve_engine()
        metrics_to_audit = FormulaRegistry.list_all(category=category)

        total_evals = 0
        reproducible_count = 0
        divergent_count = 0
        metric_stats: dict[str, dict[str, Any]] = {}

        with engine.connect() as conn:
            # Query active players in target season(s) (certified regular season 1군)
            base_filters = [
                "(level = '1군' OR level = 'KBO1' OR level IS NULL)",
                "(league = 'REGULAR' OR league IS NULL)",
            ]
            if season is not None:
                base_filters.append(f"season = {season}")

            filters_bat = [*base_filters, "at_bats IS NOT NULL"]
            filters_pit = [*base_filters, "(innings_outs > 0 OR innings_pitched > 0)"]

            where_bat = "WHERE " + " AND ".join(filters_bat)
            where_pit = "WHERE " + " AND ".join(filters_pit)
            limit_clause = f"LIMIT {sample}" if sample else "LIMIT 500"

            sql_batting = (
                f"SELECT player_id, season FROM player_season_batting {where_bat} "  # noqa: S608
                f"ORDER BY season DESC, plate_appearances DESC {limit_clause}"
            )
            sql_pitching = (
                f"SELECT player_id, season FROM player_season_pitching {where_pit} "  # noqa: S608
                f"ORDER BY season DESC, innings_outs DESC {limit_clause}"
            )

            bat_players = conn.execute(text(sql_batting)).fetchall()
            pit_players = conn.execute(text(sql_pitching)).fetchall()

            # Pre-populate constants
            seasons_set = {int(r[1]) for r in (bat_players + pit_players)}
            for s_yr in seasons_set:
                self.get_season_constants(s_yr)

            for m in metrics_to_audit:
                m_evals = 0
                m_rep = 0
                m_div = 0
                players = pit_players if m.category == MetricCategory.PITCHING else bat_players

                for p_row in players:
                    pid = int(p_row[0])
                    s_yr = int(p_row[1])
                    c_dict = self.get_season_constants(s_yr)
                    res = self.evaluate_player_metric(pid, s_yr, m.metric_id, constants=c_dict)
                    m_evals += 1
                    total_evals += 1
                    if res.is_reproducible and res.invariants_passed:
                        m_rep += 1
                        reproducible_count += 1
                    else:
                        m_div += 1
                        divergent_count += 1

                metric_stats[m.metric_id] = {
                    "category": m.category.value,
                    "evaluations": m_evals,
                    "reproducible": m_rep,
                    "divergent": m_div,
                    "reproducibility_ratio": round(m_rep / max(m_evals, 1), 4),
                }

        rep_ratio = round(reproducible_count / max(total_evals, 1), 5) if total_evals > 0 else 1.0
        dur_ms = (time.perf_counter() - start_t) * 1000.0

        git_sha = _get_git_commit_sha()
        gen_time = datetime.now(UTC).isoformat()
        content_for_hash = f"{git_sha}:{gen_time}:{total_evals}:{reproducible_count}:{rep_ratio}"
        sha_sum = hashlib.sha256(content_for_hash.encode("utf-8")).hexdigest()

        return FormulaAuditReport(
            audit_mode="SEASON" if season else "ALL_SEASONS",
            season=season,
            category=category.value if category else None,
            total_metrics_evaluated=len(metrics_to_audit),
            total_entities_checked=total_evals,
            reproducible_count=reproducible_count,
            divergent_count=divergent_count,
            reproducibility_ratio=rep_ratio,
            metric_breakdowns=metric_stats,
            duration_ms=dur_ms,
            is_compliant=divergent_count == 0,
            git_sha=git_sha,
            generated_at_utc=gen_time,
            sha256_checksum=sha_sum,
        )


__all__ = [
    "FormulaEngine",
]
