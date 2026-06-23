"""
통합 KBO 데이터 대시보드 CLI.
standings, park_factor, quality, freshness, rankings, team_defense 섹션을 한 번에 출력.

Usage:
  python -m src.cli.dashboard_report --date 20260527
  python -m src.cli.dashboard_report --date 20260527 --sections standings park_factor
  python -m src.cli.dashboard_report --date 20260527 --format json
  python -m src.cli.dashboard_report --date 20260527 --report --notify
  python -m src.cli.dashboard_report --date 20260527 --sections quality --format html
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal, get_oci_url

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
KST = __import__("zoneinfo").ZoneInfo("Asia/Seoul")
SYNC_CHECK_EXCEPTIONS = (ImportError, SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

AVAILABLE_SECTIONS = [
    "standings",
    "park_factor",
    "rankings",
    "team_defense",
    "quality",
    "freshness",
    "sync",
    "all",
]


def _r2dict(obj: object, model: type[object]) -> dict[str, Any]:
    """Convert SQLAlchemy ORM instance to dict."""
    return {c.name: getattr(obj, c.name) for c in model.__table__.columns}


def _date_or_today(date_str: str | None) -> str:
    if date_str:
        return date_str
    return datetime.now(KST).strftime("%Y%m%d")


# ─── Section builders ────────────────────────────────────────────────────


def _build_standings(session: Session, year: int, date_str: str) -> dict[str, Any]:
    from sqlalchemy import extract

    from src.models.standings import TeamStandingsDaily

    query = session.query(TeamStandingsDaily).filter(extract("year", TeamStandingsDaily.standings_date) == year)
    latest = query.order_by(TeamStandingsDaily.standings_date.desc()).first()
    if not latest:
        return {"rows": [], "date": date_str}
    d = latest.standings_date
    rows = (
        session.query(TeamStandingsDaily)
        .filter(TeamStandingsDaily.standings_date == d)
        .order_by(TeamStandingsDaily.rank)
        .all()
    )
    return {"rows": [_r2dict(r, TeamStandingsDaily) for r in rows], "date": str(d)}


def _build_park_factor(session: Session, year: int) -> dict[str, Any]:
    from src.aggregators.park_factor_calculator import ParkFactorCalculator

    calc = ParkFactorCalculator(session)
    results = calc.calculate(year)
    return {"results": results, "year": year}


def _build_rankings(session: Session, year: int) -> dict[str, Any]:
    from src.aggregators.ranking_aggregator import RankingAggregator
    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

    agg = RankingAggregator(session)
    # Load player stats
    batting = [
        {c.name: getattr(r, c.name) for c in PlayerSeasonBatting.__table__.columns}
        for r in session.query(PlayerSeasonBatting)
        .filter(
            PlayerSeasonBatting.season == year,
            PlayerSeasonBatting.league == "KBO",
        )
        .all()
    ]
    pitching = [
        {c.name: getattr(r, c.name) for c in PlayerSeasonPitching.__table__.columns}
        for r in session.query(PlayerSeasonPitching)
        .filter(
            PlayerSeasonPitching.season == year,
            PlayerSeasonPitching.league == "KBO",
        )
        .all()
    ]
    rankings = agg.generate_rankings(year, batting_stats=batting, pitching_stats=pitching, persist=False)

    top5: dict[str, list] = {}
    for r in rankings:
        cat = r.get("metric_name", "").upper()
        if cat not in top5:
            top5[cat] = []
        if len(top5[cat]) < 5:
            top5[cat].append(
                {
                    "player_name": r.get("player_name", r.get("entity_name", "?")),
                    "value": r.get("value", 0),
                    "rank": r.get("rank", 0),
                },
            )
    return {"top5": top5, "year": year}


def _build_team_defense(session: Session, year: int) -> dict[str, Any]:
    from src.models.team import TeamSeasonBaserunning, TeamSeasonFielding

    fielding = session.query(TeamSeasonFielding).filter(TeamSeasonFielding.season == year).all()
    baserunning = session.query(TeamSeasonBaserunning).filter(TeamSeasonBaserunning.season == year).all()
    return {
        "year": year,
        "fielding": [_r2dict(f, TeamSeasonFielding) for f in fielding],
        "baserunning": [_r2dict(b, TeamSeasonBaserunning) for b in baserunning],
    }


def _build_quality(session: Session, date_str: str, year: int) -> dict[str, Any]:
    from src.cli.generate_quality_report import get_daily_metrics
    from src.validators.quality_gate import run_quality_gate

    metrics = get_daily_metrics(session, date_str)
    gate_result = run_quality_gate(session, year)
    metrics["quality_gate"] = gate_result
    return metrics


def _build_freshness(session: Session, date_str: str) -> dict[str, Any]:
    from src.cli.freshness_gate import collect_freshness_issues

    issues = collect_freshness_issues(session, date_str)
    return {"date": date_str, "issues": issues, "total_issues": sum(len(v) for v in issues.values())}


def _build_sync() -> dict[str, Any]:
    try:
        from scripts.verification.verify_sync_consistency import check_table_counts

        from src.db.engine import create_engine_for_url

        oci_url = get_oci_url()
        if not oci_url:
            return {"status": "skipped", "reason": "OCI_DB_URL not set"}
        oci_engine = create_engine_for_url(oci_url)
        counts = check_table_counts(SessionLocal().bind, oci_engine)
        ok_count = sum(1 for c in counts if c["status"] == "OK")
        return {"status": "ok", "table_count": len(counts), "ok_count": ok_count, "details": counts}
    except SYNC_CHECK_EXCEPTIONS as exc:
        logger.exception("Dashboard OCI sync check failed")
        return {"status": "error", "reason": str(exc)}


# ─── Formatters ──────────────────────────────────────────────────────────


def _row_value(row: object, key: str, default: object = None) -> object:
    if hasattr(row, key):
        return getattr(row, key)
    return row.get(key, default)


def _format_standings_terminal(standings: dict[str, Any], year: int) -> None:
    date_label = standings.get("date", "") or ""
    logger.info("\n%s", "=" * 70)
    logger.info("  KBO %s년 순위표 (기준: %s)", year, date_label)
    logger.info("%s", "=" * 70)
    logger.info(
        "%4s %-6s %4s %4s %3s %7s %5s %8s %4s %8s %8s",
        "순위",
        "팀",
        "승",
        "패",
        "무",
        "승률",
        "승차",
        "최근10",
        "연속",
        "홈",
        "원정",
    )
    logger.info("%s", "-" * 70)
    for row in standings.get("rows", []):
        top5 = "★" if _row_value(row, "top_5") else " "
        current_streak = _row_value(row, "current_streak", 0)
        streak_str = f"{abs(current_streak)}연{'승' if current_streak >= 0 else '패'}" if current_streak else "-"
        recent = f"{_row_value(row, 'recent_10_wins', 0)}승{_row_value(row, 'recent_10_losses', 0)}패"
        home = f"{_row_value(row, 'home_wins', 0)}승{_row_value(row, 'home_losses', 0)}패"
        away = f"{_row_value(row, 'away_wins', 0)}승{_row_value(row, 'away_losses', 0)}패"
        logger.info(
            "  %s%2s %-6s %4s %4s %3s %7.3f %5s %8s %4s %8s %8s",
            top5,
            _row_value(row, "rank", "-"),
            _row_value(row, "team_code", "?"),
            _row_value(row, "wins", 0),
            _row_value(row, "losses", 0),
            _row_value(row, "draws", 0),
            _row_value(row, "win_pct", 0),
            _row_value(row, "games_behind", "-"),
            recent,
            streak_str,
            home,
            away,
        )
    logger.info("%s", "=" * 70)
    logger.info("  ★ 상위 5팀 (5강)")


def _format_park_factor_terminal(park_factor: dict[str, Any]) -> None:
    results = park_factor.get("results", [])
    if not results:
        return
    logger.info("\n%s", "=" * 65)
    logger.info("  KBO %s년 구장별 파크팩터", park_factor["year"])
    logger.info("%s", "=" * 65)
    logger.info("%-20s %4s %6s %6s  평가", "구장", "경기", "RPG", "PF")
    logger.info("%s", "-" * 65)
    for row in sorted(results, key=lambda x: x["park_factor"], reverse=True):
        logger.info(
            "  %-18s %4s %5.1f %5.3f  %s",
            row["stadium"],
            row["games"],
            row["runs_per_game"],
            row["park_factor"],
            row["park_factor_label"],
        )


def _format_rankings_terminal(rankings: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("  KBO %s년 세이버메트릭 TOP5", rankings["year"])
    logger.info("%s", "=" * 60)
    for category, ranked in rankings.get("top5", {}).items():
        if ranked:
            names = ", ".join(f"{row.get('player_name', '?')} ({row.get('value', 0)})" for row in ranked[:3])
            logger.info("  %-10s: %s", category, names)


def _format_team_defense_terminal(team_defense: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("  KBO %s년 팀 수비/주루", team_defense["year"])
    logger.info("%s", "=" * 60)
    logger.info("%-6s %8s %5s %5s %5s %7s", "팀", "수비율", "실책", "도루", "도실", "성공률")
    logger.info("%s", "-" * 60)
    fielding_map = {row.get("team_code", row.get("team")): row for row in team_defense.get("fielding", [])}
    baserunning_map = {row.get("team_code", row.get("team")): row for row in team_defense.get("baserunning", [])}
    all_teams = sorted(set(list(fielding_map.keys()) + list(baserunning_map.keys())))
    kbo_teams = {"SS", "LG", "KT", "KIA", "HH", "DB", "SSG", "NC", "LT", "KH"}
    for team in all_teams:
        if team not in kbo_teams:
            continue
        fielding = fielding_map.get(team, {})
        baserunning = baserunning_map.get(team, {})
        success_rate = baserunning.get("sb_success_rate", "-")
        if isinstance(success_rate, float):
            success_rate = f"{success_rate:.3f}"
        logger.info(
            "  %-6s %8s %5s %5s %5s %7s",
            team,
            str(fielding.get("fielding_pct", "-") or "-"),
            str(fielding.get("errors", "-")),
            str(baserunning.get("stolen_bases", "-")),
            str(baserunning.get("caught_stealing", "-")),
            str(success_rate),
        )


def _format_team_gate_terminal(label: str, result: dict[str, Any]) -> None:
    if result.get("checked_players", 0) <= 0:
        return
    status = "✅" if result.get("ok") else "❌"
    logger.info("  %s: %s (%s개 팀)", label, status, result.get("checked_players", 0))
    for mismatch in result.get("mismatches", []):
        logger.info("    - %s: %s", mismatch.get("team_id"), mismatch.get("issue"))
        for diff in mismatch.get("diffs", [])[:2]:
            logger.info("      %s", diff)


def _format_quality_gate_terminal(quality: dict[str, Any]) -> None:
    gate = quality.get("quality_gate", {})
    if not gate:
        return
    _format_team_gate_terminal("팀 타격 정합성", gate.get("team_batting", {}))
    _format_team_gate_terminal("팀 투수 정합성", gate.get("team_pitching", {}))


def _format_pa_trend_terminal(quality: dict[str, Any]) -> None:
    trend = quality.get("pa_formula_trend", {})
    if not trend or not trend.get("months"):
        return
    direction_icon = (
        "📈" if trend.get("direction") == "worsening" else "📉" if trend.get("direction") == "improving" else "➡️"
    )
    logger.info("  PA 추세 (%s개월): %s %s", len(trend["months"]), direction_icon, trend["direction"])
    for month in trend["months"][-3:]:
        icon = "❌" if month["violation_count"] > 0 else "✅"
        logger.info("    %s %s: %s/%s", icon, month["month"], month["violation_count"], month["total_checked"])


def _format_unified_audit_terminal(quality: dict[str, Any]) -> None:
    gate = quality.get("quality_gate", {})
    if not gate:
        return
    pa_ok = (quality.get("pa_formula_integrity") or {}).get("ok", True)
    team_bat_ok = gate.get("team_batting", {}).get("ok", True)
    team_pit_ok = gate.get("team_pitching", {}).get("ok", True)
    if pa_ok and team_bat_ok and team_pit_ok:
        logger.info("  통합 감사: ✅ 전체 통과")
        return
    issues = []
    if not pa_ok:
        issues.append("PA 공식")
    if not team_bat_ok:
        issues.append("팀 타격")
    if not team_pit_ok:
        issues.append("팀 투수")
    logger.error("  통합 감사: ❌ (%s)", ", ".join(issues))


def _format_quality_terminal(quality: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("  KBO Quality Report (%s)", quality.get("date", "?"))
    logger.info("%s", "=" * 60)
    logger.info("  경기: %s/%s 완료", quality.get("completed_count", 0), quality.get("total_games", 0))
    relay = quality.get("relay_integrity", {})
    if relay:
        logger.info("  PBP 누락: 최근 %s건", relay.get("recent_missing_count", 0))
    standings = quality.get("standings_integrity", {})
    if standings:
        logger.error("  순위 정합성: %s", "✅" if standings.get("ok") else "❌")
    pa_formula = quality.get("pa_formula_integrity", {})
    if pa_formula and pa_formula.get("ok"):
        logger.info("  PA 공식: ✅ 일치")
    elif pa_formula:
        logger.error("  PA 공식: ❌ (%s건 위반)", pa_formula.get("violation_count", 0))
    _format_quality_gate_terminal(quality)
    _format_pa_trend_terminal(quality)
    _format_unified_audit_terminal(quality)


def _format_freshness_terminal(freshness: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("  Freshness Gate (%s)", freshness.get("date", "?"))
    logger.info("%s", "=" * 60)
    logger.info("  총 %s개 이슈", freshness.get("total_issues", 0))
    for game_id, issues in freshness.get("issues", {}).items():
        for issue in issues:
            logger.warning("  ⚠️  [%s] %s", game_id, issue)


def _format_sync_terminal(sync: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("  OCI Sync Status")
    logger.info("%s", "=" * 60)
    if sync.get("status") == "ok":
        logger.info("  ✅ %s/%s tables in sync", sync.get("ok_count", 0), sync.get("table_count", 0))
    else:
        logger.warning("  ⚠️  %s: %s", sync.get("status"), sync.get("reason", ""))


def _format_terminal(data: dict[str, Any], sections: list[str]) -> None:
    year = datetime.now(KST).year
    if "standings" in sections and data.get("standings"):
        _format_standings_terminal(data["standings"], year)
    if "park_factor" in sections and data.get("park_factor"):
        _format_park_factor_terminal(data["park_factor"])
    if "rankings" in sections and data.get("rankings"):
        _format_rankings_terminal(data["rankings"])
    if "team_defense" in sections and data.get("team_defense"):
        _format_team_defense_terminal(data["team_defense"])
    if "quality" in sections and data.get("quality"):
        _format_quality_terminal(data["quality"])
    if "freshness" in sections and data.get("freshness"):
        _format_freshness_terminal(data["freshness"])
    if "sync" in sections and data.get("sync"):
        _format_sync_terminal(data["sync"])
    logger.info("")


def _format_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


# ─── Main ────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO 통합 데이터 대시보드")
    parser.add_argument("--date", help="날짜 (YYYYMMDD, 기본: 오늘)")
    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="시즌 연도")
    parser.add_argument(
        "--sections",
        nargs="+",
        default=["all"],
        choices=AVAILABLE_SECTIONS,
        help="출력할 섹션 (기본: all)",
    )
    parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="출력 포맷")
    parser.add_argument("--report", action="store_true", help="대시보드 리포트 실행 (기본과 동일)")
    parser.add_argument("--notify", action="store_true", help="대시보드를 Telegram/Slack으로 전송")
    return parser.parse_args()


def _normalize_sections(sections: list[str]) -> list[str]:
    return AVAILABLE_SECTIONS[:-1] if "all" in sections else sections


def _build_dashboard_data(sections: list[str], year: int, date_str: str) -> dict[str, Any]:
    data: dict[str, Any] = {}
    with SessionLocal() as session:
        builders = {
            "standings": lambda: _build_standings(session, year, date_str),
            "park_factor": lambda: _build_park_factor(session, year),
            "rankings": lambda: _build_rankings(session, year),
            "team_defense": lambda: _build_team_defense(session, year),
            "quality": lambda: _build_quality(session, date_str, year),
            "freshness": lambda: _build_freshness(session, date_str),
            "sync": _build_sync,
        }
        for section in sections:
            builder = builders.get(section)
            if builder:
                data[section] = builder()
    return data


def _emit_dashboard(data: dict[str, Any], sections: list[str], output_format: str) -> None:
    if output_format == "json":
        logger.info(_format_json(data))
        return
    _format_terminal(data, sections)


def _append_quality_notify_lines(msg_lines: list[str], quality: dict[str, Any]) -> None:
    msg_lines.append(f"완료: {quality.get('completed_count', 0)}/{quality.get('total_games', 0)}")
    gate = quality.get("quality_gate", {})
    if not gate:
        return
    pa_ok = (quality.get("pa_formula_integrity") or {}).get("ok", True)
    team_bat_ok = gate.get("team_batting", {}).get("ok", True)
    team_pit_ok = gate.get("team_pitching", {}).get("ok", True)
    if pa_ok and team_bat_ok and team_pit_ok:
        msg_lines.append("통합 감사: ✅ 전체 통과")
        return
    _append_quality_violation_lines(
        msg_lines,
        quality,
        gate,
        pa_ok=pa_ok,
        team_bat_ok=team_bat_ok,
        team_pit_ok=team_pit_ok,
    )


def _append_quality_violation_lines(
    msg_lines: list[str],
    quality: dict[str, Any],
    gate: dict[str, Any],
    *,
    pa_ok: bool,
    team_bat_ok: bool,
    team_pit_ok: bool,
) -> None:
    violations = []
    if not pa_ok:
        pa_formula = quality.get("pa_formula_integrity", {})
        violations.append(f"PA {pa_formula.get('violation_count', 0)}건")
    if not team_bat_ok:
        bat_mismatches = gate.get("team_batting", {}).get("mismatches", [])
        violations.append(f"팀타격 {len(bat_mismatches)}건")
    if not team_pit_ok:
        pit_mismatches = gate.get("team_pitching", {}).get("mismatches", [])
        violations.append(f"팀투수 {len(pit_mismatches)}건")
    msg_lines.append(f"통합 감사: ❌ ({', '.join(violations)})")
    _append_first_mismatch_line(msg_lines, gate, "team_batting", "팀타격", is_ok=team_bat_ok)
    _append_first_mismatch_line(msg_lines, gate, "team_pitching", "팀투수", is_ok=team_pit_ok)


def _append_first_mismatch_line(
    msg_lines: list[str],
    gate: dict[str, Any],
    gate_key: str,
    label: str,
    *,
    is_ok: bool,
) -> None:
    if is_ok:
        return
    for mismatch in gate.get(gate_key, {}).get("mismatches", [])[:1]:
        team_id = mismatch.get("team_id", "?")
        issue = mismatch.get("issue", "mismatch")
        msg_lines.append(f"  - {label} [{team_id}]: {issue}")


def _send_dashboard_notification(data: dict[str, Any], date_str: str) -> None:
    from src.utils.alerting import SlackWebhookClient

    msg_lines = [f"<b>KBO Dashboard Report ({date_str})</b>"]
    if "standings" in data:
        rows = data["standings"].get("rows", [])
        msg_lines.append(f"순위: {len(rows)}팀")
    if "quality" in data:
        _append_quality_notify_lines(msg_lines, data["quality"])
    SlackWebhookClient.send_alert("\n".join(msg_lines))


def main() -> int:
    args = _parse_args()

    date_str = _date_or_today(args.date)
    sections = _normalize_sections(args.sections)
    data = _build_dashboard_data(sections, args.year, date_str)
    _emit_dashboard(data, sections, args.format)

    if args.notify:
        _send_dashboard_notification(data, date_str)


if __name__ == "__main__":
    main()
