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
from typing import Any

from src.db.engine import SessionLocal, get_oci_url

logger = logging.getLogger(__name__)
KST = __import__("zoneinfo").ZoneInfo("Asia/Seoul")

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


def _r2dict(obj: Any, model: type) -> dict[str, Any]:
    """Convert SQLAlchemy ORM instance to dict."""
    return {c.name: getattr(obj, c.name) for c in model.__table__.columns}


def _date_or_today(date_str: str | None) -> str:
    if date_str:
        return date_str
    return datetime.now(KST).strftime("%Y%m%d")


# ─── Section builders ────────────────────────────────────────────────────


def _build_standings(session, year: int, date_str: str) -> dict[str, Any]:
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


def _build_park_factor(session, year: int) -> dict[str, Any]:
    from src.aggregators.park_factor_calculator import ParkFactorCalculator

    calc = ParkFactorCalculator(session)
    results = calc.calculate(year)
    return {"results": results, "year": year}


def _build_rankings(session, year: int) -> dict[str, Any]:
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


def _build_team_defense(session, year: int) -> dict[str, Any]:
    from src.models.team import TeamSeasonBaserunning, TeamSeasonFielding

    fielding = session.query(TeamSeasonFielding).filter(TeamSeasonFielding.season == year).all()
    baserunning = session.query(TeamSeasonBaserunning).filter(TeamSeasonBaserunning.season == year).all()
    return {
        "year": year,
        "fielding": [_r2dict(f, TeamSeasonFielding) for f in fielding],
        "baserunning": [_r2dict(b, TeamSeasonBaserunning) for b in baserunning],
    }


def _build_quality(session, date_str: str, year: int) -> dict[str, Any]:
    from src.cli.generate_quality_report import get_daily_metrics
    from src.validators.quality_gate import run_quality_gate

    metrics = get_daily_metrics(session, date_str)
    gate_result = run_quality_gate(session, year)
    metrics["quality_gate"] = gate_result
    return metrics


def _build_freshness(session, date_str: str) -> dict[str, Any]:
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
    except Exception as exc:
        logger.exception("Dashboard OCI sync check failed")
        return {"status": "error", "reason": str(exc)}


# ─── Formatters ──────────────────────────────────────────────────────────


def _format_terminal(data: dict[str, Any], sections: list[str]) -> None:
    year = datetime.now(KST).year

    if "standings" in sections and data.get("standings"):
        s = data["standings"]
        date_label = s.get("date", "") or ""
        logger.info(f"\n{'=' * 70}")
        logger.info(f"  KBO {year}년 순위표 (기준: {date_label})")
        logger.info(f"{'=' * 70}")
        logger.info(
            f"{'순위':>4} {'팀':<6} {'승':>4} {'패':>4} {'무':>3} {'승률':>7} {'승차':>5} {'최근10':>8} {'연속':>4} {'홈':>8} {'원정':>8}",
        )
        logger.info(f"{'-' * 70}")
        for r in s.get("rows", []):
            top5 = "★" if (r.top_5 if hasattr(r, "top_5") else r.get("top_5")) else " "
            cs = r.current_streak if hasattr(r, "current_streak") else r.get("current_streak", 0)
            streak_str = f"{abs(cs)}연{'승' if cs >= 0 else '패'}" if cs else "-"
            rc10_w = r.recent_10_wins if hasattr(r, "recent_10_wins") else r.get("recent_10_wins", 0)
            rc10_l = r.recent_10_losses if hasattr(r, "recent_10_losses") else r.get("recent_10_losses", 0)
            hw = r.home_wins if hasattr(r, "home_wins") else r.get("home_wins", 0)
            hl = r.home_losses if hasattr(r, "home_losses") else r.get("home_losses", 0)
            aw = r.away_wins if hasattr(r, "away_wins") else r.get("away_wins", 0)
            al = r.away_losses if hasattr(r, "away_losses") else r.get("away_losses", 0)
            recent = f"{rc10_w}승{rc10_l}패"
            home = f"{hw}승{hl}패"
            away = f"{aw}승{al}패"
            rank = r.rank if hasattr(r, "rank") else r.get("rank", "-")
            tc = r.team_code if hasattr(r, "team_code") else r.get("team_code", "?")
            w = r.wins if hasattr(r, "wins") else r.get("wins", 0)
            losses = r.losses if hasattr(r, "losses") else r.get("losses", 0)
            d = r.draws if hasattr(r, "draws") else r.get("draws", 0)
            wp = r.win_pct if hasattr(r, "win_pct") else r.get("win_pct", 0)
            gb = r.games_behind if hasattr(r, "games_behind") else r.get("games_behind", "-")
            logger.info(
                f"  {top5}{rank:>2} {tc:<6} {w:>4} {losses:>4} "
                f"{d:>3} {wp:>7.3f} {gb:>5} "
                f"{recent:>8} {streak_str:>4} {home:>8} {away:>8}",
            )
        sum(1 for r in s.get("rows", []) if (r.top_5 if hasattr(r, "top_5") else r.get("top_5")))
        logger.info(f"{'=' * 70}")
        logger.info("  ★ 상위 5팀 (5강)")

    if "park_factor" in sections and data.get("park_factor"):
        pf = data["park_factor"]
        results = pf.get("results", [])
        if results:
            logger.info(f"\n{'=' * 65}")
            logger.info(f"  KBO {pf['year']}년 구장별 파크팩터")
            logger.info(f"{'=' * 65}")
            logger.info(f"{'구장':<20} {'경기':>4} {'RPG':>6} {'PF':>6}  평가")
            logger.info(f"{'-' * 65}")
            for r in sorted(results, key=lambda x: x["park_factor"], reverse=True):
                logger.info(
                    f"  {r['stadium']:<18} {r['games']:>4} {r['runs_per_game']:>5.1f} {r['park_factor']:>5.3f}  {r['park_factor_label']}",
                )

    if "rankings" in sections and data.get("rankings"):
        rk = data["rankings"]
        logger.info(f"\n{'=' * 60}")
        logger.info(f"  KBO {rk['year']}년 세이버메트릭 TOP5")
        logger.info(f"{'=' * 60}")
        for cat, ranked in rk.get("top5", {}).items():
            if ranked:
                names = ", ".join(f"{r.get('player_name', '?')} ({r.get('value', 0)})" for r in ranked[:3])
                logger.info(f"  {cat:<10}: {names}")

    if "team_defense" in sections and data.get("team_defense"):
        td = data["team_defense"]
        logger.info(f"\n{'=' * 60}")
        logger.info(f"  KBO {td['year']}년 팀 수비/주루")
        logger.info(f"{'=' * 60}")
        logger.info(f"{'팀':<6} {'수비율':>8} {'실책':>5} {'도루':>5} {'도실':>5} {'성공률':>7}")
        logger.info(f"{'-' * 60}")
        fielding_map = {f.get("team_code", f.get("team")): f for f in td.get("fielding", [])}
        baserunning_map = {b.get("team_code", b.get("team")): b for b in td.get("baserunning", [])}
        all_teams = sorted(set(list(fielding_map.keys()) + list(baserunning_map.keys())))
        kbo_teams = {"SS", "LG", "KT", "KIA", "HH", "DB", "SSG", "NC", "LT", "KH"}
        for team in all_teams:
            if team not in kbo_teams:
                continue
            f = fielding_map.get(team, {})
            b = baserunning_map.get(team, {})
            fpct = f.get("fielding_pct", "-")
            err = f.get("errors", "-")
            sb = b.get("stolen_bases", "-")
            cs = b.get("caught_stealing", "-")
            sbr = b.get("sb_success_rate", "-")
            if isinstance(sbr, float):
                sbr = f"{sbr:.3f}"
            logger.info(f"  {team:<6} {str(fpct or '-'):>8} {str(err):>5} {str(sb):>5} {str(cs):>5} {str(sbr):>7}")

    if "quality" in sections and data.get("quality"):
        q = data["quality"]
        logger.info(f"\n{'=' * 60}")
        logger.info(f"  KBO Quality Report ({q.get('date', '?')})")
        logger.info(f"{'=' * 60}")
        logger.info(f"  경기: {q.get('completed_count', 0)}/{q.get('total_games', 0)} 완료")
        relay = q.get("relay_integrity", {})
        if relay:
            recent_miss = relay.get("recent_missing_count", 0)
            logger.info(f"  PBP 누락: 최근 {recent_miss}건")
        st = q.get("standings_integrity", {})
        if st:
            logger.error(f"  순위 정합성: {'✅' if st.get('ok') else '❌'}")
        pa = q.get("pa_formula_integrity", {})
        if pa:
            if pa.get("ok"):
                logger.info("  PA 공식: ✅ 일치")
            else:
                logger.error(f"  PA 공식: ❌ ({pa.get('violation_count', 0)}건 위반)")
        gate = q.get("quality_gate", {})
        if gate:
            team_bat = gate.get("team_batting", {})
            team_pit = gate.get("team_pitching", {})
            if team_bat.get("checked_players", 0) > 0:
                status = "✅" if team_bat.get("ok") else "❌"
                logger.info(f"  팀 타격 정합성: {status} ({team_bat.get('checked_players', 0)}개 팀)")
                for m in team_bat.get("mismatches", []):
                    logger.info(f"    - {m.get('team_id')}: {m.get('issue')}")
                    for d in m.get("diffs", [])[:2]:
                        logger.info(f"      {d}")
            if team_pit.get("checked_players", 0) > 0:
                status = "✅" if team_pit.get("ok") else "❌"
                logger.info(f"  팀 투수 정합성: {status} ({team_pit.get('checked_players', 0)}개 팀)")
                for m in team_pit.get("mismatches", []):
                    logger.info(f"    - {m.get('team_id')}: {m.get('issue')}")
                    for d in m.get("diffs", [])[:2]:
                        logger.info(f"      {d}")
        trend = q.get("pa_formula_trend", {})
        if trend and trend.get("months"):
            direction_icon = (
                "📈"
                if trend.get("direction") == "worsening"
                else "📉"
                if trend.get("direction") == "improving"
                else "➡️"
            )
            logger.info(f"  PA 추세 ({len(trend['months'])}개월): {direction_icon} {trend['direction']}")
            for m in trend["months"][-3:]:
                icon = "❌" if m["violation_count"] > 0 else "✅"
                logger.info(f"    {icon} {m['month']}: {m['violation_count']}/{m['total_checked']}")
        gate = q.get("quality_gate", {})
        if gate:
            pa_ok = (q.get("pa_formula_integrity") or {}).get("ok", True)
            team_bat_ok = gate.get("team_batting", {}).get("ok", True)
            team_pit_ok = gate.get("team_pitching", {}).get("ok", True)
            all_ok = pa_ok and team_bat_ok and team_pit_ok
            if all_ok:
                logger.info("  통합 감사: ✅ 전체 통과")
            else:
                issues = []
                if not pa_ok:
                    issues.append("PA 공식")
                if not team_bat_ok:
                    issues.append("팀 타격")
                if not team_pit_ok:
                    issues.append("팀 투수")
                logger.error(f"  통합 감사: ❌ ({', '.join(issues)})")

    if "freshness" in sections and data.get("freshness"):
        f = data["freshness"]
        logger.info(f"\n{'=' * 60}")
        logger.info(f"  Freshness Gate ({f.get('date', '?')})")
        logger.info(f"{'=' * 60}")
        logger.info(f"  총 {f.get('total_issues', 0)}개 이슈")
        for game_id, issues in f.get("issues", {}).items():
            for issue in issues:
                logger.warning(f"  ⚠️  [{game_id}] {issue}")

    if "sync" in sections and data.get("sync"):
        sync = data["sync"]
        logger.info(f"\n{'=' * 60}")
        logger.info("  OCI Sync Status")
        logger.info(f"{'=' * 60}")
        if sync.get("status") == "ok":
            logger.info(f"  ✅ {sync.get('ok_count', 0)}/{sync.get('table_count', 0)} tables in sync")
        else:
            logger.warning(f"  ⚠️  {sync.get('status')}: {sync.get('reason', '')}")

    logger.info("")


def _format_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


# ─── Main ────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO 통합 데이터 대시보드")
    parser.add_argument("--date", help="날짜 (YYYYMMDD, 기본: 오늘)")
    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="시즌 연도")
    parser.add_argument(
        "--sections", nargs="+", default=["all"], choices=AVAILABLE_SECTIONS, help="출력할 섹션 (기본: all)",
    )
    parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="출력 포맷")
    parser.add_argument("--report", action="store_true", help="대시보드 리포트 실행 (기본과 동일)")
    parser.add_argument("--notify", action="store_true", help="대시보드를 Telegram/Slack으로 전송")
    args = parser.parse_args()

    date_str = _date_or_today(args.date)
    sections = AVAILABLE_SECTIONS[:-1] if "all" in args.sections else args.sections

    data: dict[str, Any] = {}

    with SessionLocal() as session:
        if "standings" in sections:
            data["standings"] = _build_standings(session, args.year, date_str)
        if "park_factor" in sections:
            data["park_factor"] = _build_park_factor(session, args.year)
        if "rankings" in sections:
            data["rankings"] = _build_rankings(session, args.year)
        if "team_defense" in sections:
            data["team_defense"] = _build_team_defense(session, args.year)
        if "quality" in sections:
            data["quality"] = _build_quality(session, date_str, args.year)
        if "freshness" in sections:
            data["freshness"] = _build_freshness(session, date_str)
        if "sync" in sections:
            data["sync"] = _build_sync()

    if args.format == "json":
        logger.info(_format_json(data))
    else:
        _format_terminal(data, sections)

    if args.notify:
        from src.utils.alerting import SlackWebhookClient

        msg_lines = [f"<b>KBO Dashboard Report ({date_str})</b>"]
        if "standings" in data:
            rows = data["standings"].get("rows", [])
            msg_lines.append(f"순위: {len(rows)}팀")
        if "quality" in data:
            q = data["quality"]
            msg_lines.append(f"완료: {q.get('completed_count', 0)}/{q.get('total_games', 0)}")
            gate = q.get("quality_gate", {})
            if gate:
                pa_ok = (q.get("pa_formula_integrity") or {}).get("ok", True)
                team_bat_ok = gate.get("team_batting", {}).get("ok", True)
                team_pit_ok = gate.get("team_pitching", {}).get("ok", True)
                all_ok = pa_ok and team_bat_ok and team_pit_ok
                if all_ok:
                    msg_lines.append("통합 감사: ✅ 전체 통과")
                else:
                    violations = []
                    if not pa_ok:
                        pa = q.get("pa_formula_integrity", {})
                        violations.append(f"PA {pa.get('violation_count', 0)}건")
                    if not team_bat_ok:
                        tb = gate.get("team_batting", {})
                        bat_mismatches = tb.get("mismatches", [])
                        violations.append(f"팀타격 {len(bat_mismatches)}건")
                    if not team_pit_ok:
                        tp = gate.get("team_pitching", {})
                        pit_mismatches = tp.get("mismatches", [])
                        violations.append(f"팀투수 {len(pit_mismatches)}건")
                    msg_lines.append(f"통합 감사: ❌ ({', '.join(violations)})")
                    if not team_bat_ok:
                        bat_mismatches = gate.get("team_batting", {}).get("mismatches", [])
                        for m in bat_mismatches[:1]:
                            team_id = m.get("team_id", "?")
                            issue = m.get("issue", "mismatch")
                            msg_lines.append(f"  - 팀타격 [{team_id}]: {issue}")
                    if not team_pit_ok:
                        pit_mismatches = gate.get("team_pitching", {}).get("mismatches", [])
                        for m in pit_mismatches[:1]:
                            team_id = m.get("team_id", "?")
                            issue = m.get("issue", "mismatch")
                            msg_lines.append(f"  - 팀투수 [{team_id}]: {issue}")
        SlackWebhookClient.send_alert("\n".join(msg_lines))


if __name__ == "__main__":
    main()
