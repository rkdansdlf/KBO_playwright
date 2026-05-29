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
from datetime import datetime
from typing import Any

from src.db.engine import SessionLocal
from src.utils.safe_print import safe_print as print

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


def _r2dict(obj: Any, model: type) -> dict:
    """Convert SQLAlchemy ORM instance to dict."""
    return {c.name: getattr(obj, c.name) for c in model.__table__.columns}


def _date_or_today(date_str: str | None) -> str:
    if date_str:
        return date_str
    return datetime.now(KST).strftime("%Y%m%d")


# ─── Section builders ────────────────────────────────────────────────────


def _build_standings(session, year: int, date_str: str) -> dict:
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


def _build_park_factor(session, year: int) -> dict:
    from src.aggregators.park_factor_calculator import ParkFactorCalculator

    calc = ParkFactorCalculator(session)
    results = calc.calculate(year)
    return {"results": results, "year": year}


def _build_rankings(session, year: int) -> dict:
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
                }
            )
    return {"top5": top5, "year": year}


def _build_team_defense(session, year: int) -> dict:
    from src.models.team import TeamSeasonBaserunning, TeamSeasonFielding

    fielding = session.query(TeamSeasonFielding).filter(TeamSeasonFielding.season == year).all()
    baserunning = session.query(TeamSeasonBaserunning).filter(TeamSeasonBaserunning.season == year).all()
    return {
        "year": year,
        "fielding": [_r2dict(f, TeamSeasonFielding) for f in fielding],
        "baserunning": [_r2dict(b, TeamSeasonBaserunning) for b in baserunning],
    }


def _build_quality(session, date_str: str) -> dict:
    from src.cli.generate_quality_report import get_daily_metrics

    metrics = get_daily_metrics(session, date_str)
    return metrics


def _build_freshness(session, date_str: str) -> dict:
    from src.cli.freshness_gate import collect_freshness_issues

    issues = collect_freshness_issues(session, date_str)
    return {"date": date_str, "issues": issues, "total_issues": sum(len(v) for v in issues.values())}


def _build_sync() -> dict:
    try:
        import os

        from scripts.verification.verify_sync_consistency import check_table_counts

        from src.db.engine import create_engine_for_url

        oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
        if not oci_url:
            return {"status": "skipped", "reason": "OCI_DB_URL not set"}
        oci_engine = create_engine_for_url(oci_url)
        counts = check_table_counts(SessionLocal().bind, oci_engine)
        ok_count = sum(1 for c in counts if c["status"] == "OK")
        return {"status": "ok", "table_count": len(counts), "ok_count": ok_count, "details": counts}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


# ─── Formatters ──────────────────────────────────────────────────────────


def _format_terminal(data: dict[str, Any], sections: list[str]):
    year = datetime.now(KST).year

    if "standings" in sections and data.get("standings"):
        s = data["standings"]
        date_label = s.get("date", "") or ""
        print(f"\n{'=' * 70}")
        print(f"  KBO {year}년 순위표 (기준: {date_label})")
        print(f"{'=' * 70}")
        print(
            f"{'순위':>4} {'팀':<6} {'승':>4} {'패':>4} {'무':>3} {'승률':>7} {'승차':>5} {'최근10':>8} {'연속':>4} {'홈':>8} {'원정':>8}"
        )
        print(f"{'-' * 70}")
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
            print(
                f"  {top5}{rank:>2} {tc:<6} {w:>4} {losses:>4} "
                f"{d:>3} {wp:>7.3f} {gb:>5} "
                f"{recent:>8} {streak_str:>4} {home:>8} {away:>8}"
            )
        sum(1 for r in s.get("rows", []) if (r.top_5 if hasattr(r, "top_5") else r.get("top_5")))
        print(f"{'=' * 70}")
        print("  ★ 상위 5팀 (5강)")

    if "park_factor" in sections and data.get("park_factor"):
        pf = data["park_factor"]
        results = pf.get("results", [])
        if results:
            print(f"\n{'=' * 65}")
            print(f"  KBO {pf['year']}년 구장별 파크팩터")
            print(f"{'=' * 65}")
            print(f"{'구장':<20} {'경기':>4} {'RPG':>6} {'PF':>6}  평가")
            print(f"{'-' * 65}")
            for r in sorted(results, key=lambda x: x["park_factor"], reverse=True):
                print(
                    f"  {r['stadium']:<18} {r['games']:>4} {r['runs_per_game']:>5.1f} {r['park_factor']:>5.3f}  {r['park_factor_label']}"
                )

    if "rankings" in sections and data.get("rankings"):
        rk = data["rankings"]
        print(f"\n{'=' * 60}")
        print(f"  KBO {rk['year']}년 세이버메트릭 TOP5")
        print(f"{'=' * 60}")
        for cat, ranked in rk.get("top5", {}).items():
            if ranked:
                names = ", ".join(f"{r.get('player_name', '?')} ({r.get('value', 0)})" for r in ranked[:3])
                print(f"  {cat:<10}: {names}")

    if "team_defense" in sections and data.get("team_defense"):
        td = data["team_defense"]
        print(f"\n{'=' * 60}")
        print(f"  KBO {td['year']}년 팀 수비/주루")
        print(f"{'=' * 60}")
        print(f"{'팀':<6} {'수비율':>8} {'실책':>5} {'도루':>5} {'도실':>5} {'성공률':>7}")
        print(f"{'-' * 60}")
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
            print(f"  {team:<6} {str(fpct or '-'):>8} {str(err):>5} {str(sb):>5} {str(cs):>5} {str(sbr):>7}")

    if "quality" in sections and data.get("quality"):
        q = data["quality"]
        print(f"\n{'=' * 60}")
        print(f"  KBO Quality Report ({q.get('date', '?')})")
        print(f"{'=' * 60}")
        print(f"  경기: {q.get('completed_count', 0)}/{q.get('total_games', 0)} 완료")
        relay = q.get("relay_integrity", {})
        if relay:
            recent_miss = relay.get("recent_missing_count", 0)
            print(f"  PBP 누락: 최근 {recent_miss}건")
        st = q.get("standings_integrity", {})
        if st:
            print(f"  순위 정합성: {'✅' if st.get('ok') else '❌'}")

    if "freshness" in sections and data.get("freshness"):
        f = data["freshness"]
        print(f"\n{'=' * 60}")
        print(f"  Freshness Gate ({f.get('date', '?')})")
        print(f"{'=' * 60}")
        print(f"  총 {f.get('total_issues', 0)}개 이슈")
        for game_id, issues in f.get("issues", {}).items():
            for issue in issues:
                print(f"  ⚠️  [{game_id}] {issue}")

    if "sync" in sections and data.get("sync"):
        sync = data["sync"]
        print(f"\n{'=' * 60}")
        print("  OCI Sync Status")
        print(f"{'=' * 60}")
        if sync.get("status") == "ok":
            print(f"  ✅ {sync.get('ok_count', 0)}/{sync.get('table_count', 0)} tables in sync")
        else:
            print(f"  ⚠️  {sync.get('status')}: {sync.get('reason', '')}")

    print()


def _format_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


# ─── Main ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="KBO 통합 데이터 대시보드")
    parser.add_argument("--date", help="날짜 (YYYYMMDD, 기본: 오늘)")
    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="시즌 연도")
    parser.add_argument(
        "--sections", nargs="+", default=["all"], choices=AVAILABLE_SECTIONS, help="출력할 섹션 (기본: all)"
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
            data["quality"] = _build_quality(session, date_str)
        if "freshness" in sections:
            data["freshness"] = _build_freshness(session, date_str)
        if "sync" in sections:
            data["sync"] = _build_sync()

    if args.format == "json":
        print(_format_json(data))
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
        SlackWebhookClient.send_alert("\n".join(msg_lines))


if __name__ == "__main__":
    main()
