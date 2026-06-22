"""
Crawl gate: enforce data freshness and quality before allowing pipeline to proceed.
Can be used as a blocking gate or non-blocking alert.
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class CrawlGate:
    def __init__(self, session: Session, *, enforce: bool = False) -> None:
        self.session = session
        self.enforce = enforce
        self.issues: list[str] = []

    def check_freshness(self, target_date: str) -> bool:
        from src.cli.freshness_gate import collect_freshness_issues

        issues = collect_freshness_issues(self.session, target_date)
        if issues:
            for game_id, issue_list in issues.items():
                for issue in issue_list:
                    msg = f"[{game_id}] {issue}"
                    self.issues.append(msg)
                    logger.warning("  ⚠️  %s", msg)
        return len(issues) == 0

    def check_game_completion_rate(self, target_date: str) -> bool:
        from src.models.game import Game
        from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

        games = self.session.query(Game).filter(Game.game_date == target_date).all()
        if not games:
            logger.info("  ℹ️  No games on %s", target_date)
            return True

        completed = sum(1 for g in games if g.game_status in COMPLETED_LIKE_GAME_STATUSES)
        rate = completed / len(games)
        threshold = 0.8

        if rate < threshold:
            msg = f"Completion rate {rate:.0%} ({completed}/{len(games)}) below {threshold:.0%}"
            self.issues.append(msg)
            logger.warning("  ⚠️  %s", msg)
            return False
        return True

    def check_standings_integrity(self, target_date: str) -> bool:
        from src.validators.standings_integrity import validate_standings_integrity

        result = validate_standings_integrity(self.session, target_date)
        if not result.get("ok", False):
            mismatches = result.get("mismatches", [])
            missing_games = result.get("missing_score_games", [])
            if mismatches:
                msg = f"Standings: {len(mismatches)} mismatches"
                self.issues.append(msg)
                logger.warning("  ⚠️  %s", msg)
            if missing_games:
                msg = f"Standings: {len(missing_games)} games with missing scores"
                self.issues.append(msg)
                logger.warning("  ⚠️  %s", msg)
            return False
        return True

    def run_all_checks(self, target_date: str) -> bool:
        logger.info("\n%s", "=" * 50)
        logger.info("  CrawlGate: Checking %s", target_date)
        logger.info("%s", "=" * 50)

        results = [
            ("Freshness", self.check_freshness(target_date)),
            ("Completion Rate", self.check_game_completion_rate(target_date)),
            ("Standings Integrity", self.check_standings_integrity(target_date)),
        ]

        all_pass = all(r[1] for r in results)
        logger.info("\n  Results:")
        for name, ok in results:
            icon = "✅" if ok else "❌"
            logger.info("    %s %s", icon, name)

        if not all_pass and self.enforce:
            logger.error("\n  ❌ CrawlGate ENFORCE mode: blocking pipeline (%s issues)\n", len(self.issues))
            sys.exit(1)

        logger.info("")
        return all_pass


if __name__ == "__main__":
    import argparse

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as session:
        gate = CrawlGate(session, enforce=args.enforce)
        gate.run_all_checks(args.date)
