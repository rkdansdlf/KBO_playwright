"""
OCI sync — thin composite of domain-specific mixins.
"""

from __future__ import annotations

import logging
import os

from sqlalchemy.exc import SQLAlchemyError

from src.sync.sync_base import OCISyncBase
from src.sync.sync_games import GameSyncMixin
from src.sync.sync_misc import MiscSyncMixin
from src.sync.sync_players import PlayerSyncMixin
from src.sync.sync_stats import StatsSyncMixin

logger = logging.getLogger(__name__)


class OCISync(OCISyncBase, GameSyncMixin, PlayerSyncMixin, StatsSyncMixin, MiscSyncMixin):
    """Composite sync engine combining all domain mixins."""


def _report_no_data() -> None:
    logger.warning("⚠️ 동기화할 데이터가 없습니다.")
    logger.info("📌 먼저 크롤러를 실행하세요:")
    logger.info(
        "   ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save"
    )
    logger.info(
        "   ./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year 2025 --series regular --save"
    )


def _sync_batting_and_pitching(sync: OCISync, batting_count: int, pitching_count: int) -> int:
    total_synced = 0
    if batting_count > 0:
        logger.info("\n🏏 타자 데이터 동기화 중...")
        batting_synced = sync.sync_batting_data()
        sync.verify_batting_sync(batting_synced)
        total_synced += batting_synced
    if pitching_count > 0:
        logger.info("\n⚾ 투수 데이터 동기화 중...")
        pitching_synced = sync.sync_pitcher_data()
        sync.verify_pitcher_sync(pitching_synced)
        total_synced += pitching_synced
    return total_synced


def _log_sync_completion(total_synced: int, batting_count: int, pitching_count: int) -> None:
    logger.info("\n" + "=" * 50)
    logger.info("📈 동기화 완료")
    logger.info("=" * 50)
    logger.info("총 동기화된 데이터: %s건", total_synced)
    if batting_count > 0:
        logger.info("  - 타자 데이터: %s건", batting_count)
    if pitching_count > 0:
        logger.info("  - 투수 데이터: %s건", pitching_count)
    logger.info("\n🎉 OCI에서 데이터를 확인할 수 있습니다!")


def main() -> None:
    """타자 및 투수 데이터 OCI 동기화"""
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.error("❌ OCI_DB_URL environment variable not set")
        logger.info("   Set it in .env file or export it:")
        logger.info("   export OCI_DB_URL='postgresql://user:pass@host:5432/dbname'")
        return

    from src.db.engine import SessionLocal

    logger.info("\n" + "🔄" * 30)
    logger.info("KBO 데이터 OCI 동기화")
    logger.info("🔄" * 30 + "\n")

    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

    with SessionLocal() as sqlite_session:
        try:
            sync = OCISync(oci_url, sqlite_session)
            if not sync.test_connection():
                return

            batting_count = sqlite_session.query(PlayerSeasonBatting).count()
            pitching_count = sqlite_session.query(PlayerSeasonPitching).count()
            logger.info("📊 SQLite 데이터 현황:")
            logger.info("   타자 데이터: %s건", batting_count)
            logger.info("   투수 데이터: %s건", pitching_count)

            if batting_count == 0 and pitching_count == 0:
                _report_no_data()
                return

            total_synced = _sync_batting_and_pitching(sync, batting_count, pitching_count)
            sync.show_oci_data_sample()
            _log_sync_completion(total_synced, batting_count, pitching_count)
            sync.close()

        except (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError):
            logger.exception("Sync error occurred")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
