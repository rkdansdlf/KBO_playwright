"""
OCI sync — thin composite of domain-specific mixins.
"""

import os

from src.sync.sync_base import OCISyncBase
from src.sync.sync_games import GameSyncMixin
from src.sync.sync_misc import MiscSyncMixin
from src.sync.sync_players import PlayerSyncMixin
from src.sync.sync_stats import StatsSyncMixin


class OCISync(OCISyncBase, GameSyncMixin, PlayerSyncMixin, StatsSyncMixin, MiscSyncMixin):
    """Composite sync engine combining all domain mixins."""


def main():
    """타자 및 투수 데이터 OCI 동기화"""
    from src.db.engine import SessionLocal

    # Get OCI URL from environment
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL environment variable not set")
        print("   Set it in .env file or export it:")
        print("   export OCI_DB_URL='postgresql://user:pass@host:5432/dbname'")
        return

    print("\n" + "🔄" * 30)
    print("KBO 데이터 OCI 동기화")
    print("🔄" * 30 + "\n")

    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

    with SessionLocal() as sqlite_session:
        try:
            sync = OCISync(oci_url, sqlite_session)

            # Test connection
            if not sync.test_connection():
                return

            # SQLite 데이터 현황 확인
            batting_count = sqlite_session.query(PlayerSeasonBatting).count()
            pitching_count = sqlite_session.query(PlayerSeasonPitching).count()

            print("📊 SQLite 데이터 현황:")
            print(f"   타자 데이터: {batting_count}건")
            print(f"   투수 데이터: {pitching_count}건")

            if batting_count == 0 and pitching_count == 0:
                print("⚠️ 동기화할 데이터가 없습니다.")
                print("📌 먼저 크롤러를 실행하세요:")
                print(
                    "   ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save"
                )
                print(
                    "   ./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year 2025 --series regular --save"
                )
                return

            total_synced = 0

            # 타자 데이터 동기화
            if batting_count > 0:
                print("\n🏏 타자 데이터 동기화 중...")
                batting_synced = sync.sync_batting_data()
                sync.verify_batting_sync(batting_synced)
                total_synced += batting_synced

            # 투수 데이터 동기화
            if pitching_count > 0:
                print("\n⚾ 투수 데이터 동기화 중...")
                pitching_synced = sync.sync_pitcher_data()
                sync.verify_pitcher_sync(pitching_synced)
                total_synced += pitching_synced

            # OCI 데이터 샘플 표시
            sync.show_oci_data_sample()

            print("\n" + "=" * 50)
            print("📈 동기화 완료")
            print("=" * 50)
            print(f"총 동기화된 데이터: {total_synced}건")
            if batting_count > 0:
                print(f"  - 타자 데이터: {batting_count}건")
            if pitching_count > 0:
                print(f"  - 투수 데이터: {pitching_count}건")
            print("\n🎉 OCI에서 데이터를 확인할 수 있습니다!")

            sync.close()

        except Exception as e:
            print(f"\n❌ Sync error: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
