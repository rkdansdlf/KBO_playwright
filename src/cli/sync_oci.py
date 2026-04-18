"""로컬 SQLite 데이터베이스의 데이터를 원격 OCI/Postgres 데이터베이스와 동기화하는 스크립트.

이 스크립트는 SQLAlchemy를 사용하여 두 데이터베이스 간의 데이터 이관을 수행합니다.
테이블 간의 외래 키 제약 조건을 고려하여 정의된 `MODEL_ORDER` 순서에 따라 데이터를
안전하게 복사합니다. `--truncate` 옵션을 사용하면 대상 테이블의 데이터를 삭제한 후
새로 삽입할 수 있습니다.
"""
from __future__ import annotations

import argparse
import os
from typing import Iterable, List, Type

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from src.db.engine import create_engine_for_url
from src.models.base import Base

from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic
from src.models.season import KboSeason
from src.models.team import Team
from src.sync.oci_sync import OCISync
from src.db.engine import SessionLocal


# 외래 키 제약 조건을 고려한 모델 처리 순서
MODEL_ORDER: List[Type] = [
    # Team,  # Handled by specialized --teams sync due to JSON vs TEXT[] type mismatch
    KboSeason,
    PlayerBasic,
    # PlayerSeasonBatting/Pitching removed from here to enforce Bulk COPY via --season-stats
]


def clone_row(instance: object, model: Type) -> object:
    """SQLAlchemy 모델 인스턴스를 복제합니다."""
    data = {col.key: getattr(instance, col.key) for col in model.__table__.columns}
    return model(**data)


def sync_databases(source_url: str, target_url: str, truncate: bool = False) -> None:
    """원본 데이터베이스에서 대상 데이터베이스로 데이터를 동기화합니다."""
    source_engine = create_engine_for_url(source_url, disable_sqlite_wal=True)
    target_engine = create_engine_for_url(target_url, disable_sqlite_wal=True)

    # 대상 데이터베이스에 테이블이 없으면 생성합니다.
    try:
        Base.metadata.create_all(bind=target_engine)
    except Exception as e:
        print(f"⚠️ Table creation failed (might already exist or schema issue): {e}")

    SourceSession = sessionmaker(bind=source_engine, autoflush=False, autocommit=False)
    TargetSession = sessionmaker(bind=target_engine, autoflush=False, autocommit=False)

    with SourceSession() as src, TargetSession() as dst:
        for model in MODEL_ORDER:
            total = src.query(model).count()
            if total == 0:
                continue

            # --truncate 옵션이 주어지면 대상 테이블의 데이터를 삭제합니다.
            if truncate:
                if model is Team:
                    # NOTE: teams is a semi-static reference table. Do NOT truncate because
                    # OCI still has legacy tables (e.g., team_history) with FK references.
                    # Always rely on UPSERT behavior for teams.
                    print("   ⚠️  Skipping truncate for teams (reference table with legacy FKs)")
                else:
                    dst.execute(delete(model))
                    dst.commit()

            print(f"🚚 Syncing {model.__name__} ({total} rows)…")
            batch_size = 500
            offset = 0
            pk_columns = list(model.__table__.primary_key.columns)
            while offset < total:
                query = src.query(model)
                if pk_columns:
                    query = query.order_by(*pk_columns)
                rows = query.offset(offset).limit(batch_size).all()
                clones = [clone_row(row, model) for row in rows]
                for clone in clones:
                    dst.merge(clone) # UPSERT 로직 수행
                dst.commit()
                offset += len(rows)
        print("✅ Sync complete")


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Sync local SQLite data to OCI/Postgres")
    parser.add_argument(
        "--source-url",
        type=str,
        default=os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db"),
        help="원본 데이터베이스 URL (기본값: 로컬 SQLite)",
    )
    parser.add_argument(
        "--target-url",
        type=str,
        default=os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL"),
        help="대상 데이터베이스 URL (OCI/Postgres)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="데이터 삽입 전 대상 테이블의 모든 데이터를 삭제합니다.",
    )
    parser.add_argument(
        "--teams",
        action="store_true",
        help="프랜차이즈 및 팀 정보를 동기화합니다.",
    )
    parser.add_argument(
        "--game-details",
        action="store_true",
        help="경기 상세 데이터(박스스코어, 라인업, PBP 등)를 동기화합니다.",
    )
    parser.add_argument(
        "--games-only",
        action="store_true",
        help="game 테이블만 경량 동기화합니다. --year와 함께 사용 가능합니다.",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="최근 N일간의 경기 데이터만 동기화합니다.",
    )
    parser.add_argument(
        "--unsynced-only",
        action="store_true",
        help="OCI에 없거나 로컬 업데이트가 더 최근인 미동기화/수정된 데이터만 선별하여 동기화합니다. schedule-only 부모 game 행은 자동 제외됩니다.",
    )
    parser.add_argument(
        "--player-basic",
        action="store_true",
        help="선수 기본 정보(Player Basic)를 동기화합니다.",
    )
    parser.add_argument(
        "--players",
        action="store_true",
        help="마스터 선수 레코드(Players 테이블)를 동기화합니다. (사진, 상세 프로필 포함)",
    )
    parser.add_argument(
        "--daily-roster",
        action="store_true",
        help="일별 1군 등록 현황(Daily Roster)을 동기화합니다.",
    )
    parser.add_argument(
        "--player-movements",
        action="store_true",
        help="선수 이동 현황(Trade, FA 등)을 동기화합니다.",
    )
    parser.add_argument(
        "--awards",
        action="store_true",
        help="수상 내역(Awards)을 동기화합니다.",
    )
    parser.add_argument(
        "--crawl-runs",
        action="store_true",
        help="크롤링 실행 기록(Crawl Runs)을 동기화합니다.",
    )
    parser.add_argument(
        "--season-stats",
        action="store_true",
        help="선수 시즌 누적 스탯(타자, 투수)을 고속 동기화합니다.",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="특정 연도의 데이터를 동기화합니다. (e.g., 2018)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="동기화할 최대 행 수를 지정합니다.",
    )
    parser.add_argument(
        "--standings",
        action="store_true",
        help="일별 팀 순위(Standings) 스냅샷 테이블을 고속 동기화합니다.",
    )
    parser.add_argument(
        "--matchups",
        action="store_true",
        help="계산된 상대 전적(Matchup Splits) 테이블을 동기화합니다.",
    )
    parser.add_argument(
        "--rankings",
        action="store_true",
        help="계산된 stat_rankings 테이블을 동기화합니다.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    """스크립트의 메인 실행 함수."""
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not args.target_url:
        raise SystemExit("TARGET_DATABASE_URL must be provided via flag or environment variable")

    if args.game_details:
        print("🚀 Syncing Game Details using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_game_details(days=args.days, year=args.year, unsynced_only=args.unsynced_only)
            print("✅ Game Details Sync Finished")

    elif args.games_only:
        print("🚀 Syncing game table only using specialized OCISync...")
        with SessionLocal() as session:
            from src.models.game import Game

            syncer = OCISync(args.target_url, session)
            filters = [Game.game_id.like(f"{args.year}%")] if args.year else None
            synced = syncer.sync_games(filters=filters)
            print(f"✅ Game Table Sync Finished ({synced} rows)")

    elif args.daily_roster:
        print("🚀 Syncing Daily Rosters using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            synced = syncer.sync_daily_rosters()
            print("✅ Daily Roster Sync Finished")

    elif args.player_basic:
        print(f"🚀 Syncing Player Basic using specialized OCISync (limit={args.limit})...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            synced = syncer.sync_player_basic(limit=args.limit)
            print(f"✅ Player Basic Sync Finished ({synced} rows)")

    elif args.players:
        print(f"🚀 Syncing Master Players using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_players()
            print(f"✅ Master Players Sync Finished")

    elif args.player_movements:
        print("🚀 Syncing Player Movements using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_player_movements()
            print("✅ Player Movement Sync Finished")
            
    elif args.teams:
        print("🚀 Syncing Franchises & Teams using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            synced = syncer.sync_teams()
            print(f"✅ Franchises & Teams Sync Finished")

    elif args.standings:
        print("🚀 Syncing Daily Standings using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            synced = syncer.sync_standings(days=args.days, year=args.year)
            print(f"✅ Daily Standings Sync Finished ({synced} rows)")

    elif args.awards:
        print("🚀 Syncing Awards using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_awards()
            print("✅ Awards Sync Finished")

    elif args.crawl_runs:
        print("🚀 Syncing Crawl Runs using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_crawl_runs()
            print("✅ Crawl Runs Sync Finished")

    elif args.season_stats:
        print("🚀 Syncing Season Stats (Batting & Pitching) using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            print("  - Syncing Batting stats...")
            syncer.sync_player_season_batting()
            print("  - Syncing Pitching stats...")
            syncer.sync_player_season_pitching()
            print("✅ Season Stats Sync Finished")
            
    elif args.matchups:
        print("🚀 Syncing Matchup Splits using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.sync_matchups(year=args.year)
            print("✅ Matchup Splits Sync Finished")

    elif args.rankings:
        print("🚀 Syncing Stat Rankings using specialized OCISync...")
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            synced = syncer.sync_stat_rankings(year=args.year)
            print(f"✅ Stat Rankings Sync Finished ({synced} rows)")


        
    else:
        sync_databases(args.source_url, args.target_url, truncate=args.truncate)

    print("\n🚀 Resetting Sequence Identifiers on Target DB...")
    try:
        from scripts.maintenance.reset_oci_sequences import reset_sequences
        reset_sequences(args.target_url)
    except Exception as e:
        print(f"⚠️ Failed to call reset_sequences: {e}")


if __name__ == "__main__":  # pragma: no cover
    main()
