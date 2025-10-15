"""로컬 SQLite 데이터베이스의 데이터를 원격 Supabase/Postgres 데이터베이스와 동기화하는 스크립트.

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
from src.models.team import (
    Franchise,
    TeamIdentity,
    FranchiseEvent,
    Ballpark,
    HomeBallparkAssignment,
)
from src.models.player import (
    Player,
    PlayerIdentity,
    PlayerCode,
    PlayerStint,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
)
from src.models.game import (
    GameSchedule,
    Game,
    GameLineup,
    PlayerGameStats,
    PlayerGameBatting,
    PlayerGamePitching,
)

# 외래 키 제약 조건을 고려한 모델 처리 순서
MODEL_ORDER: List[Type] = [
    Franchise,
    TeamIdentity,
    FranchiseEvent,
    Ballpark,
    HomeBallparkAssignment,
    Player,
    PlayerIdentity,
    PlayerCode,
    PlayerStint,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
    GameSchedule,
    Game,
    GameLineup,
    PlayerGameStats,
    PlayerGameBatting,
    PlayerGamePitching,
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
    Base.metadata.create_all(bind=target_engine)

    SourceSession = sessionmaker(bind=source_engine, autoflush=False, autocommit=False)
    TargetSession = sessionmaker(bind=target_engine, autoflush=False, autocommit=False)

    with SourceSession() as src, TargetSession() as dst:
        for model in MODEL_ORDER:
            total = src.query(model).count()
            if total == 0:
                continue

            # --truncate 옵션이 주어지면 대상 테이블의 데이터를 삭제합니다.
            if truncate:
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
    parser = argparse.ArgumentParser(description="Sync local SQLite data to Supabase/Postgres")
    parser.add_argument(
        "--source-url",
        type=str,
        default=os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db"),
        help="원본 데이터베이스 URL (기본값: 로컬 SQLite)",
    )
    parser.add_argument(
        "--target-url",
        type=str,
        default=os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL"),
        help="대상 데이터베이스 URL (Supabase/Postgres)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="데이터 삽입 전 대상 테이블의 모든 데이터를 삭제합니다.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    """스크립트의 메인 실행 함수."""
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not args.target_url:
        raise SystemExit("TARGET_DATABASE_URL must be provided via flag or environment variable")

    sync_databases(args.source_url, args.target_url, truncate=args.truncate)


if __name__ == "__main__":  # pragma: no cover
    main()


