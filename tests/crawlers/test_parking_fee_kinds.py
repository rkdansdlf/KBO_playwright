"""The printed fee kinds reach ``parking_fee_kinds``, not vehicle classes.

``parking_fee_rules`` is keyed by vehicle class, so the stadium pages' fee kinds
(기본/추가/일일/행사/경기/무료) used to be dropped on every path -- the live
crawler, snapshot replay, and nothing else to recover from except the raw
snapshot HTML. This file pins the contract that the kinds now have a table and
that every path fills it.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.parking_crawler import TEAM_PARKING_SOURCES, ParkingCrawler
from src.models.parking_fee_kind import ParkingFeeKind
from src.models.parking_lot import ParkingLot
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.parking_lot_repository import ParkingFeeKindRepository, ParkingLotRepository
from src.services.snapshot_persist import _save_parking
from scripts.maintenance.backfill_parking_fee_kinds import backfill

SK_INFO = TEAM_PARKING_SOURCES["SK"]
SK_STADIUM = SK_INFO["stadium_id"]
SK_SOURCE_KEY = SK_INFO["source_key"]
SK_LOT_NAME = f"{SK_STADIUM} 주차장"

LOT = {
    "stadium_id": SK_STADIUM,
    "name": SK_LOT_NAME,
    "lot_type": "official",
    "is_event_day_available": True,
    "reservation_required": False,
}

# Matches the shape from `_parse_parking_page`.
ENTRY = {
    "lot": dict(LOT),
    "fee_rules": [
        {"label": "기본", "amount": 5000},
        {"label": "추가", "amount": 1000},
        {"label": "일일", "amount": 15000},
    ],
    "team_code": "SK",
}


@pytest.fixture
def factory() -> Iterator[sessionmaker]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    for table in (
        DataSource.__table__,
        RawSourceSnapshot.__table__,
        ParkingLot.__table__,
        ParkingFeeKind.__table__,
    ):
        table.create(engine)
    yield sessionmaker(bind=engine, expire_on_commit=False)


def _seed_lot(factory: sessionmaker) -> ParkingLot:
    with factory() as session:
        repo = ParkingLotRepository(session)
        lot = repo.save(dict(LOT))
        session.commit()
        return lot


class TestTheRepositoryUpsertsByLotAndKind:
    def test_a_second_write_replaces_the_amount(self, factory: sessionmaker) -> None:
        lot = _seed_lot(factory)
        with factory() as session:
            repo = ParkingFeeKindRepository(session)
            repo.save({"parking_lot_id": lot.id, "fee_kind": "기본", "amount_krw": 5000})
            repo.save({"parking_lot_id": lot.id, "fee_kind": "기본", "amount_krw": 6000})
            session.commit()
            rows = repo.get_by_lot(lot.id)

        assert len(rows) == 1
        assert rows[0].amount_krw == 6000

    def test_get_by_lot_orders_by_kind(self, factory: sessionmaker) -> None:
        lot = _seed_lot(factory)
        with factory() as session:
            repo = ParkingFeeKindRepository(session)
            for kind, amount in [("일일", 9000), ("기본", 3000), ("추가", 500)]:
                repo.save({"parking_lot_id": lot.id, "fee_kind": kind, "amount_krw": amount})
            session.commit()
            kinds = [r.fee_kind for r in repo.get_by_lot(lot.id)]

        assert kinds == ["기본", "일일", "추가"]


class TestTheLiveCrawlSavesTheKinds:
    def test_save_team_writes_lots_and_fee_kinds(self, factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr("src.crawlers.parking_crawler.SessionLocal", factory)
        with factory() as session:
            session.add(
                DataSource(source_key=SK_SOURCE_KEY, source_type="web", target_domain="parking", is_active=True)
            )
            session.commit()

        saved_team = ParkingCrawler()._save_team("SK", [dict(ENTRY)])

        assert saved_team is True
        with factory() as session:
            lots = session.scalars(select(ParkingLot)).all()
            kinds = session.scalars(select(ParkingFeeKind).order_by(ParkingFeeKind.amount_krw)).all()

        assert len(lots) == 1
        assert lots[0].name == SK_LOT_NAME
        assert [(k.fee_kind, k.amount_krw) for k in kinds] == [("추가", 1000), ("기본", 5000), ("일일", 15000)]


class TestSnapshotReplaySavesTheKinds:
    def test_save_parking_writes_fee_kinds_rather_than_vehicle_rows(self, factory: sessionmaker) -> None:
        lot = _seed_lot(factory)
        with factory() as session:
            outcome = _save_parking(session, [dict(ENTRY)])
            assert outcome.saved == 1
            session.commit()
            kinds = session.scalars(select(ParkingFeeKind).order_by(ParkingFeeKind.amount_krw)).all()

        assert len(kinds) == 3
        assert {k.fee_kind for k in kinds} == {"기본", "추가", "일일"}
        assert all(k.parking_lot_id == lot.id for k in kinds)


class TestTheBackfillReplaysTheSnapshotText:
    def test_kinds_are_recovered_from_the_raw_snapshot(self, factory: sessionmaker, tmp_path: Path) -> None:
        _seed_lot(factory)
        html = "<html><body><p>기본 요금: 5,000원</p><p>추가: 1,000원</p></body></html>"
        artifact = tmp_path / "page.bin"
        artifact.write_bytes(html.encode())
        with factory() as session:
            session.add(
                DataSource(source_key=SK_SOURCE_KEY, source_type="web", target_domain="parking", is_active=True)
            )
            session.flush()
            ds = session.scalar(select(DataSource).where(DataSource.source_key == SK_SOURCE_KEY))
            session.add(
                RawSourceSnapshot(
                    data_source_id=ds.id,
                    fetched_at=datetime(2026, 10, 1),
                    raw_html_or_json_path=str(artifact),
                    source_url="https://www.ssglanders.com/stadium/parking",
                ),
            )
            session.commit()

            report = backfill(session, apply=True)
            kinds = session.scalars(select(ParkingFeeKind).order_by(ParkingFeeKind.amount_krw)).all()

        assert report.applied is True
        assert report.kinds_found == 2
        assert [(k.fee_kind, k.amount_krw) for k in kinds] == [("추가", 1000), ("기본", 5000)]

    def test_the_same_page_replayed_twice_does_not_duplicate_rows(self, factory: sessionmaker, tmp_path: Path) -> None:
        _seed_lot(factory)
        artifact = tmp_path / "page.bin"
        artifact.write_bytes("<p>기본 요금: 5,000원</p>".encode())
        with factory() as session:
            session.add(
                DataSource(source_key=SK_SOURCE_KEY, source_type="web", target_domain="parking", is_active=True)
            )
            session.flush()
            ds = session.scalar(select(DataSource).where(DataSource.source_key == SK_SOURCE_KEY))
            for _ in range(2):
                session.add(
                    RawSourceSnapshot(
                        data_source_id=ds.id,
                        fetched_at=datetime(2026, 10, 1),
                        raw_html_or_json_path=str(artifact),
                    ),
                )
            session.commit()

            backfill(session, apply=True)
            kinds = session.scalars(select(ParkingFeeKind)).all()

        assert len(kinds) == 1
