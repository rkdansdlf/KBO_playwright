"""
Tests for StadiumOperationNotice repository and crawler infrastructure.

Covers:
  - OperationNoticeRepository upsert / bulk_upsert / dedup logic
  - get_by_game_date / get_recent / get_latest_external_id queries
  - Notice type classification helpers in LG/Doosan crawlers
"""
from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_info import StadiumInfo
from src.models.stadium_operation_notice import StadiumOperationNotice
from src.repositories.operation_notice_repository import OperationNoticeRepository


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumInfo.__table__.create(engine)
    StadiumOperationNotice.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture
def stadium(session):
    st = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장", home_team_id="LG")
    session.add(st)
    session.commit()
    return st


def _notice(
    stadium_code="JAMSIL",
    notice_type="GENERAL",
    title="테스트 공지",
    source_name="LG트윈스공식",
    external_id=None,
    published_at=None,
    game_date=None,
    is_urgent=False,
):
    return {
        "stadium_code": stadium_code,
        "notice_type": notice_type,
        "title": title,
        "content": "공지 내용",
        "source_name": source_name,
        "external_id": external_id,
        "published_at": published_at or datetime(2026, 6, 3, 10, 0),
        "game_date": game_date or date(2026, 6, 3),
        "is_urgent": is_urgent,
    }


# ─────────────────────────────────────────────
# OperationNoticeRepository Tests
# ─────────────────────────────────────────────

class TestOperationNoticeRepositoryUpsert:
    def test_insert_new_notice(self, session, stadium):
        repo = OperationNoticeRepository(session)
        data = _notice(external_id="12345")
        record, created = repo.upsert(data)
        session.commit()
        assert created is True
        assert record.id is not None
        assert record.title == "테스트 공지"

    def test_dedup_by_external_id(self, session, stadium):
        repo = OperationNoticeRepository(session)
        data = _notice(external_id="99")
        r1, created1 = repo.upsert(data)
        session.commit()

        # Second upsert with same external_id → update
        data2 = {**data, "is_urgent": True}
        r2, created2 = repo.upsert(data2)
        session.commit()

        assert created1 is True
        assert created2 is False
        assert r1.id == r2.id
        assert r2.is_urgent is True

    def test_dedup_by_title_and_published_at(self, session, stadium):
        repo = OperationNoticeRepository(session)
        pub = datetime(2026, 6, 3, 12, 0)
        data = _notice(title="우천 취소 공지", published_at=pub)
        r1, created1 = repo.upsert(data)
        session.commit()

        # Same title + published_at, no external_id → dedup
        r2, created2 = repo.upsert({**data, "content": "업데이트된 내용"})
        session.commit()

        assert created1 is True
        assert created2 is False
        assert r1.id == r2.id

    def test_different_external_id_inserts_new(self, session, stadium):
        repo = OperationNoticeRepository(session)
        r1, _ = repo.upsert(_notice(external_id="A1"))
        r2, _ = repo.upsert(_notice(external_id="A2", title="다른 공지"))
        session.commit()
        assert r1.id != r2.id

    def test_bulk_upsert_returns_counts(self, session, stadium):
        repo = OperationNoticeRepository(session)
        notices = [
            _notice(external_id=str(i), title=f"공지 {i}")
            for i in range(5)
        ]
        created, updated = repo.bulk_upsert(notices)
        session.commit()
        assert created == 5
        assert updated == 0

    def test_bulk_upsert_dedup_on_re_run(self, session, stadium):
        repo = OperationNoticeRepository(session)
        notices = [_notice(external_id=str(i), title=f"공지 {i}") for i in range(3)]
        repo.bulk_upsert(notices)
        session.commit()

        created2, updated2 = repo.bulk_upsert(notices)
        session.commit()
        assert created2 == 0
        assert updated2 == 3


class TestOperationNoticeRepositoryRead:
    def test_get_by_game_date(self, session, stadium):
        repo = OperationNoticeRepository(session)
        game_date = date(2026, 6, 3)
        pub1 = datetime(2026, 6, 3, 10, 0)
        pub2 = datetime(2026, 6, 4, 10, 0)
        repo.upsert(_notice(external_id="D1", title="공지A", game_date=game_date, published_at=pub1))
        repo.upsert(_notice(external_id="D2", title="공지B", game_date=date(2026, 6, 4), published_at=pub2))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", game_date)
        assert len(results) == 1
        assert results[0].external_id == "D1"

    def test_get_by_game_date_urgent_only(self, session, stadium):
        repo = OperationNoticeRepository(session)
        game_date = date(2026, 6, 3)
        repo.upsert(_notice(external_id="U1", game_date=game_date, is_urgent=False))
        repo.upsert(_notice(external_id="U2", game_date=game_date, is_urgent=True, title="긴급 공지"))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", game_date, urgent_only=True)
        assert len(results) == 1
        assert results[0].is_urgent is True

    def test_get_recent_with_notice_type_filter(self, session, stadium):
        repo = OperationNoticeRepository(session)
        repo.upsert(_notice(external_id="C1", notice_type="CANCEL", title="우천 취소"))
        repo.upsert(_notice(external_id="G1", notice_type="GENERAL", title="일반 공지"))
        session.commit()

        results = repo.get_recent("JAMSIL", notice_type="CANCEL")
        assert len(results) == 1
        assert results[0].notice_type == "CANCEL"

    def test_get_latest_external_id(self, session, stadium):
        repo = OperationNoticeRepository(session)
        repo.upsert(_notice(external_id="E1", published_at=datetime(2026, 6, 1)))
        repo.upsert(_notice(external_id="E2", published_at=datetime(2026, 6, 3)))
        session.commit()

        latest = repo.get_latest_external_id("JAMSIL", "LG트윈스공식")
        assert latest == "E2"

    def test_get_latest_external_id_none_when_empty(self, session, stadium):
        repo = OperationNoticeRepository(session)
        latest = repo.get_latest_external_id("JAMSIL", "LG트윈스공식")
        assert latest is None


# ─────────────────────────────────────────────
# Crawler helper tests (notice classification)
# ─────────────────────────────────────────────

class TestNoticeCrawlerHelpers:
    def test_classify_cancel(self):
        from src.crawlers.operation_notice_lg_crawler import _classify_notice
        assert _classify_notice("경기 우천 취소 안내") == "CANCEL"
        assert _classify_notice("노게임 처리") == "CANCEL"

    def test_classify_gate_change(self):
        from src.crawlers.operation_notice_lg_crawler import _classify_notice
        assert _classify_notice("게이트 변경 안내") == "GATE_CHANGE"

    def test_classify_entry_rule(self):
        from src.crawlers.operation_notice_lg_crawler import _classify_notice
        assert _classify_notice("입장 제한 사항 안내") == "ENTRY_RULE"

    def test_classify_general(self):
        from src.crawlers.operation_notice_lg_crawler import _classify_notice
        assert _classify_notice("홈 개막전 이벤트 안내") == "GENERAL"

    def test_is_urgent_detection(self):
        from src.crawlers.operation_notice_lg_crawler import _is_urgent
        assert _is_urgent("[긴급] 경기 취소") is True
        assert _is_urgent("[필독] 주요 공지") is True
        assert _is_urgent("일반 공지 사항") is False

    def test_parse_date_formats(self):
        from src.crawlers.operation_notice_lg_crawler import _parse_date
        d1 = _parse_date("2026.06.03")
        d2 = _parse_date("2026-06-03")
        d3 = _parse_date("2026/06/03")
        assert d1.year == 2026 and d1.month == 6 and d1.day == 3
        assert d1 == d2 == d3

    def test_parse_date_invalid(self):
        from src.crawlers.operation_notice_lg_crawler import _parse_date
        assert _parse_date("invalid-date") is None

    def test_extract_article_id_from_query_param(self):
        from src.crawlers.operation_notice_lg_crawler import _extract_article_id
        assert _extract_article_id("https://example.com/notice?idx=12345") == "12345"
        assert _extract_article_id("https://example.com/board?no=99") == "99"

    def test_extract_article_id_from_path(self):
        from src.crawlers.operation_notice_lg_crawler import _extract_article_id
        assert _extract_article_id("https://example.com/notice/777") == "777"

    def test_doosan_classify_cancel(self):
        from src.crawlers.operation_notice_doosan_crawler import _classify_notice
        assert _classify_notice("경기 취소 공지") == "CANCEL"

    def test_doosan_is_urgent(self):
        from src.crawlers.operation_notice_doosan_crawler import _is_urgent
        assert _is_urgent("[중요] 필독 공지") is True
