from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_congestion import StadiumCongestion
from src.repositories.congestion_repository import CongestionRepository


class TestCongestionRepositoryEdge:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumCongestion.__table__.create(engine)

    def test_upsert_skips_none_values_on_update(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            }
        )
        session.commit()

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": None,
                "source": "seoul_open_api",
            }
        )
        session.commit()

        record = session.query(StadiumCongestion).one()
        assert record.congestion_level == "very_high"
        assert record.congestion_index == 75.0

    def test_upsert_updates_timestamp(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        r1, _ = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "source": "seoul_open_api",
            }
        )
        session.commit()
        original_updated = r1.updated_at

        r2, _ = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "source": "seoul_open_api",
            }
        )
        session.commit()

        assert r2.updated_at >= original_updated

    def test_upsert_with_raw_data(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        record, created = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "source": "seoul_open_api",
                "raw_data": {"api_version": "2.0", "response_time": 0.5},
            }
        )
        session.commit()

        assert created is True
        assert record.raw_data == {"api_version": "2.0", "response_time": 0.5}

    def test_bulk_upsert_handles_duplicate_in_batch(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        created, updated = repo.bulk_upsert(
            [
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "1번게이트",
                    "measured_at": datetime(2024, 10, 15, 18, 30),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "high",
                    "source": "seoul_open_api",
                },
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "1번게이트",
                    "measured_at": datetime(2024, 10, 15, 18, 30),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "very_high",
                    "source": "seoul_open_api",
                },
            ]
        )
        session.commit()
        assert created == 1
        assert updated == 1

    def test_bulk_upsert_empty_list(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        created, updated = repo.bulk_upsert([])
        assert created == 0
        assert updated == 0

    def test_get_by_game_date_no_results(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15))
        assert results == []

    def test_get_by_game_date_both_filters(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "2번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "normal",
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "subway_station",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "normal",
                "source": "seoul_open_api",
            }
        )
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15), location_type="gate", location_label="1번게이트")
        assert len(results) == 1

    def test_get_by_game_date_orders_by_measured_at(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 19, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 17, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "normal",
                "source": "seoul_open_api",
            }
        )
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15))
        assert results[0].measured_at == datetime(2024, 10, 15, 17, 0)
        assert results[1].measured_at == datetime(2024, 10, 15, 19, 0)

    def test_get_latest_no_results(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        result = repo.get_latest("JAMSIL", "1번게이트")
        assert result is None

    def test_get_latest_different_locations(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "2번게이트",
                "measured_at": datetime(2024, 10, 15, 19, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "source": "seoul_open_api",
            }
        )
        session.commit()

        latest = repo.get_latest("JAMSIL", "2번게이트")
        assert latest is not None
        assert latest.location_label == "2번게이트"

    def test_get_peak_congestion_no_results(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        result = repo.get_peak_congestion("JAMSIL", date(2024, 10, 15))
        assert result is None

    def test_get_peak_congestion_skips_null_index(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": None,
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "2번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": 90.0,
                "source": "seoul_open_api",
            }
        )
        session.commit()

        peak = repo.get_peak_congestion("JAMSIL", date(2024, 10, 15))
        assert peak is not None
        assert peak.location_label == "2번게이트"

    def test_get_peak_congestion_multiple_dates(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 16, 18, 0),
                "game_date": date(2024, 10, 16),
                "congestion_level": "very_high",
                "congestion_index": 95.0,
                "source": "seoul_open_api",
            }
        )
        session.commit()

        peak = repo.get_peak_congestion("JAMSIL", date(2024, 10, 15))
        assert peak.congestion_index == 75.0

    def test_upsert_different_measured_at_creates_new(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "source": "seoul_open_api",
            }
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 19, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "source": "seoul_open_api",
            }
        )
        session.commit()

        assert session.query(StadiumCongestion).count() == 2
