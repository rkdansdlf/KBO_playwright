"""Tests for the Prometheus database availability/latency collector."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from sqlalchemy import create_engine, make_url

from src.monitoring.db_availability import DatabaseAvailabilityCollector, probe_engine

ROOT = Path(__file__).resolve().parents[2]


class _BrokenEngine:
    def connect(self):
        raise OSError("connection refused")


class _RefusingEngine:
    """Carries a URL like a real engine, so the log target can be asserted."""

    def __init__(self, url: str) -> None:
        self.url = make_url(url)

    def connect(self):
        raise OSError("connection refused")


class TestProbeEngine:
    def test_available_engine_reports_available(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        available, latency = probe_engine(engine)
        assert available is True
        assert latency >= 0.0

    def test_broken_engine_reports_unavailable(self) -> None:
        available, latency = probe_engine(_BrokenEngine())  # type: ignore[arg-type]
        assert available is False
        assert latency >= 0.0

    def test_failure_records_why_it_failed(self, caplog: pytest.LogCaptureFixture) -> None:
        """A boolean gate that drops the cause cannot be diagnosed.

        ``PROBE_EXCEPTIONS`` covers a refused connection, a connect timeout and
        "too many clients already" identically, and the caller logs one fixed
        sentence for all three. On 2026-10-12 two consecutive gate rejections
        were undiagnosable precisely this way -- the same job had queried the
        database successfully milliseconds earlier, so a false negative was the
        honest reading, and nothing said which failure had been swallowed.
        """
        with caplog.at_level("WARNING", logger="src.monitoring.db_availability"):
            available, _ = probe_engine(_BrokenEngine())  # type: ignore[arg-type]

        assert available is False
        assert "DB probe failed" in caplog.text
        # The cause, not just the verdict: the exception type and its message.
        assert "OSError" in caplog.text
        assert "connection refused" in caplog.text

    def test_failure_names_the_target_without_credentials(self, caplog: pytest.LogCaptureFixture) -> None:
        """The log says which database, and must not leak the password."""
        with caplog.at_level("WARNING", logger="src.monitoring.db_availability"):
            probe_engine(_RefusingEngine("postgresql://user:hunter2@db.example:5432/prod"))

        assert "db.example:5432/prod" in caplog.text
        assert "hunter2" not in caplog.text


class TestCollector:
    def test_collect_yields_both_gauges(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        collector = DatabaseAvailabilityCollector(engine)

        families = {family.name: family for family in collector.collect()}

        assert families["kbo_db_available"].samples[0].value == 1.0
        assert families["kbo_db_ping_latency_seconds"].samples[0].value >= 0.0

    def test_collect_reports_zero_when_down(self) -> None:
        collector = DatabaseAvailabilityCollector(_BrokenEngine())  # type: ignore[arg-type]
        families = {family.name: family for family in collector.collect()}
        assert families["kbo_db_available"].samples[0].value == 0.0


class TestPrometheusConfig:
    def test_database_alert_rules_present(self) -> None:
        rules = yaml.safe_load((ROOT / "monitoring/prometheus/alert_rules.yml").read_text(encoding="utf-8"))
        names = {rule["alert"] for group in rules["groups"] for rule in group["rules"]}
        assert "KBODatabaseUnavailable" in names
        assert "KBODatabaseLatencyHigh" in names

    def test_api_server_is_scraped(self) -> None:
        config = yaml.safe_load((ROOT / "monitoring/prometheus/prometheus.yml").read_text(encoding="utf-8"))
        jobs = {job["job_name"] for job in config["scrape_configs"]}
        assert "kbo-api-server" in jobs

    def test_alertmanager_sends_resolved(self) -> None:
        config = yaml.safe_load((ROOT / "monitoring/alertmanager/alertmanager.yml").read_text(encoding="utf-8"))
        for receiver in config["receivers"]:
            for telegram in receiver["telegram_configs"]:
                assert telegram["send_resolved"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
