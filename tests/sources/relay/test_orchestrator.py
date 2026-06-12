from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.sources.relay.base import CapabilityRecord, NormalizedRelayResult, RelaySourceAdapter
from src.sources.relay.circuit_breaker import SourceCircuitBreaker
from src.sources.relay.orchestrator import RelayRecoveryOrchestrator


@pytest.fixture
def mock_adapter():
    adapter = MagicMock(spec=RelaySourceAdapter)
    adapter.source_name = "test_source"
    adapter.supports_bucket_probe = True
    adapter.cache_negative_probe = True
    adapter.fetch_game = AsyncMock(
        return_value=NormalizedRelayResult(
            game_id="G1",
            source_name="test_source",
            events=[{"id": 1}],
            has_event_state=True,
        ),
    )
    return adapter


@pytest.fixture
def empty_adapter():
    adapter = MagicMock(spec=RelaySourceAdapter)
    adapter.source_name = "empty_source"
    adapter.supports_bucket_probe = True
    adapter.cache_negative_probe = True
    adapter.fetch_game = AsyncMock(
        return_value=NormalizedRelayResult(
            game_id="G1",
            source_name="empty_source",
            events=[],
        ),
    )
    return adapter


@pytest.fixture
def failing_adapter():
    adapter = MagicMock(spec=RelaySourceAdapter)
    adapter.source_name = "failing_source"
    adapter.supports_bucket_probe = True
    adapter.cache_negative_probe = True
    adapter.fetch_game = AsyncMock(side_effect=RuntimeError("connection failed"))
    return adapter


@pytest.fixture
def orchestrator(tmp_path, mock_adapter):
    return RelayRecoveryOrchestrator(
        adapters={"test_source": mock_adapter},
        capability_path=tmp_path / "capability.json",
        sample_size=2,
        timeout_seconds=5.0,
    )


class TestRelayRecoveryOrchestrator:
    def test_init(self, tmp_path):
        cb = SourceCircuitBreaker()
        orch = RelayRecoveryOrchestrator(
            adapters={},
            capability_path=tmp_path / "cap.json",
            circuit_breaker=cb,
        )
        assert orch.circuit_breaker is cb
        assert orch.sample_size == 3
        assert orch.timeout_seconds == 30.0

    def test_source_order_for_bucket_default(self, orchestrator):
        with patch("src.sources.relay.orchestrator.default_source_order_for_bucket", return_value=["a", "b"]):
            result = orchestrator.source_order_for_bucket("test_bucket")
            assert result == ["a", "b"]

    def test_source_order_for_bucket_with_override(self, orchestrator):
        result = orchestrator.source_order_for_bucket("test_bucket", override=["source1", "source2"])
        assert result == ["source1", "source2"]

    def test_source_order_for_bucket_override_strips_empty(self, orchestrator):
        result = orchestrator.source_order_for_bucket("test_bucket", override=["src1", "", "src2"])
        assert result == ["src1", "src2"]

    def test_uses_bucket_probe(self, orchestrator, mock_adapter):
        assert orchestrator._uses_bucket_probe(mock_adapter) is True
        mock_adapter.supports_bucket_probe = False
        assert orchestrator._uses_bucket_probe(mock_adapter) is False

    def test_uses_bucket_probe_none(self, orchestrator):
        assert orchestrator._uses_bucket_probe(None) is False

    def _make_capability(self, supported: bool) -> CapabilityRecord:
        return CapabilityRecord(
            bucket_id="b1",
            source_name="s1",
            sample_size=2,
            supported=supported,
            last_checked_at="2025-01-01T00:00:00",
        )

    def test_can_skip_from_capability_unsupported_with_cache(self, orchestrator):
        adapter = MagicMock(spec=RelaySourceAdapter)
        adapter.supports_bucket_probe = True
        adapter.cache_negative_probe = True
        capability = self._make_capability(supported=False)
        assert orchestrator._can_skip_from_capability(adapter, capability) is True

    def test_can_skip_from_capability_supported(self, orchestrator):
        capability = self._make_capability(supported=True)
        assert orchestrator._can_skip_from_capability(None, capability) is False

    def test_can_skip_from_capability_none(self, orchestrator):
        assert orchestrator._can_skip_from_capability(None, None) is False

    @pytest.mark.asyncio
    async def test_fetch_with_timeout_success(self, orchestrator, mock_adapter):
        result, status = await orchestrator._fetch_with_timeout(mock_adapter, "G1")
        assert status == "success"
        assert not result.is_empty

    @pytest.mark.asyncio
    async def test_fetch_with_timeout_miss(self, orchestrator, empty_adapter):
        result, status = await orchestrator._fetch_with_timeout(empty_adapter, "G1")
        assert status == "miss"
        assert result.is_empty

    @pytest.mark.asyncio
    async def test_fetch_with_timeout_exception(self, orchestrator, failing_adapter):
        result, status = await orchestrator._fetch_with_timeout(failing_adapter, "G1")
        assert status == "exception"
        assert "connection failed" in (result.notes or "")

    @pytest.mark.asyncio
    async def test_fetch_game_success_first_source(self, orchestrator, mock_adapter):
        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["test_source"],
        )
        assert not result.is_empty
        assert len(attempts) == 1
        assert attempts[0]["status"] == "success"

    @pytest.mark.asyncio
    async def test_fetch_game_all_sources_exhausted(self, orchestrator, empty_adapter):
        orchestrator.adapters["empty_source"] = empty_adapter
        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["empty_source"],
        )
        assert result.is_empty
        assert "All configured relay sources missed" in (result.notes or "")
        assert len(attempts) == 1

    @pytest.mark.asyncio
    async def test_fetch_game_circuit_breaker_open(self, orchestrator, mock_adapter):
        orchestrator.circuit_breaker.record_failure("test_source", "test_bucket")
        orchestrator.circuit_breaker.record_failure("test_source", "test_bucket")
        orchestrator.circuit_breaker.record_failure("test_source", "test_bucket")

        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["test_source"],
        )
        assert result.is_empty
        assert attempts[0]["status"] == "cb_open"

    @pytest.mark.asyncio
    async def test_fetch_game_missing_adapter(self, orchestrator):
        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["nonexistent_source"],
        )
        assert result.is_empty
        assert attempts[0]["status"] == "missing_adapter"

    @pytest.mark.asyncio
    async def test_fetch_game_validation_error(self, orchestrator, mock_adapter):
        def validator(result):
            return "validation failed"

        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["test_source"],
            validator=validator,
        )
        assert result.is_empty
        assert attempts[0]["status"] == "skipped_validation"
        assert attempts[0]["notes"] == "validation failed"

    @pytest.mark.asyncio
    async def test_fetch_game_validation_success(self, orchestrator, mock_adapter):
        def validator(result):
            return None

        result, attempts = await orchestrator.fetch_game(
            game_id="G1",
            bucket_id="test_bucket",
            source_order=["test_source"],
            validator=validator,
        )
        assert not result.is_empty
        assert attempts[0]["status"] == "success"

    @pytest.mark.asyncio
    async def test_probe_bucket_no_game_ids(self, orchestrator):
        records = await orchestrator.probe_bucket("bucket1", [], ["test_source"])
        assert records == {}

    @pytest.mark.asyncio
    async def test_probe_bucket_success(self, orchestrator, mock_adapter):
        records = await orchestrator.probe_bucket("bucket1", ["G1"], ["test_source"])
        assert "test_source" in records
        assert records["test_source"].supported is True

    @pytest.mark.asyncio
    async def test_probe_bucket_miss(self, orchestrator, empty_adapter):
        orchestrator.adapters["empty_source"] = empty_adapter
        records = await orchestrator.probe_bucket("bucket1", ["G1"], ["empty_source"])
        assert "empty_source" in records
        assert records["empty_source"].supported is False

    def test_load_capability(self, orchestrator, tmp_path):
        cap_file = tmp_path / "capability.csv"
        cap_file.write_text("bucket_id,source_name,sample_size,supported,last_checked_at,notes\n")
        result = orchestrator._load_capability()
        assert result == {}

    def test_get_capability(self, orchestrator):
        record = CapabilityRecord(
            bucket_id="b1",
            source_name="s1",
            sample_size=1,
            supported=True,
            last_checked_at="2025-01-01T00:00:00",
        )
        with patch.object(orchestrator, "_load_capability", return_value={("b1", "s1"): record}):
            result = orchestrator.get_capability("b1", "s1")
            assert result is record

    def test_get_capability_missing(self, orchestrator):
        with patch.object(orchestrator, "_load_capability", return_value={}):
            result = orchestrator.get_capability("b1", "missing")
            assert result is None

    def test_invalidate_capability_cache(self, orchestrator):
        orchestrator._capability_cache = {"key": "value"}
        orchestrator._invalidate_capability_cache()
        assert orchestrator._capability_cache is None
