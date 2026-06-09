from __future__ import annotations

from unittest.mock import MagicMock

from src.services.game_write_contract import GameWriteContract, GameWriteSource, _format_value


class TestGameWriteSource:
    def test_label_returns_joined_parts(self):
        source = GameWriteSource("detail", "DetailCrawler", "recovery")
        assert source.label() == "detail/DetailCrawler/recovery"

    def test_label_without_reason(self):
        source = GameWriteSource("schedule", "ScheduleCrawler")
        assert source.label() == "schedule/ScheduleCrawler"


class TestGameWriteContract:
    def test_initial_state(self):
        contract = GameWriteContract()
        assert contract.updated_fields == 0
        assert contract.duplicate_fields == 0
        assert contract.replaced_datasets == 0
        assert contract.duplicate_datasets == 0
        assert contract.claimed_games == {}
        assert contract.field_claims == {}

    def test_claim_game_first_time(self):
        contract = GameWriteContract()
        source = GameWriteSource("detail", "DC")
        contract.claim_game("G1", source)
        assert "G1" in contract.claimed_games
        assert source in contract.claimed_games["G1"]

    def test_claim_game_duplicate(self):
        log = MagicMock()
        contract = GameWriteContract(log=log)
        source1 = GameWriteSource("detail", "DC", "first")
        source2 = GameWriteSource("relay", "RC", "second")
        contract.claim_game("G1", source1)
        contract.claim_game("G1", source2)
        assert log.call_count >= 2

    def test_field_updated_tracks(self):
        contract = GameWriteContract()
        source = GameWriteSource("detail", "DC")
        contract.field_updated("G1", source, "score", None, 5)
        assert contract.updated_fields == 1
        assert contract.field_claims[("G1", "score")] == source

    def test_field_duplicate_counts(self):
        contract = GameWriteContract(log_duplicate_fields=True)
        log = MagicMock()
        contract.log = log
        source = GameWriteSource("detail", "DC")
        contract.field_duplicate("G1", source, "score", 5)
        assert contract.duplicate_fields == 1
        log.assert_called_once()

    def test_field_duplicate_suppressed_when_not_logging(self):
        contract = GameWriteContract(log_duplicate_fields=False)
        log = MagicMock()
        contract.log = log
        source = GameWriteSource("detail", "DC")
        contract.field_duplicate("G1", source, "score", 5)
        assert contract.duplicate_fields == 1
        log.assert_not_called()

    def test_dataset_replaced(self):
        contract = GameWriteContract()
        source = GameWriteSource("detail", "DC")
        contract.dataset_replaced("G1", source, "batting", 10)
        assert contract.replaced_datasets == 1

    def test_dataset_duplicate(self):
        contract = GameWriteContract()
        source = GameWriteSource("detail", "DC")
        contract.dataset_duplicate("G1", source, "batting", 10)
        assert contract.duplicate_datasets == 1

    def test_summary_format(self):
        contract = GameWriteContract(run_label="test-run")
        source = GameWriteSource("detail", "DC")
        contract.claim_game("G1", source)
        contract.field_updated("G1", source, "score", None, 5)
        summary = contract.summary()
        assert "test-run" in summary
        assert "field_updates=1" in summary

    def test_field_overlap_detected(self):
        log = MagicMock()
        contract = GameWriteContract(log=log)
        source1 = GameWriteSource("detail", "DC")
        source2 = GameWriteSource("relay", "RC")
        contract.field_updated("G1", source1, "score", None, 5)
        contract.field_updated("G1", source2, "score", 5, 10)
        messages = [call[0][0] for call in log.call_args_list]
        assert any("FIELD-OVERLAP" in msg for msg in messages)


class TestFormatValue:
    def test_short_value(self):
        assert _format_value(42) == "42"

    def test_long_value_truncated(self):
        long_val = "x" * 100
        result = _format_value(long_val)
        assert len(result) <= 80
        assert result.endswith("...")
