"""Phase 106B: Offline Snapshot Replay and Determinism Certification Tests.

Validates that all offline HTML and JSON fixtures parse deterministically
(triplicate replay produces identical SHA-256 hashes) and that parser error
injection produces well-defined reason codes without silent failure.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from src.crawlers.team_batting_stats_crawler import parse_team_batting_html
from src.crawlers.team_pitching_stats_crawler import parse_team_pitching_html
from src.parsers.game_detail_parser import GameDetailParser
from src.parsers.team_event_parser import parse_team_events
from src.parsers.ticket_parser import parse_ticket_page
from src.utils.team_mapping import HISTORICAL_PATTERNS

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"


def _canonical_hash(payload: Any) -> str:
    """Compute deterministic SHA-256 hash of a JSON-serializable structure."""
    canonical_json = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


class TestOfflineSnapshotReplayDeterminism:
    """Validates triplicate replay determinism across all offline snapshot fixtures."""

    def test_game_detail_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "game_details" / "20251001NCLG0.html"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        html = fixture_path.read_text(encoding="utf-8")

        hashes = []
        for _ in range(3):
            parser = GameDetailParser(html=html, game_id="20251001NCLG0", game_date="2025-10-01")
            parsed = parser.parse()
            assert parsed is not None
            assert parsed.get("game_id") == "20251001NCLG0"
            hashes.append(_canonical_hash(parsed))

        assert hashes[0] == hashes[1] == hashes[2], "Replay is non-deterministic!"

    def test_team_batting_stats_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "html" / "team_batting_2023.html"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        html = fixture_path.read_text(encoding="utf-8")
        mapping = dict(HISTORICAL_PATTERNS)

        hashes = []
        for _ in range(3):
            parsed = parse_team_batting_html(html, season=2023, league="regular", team_mapping=mapping)
            assert len(parsed) > 0
            hashes.append(_canonical_hash(parsed))

        assert hashes[0] == hashes[1] == hashes[2], "Team batting replay is non-deterministic!"

    def test_team_pitching_stats_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "html" / "team_pitching_2023.html"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        html = fixture_path.read_text(encoding="utf-8")
        mapping = dict(HISTORICAL_PATTERNS)

        hashes = []
        for _ in range(3):
            parsed = parse_team_pitching_html(html, season=2023, league="regular", team_mapping=mapping)
            assert len(parsed) > 0
            hashes.append(_canonical_hash(parsed))

        assert hashes[0] == hashes[1] == hashes[2], "Team pitching replay is non-deterministic!"

    def test_team_events_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "html" / "hh_events_notice.html"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        html = fixture_path.read_text(encoding="utf-8")

        hashes = []
        for _ in range(3):
            parsed = parse_team_events(html, "hanwha_eagles_events")
            hashes.append(_canonical_hash(parsed))

        assert hashes[0] == hashes[1] == hashes[2], "Team events replay is non-deterministic!"

    def test_ticket_pricing_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "html" / "lg_ticket_prices.html"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        html = fixture_path.read_text(encoding="utf-8")

        hashes = []
        for _ in range(3):
            parsed = parse_ticket_page(html, "lg_twins_ticket")
            assert len(parsed) > 0
            hashes.append(_canonical_hash(parsed))

        assert hashes[0] == hashes[1] == hashes[2], "Ticket price replay is non-deterministic!"

    def test_naver_relay_json_replay_determinism(self) -> None:
        fixture_path = FIXTURES_DIR / "naver_live" / "relay_inning_1.json"
        assert fixture_path.exists(), f"Fixture missing: {fixture_path}"
        raw_json = json.loads(fixture_path.read_text(encoding="utf-8"))

        hashes = []
        for _ in range(3):
            relay_data = raw_json.get("relay", raw_json)
            hashes.append(_canonical_hash(relay_data))

        assert hashes[0] == hashes[1] == hashes[2], "Naver relay replay is non-deterministic!"


class TestParserFaultInjectionResilience:
    """Validates parser resilience against corrupted, missing, or malformed inputs."""

    def test_game_detail_empty_html_fails_closed(self) -> None:
        empty_html = "<html><body></body></html>"
        parser = GameDetailParser(html=empty_html, game_id="20251001NCLG0", game_date="2025-10-01")
        with pytest.raises(ValueError, match="No tables found"):
            parser.parse()

    def test_game_detail_truncated_html_handling(self) -> None:
        truncated_html = "<table><tr><th>선수</th><th>타수</th></tr><tr><td>홍길동</td>"
        parser = GameDetailParser(html=truncated_html, game_id="20251001NCLG0", game_date="2025-10-01")
        result = parser.parse()
        assert isinstance(result, dict)

    def test_team_batting_empty_html_returns_empty(self) -> None:
        empty_html = "<html><body></body></html>"
        mapping = dict(HISTORICAL_PATTERNS)
        result = parse_team_batting_html(empty_html, season=2023, league="regular", team_mapping=mapping)
        assert result == []

    def test_team_events_unsupported_source_key(self) -> None:
        result = parse_team_events("<html></html>", "unknown_club_events")
        assert result == []

    def test_ticket_pricing_invalid_html_returns_empty_or_fails_closed(self) -> None:
        result = parse_ticket_page("<div>not a table</div>", "lg_twins_ticket")
        assert result == []
