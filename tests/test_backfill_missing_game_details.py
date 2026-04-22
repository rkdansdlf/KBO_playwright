from __future__ import annotations

import asyncio
import csv
from types import SimpleNamespace

import scripts.maintenance.backfill_missing_game_details as backfill


class _FakeDetailCrawler:
    def __init__(self, request_delay):
        self.request_delay = request_delay


class _FakeRelayCrawler:
    def __init__(self, request_delay):
        self.request_delay = request_delay


def _read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_backfill_uses_shared_collection_result_for_apply_report(monkeypatch, tmp_path):
    manifest_path = tmp_path / "manifest.csv"
    output_path = tmp_path / "results.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "game_id,game_date,classification",
                "20250401LGSS0,20250401,pending_recrawl",
                "20250402LGSS0,20250402,pending_recrawl",
                "20250403LGSS0,20250403,duplicate_conflict",
                "20250404LGSS0,20250404,pending_recrawl",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(backfill, "GameDetailCrawler", _FakeDetailCrawler)
    monkeypatch.setattr(backfill, "RelayCrawler", _FakeRelayCrawler)
    monkeypatch.setattr(backfill, "_backup_sqlite_database", lambda _output_dir: None)
    monkeypatch.setattr(backfill, "_game_has_required_details", lambda game_id: game_id == "20250404LGSS0")

    seen = {}

    async def _fake_collect(games, *, detail_crawler, relay_crawler, force, concurrency, log):
        seen["games"] = list(games)
        seen["detail_delay"] = detail_crawler.request_delay
        seen["relay_delay"] = relay_crawler.request_delay
        seen["force"] = force
        seen["concurrency"] = concurrency
        return SimpleNamespace(
            items={
                "20250401LGSS0": SimpleNamespace(
                    detail_status="saved",
                    detail_saved=True,
                    relay_rows_saved=7,
                    failure_reason=None,
                ),
                "20250402LGSS0": SimpleNamespace(
                    detail_status="crawl_failed",
                    detail_saved=False,
                    relay_rows_saved=0,
                    failure_reason="missing",
                ),
            }
        )

    monkeypatch.setattr(backfill, "crawl_and_save_game_details", _fake_collect)

    rc = asyncio.run(
        backfill.run(
            SimpleNamespace(
                manifest=str(manifest_path),
                output=str(output_path),
                apply=True,
                relay=True,
                limit=None,
                delay=1.5,
                concurrency=4,
                no_backup=True,
            )
        )
    )

    rows = _read_csv(output_path)
    by_id = {row["game_id"]: row for row in rows}

    assert rc == 0
    assert [row["game_id"] for row in seen["games"]] == ["20250401LGSS0", "20250402LGSS0"]
    assert seen["detail_delay"] == 1.5
    assert seen["relay_delay"] == 1.5
    assert seen["force"] is True
    assert seen["concurrency"] == 4
    assert by_id["20250401LGSS0"]["classification"] == "recrawl_saved"
    assert by_id["20250401LGSS0"]["detail_saved"] == "1"
    assert by_id["20250401LGSS0"]["relay_rows"] == "7"
    assert by_id["20250402LGSS0"]["classification"] == "crawl_failed"
    assert by_id["20250402LGSS0"]["failure_reason"] == "missing"
    assert by_id["20250403LGSS0"]["classification"] == "duplicate_conflict"
    assert by_id["20250403LGSS0"]["failure_reason"] == "non_actionable_manifest_row"
    assert by_id["20250404LGSS0"]["classification"] == "already_has_detail"
