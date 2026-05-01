from __future__ import annotations

from src.services.game_write_contract import GameWriteContract, GameWriteSource


def test_write_contract_logs_game_overlap_and_field_claims():
    logs: list[str] = []
    contract = GameWriteContract(run_label="unit-run", log=logs.append)

    schedule = GameWriteSource("schedule", "ScheduleCrawler", "monthly_schedule_refresh:2025-04")
    detail = GameWriteSource("detail", "GameDetailCrawler", "postgame_finalize:20250401")

    contract.claim_game("20250401LGSS0", schedule)
    contract.field_updated("20250401LGSS0", schedule, "game.game_date", None, "2025-04-01")
    contract.claim_game("20250401LGSS0", detail)
    contract.field_updated("20250401LGSS0", detail, "game.home_score", None, 3)

    assert any(line.startswith("[CLAIM]") and "stage=schedule" in line for line in logs)
    assert any(line.startswith("[OVERLAP]") and "monthly_schedule_refresh" in line for line in logs)
    assert any(line.startswith("[WRITE]") and "field=game.home_score" in line for line in logs)
    assert contract.summary() == (
        "[WRITE-SUMMARY] run=unit-run games=1 field_updates=2 field_duplicates=0 "
        "dataset_replacements=0 dataset_duplicates=0"
    )
