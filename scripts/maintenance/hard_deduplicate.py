from pathlib import Path

from src.services.game_deduplication_service import DeduplicationWindow, mark_primary_games


DB_PATH = Path("data/kbo_dev.db")

REGULAR_SEASONS = [
    DeduplicationWindow("2024 regular", "2024-03-23", "2024-10-31"),
    DeduplicationWindow("2025 regular", "2025-03-22", "2025-10-31"),
    DeduplicationWindow("2026 regular", "2026-03-28", "2026-10-31"),
]


def hard_deduplicate_all():
    print("Hard reset: Setting all is_primary to 0...")
    result = mark_primary_games(DB_PATH, windows=REGULAR_SEASONS, reset_all=True)
    print(f"Successfully marked {result.marked_primary} primary games across 2024-2026.")


if __name__ == "__main__":
    hard_deduplicate_all()
