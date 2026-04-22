from src.services.game_deduplication_service import DeduplicationWindow, mark_primary_games


WINDOWS = [
    DeduplicationWindow("2024 regular", "2024-03-23", "2024-09-30"),
    DeduplicationWindow("2025 regular", "2025-03-22", "2025-10-31"),
    DeduplicationWindow("2026 regular", "2026-03-28", "2026-11-30"),
]


def absolute_completeness():
    print("Zeroing all is_primary...")
    result = mark_primary_games("data/kbo_dev.db", windows=WINDOWS, reset_all=True)
    print(f"Completeness calibration done. {result.marked_primary} primary games marked.")


if __name__ == "__main__":
    absolute_completeness()
