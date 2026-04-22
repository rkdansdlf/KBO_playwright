from pathlib import Path

from src.services.game_deduplication_service import mark_primary_games


DB_PATH = Path("data/kbo_dev.db")


def mark_primary_games_for_duplicates():
    print("Identifying duplicate game slots...")
    result = mark_primary_games(DB_PATH, reset_all=True)
    print(f"Analyzed {result.scanned_slots} game slots.")
    print(f"Successfully marked {result.marked_primary} primary games.")


def mark_primary_games():
    """Backward-compatible entrypoint for existing shell invocations."""
    mark_primary_games_for_duplicates()


if __name__ == "__main__":
    mark_primary_games()
