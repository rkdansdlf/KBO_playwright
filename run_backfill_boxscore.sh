#!/bin/bash
export PYTHONPATH=$PYTHONPATH:.

# 2024, 2025 전체 시즌
YEARS_FULL=(2024 2025)
MONTHS_FULL=(03 04 05 06 07 08 09 10)

# 2026 현재 시즌 (4월까지)
YEARS_CURRENT=(2026)
MONTHS_CURRENT=(03 04)

echo "🚀 Starting Comprehensive Box Score Backfill (2024-2026)"

# Process 2024-2025
for YEAR in "${YEARS_FULL[@]}"; do
    for MONTH in "${MONTHS_FULL[@]}"; do
        echo "📅 Processing $YEAR-$MONTH..."
        .venv/bin/python3 src/cli/crawl_game_details.py --year $YEAR --month $MONTH --concurrency 3 --delay 0.5 >> backfill_boxscore.log 2>&1
        echo "✅ Finished $YEAR-$MONTH"
    done
done

# Process 2026
for YEAR in "${YEARS_CURRENT[@]}"; do
    for MONTH in "${MONTHS_CURRENT[@]}"; do
        echo "📅 Processing $YEAR-$MONTH..."
        .venv/bin/python3 src/cli/crawl_game_details.py --year $YEAR --month $MONTH --concurrency 3 --delay 0.5 >> backfill_boxscore.log 2>&1
        echo "✅ Finished $YEAR-$MONTH"
    done
done

echo "🎉 All requested years (2024-2026) processed."
