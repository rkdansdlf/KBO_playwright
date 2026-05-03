#!/bin/bash
# KBO Advanced Stats Backfill Script
# Populates historical fielding, baserunning, and team stats for a range of years.

START_YEAR=${1:-2020}
END_YEAR=${2:-2025}

echo "===================================================="
echo "🚀 KBO Advanced Stats Backfill: $START_YEAR to $END_YEAR"
echo "===================================================="

# Ensure OCI_DB_URL is set if sync is needed
if [ -z "$OCI_DB_URL" ]; then
    echo "⚠️  OCI_DB_URL not set. Running local backfill only (no sync)."
    SYNC_FLAG=""
else
    echo "✅ OCI_DB_URL detected. Data will be synced to OCI."
    SYNC_FLAG="--sync"
fi

for YEAR in $(seq $START_YEAR $END_YEAR); do
    echo ""
    echo "----------------------------------------------------"
    echo "📅 Processing Year: $YEAR"
    echo "----------------------------------------------------"
    
    python3 -m src.cli.run_advanced_daily --year $YEAR $SYNC_FLAG
    
    if [ $? -ne 0 ]; then
        echo "❌ Error processing year $YEAR. Continuing to next..."
    fi
    
    # Sleep a bit to avoid overwhelming the KBO site
    sleep 2
done

echo ""
echo "===================================================="
echo "🏁 Backfill Completed for $START_YEAR - $END_YEAR"
echo "===================================================="
