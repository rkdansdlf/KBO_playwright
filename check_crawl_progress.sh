#!/bin/bash
# Check crawling progress

echo "🔍 Crawl Progress Check"
echo "======================="
echo ""

# Check if process is running
PID=$(ps aux | grep "fix_player_names.py --crawl --save" | grep -v grep | awk '{print $2}' | head -1)
if [ -n "$PID" ]; then
    echo "✅ Crawl process running (PID: $PID)"
else
    echo "❌ Crawl process not running"
fi
echo ""

# Check database
COUNT=$(sqlite3 data/kbo_dev.db "SELECT COUNT(*) FROM player_basic;" 2>/dev/null)
echo "📊 Players in database: $COUNT"

if [ "$COUNT" -gt "0" ]; then
    echo ""
    echo "🔢 Sample of latest players:"
    sqlite3 data/kbo_dev.db "SELECT player_id, name, team FROM player_basic ORDER BY player_id DESC LIMIT 5;"

    echo ""
    echo "🔍 Checking for 'Unknown Player' entries:"
    UNKNOWN=$(sqlite3 data/kbo_dev.db "SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player';" 2>/dev/null)
    if [ "$UNKNOWN" -eq "0" ]; then
        echo "✅ No 'Unknown Player' entries found"
    else
        echo "⚠️  Found $UNKNOWN 'Unknown Player' entries"
    fi
fi

echo ""
echo "Target: ~2,710 players"
