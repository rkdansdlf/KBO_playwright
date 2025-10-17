#!/bin/bash

# Monitor player crawl progress

while true; do
    clear
    echo "=== KBO Player Crawl Monitor ==="
    echo ""
    echo "Last 5 lines of log:"
    tail -5 player_full_crawl.log 2>/dev/null || echo "No log yet..."
    echo ""
    echo "Current database count:"
    source venv/bin/activate && python -c "
from src.repositories.player_basic_repository import PlayerBasicRepository
repo = PlayerBasicRepository()
count = repo.count()
print(f'  Total players: {count} / 5120 ({count*100//5120}%)')
" 2>/dev/null || echo "  Error checking database"
    echo ""
    echo "Process status:"
    ps aux | grep "src.crawlers.player_search_crawler" | grep -v grep | head -1 || echo "  Not running"
    echo ""
    echo "Press Ctrl+C to exit monitoring"
    sleep 10
done
