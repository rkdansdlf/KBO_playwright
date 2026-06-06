#!/bin/bash
set -e

# Sync previously completed years to OCI so we don't lock SQLite later while crawling
echo "🚀 Syncing 2023 game data to OCI..."
venv/bin/python3 -m src.cli.sync_oci --game-details --year 2023

echo "🚀 Syncing 2022 game data to OCI..."
venv/bin/python3 -m src.cli.sync_oci --game-details --year 2022

echo "🚀 Syncing 2021 game data to OCI..."
venv/bin/python3 -m src.cli.sync_oci --game-details --year 2021

# Now that OCI sync is done reading sequentially, resume 2020 backfill safely
echo "🚀 Resuming 2020 Backfill without DB locks..."
venv/bin/python3 -m src.cli.collect_games --year 2020

# Finally, sync the last completed 2020 data
echo "🚀 Syncing 2020 game data to OCI..."
venv/bin/python3 -m src.cli.sync_oci --game-details --year 2020

echo "✅ ALL Historical Backfill Pipeline Fully Completed!"
