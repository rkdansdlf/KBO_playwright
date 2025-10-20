# KBO Playwright Data Crawler

## Project Overview

This project is a Python-based data crawling and management system designed to collect KBO (Korean Baseball Organization) player statistics across various series and seasons. It leverages web scraping techniques to extract data from the KBO official website and stores it in a structured local SQLite database. The system also supports synchronization with a remote Supabase (PostgreSQL) database.

The core functionalities include:
-   **Web Scraping:** Utilizing Playwright to navigate KBO record pages and extract player statistics.
-   **Data Parsing:** Processing raw HTML data into structured Python objects.
-   **Data Persistence:** Storing structured data in a local SQLite database using SQLAlchemy ORM.
-   **Database Schema Management:** Defining clear ORM models with Foreign Key constraints for data integrity.
-   **Data Seeding:** Populating essential metadata tables (e.g., teams, seasons) from CSV files.
-   **Supabase Synchronization:** A dedicated CLI tool to sync local database changes to a remote Supabase instance.

## Main Technologies

-   **Language:** Python
-   **Web Scraping:** Playwright
-   **Database ORM:** SQLAlchemy
-   **Local Database:** SQLite
-   **Remote Database (Sync):** Supabase (PostgreSQL)
-   **Dependency Management:** `pip` with `requirements.txt`
-   **CLI Argument Parsing:** `argparse`

## Architecture

-   **`src/crawlers/`**: Contains scripts responsible for navigating web pages, interacting with UI elements, and initiating data extraction for specific types of data (e.g., `player_batting_all_series_crawler.py`, `player_pitching_all_series_crawler.py`).
-   **`src/parsers/`**: (Implicitly used) Would contain logic for parsing raw HTML into structured data.
-   **`src/models/`**: Defines SQLAlchemy ORM models for all database tables (e.g., `player.py`, `season.py`, `team.py`). These models include schema definitions and relationships.
-   **`src/repositories/`**: Provides an abstraction layer for database operations (CRUD, UPSERT) for specific models (e.g., `player_season_batting_repository.py`, `player_season_pitching_repository.py`).
-   **`src/db/`**: Contains database engine configuration and session management (`engine.py`).
-   **`src/cli/`**: Houses command-line interface tools for various tasks (e.g., `sync_supabase.py`).
-   **`src/utils/`**: Utility functions and helpers.
-   **`data/`**: Stores the local SQLite database file (`kbo_dev.db`) and CSV files for seeding.
-   **`Docs/`**: Project documentation, including database schema definitions.

## Building and Running

### 1. Environment Setup

It is highly recommended to use a Python virtual environment.

```bash
# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
# or if pip is linked to python3
pip3 install -r requirements.txt
```

### 2. Database Initialization

Initialize the local SQLite database (`data/kbo_dev.db`) and create all necessary tables based on the ORM models.

```bash
venv/bin/python3 init_db.py
```

### 3. Data Seeding

Populate essential metadata tables (`teams`, `kbo_seasons`) with initial data from CSV files. This is crucial for satisfying Foreign Key constraints.

```bash
venv/bin/python3 seed_data.py
```

### 4. Running Crawlers

Execute the crawlers to scrape data and save it to the local SQLite database.

```bash
# Example: Crawl all series for pitchers in 2025 and save to local DB
venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year 2025 --save

# Example: Crawl a specific series for batters in 2025 and save to local DB
venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save
```

### 5. Syncing with Supabase

Synchronize data from the local SQLite database to a remote Supabase (PostgreSQL) instance. Ensure your `TARGET_DATABASE_URL` or `SUPABASE_DB_URL` environment variable is set.

```bash
# Example: Sync all tables to Supabase
venv/bin/python3 -m src.cli.sync_supabase

# Example: Sync only the 'player_season_pitching' table to Supabase
venv/bin/python3 -m src.cli.sync_supabase --table player_season_pitching

# Example: Sync all tables after truncating (clearing) them on Supabase
venv/bin/python3 -m src.cli.sync_supabase --truncate
```

## Development Conventions

-   **ORM-centric Database Interaction:** All database operations should primarily use SQLAlchemy ORM models and repositories.
-   **Clear Separation of Concerns:** Logic is divided into crawlers, parsers, models, and repositories for maintainability.
-   **Data Integrity:** Emphasis on using Foreign Key constraints at the database level to ensure data consistency.
-   **Command-Line Interface:** Scripts are designed to be run via the command line with `argparse` for flexible execution.
-   **Virtual Environments:** Development should always occur within a Python virtual environment.
-   **Code Style:** Adherence to standard Python best practices and style guides (e.g., PEP 8).
