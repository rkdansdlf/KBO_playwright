#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "=== KBO Playwright Setup ==="

# 1. Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

# 2. Install dependencies
echo "Installing Python dependencies..."
pip install -q -r requirements.txt

# 3. Install Playwright browser
echo "Installing Playwright browser (chromium)..."
playwright install chromium

# 4. Create .env from template if not exists
if [ ! -f ".env" ]; then
    echo "Creating .env from env.example..."
    cp env.example .env
    echo "  -> Edit .env to set your configuration"
else
    echo ".env already exists, skipping"
fi

# 5. Initialize database
echo "Initializing database..."
python3 -c "from src.db.engine import init_db; init_db()"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit .env with your configuration"
echo "  2. Run: source venv/bin/activate"
echo "  3. Run: python3 -m scripts.scheduler"
echo ""
echo "See AGENTS.md for full command reference."
