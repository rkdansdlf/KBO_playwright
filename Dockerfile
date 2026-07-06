FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# System dependencies (Playwright + PostgreSQL clients)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    build-essential \
    curl \
    wget \
    gnupg \
    unzip \
    fonts-liberation \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    gosu \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    groupadd -r appuser && useradd -r -g appuser appuser

RUN mkdir -p /ms-playwright && chmod 777 /ms-playwright && \
    su appuser -c "python -m playwright install chromium"

COPY . .

RUN chown -R appuser:appuser /app && \
    chmod -R u+rwX,go+rX /app

VOLUME /app/data

# USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -m src.cli.sqlite_integrity_guard --database-url "${DATABASE_URL:-sqlite:////app/data/kbo_dev.db}" --action none --strict --json >/tmp/sqlite_healthcheck.json && \
        python -c "from sqlalchemy import text; from src.db.engine import SessionLocal; s=SessionLocal(); s.execute(text('SELECT 1')); s.close()" || exit 1

ENTRYPOINT ["bash", "docker/entrypoint.sh"]
CMD ["python", "-m", "scripts.scheduler"]
