###############################################################################
# Stage 1: Builder — compile native extensions, install all pip packages
###############################################################################
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt ./
RUN uv pip install --system --no-cache -r requirements.txt

###############################################################################
# Stage 2: Runtime — lean image without build-essential (~250 MB smaller)
###############################################################################
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Runtime-only system dependencies (Playwright libs + PostgreSQL client)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
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

# Copy installed Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install uv (needed at runtime for potential pip operations)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create appuser
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install Playwright Chromium as appuser
RUN mkdir -p /ms-playwright && chmod 777 /ms-playwright && \
    su appuser -c "python -m playwright install chromium"

# Copy source code with correct ownership (avoids extra chown layer)
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser migrations/ ./migrations/
COPY --chown=appuser:appuser docker/ ./docker/
COPY --chown=appuser:appuser pyproject.toml ./

VOLUME /app/data

# entrypoint.sh starts as root, adjusts volume ownership, then drops
# to appuser via gosu.  This USER directive is intentionally omitted
# so that the entrypoint can perform the initial chown.  The actual
# process runs as appuser after gosu exec.
# USER appuser  -- see docker/entrypoint.sh for privilege-drop logic

EXPOSE 8000

# Full SQLite integrity checks remain in the startup guard. Runtime healthchecks
# stay lightweight so normal write contention does not mark the scheduler unhealthy.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "from sqlalchemy import text; from src.db.engine import Engine; conn = Engine.connect(); conn.execute(text('SELECT 1')); conn.close()" || exit 1

ENTRYPOINT ["bash", "docker/entrypoint.sh"]
CMD ["python", "-m", "scripts.scheduler"]
