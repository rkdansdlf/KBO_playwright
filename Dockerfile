###############################################################################
# Stage 1: Builder — compile native extensions, install all pip packages
###############################################################################
FROM python:3.14-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Dependencies come from pyproject.toml, the single source of truth.
# The dependency layer is split from the project build: `uv pip` cannot
# read PEP 621 deps directly, so compile to requirements first. A
# source-only change then reuses the cached dependency layer, and uv's
# HTTP cache survives across builds via the cache mount.
COPY pyproject.toml README.md ./
# NOTE: `--extra postgres` is required, not optional, for this image. The
# driver lives in `[project.optional-dependencies]` because a pure-SQLite
# local run skips it, but every container here talks to PostgreSQL
# (`src/db/engine.py`, `postgresql+psycopg2://` URLs). Omitting the extra
# builds an image that crashes on startup with `No module named 'psycopg2'`.
RUN --mount=type=cache,id=uv-cache-v2,target=/root/.cache/uv \
    uv pip compile pyproject.toml --extra postgres -o /tmp/requirements.txt && \
    uv pip install --system -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt && \
    uv pip install --system setuptools wheel
# NOTE: the `kbo-playwright` project itself is intentionally NOT installed
# here. Its setuptools build hangs indefinitely under this host's buildkit
# executor (reproduced 3x; the identical `uv pip install` finishes in ~30s
# in `docker run`), while nothing at runtime needs its dist-info: there are
# no `[project.scripts]` entry points and no `importlib.metadata` lookups.
# The runtime stage COPYs a fresh `src/` anyway, so the builder never needs
# the source tree at all.

###############################################################################
# Stage 2: Runtime — lean image without build-essential (~250 MB smaller)
###############################################################################
FROM python:3.14-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Runtime-only system dependencies (Playwright libs + PostgreSQL client)
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
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
COPY --from=builder /usr/local/lib/python3.14/site-packages /usr/local/lib/python3.14/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install uv (needed at runtime for potential pip operations)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create appuser
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install Playwright Chromium as appuser
RUN mkdir -p /ms-playwright && chown appuser:appuser /ms-playwright && chmod 755 /ms-playwright && \
    su appuser -c "python -m playwright install chromium"

# Copy source code with correct ownership (avoids extra chown layer)
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser migrations/ ./migrations/
COPY --chown=appuser:appuser docker/ ./docker/
# Per-host robots policies are read at runtime by the source inventory job and
# by the crawler compliance check, both of which resolve them by relative path
# (source_inventory.ROBOTS_DIR, host_policy.POLICY_DIR, compliance). Without
# this COPY the directory does not exist inside the image, every host comes back
# unreachable, and the policy reads as enforced while enforcing nothing.
# Dockerfile.playwright already copies all of Docs/; this stage copies only what
# the runtime reads. The name is case-sensitive on Linux -- it must stay `Docs`.
COPY --chown=appuser:appuser Docs/robots/ ./Docs/robots/
COPY --chown=appuser:appuser pyproject.toml ./

# Strip Windows CRLF from shell scripts (host checkout may use CRLF).
RUN sed -i 's/\r$//' docker/entrypoint.sh && chmod +x docker/entrypoint.sh

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
