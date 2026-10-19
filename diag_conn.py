"""Quick TCP + psycopg2 connect diagnostic for the remote PostgreSQL hosts (read-only)."""

from __future__ import annotations

import os
import socket
import sys
import time

from dotenv import load_dotenv

load_dotenv()

TARGETS = [("100.81.73.13", 5432), ("100.81.73.13", 55433)]


def tcp_check(host: str, port: int, timeout: float = 8.0) -> str:
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return f"TCP OK ({time.perf_counter() - t0:.1f}s)"
    except OSError as exc:
        return f"TCP FAIL: {exc} ({time.perf_counter() - t0:.1f}s)"


def pg_check(name: str, url: str, timeout: int = 8) -> None:
    if not url:
        print(f"{name}: (unset)", flush=True)
        return
    try:
        import psycopg2

        t0 = time.perf_counter()
        url = url.replace("postgresql+psycopg2://", "postgresql://")
        dsn = url.split("?", 1)[0]
        kw = {"connect_timeout": timeout, "sslmode": "prefer"}
        conn = psycopg2.connect(dsn, **kw)
        with conn.cursor() as cur:
            cur.execute("select version(), current_database()")
            row = cur.fetchone()
        conn.close()
        print(f"{name}: OK ({time.perf_counter() - t0:.1f}s) {row}", flush=True)
    except Exception as exc:  # noqa: BLE001 - diagnostic
        print(f"{name}: FAIL after {time.perf_counter() - t0:.1f}s -> {type(exc).__name__}: {exc}", flush=True)


def main() -> int:
    print("== TCP ==", flush=True)
    for host, port in TARGETS:
        print(f"{host}:{port} -> {tcp_check(host, port)}", flush=True)
    print("== psycopg2 ==", flush=True)
    pg_check("DATABASE_URL(5432)", os.environ.get("DATABASE_URL", ""))
    pg_check("PGVECTOR_URL(55433)", os.environ.get("PGVECTOR_URL", ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
