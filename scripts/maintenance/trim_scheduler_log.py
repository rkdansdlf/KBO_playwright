"""Trim a large append-only log in place, archiving the discarded head as gzip."""

from __future__ import annotations

import argparse
import gzip
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Sequence

_DEFAULT_LOG = Path("logs") / "scheduler.launchd.err.log"
_DEFAULT_KEEP_BYTES = 16 * 1024 * 1024
_DEFAULT_ARCHIVE_DIR = Path("data") / "archive" / "logs"


def _parse_size(raw: str) -> int:
    """Parse a human size like '8M' or '52428800' into bytes."""
    text = raw.strip().upper()
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3}
    if text.endswith(tuple(multipliers)):
        return int(float(text[:-1]) * multipliers[text[-1]])
    return int(float(text))


def _archive_head(head: bytes, source: Path, archive_dir: Path) -> Path:
    """Write the trimmed-off head bytes next to prior archives and return the path."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    target = archive_dir / f"{source.name}.{stamp}.gz"
    with gzip.open(target, "wb", compresslevel=6) as handle:
        handle.write(head)
    return target


def trim_log(
    path: Path,
    *,
    keep_bytes: int = _DEFAULT_KEEP_BYTES,
    archive_dir: Path | None = None,
) -> dict[str, object]:
    """Keep the trailing ``keep_bytes`` of ``path``, gzip-archiving the head in place.

    The file is never renamed so a running process holding the descriptor keeps
    appending safely; concurrent writes between read and rewrite are best-effort.
    """
    size = path.stat().st_size
    if size <= keep_bytes:
        return {"trimmed": False, "size": size}

    with path.open("rb+") as handle:
        handle.seek(-keep_bytes, 2)
        tail = handle.read()
        handle.seek(0)
        head = handle.read(size - keep_bytes)

    result: dict[str, object] = {"trimmed": True, "size": size, "kept": len(tail)}
    if archive_dir is not None:
        archived = _archive_head(head, path, archive_dir)
        result["archived"] = str(archived)
        result["archived_bytes"] = len(head)

    with path.open("rb+") as handle:
        handle.seek(0)
        handle.write(tail)
        handle.truncate(len(tail))
    return result


def main(argv: Sequence[str] | None = None) -> int:
    """Run the log trimmer CLI."""
    parser = argparse.ArgumentParser(description="Trim a large log file in place (launchd-safe)")
    parser.add_argument("--file", type=Path, default=_DEFAULT_LOG, help="Log file to trim")
    parser.add_argument("--keep", default="16M", help="Bytes to retain at the tail, e.g. 8M")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=_DEFAULT_ARCHIVE_DIR,
        help="Directory for gzipped head archives (pass empty string to discard)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report sizes without writing")
    args = parser.parse_args(argv)
    if not args.file.exists():
        sys.stderr.write(f"file not found: {args.file}\n")
        return 1

    keep_bytes = _parse_size(args.keep)
    size = args.file.stat().st_size
    if args.dry_run:
        would_trim = size > keep_bytes
        sys.stdout.write(f"dry-run: {args.file} size={size} keep={keep_bytes} would_trim={would_trim}\n")
        return 0

    archive_dir = Path(args.archive_dir) if str(args.archive_dir) else None
    result = trim_log(args.file, keep_bytes=keep_bytes, archive_dir=archive_dir)
    sys.stdout.write(str(result) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
