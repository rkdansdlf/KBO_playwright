"""Deterministic serialization and comparison helpers for crawl evidence."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal


def canonicalize(value: object) -> object:
    """Convert supported values into a stable JSON-compatible structure."""
    if isinstance(value, Mapping):
        return {str(key): canonicalize(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float) and not math.isfinite(value):
        msg = "Non-finite float cannot be included in crawl evidence"
        raise ValueError(msg)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def canonical_json(value: object) -> str:
    """Serialize a value with stable key ordering and separators."""
    return json.dumps(
        canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_bytes(value: bytes) -> str:
    """Return the full SHA-256 digest for bytes."""
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: object) -> str:
    """Return the full SHA-256 digest for canonical JSON."""
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def diff_values(expected: object, actual: object, *, path: str = "$") -> list[dict[str, object]]:
    """Return field-level differences between two JSON-compatible values."""
    if isinstance(expected, Mapping) and isinstance(actual, Mapping):
        differences: list[dict[str, object]] = []
        keys = sorted({str(key) for key in expected} | {str(key) for key in actual})
        for key in keys:
            expected_value = expected.get(key) if key in expected else _MISSING
            actual_value = actual.get(key) if key in actual else _MISSING
            differences.extend(diff_values(expected_value, actual_value, path=f"{path}.{key}"))
        return differences
    if isinstance(expected, list) and isinstance(actual, list):
        differences = []
        for index in range(max(len(expected), len(actual))):
            expected_value = expected[index] if index < len(expected) else _MISSING
            actual_value = actual[index] if index < len(actual) else _MISSING
            differences.extend(diff_values(expected_value, actual_value, path=f"{path}[{index}]"))
        return differences
    if expected == actual:
        return []
    return [{"path": path, "expected": _display_value(expected), "actual": _display_value(actual)}]


class _Missing:
    """Sentinel used to distinguish a missing key from a JSON null."""


_MISSING = _Missing()


def _display_value(value: object) -> object:
    return "<missing>" if isinstance(value, _Missing) else value
