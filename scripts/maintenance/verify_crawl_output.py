#!/usr/bin/env python3
"""
Compare crawler outputs by producing a stable hash.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from typing import Any, Iterable, List


DEFAULT_EXCLUDE_KEYS = {"created_at", "updated_at"}


def load_payload(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_items(payload: Any) -> List[Any]:
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("results"), list):
            return payload["results"]
        return [payload]
    if isinstance(payload, list):
        return payload
    return [payload]


def normalize_value(value: Any, *, float_digits: int, exclude_keys: set[str]) -> Any:
    if isinstance(value, dict):
        normalized = {}
        for key in sorted(value.keys()):
            if key in exclude_keys:
                continue
            normalized[key] = normalize_value(
                value[key],
                float_digits=float_digits,
                exclude_keys=exclude_keys,
            )
        return normalized
    if isinstance(value, list):
        return [
            normalize_value(item, float_digits=float_digits, exclude_keys=exclude_keys)
            for item in value
        ]
    if isinstance(value, float):
        return round(value, float_digits)
    return value


def sort_items(items: Iterable[Any], sort_keys: List[str]) -> List[Any]:
    if not sort_keys:
        return list(items)

    def sort_key(item: Any) -> tuple:
        if not isinstance(item, dict):
            return tuple()
        return tuple(str(item.get(key, "")) for key in sort_keys)

    return sorted(items, key=sort_key)


def hash_payload(
    payload: Any,
    *,
    sort_keys: List[str],
    float_digits: int,
    exclude_keys: set[str],
) -> tuple[str, int]:
    items = normalize_items(payload)
    items = sort_items(items, sort_keys)
    normalized = [
        normalize_value(item, float_digits=float_digits, exclude_keys=exclude_keys)
        for item in items
    ]
    packed = json.dumps(
        {"count": len(items), "items": normalized},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    digest = hashlib.sha256(packed.encode("utf-8")).hexdigest()
    return digest, len(items)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify crawler output hashes.")
    parser.add_argument("--left", required=True, help="Path to left JSON payload.")
    parser.add_argument("--right", help="Optional right JSON payload to compare.")
    parser.add_argument(
        "--sort-keys",
        default="",
        help="Comma-separated keys for stable sorting (e.g., player_id,team_code).",
    )
    parser.add_argument(
        "--float-digits",
        type=int,
        default=4,
        help="Digits to round floats for hash stability.",
    )
    parser.add_argument(
        "--exclude-keys",
        default="created_at,updated_at",
        help="Comma-separated keys to drop before hashing.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sort_keys = [key.strip() for key in args.sort_keys.split(",") if key.strip()]
    exclude_keys = {key.strip() for key in args.exclude_keys.split(",") if key.strip()}
    exclude_keys |= DEFAULT_EXCLUDE_KEYS

    left_payload = load_payload(args.left)
    left_hash, left_count = hash_payload(
        left_payload,
        sort_keys=sort_keys,
        float_digits=args.float_digits,
        exclude_keys=exclude_keys,
    )
    print(f"[LEFT]  rows={left_count} hash={left_hash}")

    if not args.right:
        return 0

    right_payload = load_payload(args.right)
    right_hash, right_count = hash_payload(
        right_payload,
        sort_keys=sort_keys,
        float_digits=args.float_digits,
        exclude_keys=exclude_keys,
    )
    print(f"[RIGHT] rows={right_count} hash={right_hash}")

    if left_hash != right_hash or left_count != right_count:
        print("Mismatch detected")
        return 1

    print("Hashes match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
