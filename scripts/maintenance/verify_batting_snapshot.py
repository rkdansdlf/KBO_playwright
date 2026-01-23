#!/usr/bin/env python3
"""
Compare batting parsers on the same Basic1/Basic2 HTML snapshot.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from playwright.sync_api import sync_playwright

from src.crawlers.player_batting_all_series_crawler import (
    parse_batting_stats_table,
    parse_basic2_header_data,
)


def _normalize_value(value: Any, float_digits: int) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_value(v, float_digits) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_normalize_value(v, float_digits) for v in value]
    if isinstance(value, float):
        return round(value, float_digits)
    return value


def _hash_items(items: List[Dict[str, Any]], sort_keys: List[str], float_digits: int) -> Tuple[str, int]:
    def sort_key(item: Dict[str, Any]) -> Tuple[str, ...]:
        return tuple(str(item.get(k, "")) for k in sort_keys)

    ordered = sorted(items, key=sort_key) if sort_keys else list(items)
    normalized = [_normalize_value(item, float_digits) for item in ordered]
    packed = json.dumps(
        {"count": len(normalized), "items": normalized},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(packed.encode("utf-8")).hexdigest(), len(normalized)


def _first_diff(
    left: List[Dict[str, Any]],
    right: List[Dict[str, Any]],
    sort_keys: List[str],
) -> Optional[str]:
    def key(item: Dict[str, Any]) -> Tuple[str, ...]:
        return tuple(str(item.get(k, "")) for k in sort_keys)

    left_map = {key(item): item for item in left}
    right_map = {key(item): item for item in right}
    for k in sorted(set(left_map) | set(right_map)):
        lval = left_map.get(k)
        rval = right_map.get(k)
        if lval is None or rval is None:
            return f"Missing {k}: {'left' if lval is None else 'right'}"
        if lval != rval:
            lines = [f"Diff for {k}"]
            for field in sorted(set(lval.keys()) | set(rval.keys())):
                if lval.get(field) != rval.get(field):
                    lines.append(f"  {field}: left={lval.get(field)} right={rval.get(field)}")
            return "\n".join(lines)
    return None


def _load_html(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify batting snapshot parsing.")
    parser.add_argument("--basic1", type=str, required=True)
    parser.add_argument("--basic2", type=str, required=True)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument("--basic2-header", type=str, default="BB")
    parser.add_argument("--float-digits", type=int, default=4)
    parser.add_argument("--sort-keys", type=str, default="player_id,team_code")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sort_keys = [k.strip() for k in args.sort_keys.split(",") if k.strip()]

    basic1_html = _load_html(Path(args.basic1))
    basic2_html = _load_html(Path(args.basic2))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()

        page.set_content(basic1_html, wait_until="domcontentloaded")
        legacy_basic1 = parse_batting_stats_table(page, args.series, args.year, use_fast=False)
        fast_basic1 = parse_batting_stats_table(page, args.series, args.year, use_fast=True)

        page.set_content(basic2_html, wait_until="domcontentloaded")
        legacy_basic2 = list(
            parse_basic2_header_data(
                page,
                args.basic2_header,
                args.basic2_header,
                args.year,
                use_fast=False,
            ).values()
        )
        fast_basic2 = list(
            parse_basic2_header_data(
                page,
                args.basic2_header,
                args.basic2_header,
                args.year,
                use_fast=True,
            ).values()
        )

        browser.close()

    b1_hash_left, b1_count_left = _hash_items(legacy_basic1, sort_keys, args.float_digits)
    b1_hash_right, b1_count_right = _hash_items(fast_basic1, sort_keys, args.float_digits)
    print(f"[Basic1] legacy={b1_count_left} {b1_hash_left}")
    print(f"[Basic1] fast  ={b1_count_right} {b1_hash_right}")
    if b1_hash_left != b1_hash_right or b1_count_left != b1_count_right:
        diff = _first_diff(legacy_basic1, fast_basic1, sort_keys)
        if diff:
            print(diff)
        return 1

    b2_hash_left, b2_count_left = _hash_items(legacy_basic2, sort_keys, args.float_digits)
    b2_hash_right, b2_count_right = _hash_items(fast_basic2, sort_keys, args.float_digits)
    print(f"[Basic2] legacy={b2_count_left} {b2_hash_left}")
    print(f"[Basic2] fast  ={b2_count_right} {b2_hash_right}")
    if b2_hash_left != b2_hash_right or b2_count_left != b2_count_right:
        diff = _first_diff(legacy_basic2, fast_basic2, sort_keys)
        if diff:
            print(diff)
        return 1

    print("Hashes match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
