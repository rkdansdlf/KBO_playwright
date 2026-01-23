#!/usr/bin/env python3
"""
Capture Basic1/Basic2 HTML snapshots for batting tables.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright

from src.crawlers.player_batting_all_series_crawler import get_series_mapping
from src.utils.playwright_blocking import install_sync_resource_blocking


BASIC1_URL = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
BASIC2_URL = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx"

BASIC2_SORT_CODES = {
    "BB": "BB_CN",
    "IBB": "IB_CN",
    "HBP": "HP_CN",
    "SO": "KK_CN",
    "GDP": "GD_CN",
    "SLG": "SLG_RT",
    "OBP": "OBP_RT",
    "OPS": "OPS_RT",
    "MH": "MH_HITTER_CN",
    "RISP": "SP_HRA_RT",
    "PH-BA": "PH_HRA_RT",
}


def _select_year_series(page, year: int, series_value: str) -> None:
    season_selector = (
        'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
    )
    series_selector = (
        'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
    )
    page.select_option(season_selector, str(year))
    page.wait_for_timeout(500)
    page.select_option(series_selector, value=series_value)
    page.wait_for_timeout(500)
    page.wait_for_load_state("networkidle", timeout=30000)


def _apply_sort(page, sort_code: str) -> None:
    selector = f'a[href="javascript:sort(\'{sort_code}\');"]'
    link = page.query_selector(selector)
    if link:
        link.click()
        page.wait_for_load_state("networkidle", timeout=30000)
        page.wait_for_timeout(500)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot Basic1/Basic2 batting HTML.")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument("--out-dir", type=str, default="snapshots")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--basic2-header",
        type=str,
        default=None,
        help="Optional Basic2 sort header (e.g., BB, SO, OPS).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    series_mapping = get_series_mapping()
    if args.series not in series_mapping:
        raise SystemExit(f"Unsupported series: {args.series}")

    series_info = series_mapping[args.series]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=args.headless)
        page = browser.new_page()
        page.set_default_timeout(30000)
        install_sync_resource_blocking(page)

        page.goto(BASIC1_URL, wait_until="load", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        _select_year_series(page, args.year, series_info["value"])
        _apply_sort(page, "PA_CN")

        basic1_path = out_dir / f"basic1_{args.year}_{args.series}.html"
        basic1_path.write_text(page.content(), encoding="utf-8")
        print(f"Saved Basic1 snapshot: {basic1_path}")

        page.goto(BASIC2_URL, wait_until="load", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        _select_year_series(page, args.year, series_info["value"])

        if args.basic2_header:
            sort_code = BASIC2_SORT_CODES.get(args.basic2_header.upper())
            if sort_code:
                _apply_sort(page, sort_code)
            else:
                print(f"Unknown Basic2 header: {args.basic2_header}")

        basic2_path = out_dir / f"basic2_{args.year}_{args.series}.html"
        basic2_path.write_text(page.content(), encoding="utf-8")
        print(f"Saved Basic2 snapshot: {basic2_path}")

        browser.close()


if __name__ == "__main__":
    main()
