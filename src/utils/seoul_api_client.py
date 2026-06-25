"""Seoul Open Data API client for stadium congestion data.

Primary data sources:
  1. 서울시 실시간 도시데이터 API (Seoul Real-time City Data)
     https://data.seoul.go.kr/dataList/OA-21285/S/1/datasetView.do
     - Area congestion index for major Seoul landmarks
     - Covers: 잠실 야구장, 잠실역 zones

  2. 서울 생활인구 API (Seoul Population Density)
     https://data.seoul.go.kr/dataList/OA-15379/S/1/datasetView.do
     - Estimated floating population by time/zone

Environment variables:
    SEOUL_OPEN_DATA_API_KEY  — 서울 열린데이터광장 API 인증키
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

SEOUL_REALTIME_CONGESTION_URL = "http://openapi.seoul.go.kr:8088/{api_key}/json/citydata_ppltn/1/5/{area_name}"

# Jamsil area codes registered in Seoul Real-time City Data API
JAMSIL_AREA_CODES = [
    "잠실 야구장",  # 잠실 Baseball Stadium area
    "잠실역(2호선)",  # 잠실Station Line 2
    "석촌호수(동호)",  # Seokchon Lake East (nearby)
]


@dataclass
class CongestionSnapshot:
    """CongestionSnapshot class."""

    location_label: str
    congestion_level: str  # 여유 / 보통 / 약간 붐빔 / 붐빔 → mapped to low/normal/high/very_high
    congestion_index: float | None
    people_count: int | None
    source: str
    raw_data: dict


LEVEL_MAP = {
    "여유": "low",
    "보통": "normal",
    "약간 붐빔": "high",
    "붐빔": "very_high",
    "매우 붐빔": "very_high",
}


async def get_area_congestion(area_name: str) -> CongestionSnapshot | None:
    """Fetch real-time congestion for a specific Seoul area from the Seoul Open Data API.

    The API returns congestion_lvl (여유/보통/약간 붐빔/붐빔) and area_ppltn_max/min.
    """
    api_key = os.getenv("SEOUL_OPEN_DATA_API_KEY", "")
    if not api_key:
        logger.warning("[SeoulAPI] SEOUL_OPEN_DATA_API_KEY not set")
        return None

    url = SEOUL_REALTIME_CONGESTION_URL.format(api_key=api_key, area_name=area_name)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        # Response structure: SeoulRtd.citydata_ppltn[0]
        records = data.get("SeoulRtd.citydata_ppltn", {}).get("RESULT", {})
        if not isinstance(records, list) or not records:
            # Try alternate key
            items = data.get("CITYDATA", {}).get("LIVE_PPLTN_STTS", [])
            if not items:
                return None
            record = items[0]
        else:
            record = records[0]

        raw_level = record.get("AREA_CONGEST_LVL", "보통")
        level = LEVEL_MAP.get(raw_level, "normal")

        ppltn_max = record.get("AREA_PPLTN_MAX")
        ppltn_min = record.get("AREA_PPLTN_MIN")
        people_count = None
        if ppltn_max and ppltn_min:
            try:
                people_count = (int(ppltn_max) + int(ppltn_min)) // 2
            except (ValueError, TypeError):
                logger.debug("Invalid people count values: max=%s min=%s", ppltn_max, ppltn_min)

        # Congestion index: map levels to 0~100
        index_map = {"low": 20.0, "normal": 50.0, "high": 75.0, "very_high": 95.0}
        congestion_index = index_map.get(level, 50.0)

        return CongestionSnapshot(
            location_label=area_name,
            congestion_level=level,
            congestion_index=congestion_index,
            people_count=people_count,
            source="seoul_open_api",
            raw_data=record,
        )

    except httpx.HTTPError as e:
        logger.warning("[SeoulAPI] HTTP error for area=%s: %s", area_name, e)
    except (KeyError, TypeError, ValueError):
        logger.exception("[SeoulAPI] Unexpected error for area=%s", area_name)

    return None


async def get_jamsil_congestion_batch() -> list[CongestionSnapshot]:
    """Fetch congestion data for all Jamsil-area zones."""
    import asyncio

    tasks = [get_area_congestion(area) for area in JAMSIL_AREA_CODES]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if isinstance(r, CongestionSnapshot)]
