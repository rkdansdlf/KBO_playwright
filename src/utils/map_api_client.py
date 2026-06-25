"""
Map API client utilities for transit time measurement.

Provides a unified interface to call Kakao, Naver, and TMAP direction APIs
to measure real travel times from transit hubs to the stadium.

API priority order (for each request):
    1. Kakao Directions API
    2. Naver Directions API
    3. TMAP Pedestrian/Transit API

Environment variables required (at least one group):
    KAKAO_REST_API_KEY      — Kakao REST API key
    NAVER_CLIENT_ID         — Naver Maps API client ID
    NAVER_CLIENT_SECRET     — Naver Maps API client secret
    TMAP_API_KEY            — TMAP (SK) API key
    GOOGLE_MAPS_API_KEY     — Google Maps Directions API (fallback)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Literal

import httpx

logger = logging.getLogger(__name__)

TransportMode = Literal["subway", "bus", "walk", "car", "mixed"]


@dataclass
class TransitResult:
    """Unified result from any map API."""

    origin_label: str
    transport_mode: TransportMode
    duration_minutes: int
    distance_meters: int | None
    source_api: str
    raw_response: dict


# ─────────────────────────────────────────────────────────
# Kakao Directions API
# https://developers.kakao.com/docs/latest/ko/local/dev-guide
# ─────────────────────────────────────────────────────────

KAKAO_TRANSIT_URL = "https://apis.openapi.sk.com/transit/routes"  # TMAP transit
KAKAO_DIRECTIONS_URL = "https://apis-navi.kakaomobility.com/v1/directions"


async def _call_kakao(
    client: httpx.AsyncClient,
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    _mode: TransportMode,
) -> dict[str, Any] | None:
    api_key = os.getenv("KAKAO_REST_API_KEY", "")
    if not api_key:
        return None

    # Kakao car/walk directions
    url = KAKAO_DIRECTIONS_URL
    params = {
        "origin": f"{origin_lng},{origin_lat}",
        "destination": f"{dest_lng},{dest_lat}",
        "priority": "RECOMMEND",
    }
    headers = {"Authorization": f"KakaoAK {api_key}"}
    try:
        resp = await client.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        routes = data.get("routes", [])
        if routes and routes[0].get("result_code") == 0:
            summary = routes[0].get("summary", {})
            return {
                "duration_seconds": summary.get("duration", 0),
                "distance_meters": summary.get("distance", 0),
                "raw": data,
            }
    except httpx.HTTPError as e:
        logger.warning("[Kakao API] %s", e)
    return None


# ─────────────────────────────────────────────────────────
# Naver Directions API
# https://api.ncloud-docs.com/docs/ai-naver-mapsdirections-driving
# ─────────────────────────────────────────────────────────

NAVER_DIRECTIONS_URL = "https://naveropenapi.apigw.ntruss.com/map-direction/v1/driving"


async def _call_naver(
    client: httpx.AsyncClient,
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    _mode: TransportMode,
) -> dict[str, Any] | None:
    client_id = os.getenv("NAVER_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        return None

    url = NAVER_DIRECTIONS_URL
    params = {
        "start": f"{origin_lng},{origin_lat}",
        "goal": f"{dest_lng},{dest_lat}",
        "option": "trafast",
    }
    headers = {
        "X-NCP-APIGW-API-KEY-ID": client_id,
        "X-NCP-APIGW-API-KEY": client_secret,
    }
    try:
        resp = await client.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        route = (data.get("route") or {}).get("trafast", [])
        if route:
            summary = route[0].get("summary", {})
            return {
                "duration_seconds": summary.get("duration", 0) // 1000,  # Naver returns ms
                "distance_meters": summary.get("distance", 0),
                "raw": data,
            }
    except httpx.HTTPError as e:
        logger.warning("[Naver API] %s", e)
    return None


# ─────────────────────────────────────────────────────────
# TMAP Transit API (SK Telecom)
# https://tmapapi.sktelecom.com/main.html#webservice/docs/transit-route
# ─────────────────────────────────────────────────────────

TMAP_TRANSIT_URL = "https://apis.openapi.sk.com/transit/routes"


async def _call_tmap(
    client: httpx.AsyncClient,
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    _mode: TransportMode,
) -> dict[str, Any] | None:
    api_key = os.getenv("TMAP_API_KEY", "")
    if not api_key:
        return None

    payload = {
        "startX": str(origin_lng),
        "startY": str(origin_lat),
        "endX": str(dest_lng),
        "endY": str(dest_lat),
        "count": 1,
        "lang": 0,
        "format": "json",
    }
    headers = {"appKey": api_key, "Content-Type": "application/json"}
    try:
        resp = await client.post(TMAP_TRANSIT_URL, json=payload, headers=headers, timeout=12)
        resp.raise_for_status()
        data = resp.json()
        itineraries = data.get("metaData", {}).get("plan", {}).get("itineraries", [])
        if itineraries:
            best = itineraries[0]
            return {
                "duration_seconds": best.get("totalTime", 0),
                "distance_meters": best.get("totalDistance", 0),
                "raw": data,
            }
    except httpx.HTTPError as e:
        logger.warning("[TMAP API] %s", e)
    return None


# ─────────────────────────────────────────────────────────
# Public unified interface
# ─────────────────────────────────────────────────────────

JAMSIL_LAT = 37.5121
JAMSIL_LNG = 127.0719


@dataclass
class TransitRequest:
    origin_label: str
    origin_lat: float
    origin_lng: float
    mode: TransportMode = "mixed"
    dest_lat: float = JAMSIL_LAT
    dest_lng: float = JAMSIL_LNG


async def get_transit_time(req: TransitRequest) -> TransitResult | None:
    """
    Fetch transit duration from origin to Jamsil Stadium using available APIs.

    Falls through Kakao → Naver → TMAP until one succeeds.
    Returns None if all APIs fail or no keys are configured.
    """
    async with httpx.AsyncClient(timeout=15) as client:
        for caller, api_name in [
            (_call_kakao, "kakao"),
            (_call_naver, "naver"),
            (_call_tmap, "tmap"),
        ]:
            result = await caller(client, req.origin_lat, req.origin_lng, req.dest_lat, req.dest_lng, req.mode)
            if result:
                duration_minutes = max(1, round(result["duration_seconds"] / 60))
                return TransitResult(
                    origin_label=req.origin_label,
                    transport_mode=req.mode,
                    duration_minutes=duration_minutes,
                    distance_meters=result.get("distance_meters"),
                    source_api=api_name,
                    raw_response=result.get("raw", {}),
                )

    logger.warning("[MapAPI] All APIs failed for origin=%s", req.origin_label)
    return None


async def get_transit_times_batch(
    origins: list[dict],
    mode: TransportMode = "mixed",
    dest_lat: float = JAMSIL_LAT,
    dest_lng: float = JAMSIL_LNG,
) -> list[TransitResult]:
    """
    Batch transit time lookup for multiple origins.

    origins: list of {"label": str, "lat": float, "lng": float}
    """
    import asyncio

    tasks = [
        get_transit_time(TransitRequest(o["label"], o["lat"], o["lng"], mode, dest_lat, dest_lng)) for o in origins
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if isinstance(r, TransitResult)]
