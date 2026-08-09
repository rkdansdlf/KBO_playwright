"""API Authentication & Key Validation module."""

from __future__ import annotations

import os

from fastapi import Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


def get_api_key(api_key: str | None = Depends(api_key_header)) -> str | None:
    """Validate API Key if REST_API_KEY environment variable is configured."""
    expected_key = os.getenv("REST_API_KEY")
    if not expected_key:
        return api_key

    if not api_key or api_key != expected_key:
        raise HTTPException(
            status_code=403,
            detail="Could not validate credentials",
        )
    return api_key
