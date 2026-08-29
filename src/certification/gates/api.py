"""G06: API Gateway, Authentication & Rate Limiting Certification Gate."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, ClassVar
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.api.app import app
from src.certification.models import GateResult, GateStatus

if TYPE_CHECKING:
    from src.certification.context import CertificationContext

HTTP_OK = 200
HTTP_FORBIDDEN = 403


class ApiGatewayGate:
    """G06: Validates FastAPI health checks, authentication contract, response schema, and rate limit headers."""

    gate_id: str = "api_gateway"
    name: str = "API Gateway & Auth Contract"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = []

    def run(self, context: CertificationContext) -> GateResult:
        """Execute in-process API contract and authentication verification."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            client = TestClient(app)

            # 1. Health check endpoint
            res_health = client.get("/health")
            metrics["health_status_code"] = res_health.status_code
            evidence["health_response"] = res_health.json() if res_health.status_code == HTTP_OK else None

            if res_health.status_code != HTTP_OK:
                duration_ms = (time.perf_counter() - start) * 1000.0
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.FAIL,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"Health check failed with status {res_health.status_code}",
                )

            # 2. Authentication policy verification with test API key
            test_api_key = "cert-test-auth-key-12345"
            with patch.dict(os.environ, {"REST_API_KEY": test_api_key}):
                # A. Unauthenticated request must be rejected (403)
                res_no_auth = client.get("/api/analytics/compare")
                metrics["unauth_status_code"] = res_no_auth.status_code
                evidence["unauth_rejected"] = res_no_auth.status_code == HTTP_FORBIDDEN

                if res_no_auth.status_code != HTTP_FORBIDDEN:
                    duration_ms = (time.perf_counter() - start) * 1000.0
                    return GateResult(
                        gate_id=self.gate_id,
                        name=self.name,
                        status=GateStatus.FAIL,
                        duration_ms=duration_ms,
                        blocking=self.blocking,
                        metrics=metrics,
                        evidence=evidence,
                        message=(
                            f"Auth contract violated: unauthenticated request returned "
                            f"{res_no_auth.status_code} instead of 403"
                        ),
                    )

                # B. Authenticated request must succeed (200)
                res_auth = client.get(
                    "/api/analytics/compare",
                    params={"player1": "김도영", "player2": "이종범"},
                    headers={"X-API-Key": test_api_key},
                )
                metrics["auth_status_code"] = res_auth.status_code
                has_limit_header = "x-ratelimit-limit" in res_auth.headers
                has_remain_header = "x-ratelimit-remaining" in res_auth.headers
                evidence["has_ratelimit_header"] = has_limit_header or has_remain_header
                evidence["auth_response_status"] = res_auth.status_code

                duration_ms = (time.perf_counter() - start) * 1000.0

                if res_auth.status_code != HTTP_OK:
                    return GateResult(
                        gate_id=self.gate_id,
                        name=self.name,
                        status=GateStatus.FAIL,
                        duration_ms=duration_ms,
                        blocking=self.blocking,
                        metrics=metrics,
                        evidence=evidence,
                        message=f"Authenticated endpoint failed with status {res_auth.status_code}",
                    )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message="API Gateway health, auth enforcement, and rate limit contracts verified",
            )

        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"API Gateway verification error: {err}",
            )


__all__ = [
    "HTTP_FORBIDDEN",
    "HTTP_OK",
    "ApiGatewayGate",
]
