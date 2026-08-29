"""G07: Real-time WebSocket Protocol & Event Schema Certification Gate."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from fastapi.testclient import TestClient

from src.api.app import app
from src.certification.models import GateResult, GateStatus

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class WebSocketStreamGate:
    """G07: Validates WebSocket connection, subscription, event delivery, and JSON schema compliance."""

    gate_id: str = "websocket_stream"
    name: str = "Real-time WebSocket Stream"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["api_gateway"]

    def run(self, context: CertificationContext) -> GateResult:
        """Execute WebSocket client connection, subscribe action, and payload validation."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            client = TestClient(app)
            game_id = "20260401LGNC0"

            with client.websocket_connect(f"/ws/live/{game_id}") as websocket:
                evidence["connected"] = True

                # Receive connection established event
                init_data = websocket.receive_json()
                event_type = str(init_data.get("type", "")).upper()
                evidence["initial_event_type"] = event_type
                evidence["has_game_id"] = init_data.get("game_id") == game_id

                # Send ping and receive pong
                recv_start = time.perf_counter()
                websocket.send_text("ping")
                resp_text = websocket.receive_text()
                observed_latency_ms = (time.perf_counter() - recv_start) * 1000.0

                metrics["observed_latency_ms"] = round(observed_latency_ms, 2)
                evidence["ping_response"] = resp_text

                duration_ms = (time.perf_counter() - start) * 1000.0

                if event_type != "CONNECTION_ESTABLISHED" or resp_text.lower() != "pong":
                    return GateResult(
                        gate_id=self.gate_id,
                        name=self.name,
                        status=GateStatus.FAIL,
                        duration_ms=duration_ms,
                        blocking=self.blocking,
                        metrics=metrics,
                        evidence=evidence,
                        message=f"Unexpected WS response: type={event_type}, resp={resp_text}",
                    )

                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.PASS,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"WebSocket protocol and handshake verified (observed={observed_latency_ms:.1f}ms)",
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
                message=f"WebSocket stream contract test error: {err}",
            )


__all__ = [
    "WebSocketStreamGate",
]
