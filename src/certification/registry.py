"""Gate registry and dependency ordering for Certification Runner."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.certification.models import CertificationGate


class GateRegistry:
    """Registry maintaining certified gate definitions and execution dependencies."""

    def __init__(self) -> None:
        """Initialize empty gate registry."""
        self._gates: dict[str, CertificationGate] = {}
        self._order: list[str] = []

    def register(self, gate: CertificationGate) -> None:
        """Register a certification gate."""
        self._gates[gate.gate_id] = gate
        if gate.gate_id not in self._order:
            self._order.append(gate.gate_id)

    def get_gate(self, gate_id: str) -> CertificationGate | None:
        """Retrieve a registered gate by ID."""
        return self._gates.get(gate_id)

    def list_gates(self, filter_id: str | None = None) -> list[CertificationGate]:
        """Return registered gates in registration/dependency order, optionally filtered."""
        if filter_id:
            normalized = filter_id.lower().replace("-", "_")
            return [g for g in self._gates.values() if g.gate_id == normalized or filter_id in g.gate_id]

        return [self._gates[gid] for gid in self._order if gid in self._gates]

    @classmethod
    def create_default(cls) -> GateRegistry:
        """Construct the default registry loaded with core production and historical gates."""
        from src.certification.gates.api import ApiGatewayGate
        from src.certification.gates.data_integrity import DataIntegrityGate
        from src.certification.gates.historical import HistoricalCertificationGate
        from src.certification.gates.idempotency import UpsertIdempotencyGate
        from src.certification.gates.locks import SchedulerLocksGate
        from src.certification.gates.schema import SchemaMigrationGate
        from src.certification.gates.security_drift import SecurityDriftGate
        from src.certification.gates.transaction import TransactionAtomicityGate
        from src.certification.gates.vector_rag import VectorRagGate
        from src.certification.gates.websocket import WebSocketStreamGate

        registry = cls()
        registry.register(SchemaMigrationGate())
        registry.register(TransactionAtomicityGate())
        registry.register(UpsertIdempotencyGate())
        registry.register(VectorRagGate())
        registry.register(DataIntegrityGate())
        registry.register(ApiGatewayGate())
        registry.register(WebSocketStreamGate())
        registry.register(SchedulerLocksGate())
        registry.register(SecurityDriftGate())
        registry.register(HistoricalCertificationGate())
        return registry


__all__ = [
    "GateRegistry",
]
