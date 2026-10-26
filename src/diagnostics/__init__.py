"""Unified System Diagnostics package."""

from __future__ import annotations

from src.diagnostics.dto import (
    AutoHealResult,
    DiagnosticSeverity,
    SubsystemCheckItem,
    SubsystemType,
    UnifiedDiagnosticsReport,
)
from src.diagnostics.engine import HEALABLE_SUBSYSTEMS, SystemDiagnosticsEngine

__all__ = [
    "HEALABLE_SUBSYSTEMS",
    "AutoHealResult",
    "DiagnosticSeverity",
    "SubsystemCheckItem",
    "SubsystemType",
    "SystemDiagnosticsEngine",
    "UnifiedDiagnosticsReport",
]
