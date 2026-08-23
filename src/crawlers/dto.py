"""Standard Data Transfer Objects (DTOs) for Crawler Execution and Data Extraction."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class CrawlRequest:
    """Represents a targeted crawl or fetch request."""

    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    retry_limit: int = 3
    priority: int = 10  # Lower number = higher priority

    def to_dict(self) -> dict[str, Any]:
        """Convert request to dictionary."""
        return asdict(self)


@dataclass
class CrawlResponse:
    """Represents the raw response from a crawl execution."""

    url: str
    status_code: int = 200
    text: str = ""
    elapsed_seconds: float = 0.0
    from_cache: bool = False
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert response to dictionary."""
        return {
            "url": self.url,
            "status_code": self.status_code,
            "text_length": len(self.text),
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "from_cache": self.from_cache,
            "headers": self.headers,
        }


@dataclass
class CrawlExecutionStats:
    """Aggregated execution metrics for a crawler run."""

    requests_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    retried_count: int = 0
    throttled_seconds: float = 0.0
    total_duration_seconds: float = 0.0

    def record_request(self, *, success: bool = True, retried: bool = False) -> None:
        """Record a single request event."""
        self.requests_count += 1
        if success:
            self.success_count += 1
        else:
            self.failed_count += 1
        if retried:
            self.retried_count += 1

    def record_throttle(self, seconds: float) -> None:
        """Record throttle wait duration."""
        self.throttled_seconds += seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert execution stats to dictionary."""
        return {
            "requests_count": self.requests_count,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "retried_count": self.retried_count,
            "throttled_seconds": round(self.throttled_seconds, 3),
            "total_duration_seconds": round(self.total_duration_seconds, 3),
        }


@dataclass
class ExtractorResult:
    """Represents structured records extracted from raw HTML/JSON."""

    records: list[dict[str, Any]] = field(default_factory=list)
    total_records: int = 0
    validation_errors: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Set total_records from records length if not explicitly provided."""
        if self.records and self.total_records == 0:
            self.total_records = len(self.records)

    def to_dict(self) -> dict[str, Any]:
        """Convert extractor result to dictionary."""
        return {
            "total_records": self.total_records,
            "validation_errors": self.validation_errors,
            "records": self.records,
        }
