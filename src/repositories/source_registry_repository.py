"""Repository for DataSource and RawSourceSnapshot operations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.crawl_evidence_repository import evidence_root
from src.utils.data_lineage import canonical_json, sha256_bytes

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


class DataSourceRepository:
    """DataSourceRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> DataSource:
        """Save save.

        Args:
            data: Data.
            data: Data.
            data: Data.

        Returns:
            DataSource instance.

        """
        source_key = data["source_key"]

        stmt = select(DataSource).where(DataSource.source_key == source_key)
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key != "source_key" and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = DataSource(**data)
        self.session.add(new_record)
        return new_record

    def get_by_key(self, source_key: str) -> DataSource | None:
        """Get by key.

        Args:
            source_key: Source Key.
            source_key: Source Key.
            source_key: Source Key.

        Returns:
            The result of the operation.

        """
        stmt = select(DataSource).where(DataSource.source_key == source_key)

        return self.session.execute(stmt).scalar_one_or_none()

    def get_by_domain(self, target_domain: str) -> list[DataSource]:
        """Get by domain.

        Args:
            target_domain: Target Domain.
            target_domain: Target Domain.
            target_domain: Target Domain.

        Returns:
            List of results.

        """
        stmt = select(DataSource).where(DataSource.target_domain == target_domain).order_by(DataSource.source_key)

        return list(self.session.execute(stmt).scalars().all())

    def get_active_by_domain(self, target_domain: str) -> list[DataSource]:
        """Get active by domain.

        Args:
            target_domain: Target Domain.
            target_domain: Target Domain.
            target_domain: Target Domain.

        Returns:
            List of results.

        """
        stmt = (
            select(DataSource)
            .where(DataSource.target_domain == target_domain, DataSource.is_active)
            .order_by(DataSource.source_key)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_all_active(self) -> list[DataSource]:
        """Get all active.

        Returns:
            List of results.

        """
        stmt = select(DataSource).where(DataSource.is_active).order_by(DataSource.source_key)

        return list(self.session.execute(stmt).scalars().all())

    def mark_success(self, source_key: str, content_hash: str) -> DataSource | None:
        """Handle the mark success operation.

        Args:
            source_key: Source Key.
            content_hash: Content Hash.
            source_key: Source Key.
            content_hash: Content Hash.
            source_key: Source Key.
            content_hash: Content Hash.

        Returns:
            The result of the operation.

        """
        now = datetime.now(UTC).replace(tzinfo=None)

        stmt = (
            update(DataSource)
            .where(DataSource.source_key == source_key)
            .values(last_success_at=now, last_content_hash=content_hash)
        )
        self.session.execute(stmt)
        return self.get_by_key(source_key)

    def get_stale_sources(self, max_hours: int = 48) -> list[DataSource]:
        """Get stale sources.

        Args:
            max_hours: Max Hours.
            max_hours: Max Hours.
            max_hours: Max Hours.

        Returns:
            List of results.

        """
        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=max_hours)

        stmt = select(DataSource).where(
            DataSource.is_active,
            DataSource.last_success_at < cutoff,
        )
        return list(self.session.execute(stmt).scalars().all())


class RawSourceSnapshotRepository:
    """RawSourceSnapshotRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> RawSourceSnapshot:
        """Save save.

        Args:
            data: Data.
            data: Data.
            data: Data.

        Returns:
            RawSourceSnapshot instance.

        """
        new_record = RawSourceSnapshot(**data)

        self.session.add(new_record)
        self.session.flush()
        return new_record

    def get_by_source_id(self, data_source_id: int, limit: int = 50) -> list[RawSourceSnapshot]:
        """Get by source id.

        Args:
            data_source_id: Data Source ID.
            limit: Limit.
            data_source_id: Data Source ID.
            limit: Limit.
            data_source_id: Data Source ID.
            limit: Limit.

        Returns:
            List of results.

        """
        stmt = (
            select(RawSourceSnapshot)
            .where(RawSourceSnapshot.data_source_id == data_source_id)
            .order_by(RawSourceSnapshot.fetched_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_hash(self, data_source_id: int, content_hash: str) -> RawSourceSnapshot | None:
        """Get by hash.

        Args:
            data_source_id: Data Source ID.
            content_hash: Content Hash.
            data_source_id: Data Source ID.
            content_hash: Content Hash.
            data_source_id: Data Source ID.
            content_hash: Content Hash.

        Returns:
            The result of the operation.

        """
        stmt = select(RawSourceSnapshot).where(
            RawSourceSnapshot.data_source_id == data_source_id,
            RawSourceSnapshot.content_hash == content_hash,
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_unparsed(self, limit: int = 100) -> list[RawSourceSnapshot]:
        """Get unparsed.

        Args:
            limit: Limit.
            limit: Limit.
            limit: Limit.

        Returns:
            List of results.

        """
        stmt = (
            select(RawSourceSnapshot)
            .where(RawSourceSnapshot.parse_status == "pending")
            .order_by(RawSourceSnapshot.fetched_at.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_failed_for_retry(self, retry_after_hours: int = 1, limit: int = 50) -> list[RawSourceSnapshot]:
        """Get failed for retry.

        Args:
            retry_after_hours: Retry After Hours.
            limit: Limit.
            retry_after_hours: Retry After Hours.
            limit: Limit.
            retry_after_hours: Retry After Hours.
            limit: Limit.

        Returns:
            List of results.

        """
        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=retry_after_hours)

        from sqlalchemy import and_

        stmt = (
            select(RawSourceSnapshot)
            .where(
                and_(
                    RawSourceSnapshot.parse_status == "failed",
                    RawSourceSnapshot.fetched_at < cutoff,
                ),
            )
            .order_by(RawSourceSnapshot.fetched_at.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_reprocess_pending(self, limit: int = 100) -> list[RawSourceSnapshot]:
        """Get reprocess pending.

        Args:
            limit: Limit.
            limit: Limit.
            limit: Limit.

        Returns:
            List of results.

        """
        stmt = (
            select(RawSourceSnapshot)
            .where(RawSourceSnapshot.reprocess_status == "pending")
            .order_by(RawSourceSnapshot.fetched_at.asc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def update_parse_status(
        self,
        snapshot_id: int,
        status: str,
        parser_version: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Update status.

        Args:
            snapshot_id: Snapshot ID.
            status: Status.
            parser_version: Parser Version.
            error_message: Error Message.
            snapshot_id: Snapshot ID.
            status: Status.
            parser_version: Parser Version.
            error_message: Error Message.
            snapshot_id: Snapshot ID.
            status: Status.
            parser_version: Parser Version.
            error_message: Error Message.

        """
        record = self.session.get(RawSourceSnapshot, snapshot_id)

        if record:
            record.parse_status = status
            if parser_version:
                record.parser_version = parser_version
            if error_message:
                record.error_message = error_message


def save_raw_snapshots(session: Session, raw_pages: list[dict]) -> int:
    """Save a list of raw page dicts as RawSourceSnapshot records.

        Returns count saved.

    Args:
        session: Session.
        raw_pages: Raw Pages.
        session: Session.
        raw_pages: Raw Pages.

    """
    snap_repo = RawSourceSnapshotRepository(session)
    ds_repo = DataSourceRepository(session)
    saved = 0
    for page in raw_pages:
        source_key = page.get("source_key")
        if not source_key:
            continue
        ds = ds_repo.get_by_key(source_key)
        if not ds:
            continue
        raw_body = page.get("html", page.get("body", ""))
        if isinstance(raw_body, (dict, list)):
            raw_content = canonical_json(raw_body).encode("utf-8")
        else:
            raw_content = raw_body if isinstance(raw_body, bytes) else str(raw_body).encode("utf-8")
        content_hash = sha256_bytes(raw_content)
        ds_repo.mark_success(source_key, content_hash)
        existing_snapshot = snap_repo.get_by_hash(ds.id, content_hash)
        if existing_snapshot is None:
            artifact_path = evidence_root() / "raw" / f"{content_hash}.bin"
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            if not artifact_path.exists():
                artifact_path.write_bytes(raw_content)
            snap_repo.save(
                {
                    "data_source_id": ds.id,
                    "raw_html_or_json_path": str(Path(artifact_path)),
                    "source_url": page.get("url"),
                    "content_type": page.get("content_type"),
                    "raw_size": len(raw_content),
                    "capture_metadata": page.get("capture_metadata"),
                    "content_hash": content_hash,
                    "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                    "status_code": page["status_code"],
                    "parse_status": page.get("parse_status", "pending"),
                    "parser_version": page.get("parser_version"),
                    "error_message": page.get("error_message"),
                },
            )
            saved += 1
        elif page.get("parse_status") or page.get("parser_version") or page.get("error_message"):
            snap_repo.update_parse_status(
                existing_snapshot.id,
                page.get("parse_status", existing_snapshot.parse_status),
                parser_version=page.get("parser_version"),
                error_message=page.get("error_message"),
            )
    return saved
