"""KBO 전체 데이터를 청킹하여 Oracle AI Vector RAG 인덱스를 구축합니다.

Oracle source tables에서 데이터를 읽어 임베딩을 생성하고 동일한 Oracle
``rag_chunks`` 테이블의 native VECTOR column에 저장합니다.
챗봇이 KBO 관련 모든 질문에 답할 수 있도록 포괄적인 소스를 커버합니다.

지원 소스:
  players     — 선수 프로필 (player_basic)
  batting     — 타자 시즌 기록 (player_season_batting)
  pitching    — 투수 시즌 기록 (player_season_pitching)
  games       — 경기 결과 (game)
  standings   — 팀 순위 (team_standings_daily, 전체 날짜)
  events      — 구단 이벤트/뉴스 (team_events)
  movements   — 선수 이동/트레이드 (player_movements)
  awards      — KBO 시상 (awards)
  lineups     — 경기별 라인업 (game_lineups)
  rankings    — 시즌별 통계 순위 (stat_rankings)
  teams       — 구단 정보 (teams)
   team_history — 구단 변천사 (team_history)
   highlights  — 경기 하이라이트/명장면 (game_highlights)
   markdown_docs — 일반 로컬 Markdown 문서 (Docs/baseball)
   kbo_definitions — KBO 용어/지표 정의 Markdown 문서
   kbo_regulations — KBO 규정/규칙 Markdown 문서
   staging_rag_chunks — 기존 staging rag_chunks 본문 (재임베딩)
   all         — 위 소스 전체

사용법:
    python -m src.cli.build_rag_index --source all
    python -m src.cli.build_rag_index --source players --season 2025
    python -m src.cli.build_rag_index --source games --limit 500 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST, RAG_EMBEDDING_DIMENSION
from src.parsers.text_transformer import TextTransformer
from src.services.markdown_document_loader import load_local_markdown_docs, markdown_source_table
from src.services.rag_incremental_selection import (
    IncrementalDecisionOptions,
    RagIndexState,
    decide_incremental_candidate,
    embedding_fingerprint,
    index_state_from_projection,
)
from src.services.rag_index_identity import (
    PbpSourceIdentity,
    chunk_content_hash,
    current_index_version,
    stable_award_source_row_id,
    stable_highlight_source_row_id,
    stable_pbp_source_row_id,
    stable_player_movement_source_row_id,
    stable_team_history_source_row_id,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

from src.config.env_loader import load_project_env

logger = logging.getLogger(__name__)

load_project_env()

# 임베딩 배치 크기: API 호출 당 처리할 청크 수.
_BATCH_SIZE = max(1, int(os.getenv("RAG_EMBED_BATCH_SIZE", "50")))
# DB 커밋 간격
_COMMIT_EVERY = 100

_VALID_SOURCES = (
    "players",
    "batting",
    "pitching",
    "games",
    "standings",
    "events",
    "movements",
    "awards",
    "lineups",
    "rankings",
    "teams",
    "team_history",
    "highlights",
    "inning_scores",
    "pbp",
    "markdown_docs",
    "kbo_definitions",
    "kbo_regulations",
    "staging_rag_chunks",
    "all",
)

# 정규 시즌 리그 코드 (kbo_seasons.league_type_code 0 = 정규시즌)
_REGULAR_SEASON_CODE = 0
# 전 소스 공통 언어
_DEFAULT_LANGUAGE = "ko"
# 순위 청크당 상위 N위까지만 포함
_RANKING_TOP_N = 20
_PBP_SCAN_BATCH_SIZE = 5_000
# staging keyset 커서: 마지막으로 처리한 rag_chunks.id (재개용)
_STAGING_CURSOR_MAX_ID = [0]
_SEASON_ID_YEAR_THRESHOLD = 100_000
_SEASON_ID_YEAR_MULTIPLIER = 100
_PBP_KEYWORDS = (
    "홈런",
    "적시타",
    "안타",
    "삼진",
    "병살타",
    "도루",
    "결승타",
    "타점",
    "끝내기",
    "만루홈런",
    "역전타",
    "득점권",
)


def _regular_season_ids(season: int) -> Select:
    """Return all stored IDs for a regular season year."""
    from src.models.season import KboSeason

    return select(KboSeason.season_id).where(
        KboSeason.season_year == season,
        KboSeason.league_type_code == _REGULAR_SEASON_CODE,
    )


def _season_year_from_id(season_id: int | None) -> int | None:
    """Return the calendar year represented by a canonical season ID."""
    if season_id is None:
        return None
    return season_id // _SEASON_ID_YEAR_MULTIPLIER if season_id >= _SEASON_ID_YEAR_THRESHOLD else season_id


@dataclass(frozen=True, slots=True)
class BuildTargets:
    """Describe the source and index targets for one RAG build."""

    source_db: str
    sparse_index_db: str
    vector_db: str
    target_environment: str
    write_enabled: bool

    def display(self) -> dict[str, object]:
        """Return targets with credentials removed."""
        return {
            "source_db": _redact_url(self.source_db),
            "sparse_index_db": _redact_url(self.sparse_index_db),
            "vector_db": _redact_url(self.vector_db),
            "target_environment": self.target_environment,
            "write_enabled": self.write_enabled,
        }

    def urls(self) -> frozenset[str]:
        """Return every distinct database URL this build will open.

        A build can reach up to three stores, and they overlap: an Oracle sparse
        target doubles as the vector store, so ``sparse_index_db`` and
        ``vector_db`` can be the same URL. De-duplicating matters because the
        scheduler probes this set before taking a tier lock, and a repeated
        entry would mean paying for the same query twice.

        Read off the resolved targets rather than recomputed from the
        environment: the resolution has real precedence (``PGVECTOR_TEST_URL``
        beats ``PGVECTOR_URL``, an Oracle sparse target becomes the vector store)
        and a second implementation of those rules would eventually disagree with
        the build it is meant to describe.
        """
        return frozenset(url for url in (self.source_db, self.sparse_index_db, self.vector_db) if url)


@dataclass(frozen=True, slots=True)
class IncrementalSelectionOptions:
    """Describe target state used by content-aware candidate selection."""

    source_table: str | None = None
    vector_session: Session | None = None
    desired_index_version: str | None = None
    desired_embedding_fingerprint: str | None = None
    include_healthy: bool = False
    repair_legacy_fingerprint: bool = False
    vector_key_presence: bool = False
    content_presence: bool = False
    play_presence: bool = False


@dataclass(frozen=True, slots=True)
class BuildSourceOptions:
    """Describe source traversal and incremental filtering options."""

    season: int | None = None
    limit: int | None = None
    offset: int = 0
    skip_existing: bool = False
    desired_embedding_fingerprint: str | None = None
    repair_legacy_fingerprint: bool = False
    vector_key_presence: bool = False
    content_presence: bool = False
    play_presence: bool = False
    exclude_ids: frozenset[str] = frozenset()


def _redact_url(url: str | None) -> str:
    """Remove credentials from a database URL for logs and reports."""
    if not url:
        return ""
    try:
        parsed = urlsplit(url)
        if not parsed.scheme:
            return url
        host = parsed.hostname or ""
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
    except ValueError:
        return "<redacted-invalid-url>"


def _is_oracle_url(url: str | None) -> bool:
    """Return whether a database URL targets Oracle."""
    return bool(url and url.lower().startswith("oracle"))


def _provider_target_errors(
    embedding_mode: str,
    target_environment: str,
    target_url: str,
    vector_url: str,
) -> list[str]:
    """Return provider and vector target validation errors."""
    errors: list[str] = []
    if not vector_url and not target_url.startswith("oracle"):
        errors.append("PGVECTOR_URL or PGVECTOR_TEST_URL is required for PostgreSQL vector targets")
    if embedding_mode == "deterministic" and target_environment != "staging":
        errors.append("deterministic embedding requires RAG_TARGET_ENV=staging")
    if embedding_mode == "configured" and not os.getenv("OPENROUTER_API_KEY"):
        errors.append("configured embedding requires OPENROUTER_API_KEY")
    return errors


def _oracle_write_target_errors(
    sparse_url: str,
    vector_url: str,
    target_environment: str,
) -> list[str]:
    """Return Oracle single-store safety errors."""
    errors: list[str] = []
    if target_environment == "production" and sparse_url and not sparse_url.startswith("oracle"):
        errors.append("Oracle production builds must leave RAG_INDEX_DB_URL unset or target Oracle")
    if target_environment == "production" and vector_url and not vector_url.startswith("oracle"):
        errors.append("Oracle production builds must not use PGVECTOR_URL or PGVECTOR_TEST_URL")
    if not sparse_url and (os.getenv("PGVECTOR_TEST_URL") or os.getenv("PGVECTOR_URL")):
        errors.append("Oracle single-store builds must leave PGVECTOR_URL and PGVECTOR_TEST_URL unset")
    return errors


def _index_target_errors(source_url: str, target_url: str, sparse_url: str, vector_url: str) -> list[str]:
    """Return cross-database target separation and dialect errors."""
    errors: list[str] = []
    sparse_is_oracle = _is_oracle_url(sparse_url or target_url)
    if sparse_url and _redact_url(source_url) == _redact_url(sparse_url):
        errors.append("source and sparse index targets must be different")
    if sparse_url and vector_url and not sparse_is_oracle and _redact_url(sparse_url) == _redact_url(vector_url):
        errors.append("sparse index and vector targets must be different")
    if sparse_url and not sparse_is_oracle and not urlsplit(sparse_url).scheme.startswith("postgres"):
        errors.append("RAG_INDEX_DB_URL must target PostgreSQL for staging writes")
    return errors


def _sparse_vector_target_errors(sparse_url: str, target_url: str, vector_url: str) -> list[str]:
    """Return errors for the effective sparse/vector target shape."""
    effective_sparse_url = sparse_url or target_url
    if _is_oracle_url(effective_sparse_url):
        if vector_url:
            return ["Oracle sparse targets require an empty PGVECTOR_URL"]
        return []
    if not vector_url:
        return ["non-Oracle sparse targets require PGVECTOR_URL or PGVECTOR_TEST_URL"]
    if not vector_url.lower().startswith("postgres"):
        return ["non-Oracle sparse targets require a PostgreSQL vector URL"]
    return []


def _write_target_errors(
    source_url: str,
    target_url: str,
    sparse_url: str,
    vector_url: str,
    target_environment: str,
) -> list[str]:
    """Return write-target safety errors for a non-dry-run build.

    ``production`` used to be reserved for Oracle, because Oracle was the only
    production vector store when this was written. The vector store now lives in
    a pgvector database, so the environment and the engine are separate
    questions; what must not separate is the opt-in, which every production
    write still needs.

    A production build may leave ``RAG_INDEX_DB_URL`` unset, meaning the sparse
    index lives in the target database -- which is where it lives today, inside
    the operational database next to the rows it indexes. Staging keeps the
    stricter rule: a staging build must name its own sparse store, so an
    experiment cannot default into the operational one.
    """
    errors: list[str] = []
    is_oracle = _is_oracle_url(target_url)
    if not sparse_url and not is_oracle and target_environment != "production":
        errors.append("non-dry-run staging builds require an explicit RAG_INDEX_DB_URL")
    if target_environment not in {"staging", "production"}:
        errors.append("non-dry-run builds require RAG_TARGET_ENV=staging or production")
    if os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
        errors.append("non-dry-run builds require RAG_INDEX_ALLOW_WRITE=1")
    if target_environment == "production" and os.getenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE") != "1":
        errors.append("production builds require RAG_INDEX_ALLOW_PRODUCTION_WRITE=1")
    if is_oracle:
        errors.extend(_oracle_write_target_errors(sparse_url, vector_url, target_environment))
    errors.extend(_index_target_errors(source_url, target_url, sparse_url, vector_url))
    return errors


#: Environment variables that can point a RAG build at a database other than
#: ``DATABASE_URL``. A build resolves its source from
#: :data:`SOURCE_URL_ENV_VARS`, its sparse store from
#: :data:`RAG_INDEX_URL_ENV_VAR`, and its vector store from
#: :data:`VECTOR_URL_ENV_VARS` -- so a single-store deployment is not the only
#: shape, and anything asking "which databases will this touch?" has to resolve
#: rather than assume the default.
SOURCE_URL_ENV_VARS = ("RAG_SOURCE_DB_URL",)
RAG_INDEX_URL_ENV_VAR = "RAG_INDEX_DB_URL"
VECTOR_URL_ENV_VARS = ("PGVECTOR_TEST_URL", "PGVECTOR_URL")


def planned_rag_target_urls(*, embedding_mode: str = "oracle") -> frozenset[str]:
    """Return the databases a build would open, without validating the write guards.

    The scheduler asks this *before* taking a tier lock, so it needs the answer
    without the exceptions that a real build raises. Resolution happens through
    :func:`_resolve_build_targets` in dry-run mode for exactly that reason: the
    precedence between the vector variables is not something to restate here,
    because a second implementation is a second opinion that eventually disagrees
    with the build it claims to describe.

    A configuration the build would reject still reports the operational
    database rather than nothing. Failing closed here would skip the job and
    leave the operator with silence instead of the guard's own message. The one
    exception is a deployment with **no dense target at all**, which reports
    nothing: the build cannot start, so there is no database for the gate to
    protect, and naming one would only let the job take a tier lock to fail on a
    precondition it could have read before acquiring it.
    """
    # Imported here for the same reason `main` does it: `src.db.engine` builds
    # the operational engine at import time, and this module is imported by the
    # scheduler on every job tick purely to answer "which databases would a build
    # touch".
    from src.db.engine import DATABASE_URL

    resolved = _resolve_target_databases(DATABASE_URL)
    if not resolved.vector_db:
        # No dense target at all: neither Oracle, nor a sparse store to inherit
        # the dense column from, nor a vector URL. The build stops on this
        # precondition inside ``main``, so there is nothing for a scheduler to
        # open. Naming the operational database instead would let the job take a
        # tier lock only to fail on a check it could have read beforehand.
        return frozenset()

    try:
        targets = _resolve_build_targets(
            os.getenv(SOURCE_URL_ENV_VARS[0]) or DATABASE_URL,
            target_db_url=DATABASE_URL,
            embedding_mode=embedding_mode,
            dry_run=True,
        )
    except ValueError, TypeError:
        # A configuration the build will reject anyway. Reporting the
        # operational database keeps the gate honest -- the job still has to
        # read before its own guard rejects the write -- and reporting nothing
        # would let a broken deployment look like a reachable one.
        return frozenset({DATABASE_URL})

    # ``BuildTargets`` omits ``target_db_url`` when a separate sparse store is
    # configured, but the build opens the operational database regardless,
    # through the shared engine and the vector-backend checks in ``main``. The
    # union is what the gate needs: under-reporting a database the job touches
    # is exactly the hole this function exists to close.
    return targets.urls() | {DATABASE_URL}


@dataclass(frozen=True, slots=True)
class ResolvedTargetDatabases:
    """Where a build would read and write, before any guard runs.

    Split out of :func:`_resolve_build_targets` because the scheduler has to
    answer "does this deployment have a dense target at all?" without the
    write guards, and asking a question of a function whose job is to enforce
    them means either duplicating the precedence or triggering the guards.
    """

    sparse_index_db: str
    configured_vector_url: str
    vector_db: str


def _resolve_target_databases(target_db_url: str) -> ResolvedTargetDatabases:
    """Resolve the sparse and vector stores for a target database.

    The precedence between the vector environment variables is defined once, here,
    because a second implementation is a second opinion that eventually disagrees
    with the build it claims to describe.
    """
    configured_sparse_url = os.getenv(RAG_INDEX_URL_ENV_VAR, "")
    configured_vector_url = next((os.getenv(name, "") for name in VECTOR_URL_ENV_VARS if os.getenv(name)), "")
    if configured_sparse_url:
        vector_db = configured_sparse_url if _is_oracle_url(configured_sparse_url) else configured_vector_url
    elif _is_oracle_url(target_db_url):
        vector_db = target_db_url
    else:
        vector_db = configured_vector_url
    return ResolvedTargetDatabases(
        sparse_index_db=configured_sparse_url,
        configured_vector_url=configured_vector_url,
        vector_db=vector_db,
    )


def _resolve_build_targets(
    source_db_url: str,
    *,
    target_db_url: str | None = None,
    embedding_mode: str,
    dry_run: bool,
) -> BuildTargets:
    """Resolve and validate explicit source, sparse, and vector build targets."""
    target_db_url = target_db_url or source_db_url
    resolved = _resolve_target_databases(target_db_url)
    configured_sparse_url = resolved.sparse_index_db
    configured_vector_url = resolved.configured_vector_url
    vector_db = resolved.vector_db
    target_environment = os.getenv("RAG_TARGET_ENV", "").strip().lower()
    errors = _provider_target_errors(embedding_mode, target_environment, target_db_url, vector_db)
    if not dry_run:
        errors.extend(_sparse_vector_target_errors(configured_sparse_url, target_db_url, configured_vector_url))
        errors.extend(
            _write_target_errors(
                source_db_url,
                target_db_url,
                configured_sparse_url,
                vector_db,
                target_environment,
            )
        )
    if errors:
        details = "; ".join(errors)
        error_message = f"RAG target guard failed: {details}"
        raise ValueError(error_message)
    return BuildTargets(
        source_db=source_db_url,
        sparse_index_db=configured_sparse_url or target_db_url,
        vector_db=vector_db,
        target_environment=target_environment,
        write_enabled=not dry_run,
    )


def _embedding_service(mode: str, *, cache_enabled: bool = True) -> object:
    """Build the configured or offline deterministic embedding provider."""
    if mode == "deterministic":
        from src.services.rag_eval_corpus import DeterministicEmbeddingService

        return DeterministicEmbeddingService()
    if mode != "configured":
        err_mode = f"Unsupported embedding mode: {mode}"
        raise ValueError(err_mode)
    from src.services.embedding_service import EmbeddingService

    return EmbeddingService(cache_enabled=cache_enabled)


def _validate_embeddings(embeddings: list[list[float]], expected_count: int) -> None:
    """Reject incomplete or zero-vector batches before index persistence."""
    if len(embeddings) != expected_count:
        message = (
            f"Embedding provider returned {len(embeddings)} vectors for {expected_count} chunks; refusing to index."
        )
        raise RuntimeError(message)
    if any(not embedding or all(value == 0.0 for value in embedding) for embedding in embeddings):
        message = "Embedding provider returned an empty or zero vector; refusing to index."
        raise RuntimeError(message)


# ─── 청크 생성 함수 ────────────────────────────────────────────────────────────

# Values a source uses to say "I do not have this". They are truthy strings, so a
# plain `a or b` fallback never reaches the next candidate.
_SENTINEL_TEXT_VALUES = frozenset({"unknown", "none", "null", "nan", "undefined", "n/a"})

#: ``player_basic.status`` records crawl outcomes, not player facts. ``NOT_FOUND``
#: (1,615 rows), ``PSEUDO`` and ``Unknown/Runtime`` are how the crawler says it could
#: not find the record; rendering them states something the row does not support.
_SENTINEL_STATUS_VALUES = frozenset({"not_found", "pseudo", "unknown/runtime"})

#: ``bats``/``throws`` are single letters. The index is searched in Korean, and
#: ``타석: R`` is not reachable by a reader who would type ``우타``.
_BATS_LABELS = {"R": "우타", "L": "좌타", "S": "양타"}
_THROWS_LABELS = {"R": "우투", "L": "좌투"}


def _real_text(value: object | None) -> str | None:
    """Return the value only when it carries information rather than a placeholder.

    `game_lineups.standard_position` holds the literal string ``UNKNOWN`` for every
    2026 row (20,919 of them) instead of NULL, so `standard_position or position`
    never falls through. Rendering that sentinel as a position states a fact the
    row does not support, which is how ``(UNKNOWN)`` reached the index.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in _SENTINEL_TEXT_VALUES:
        return None
    return text


def _player_label(player: object | None, player_id: object | None) -> str:
    """Name for a chunk title, falling back to the id when the name is unusable."""
    name = _real_text(getattr(player, "name", None))
    if name is not None and name.lower().startswith("unknown"):
        # ``player_basic`` stores ``Unknown 74164`` when the crawl matched nothing
        # but a placeholder. The name is the lookup key, so a filler that merely
        # starts with "Unknown" is as useless as the bare sentinel.
        name = None
    return name or f"ID:{player_id}"


def _team_label_map(session: Session) -> dict[str, str]:
    """Map ``team_id`` to the short Korean name used in index text.

    ``player_basic.team`` carries codes (``SS``, ``HT``, ``KIA``), but the index is
    searched in Korean, so a chunk reading ``팀: SS`` is not reachable by someone
    typing ``삼성``.
    """
    from src.models.team import Team

    return {team.team_id: (team.team_short_name or team.team_name) for team in session.query(Team).all()}


def _team_name_map(session: Session) -> dict[str, str]:
    """Map ``team_id`` to the full Korean name a line score is written with.

    Line scores read ``롯데 자이언츠 9 - 9 kt wiz``. The short name the player chunks
    use (``롯데``) would rewrite every line score the index already holds, so the two
    conventions stay apart: player rows are labelled for the person, line scores for
    the club as it appears in a scoreboard.

    The name is the club's current one, taken from the code the ``game`` row carries
    (``SSG``, ``KH``), not the name the club played under that season (``SK``,
    ``넥센``). A 2018 line score therefore reads ``키움 히어로즈``. Era names stay
    reachable because the search path resolves them to the same code — ``extract_kbo_entities``
    maps ``넥센`` and ``와이번스`` to ``KH`` and ``SSG`` — while the reverse is not true:
    naming per season would tie every historical chunk to ``team_history`` rows whose
    ranges overlap, so a later franchise edit would rewrite text the index has embedded.
    """
    from src.models.team import Team

    return {team.team_id: (team.team_name or team.team_short_name) for team in session.query(Team).all()}


def _colliding_player_labels(session: Session, team_labels: dict[str, str]) -> set[tuple[str, str]]:
    """``(name, club)`` pairs produced by more than one ``player_basic`` row.

    A shared name alone is not ambiguity: ``김태훈 (삼성)`` and ``김태훈 (두산)`` are
    already distinct. Suffixing every same-name row would put an id on 4,595 titles
    for no gain, when only 1,976 rows in 784 pairs still collide once the club is
    part of the label.
    """
    rows = session.execute(text("SELECT name, team FROM player_basic WHERE name IS NOT NULL")).fetchall()
    counts = Counter((name, team_labels.get(team or "") or _real_text(team) or "") for name, team in rows)
    return {key for key, count in counts.items() if count > 1}


def _iter_player_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_basic 테이블에서 선수 프로파일 청크를 생성합니다."""
    from src.models.player import PlayerBasic

    team_labels = _team_label_map(session)
    colliding_labels = _colliding_player_labels(session, team_labels)

    query = session.query(PlayerBasic).filter(PlayerBasic.name.is_not(None)).order_by(PlayerBasic.player_id)
    if limit:
        query = query.limit(limit)

    for player in query.yield_per(200):
        team_label = team_labels.get(player.team or "") or _real_text(player.team)

        label = _player_label(player, player.player_id)
        if (player.name, team_label or "") in colliding_labels:
            # Two players sharing a name *and* a club would otherwise produce the
            # same title; the id is the only thing that separates them.
            label = f"{label} #{player.player_id}"
        status = _real_text(player.status)
        if status is not None and status.lower() in _SENTINEL_STATUS_VALUES:
            status = None

        parts = [f"선수: {label}"]
        details = [
            ("팀", team_label),
            ("포지션", _real_text(player.position)),
            ("생년월일", _real_text(player.birth_date)),
            ("신장", f"{player.height_cm}cm" if player.height_cm else None),
            ("체중", f"{player.weight_kg}kg" if player.weight_kg else None),
            ("타석", _BATS_LABELS.get(player.bats or "") or _real_text(player.bats)),
            ("투구", _THROWS_LABELS.get(player.throws or "") or _real_text(player.throws)),
            ("데뷔", f"{player.debut_year}년" if player.debut_year else None),
            ("출신교", _real_text(player.career)),
            ("상태", status),
            ("드래프트", _real_text(player.draft_info)),
            ("연봉", _real_text(player.salary_original)),
        ]
        parts.extend(f"{field_name}: {value}" for field_name, value in details if value)
        if len(parts) == 1:
            # 1,062 players carry no attribute at all -- not team, position, birth
            # date, physicals, career, draft or salary. The chunk would say nothing
            # but a name: a weak match for every player question and a poor answer
            # to any of them. The row stays in player_basic.
            continue

        content = ", ".join(parts)
        yield {
            "source_table": "player_basic",
            "source_row_id": str(player.player_id),
            # ``무소속`` used to be rendered for the 2,119 players whose team is
            # unknown, which states a fact the row does not support.
            "title": f"{label} ({team_label})" if team_label else label,
            "content": content,
            "team_id": player.team,
            "player_id": str(player.player_id),
            "season_year": None,
            "document_type": "player_profile",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": "ko",
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"player_id": player.player_id, "position": player.position, "status": player.status},
        }


def _iter_batting_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_season_batting에서 타자 시즌 기록 청크를 생성합니다."""
    from src.models.player import PlayerBasic, PlayerSeasonBatting

    query = session.query(PlayerSeasonBatting, PlayerBasic).join(
        PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id, isouter=True
    )
    if season:
        query = query.filter(PlayerSeasonBatting.season == season)
    query = query.filter(PlayerSeasonBatting.level == "KBO1")
    if limit:
        query = query.limit(limit)

    for batting, player in query.yield_per(200):
        name = _player_label(player, batting.player_id)

        def _fmt(val: float | None, decimals: int = 3) -> str:
            return f"{val:.{decimals}f}" if val is not None else "-"

        parts = [
            f"선수: {name}",
            f"시즌: {batting.season}년",
            f"팀: {batting.team_code or '-'}",
            f"경기: {batting.games or 0}",
            f"타율: {_fmt(batting.avg)}",
            f"출루율: {_fmt(batting.obp)}",
            f"장타율: {_fmt(batting.slg)}",
            f"OPS: {_fmt(batting.ops)}",
            f"안타: {batting.hits or 0}",
            f"홈런: {batting.home_runs or 0}",
            f"타점: {batting.rbi or 0}",
            f"득점: {batting.runs or 0}",
            f"도루: {batting.stolen_bases or 0}",
            f"삼진: {batting.strikeouts or 0}",
            f"볼넷: {batting.walks or 0}",
        ]
        content = f"{name} {batting.season}시즌 타격기록: " + ", ".join(parts)
        yield {
            "source_table": "player_season_batting",
            "source_row_id": (
                f"{batting.player_id}_{batting.season}_{batting.team_code or 'NA'}_{batting.league or 'UNKNOWN'}"
            ),
            "title": f"{name} {batting.season}시즌 타격기록",
            "content": content,
            "team_id": batting.team_code,
            "player_id": str(batting.player_id),
            "season_year": batting.season,
            "document_type": "season_batting",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "avg": batting.avg,
                "hr": batting.home_runs,
                "rbi": batting.rbi,
                "ops": batting.ops,
            },
        }


def _iter_pitching_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_season_pitching에서 투수 시즌 기록 청크를 생성합니다."""
    from src.models.player import PlayerBasic, PlayerSeasonPitching

    query = session.query(PlayerSeasonPitching, PlayerBasic).join(
        PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id, isouter=True
    )
    if season:
        query = query.filter(PlayerSeasonPitching.season == season)
    query = query.filter(PlayerSeasonPitching.level == "KBO1")
    if limit:
        query = query.limit(limit)

    for pitching, player in query.yield_per(200):
        name = _player_label(player, pitching.player_id)

        def _fmt(val: float | None, decimals: int = 2) -> str:
            return f"{val:.{decimals}f}" if val is not None else "-"

        parts = [
            f"선수: {name}",
            f"시즌: {pitching.season}년",
            f"팀: {pitching.team_code or '-'}",
            f"경기: {pitching.games or 0}",
            f"선발: {pitching.games_started or 0}",
            f"승: {pitching.wins or 0}",
            f"패: {pitching.losses or 0}",
            f"세이브: {pitching.saves or 0}",
            f"홀드: {pitching.holds or 0}",
            f"ERA: {_fmt(pitching.era)}",
            f"이닝: {_fmt(pitching.innings_pitched)}",
            f"WHIP: {_fmt(pitching.whip)}",
            f"삼진: {pitching.strikeouts or 0}",
            f"볼넷: {pitching.walks_allowed or 0}",
        ]
        content = f"{name} {pitching.season}시즌 투구기록: " + ", ".join(parts)
        yield {
            "source_table": "player_season_pitching",
            "source_row_id": (
                f"{pitching.player_id}_{pitching.season}_{pitching.team_code or 'NA'}_{pitching.league or 'UNKNOWN'}"
            ),
            "title": f"{name} {pitching.season}시즌 투구기록",
            "content": content,
            "team_id": pitching.team_code,
            "player_id": str(pitching.player_id),
            "season_year": pitching.season,
            "document_type": "season_pitching",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "era": pitching.era,
                "wins": pitching.wins,
                "losses": pitching.losses,
                "saves": pitching.saves,
            },
        }


def _iter_game_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Game 테이블에서 경기 결과 청크를 생성합니다."""
    from src.models.game import Game
    from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

    query = session.query(Game).filter(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
    if season:
        query = query.filter(Game.season_id.in_(_regular_season_ids(season)))
    query = query.order_by(Game.game_date.desc())
    if limit:
        query = query.limit(limit)

    for game in query.yield_per(500):
        date_str = str(game.game_date) if game.game_date else "날짜불명"
        home = game.home_team or "?"
        away = game.away_team or "?"
        home_score = game.home_score if game.home_score is not None else "?"
        away_score = game.away_score if game.away_score is not None else "?"
        winner = game.winning_team or "무승부"
        stadium = game.stadium or "-"

        content = f"{date_str} KBO 경기: {away} {away_score} - {home} {home_score} (구장: {stadium}, 결과: {winner} 승)"
        yield {
            "source_table": "game",
            "source_row_id": game.game_id,
            "title": f"{date_str} {away} vs {home}",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": (
                game.game_date.year if game.game_date else _season_year_from_id(cast("int | None", game.season_id))
            ),
            "document_type": "game_result",
            "game_date": game.game_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "home_team": home,
                "away_team": away,
                "home_score": game.home_score,
                "away_score": game.away_score,
                "winner": winner,
                "stadium": stadium,
            },
        }


#: PBP 문구로 조기 종료를 알리는 낱말. 이닝 수를 도메인보다 짧게 확정할 수 있는 근거다.
#: ``중단``은 넣지 않는다 — 우천으로 멈췄다가 이어서 9회까지 마친 경기 265건에 붙어 있어,
#: 그것을 조기 종료로 읽으면 그 경기들의 이닝을 도리어 깎는다.
_CURTAILED_PBP_KEYWORDS = ("콜드", "순연", "서스펜디드")
#: 9회 경기는 통상 170분 이상 걸린다. 130분 이하는 조기 종료로 본다.
_SHORT_GAME_MINUTES = 130
#: 무승부가 아니면 경기가 반드시 치르는 이닝 수.
_REGULATION_INNINGS = 9


def _decided_inning(by_inning: dict[int, dict[str, int]]) -> int | None:
    """Return the inning a game was decided in, or ``None`` when it ended tied.

    A game cannot end before the ninth, and the home team does not bat once it is
    already ahead in the top half. Both facts are needed: without the first, the
    earliest lead would end the game; without the second, a 3-2 game would look
    undecided only because the bottom of the ninth was never recorded.
    """
    away = home = 0
    for inning in range(1, max(by_inning, default=0) + 1):
        halves = by_inning.get(inning, {})
        away += halves.get("away", 0)
        if inning >= _REGULATION_INNINGS and home > away:
            return inning
        home += halves.get("home", 0)
        if inning >= _REGULATION_INNINGS and away != home:
            return inning
    return None


def _last_scoring_inning(by_inning: dict[int, dict[str, int]]) -> int:
    """Return the last inning either side scored in, and at least one inning.

    Every inning up to a run that was actually scored was played, so this is a floor
    the index can stand on when the game's length is unknown.
    """
    scored = [inning for inning, halves in by_inning.items() if sum(halves.values()) > 0]
    return max(scored) if scored else 1


def _played_innings(
    by_inning: dict[int, dict[str, int]],
    *,
    domain_max: int,
    pbp_max: int,
    game_minutes: int | None,
    curtailed: bool,
) -> tuple[int, str]:
    """Return how many innings were played, with the evidence behind that number.

    ``game_inning_scores`` fills every game out to 12 innings in 2002~2019 and
    2024~2025 — 0-0 rows for innings nobody played — and stops at the real inning in
    2021~2023, so the table alone cannot say how far a game went. Believing it either
    way has already put false text in the index: ``20240630SSKT1`` was called after
    nine innings and its chunk read ``연장 3회``. Two things settle it instead. The
    score ends a game exactly when the game ends, which trims the filling; and a
    curtailment caption or a running time no full game fits in says play stopped early,
    which trims the innings nobody played at all.
    """
    if curtailed or (game_minutes is not None and game_minutes <= _SHORT_GAME_MINUTES):
        if pbp_max:
            return min(pbp_max, domain_max), "curtailed"
        # 조기 종료는 확실하지만 몇 회까지 치렀는지 기록이 없다. 득점이 확인된
        # 마지막 이닝까지만 적으면 적힌 모든 줄이 실제로 치른 이닝이 된다.
        return _last_scoring_inning(by_inning), "curtailed_unknown"
    decided = _decided_inning(by_inning)
    if decided:
        return decided, "decided"
    if pbp_max >= _REGULATION_INNINGS:
        return pbp_max, "pbp"
    return domain_max, "domain"


def _line_score_notes(innings: int, away_score: int | None, home_score: int | None, evidence: str) -> str:
    """Add only the notes the innings count and the score support.

    A tie is a fact of the score, not of ``game_status``: ``20210529NCLT1`` is stored
    ``COMPLETED`` at 10-10 and would otherwise read like a win.
    """
    notes: list[str] = []
    if innings > _REGULATION_INNINGS:
        notes.append(f"연장 {innings - _REGULATION_INNINGS}회")
    if away_score is not None and home_score is not None and away_score == home_score:
        notes.append("무승부")
    elif evidence == "curtailed_unknown":
        notes.append("조기 종료")
    elif innings < _REGULATION_INNINGS:
        notes.append(f"{innings}회 종료")
    return f" ({', '.join(notes)})" if notes else ""


def _pbp_inning_evidence(session: Session) -> tuple[dict[str, int], set[str]]:
    """Collect the two play-by-play facts an innings count may lean on.

    ``max(inning)`` also carries the inning header of a game that never got there —
    the closing marker and the winning pitcher are filed under the next inning number —
    so it is only used downwards, where the true last inning cannot be later than the
    last one recorded.
    """
    from sqlalchemy import func, or_

    from src.models.game import GamePlayByPlay

    maxima = {
        game_id: int(inning_max)
        for game_id, inning_max in session.query(GamePlayByPlay.game_id, func.max(GamePlayByPlay.inning)).group_by(
            GamePlayByPlay.game_id
        )
        if inning_max
    }
    curtailed = {
        game_id
        for (game_id,) in session.query(GamePlayByPlay.game_id)
        .filter(or_(*[GamePlayByPlay.play_description.like(f"%{keyword}%") for keyword in _CURTAILED_PBP_KEYWORDS]))
        .distinct()
    }
    return maxima, curtailed


def _game_minutes_by_id(session: Session) -> dict[str, int]:
    """Map ``game_id`` to its recorded running time, where one was recorded."""
    from src.models.game import GameMetadata

    return {
        game_id: int(minutes)
        for game_id, minutes in session.query(GameMetadata.game_id, GameMetadata.game_time_minutes)
        if minutes is not None
    }


def _iter_inning_score_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_inning_scores에서 경기 라인스코어 청크를 생성합니다 (경기당 1청크)."""
    from itertools import groupby

    from sqlalchemy import or_

    from src.models.game import Game, GameInningScore

    query = (
        session.query(GameInningScore, Game)
        .join(Game, GameInningScore.game_id == Game.game_id)
        .filter(or_(Game.game_status.is_(None), Game.game_status.notin_(("CANCELLED", "POSTPONED"))))
        .order_by(
            Game.game_date,
            Game.game_id,
            GameInningScore.inning,
            GameInningScore.team_side,
        )
    )
    if season:
        query = query.filter(Game.season_id.in_(_regular_season_ids(season)))

    pbp_maxima, curtailed_games = _pbp_inning_evidence(session)
    minutes_by_game = _game_minutes_by_id(session)
    team_names = _team_name_map(session)

    for count, (game_id, group) in enumerate(
        groupby(query.yield_per(5000), key=lambda row: row[0].game_id),
        start=1,
    ):
        if limit and count > limit:
            break
        by_inning: dict[int, dict[str, int]] = {}
        game = None
        for inning_row, game_row in group:
            game = game_row
            by_inning.setdefault(inning_row.inning, {})[inning_row.team_side] = inning_row.runs or 0
        if game is None or not by_inning:
            continue

        innings, evidence = _played_innings(
            by_inning,
            domain_max=max(by_inning),
            pbp_max=pbp_maxima.get(game_id, 0),
            game_minutes=minutes_by_game.get(game_id),
            curtailed=game_id in curtailed_games,
        )
        away = team_names.get(game.away_team or "", game.away_team or "?")
        home = team_names.get(game.home_team or "", game.home_team or "?")
        date_str = str(game.game_date) if game.game_date else "날짜불명"
        away_score = game.away_score if game.away_score is not None else "?"
        home_score = game.home_score if game.home_score is not None else "?"
        notes = _line_score_notes(innings, game.away_score, game.home_score, evidence)
        inning_runs = [
            (number, by_inning.get(number, {}).get("away", 0), by_inning.get(number, {}).get("home", 0))
            for number in range(1, innings + 1)
        ]
        content = "\n".join(
            [
                "경기 라인스코어",
                f"경기: {game_id}",
                f"경기일: {date_str}",
                f"{away} {away_score} - {home_score} {home}{notes}",
                "이닝별: " + ", ".join(f"{number}회 {a}-{h}" for number, a, h in inning_runs),
            ]
        )
        yield {
            "source_table": "game_inning_scores",
            "source_row_id": f"{game_id}_line",
            "title": f"[{date_str}] {away} vs {home} 라인스코어",
            "content": content,
            "team_id": game.home_team,
            "player_id": None,
            "season_year": (
                game.game_date.year if game.game_date else _season_year_from_id(cast("int | None", game.season_id))
            ),
            "document_type": "game_line_score",
            "game_date": game.game_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "kind": "game_line_score",
                "game_id": game_id,
                "innings_played": innings,
                "innings_evidence": evidence,
                "innings": [
                    {"inning": number, "away_runs": runs_away, "home_runs": runs_home}
                    for number, runs_away, runs_home in inning_runs
                ],
            },
        }


def _iter_standings_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_standings_daily에서 전체 날짜의 팀 순위 청크를 생성합니다."""
    from sqlalchemy import extract, func

    from src.models.standings import TeamStandingsDaily

    sub = session.query(func.max(TeamStandingsDaily.standings_date)).scalar()
    if not sub:
        return

    query = session.query(TeamStandingsDaily)
    if season:
        query = query.filter(extract("year", TeamStandingsDaily.standings_date) == season)
    query = query.order_by(TeamStandingsDaily.standings_date.desc(), TeamStandingsDaily.rank)
    if limit:
        query = query.limit(limit)

    for row in query.all():
        if row.current_streak > 0:
            streak_str = f"{row.current_streak}연승"
        elif row.current_streak < 0:
            streak_str = f"{abs(row.current_streak)}연패"
        else:
            streak_str = "연속없음"
        content = (
            f"{row.standings_date} 기준 {row.team_code} 팀: "
            f"{row.rank}위, {row.games_played}경기 {row.wins}승 {row.losses}패 {row.draws}무, "
            f"승률 {row.win_pct:.3f}, 게차 {row.games_behind}, 현재 {streak_str}, "
            f"득점 {row.runs_scored} 실점 {row.runs_allowed}"
        )
        yield {
            "source_table": "team_standings_daily",
            "source_row_id": f"{row.standings_date}_{row.team_code}",
            "title": f"{row.standings_date} {row.team_code} 순위",
            "content": content,
            "team_id": row.team_code,
            "player_id": None,
            "season_year": row.standings_date.year if row.standings_date else None,
            "document_type": "standings",
            "game_date": row.standings_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "rank": row.rank,
                "wins": row.wins,
                "losses": row.losses,
                "win_pct": row.win_pct,
                "games_behind": row.games_behind,
            },
        }


def _iter_event_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_events에서 구단 이벤트/뉴스 청크를 생성합니다."""
    from src.models.team_event import TeamEvent

    query = session.query(TeamEvent).filter(TeamEvent.title.is_not(None))
    if limit:
        query = query.limit(limit)

    for event in query.yield_per(200):
        parts = [f"이벤트: {event.title}"]
        if event.event_type:
            parts.append(f"유형: {event.event_type}")
        if event.team_id:
            parts.append(f"팀: {event.team_id}")
        if event.description:
            parts.append(f"내용: {event.description[:200]}")
        if event.benefit_text:
            parts.append(f"혜택: {event.benefit_text[:100]}")
        if event.target_audience:
            parts.append(f"대상: {event.target_audience}")
        if event.event_start_at:
            parts.append(f"시작일: {event.event_start_at.strftime('%Y-%m-%d')}")
        if event.status:
            parts.append(f"상태: {event.status}")

        content = " | ".join(parts)
        yield {
            "source_table": "team_events",
            "source_row_id": str(event.id),
            "title": event.title,
            "content": content,
            "team_id": event.team_id,
            "player_id": None,
            "season_year": event.event_start_at.year if event.event_start_at else None,
            "document_type": "team_event",
            "game_date": event.event_start_at.date() if event.event_start_at else None,
            "published_at": event.published_at,
            "source_url": event.source_url,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"event_type": event.event_type, "status": event.status},
        }


def _iter_movement_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """player_movements에서 선수 이동/트레이드 청크를 생성합니다."""
    from src.models.player import PlayerMovement

    query = session.query(PlayerMovement)
    if season:
        from sqlalchemy import extract

        query = query.filter(extract("year", PlayerMovement.movement_date) == season)
    query = query.order_by(PlayerMovement.movement_date.desc())
    if limit:
        query = query.limit(limit)

    for mv in query.yield_per(200):
        date_str = str(mv.movement_date) if mv.movement_date else "날짜불명"
        content = f"{date_str}: {mv.player_name} 선수 {mv.section} (팀: {mv.team_code})"
        if mv.remarks:
            content += f" — {mv.remarks[:150]}"

        yield {
            "source_table": "player_movements",
            "source_row_id": stable_player_movement_source_row_id(
                mv.movement_date,
                mv.team_code,
                mv.player_name,
                mv.section,
            ),
            "title": f"{mv.player_name} {mv.section}",
            "content": content,
            "team_id": mv.canonical_team_id or mv.team_code,
            "player_id": str(mv.player_basic_id) if mv.player_basic_id else None,
            "season_year": mv.movement_date.year if mv.movement_date else None,
            "document_type": "player_movement",
            "game_date": mv.movement_date,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"section": mv.section, "team_code": mv.team_code},
        }


def _iter_lineup_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_lineups에서 경기 라인업 청크를 생성합니다 (게임+팀별 1청크)."""
    from itertools import groupby

    from src.models.game import Game, GameLineup

    query = (
        session.query(GameLineup, Game)
        .join(Game, GameLineup.game_id == Game.game_id)
        .order_by(
            Game.game_date,
            GameLineup.game_id,
            GameLineup.team_side,
            GameLineup.batting_order,
            GameLineup.appearance_seq,
        )
    )
    if season:
        query = query.filter(Game.season_id.in_(_regular_season_ids(season)))

    for count, (key, group) in enumerate(
        groupby(query.yield_per(2000), key=lambda row: (row[0].game_id, row[0].team_side)),
        start=1,
    ):
        game_id, team_side = key
        lines: list[str] = []
        game_date = None
        season_id = None
        team_code = None
        for lineup, game in group:
            game_date = game.game_date
            season_id = game.season_id
            team_code = lineup.team_code or game.home_team if lineup.team_side == "home" else game.away_team
            pos = _real_text(lineup.standard_position) or _real_text(lineup.position) or ""
            # A missing batting order is a source-data gap, not a substitution. Rendering it as
            # "None번" and tagging it "[교체]" states two facts the row does not support: every
            # one of the 2,278 such rows (2,201 of them in 2001) carries neither a slot nor a
            # starter flag, so the index has never seen a real substitute through this branch.
            slot = f"{lineup.batting_order}번 " if lineup.batting_order is not None else ""
            substitute = " [교체]" if lineup.is_starter is False and lineup.batting_order is not None else ""
            lines.append(
                f"{slot}{lineup.player_name} ({pos}){substitute}" if pos else f"{slot}{lineup.player_name}{substitute}"
            )
        content = (
            f"경기일: {game_date}, 경기 ID: {game_id}, "
            f"{'홈' if team_side == 'home' else '원정'}팀 {team_code} 라인업: " + ", ".join(lines)
        )
        yield {
            "source_table": "game_lineups",
            "source_row_id": f"{game_id}_{team_side}",
            "title": f"[{game_date}] {team_code} 라인업 ({game_id})",
            "content": content,
            "team_id": team_code,
            "player_id": None,
            "season_year": game_date.year if game_date else _season_year_from_id(season_id),
            "document_type": "lineup",
            "game_date": str(game_date) if game_date else None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"game_id": game_id, "team_side": team_side, "team_code": team_code},
        }
        if limit and count >= limit:
            break


def _iter_rankings_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """stat_rankings에서 시즌별/지표별 상위 순위 청크를 생성합니다."""
    from itertools import groupby

    from src.models.rankings import StatRanking

    query = session.query(StatRanking)
    if season:
        query = query.filter(StatRanking.season == season)
    query = query.order_by(
        StatRanking.season.desc(),
        StatRanking.metric,
        StatRanking.rank.asc(),
        StatRanking.entity_label,
        StatRanking.entity_id,
        StatRanking.team_id,
    )
    if limit:
        query = query.limit(limit)

    for count, (r_season, metric, group) in enumerate(
        (
            (r_season, metric, tuple(rows))
            for (r_season, metric), rows in groupby(query.yield_per(1000), key=lambda r: (r.season, r.metric))
        ),
        start=1,
    ):
        top = [
            f"{row.rank}위 {row.entity_label}" + (f" ({row.team_id})" if row.team_id else "") + f" {row.value}"
            for row in group
            if row.rank <= _RANKING_TOP_N
        ]
        if not top:
            continue
        content = f"{r_season}시즌 KBO {metric} 순위: " + ", ".join(top)
        yield {
            "source_table": "stat_rankings",
            "source_row_id": f"{r_season}_{metric}",
            "title": f"{r_season}시즌 {metric} 순위",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": r_season,
            "document_type": "ranking",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {"metric": metric, "season": r_season},
        }
        if limit and count >= limit:
            break


def _clean_aliases(aliases: object) -> list[str] | None:
    """teams.aliases는 시드 이슈로 깨진 JSON('["{", "}"]')일 수 있어 실제 별칭만 추출합니다."""
    if isinstance(aliases, str):
        try:
            import json

            parsed = json.loads(aliases)
        except ValueError, TypeError:
            return None
    else:
        parsed = aliases
    if not isinstance(parsed, list):
        return None
    clean = [a for a in parsed if isinstance(a, str) and len(a) > 1 and a not in ("{", "}", ",")]
    return clean or None


def _iter_team_chunks(session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """teams에서 구단 정보 청크를 생성합니다."""
    from src.models.team import Team

    query = session.query(Team)
    if limit:
        query = query.limit(limit)

    for team in query.yield_per(100):
        aliases = _clean_aliases(team.aliases)
        aliases_str = f" (별칭: {', '.join(aliases)})" if aliases else ""
        status_str = "" if team.is_active else " (해체/휴면 구단)"
        content = (
            f"구단: {team.team_name} ({team.team_short_name}){status_str} — "
            f"연고: {team.city}, 창단: {team.founded_year}년"
            + (f", 홈구장: {team.stadium_name}" if team.stadium_name else "")
            + aliases_str
        )
        yield {
            "source_table": "teams",
            "source_row_id": team.team_id,
            "title": f"구단 정보: {team.team_name}",
            "content": content,
            "team_id": team.team_id,
            "player_id": None,
            "season_year": None,
            "document_type": "team_info",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "team_id": team.team_id,
                "team_name": team.team_name,
                "city": team.city,
                "is_active": team.is_active,
            },
        }


def _iter_team_history_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """team_history에서 구단 변천사 청크를 생성합니다 (연도별 1청크)."""
    from src.models.team_history import TeamHistory

    query = session.query(TeamHistory)
    if season:
        query = query.filter(TeamHistory.season == season)
    query = query.order_by(TeamHistory.season.desc())
    if limit:
        query = query.limit(limit)

    for row in query.yield_per(200):
        ranking_str = f"{row.ranking}위" if row.ranking else "순위불명"
        content = (
            f"{row.season}년 {row.team_name} ({row.team_code}): {ranking_str}, "
            f"구장: {row.stadium or '불명'}, 도시: {row.city or '불명'}"
        )
        yield {
            "source_table": "team_history",
            "source_row_id": stable_team_history_source_row_id(row.season, row.team_code),
            "title": f"{row.season} {row.team_name}",
            "content": content,
            "team_id": row.team_code,
            "player_id": None,
            "season_year": row.season,
            "document_type": "team_history",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "franchise_id": row.franchise_id,
                "team_name": row.team_name,
                "ranking": row.ranking,
            },
        }


def _iter_highlight_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_highlights에서 경기 명장면 청크를 생성합니다."""
    from src.models.game import Game, GameHighlight

    query = (
        session.query(GameHighlight, Game)
        .join(Game, GameHighlight.game_id == Game.game_id)
        .order_by(Game.game_date.desc(), GameHighlight.wpa.desc())
    )
    if season:
        query = query.filter(Game.season_id.in_(_regular_season_ids(season)))
    if limit:
        query = query.limit(limit)

    for highlight, game in query.yield_per(200):
        tags_str = f" [태그: {', '.join(highlight.tags)}]" if highlight.tags else ""
        wpa_str = f" (WPA {highlight.wpa:+.3f})" if highlight.wpa is not None else ""
        content = (
            f"{game.game_date} {game.away_team} vs {game.home_team} "
            f"{highlight.inning}회{highlight.inning_half or ''} "
            f"{highlight.highlight_type}: {highlight.description}{tags_str}{wpa_str}"
        )
        yield {
            "source_table": "game_highlights",
            "source_row_id": stable_highlight_source_row_id(
                game.game_id,
                highlight.highlight_type,
                highlight.event_seq,
                highlight.description,
            ),
            "title": f"[{game.game_date}] {game.away_team} vs {game.home_team} 하이라이트",
            "content": content,
            "team_id": game.home_team,
            "player_id": None,
            "season_year": game.game_date.year if game.game_date else _season_year_from_id(game.season_id),
            "document_type": "highlight",
            "game_date": str(game.game_date),
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "game_id": game.game_id,
                "highlight_type": highlight.highlight_type,
                "inning": highlight.inning,
                "tags": highlight.tags,
            },
        }


def _iter_award_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """awards에서 KBO 시상 내역 청크를 생성합니다."""
    from src.models.award import Award

    query = session.query(Award)
    if season:
        query = query.filter(Award.year == season)
    query = query.order_by(Award.year.desc())
    if limit:
        query = query.limit(limit)

    for award in query.yield_per(200):
        category_str = f" ({award.category})" if award.category else ""
        content = f"{award.year}시즌 KBO {award.award_type}{category_str}: {award.player_name} ({award.team_name})"
        yield {
            "source_table": "awards",
            "source_row_id": stable_award_source_row_id(
                award.year,
                award.award_type,
                award.category,
                award.player_name,
            ),
            "title": f"{award.year} {award.award_type}{category_str}",
            "content": content,
            "team_id": None,
            "player_id": None,
            "season_year": award.year,
            "document_type": "award",
            "game_date": None,
            "published_at": None,
            "source_url": None,
            "language": _DEFAULT_LANGUAGE,
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "award_type": award.award_type,
                "category": award.category,
                "player_name": award.player_name,
                "team_name": award.team_name,
            },
        }


def _iter_pbp_chunks(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """game_play_by_play에서 주요 승부처 및 득점 이벤트 청크를 생성합니다."""
    from src.models.game import Game, GamePlayByPlay

    query = (
        session.query(GamePlayByPlay, Game)
        .join(Game, GamePlayByPlay.game_id == Game.game_id)
        .filter(GamePlayByPlay.play_description.is_not(None))
    )
    if season:
        query = query.filter(Game.season_id.in_(_regular_season_ids(season)))

    keywords = [
        "홈런",
        "적시타",
        "안타",
        "삼진",
        "병살타",
        "도루",
        "결승타",
        "타점",
        "끝내기",
        "만루홈런",
        "역전타",
        "득점권",
    ]
    count = 0
    for pbp, game in query.yield_per(500):
        desc = pbp.play_description or ""
        if not any(kw in desc for kw in keywords):
            continue

        content = (
            f"경기일: {game.game_date}, 경기: {game.away_team} vs {game.home_team}, "
            f"이닝: {pbp.inning}회{pbp.inning_half or ''}, "
            f"투수: {pbp.pitcher_name or '미상'}, 타자: {pbp.batter_name or '미상'}, "
            f"내용: {desc}"
        )
        yield {
            "source_table": "game_play_by_play",
            "source_row_id": stable_pbp_source_row_id(
                PbpSourceIdentity(
                    game_id=pbp.game_id,
                    source_row_index=pbp.source_row_index,
                    inning=pbp.inning,
                    inning_half=pbp.inning_half,
                    pitcher_name=pbp.pitcher_name,
                    batter_name=pbp.batter_name,
                    play_description=desc,
                    event_type=pbp.event_type,
                    result=pbp.result,
                    source_name=pbp.source_name,
                )
            ),
            "title": (
                f"[{game.game_date}] {game.away_team} vs {game.home_team} "
                f"{pbp.inning}회{pbp.inning_half or ''} {pbp.batter_name or ''}"
            ),
            "content": content,
            "team_id": game.home_team,
            "player_id": str(pbp.player_id) if pbp.player_id else None,
            "season_year": game.game_date.year if game.game_date else _season_year_from_id(game.season_id),
            "document_type": "game_play_by_play",
            "game_date": str(game.game_date),
            "published_at": None,
            "source_url": None,
            "language": "ko",
            "league_type_code": _REGULAR_SEASON_CODE,
            "meta": {
                "game_id": game.game_id,
                "inning": pbp.inning,
                "event_type": pbp.event_type,
            },
        }
        count += 1
        if limit and count >= limit:
            break


def _iter_pbp_chunks_keyset(session: Session, season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Stream PBP rows with SQL keyword filtering and an integer keyset."""
    from sqlalchemy import or_

    from src.models.game import Game, GamePlayByPlay

    last_id = 0
    emitted = 0
    keyword_filter = or_(*(GamePlayByPlay.play_description.contains(keyword) for keyword in _PBP_KEYWORDS))
    while True:
        query = (
            session.query(
                GamePlayByPlay.id,
                GamePlayByPlay.game_id,
                GamePlayByPlay.inning,
                GamePlayByPlay.inning_half,
                GamePlayByPlay.pitcher_name,
                GamePlayByPlay.batter_name,
                GamePlayByPlay.play_description,
                GamePlayByPlay.event_type,
                GamePlayByPlay.result,
                GamePlayByPlay.player_id,
                GamePlayByPlay.source_row_index,
                GamePlayByPlay.source_name,
                Game.game_date,
                Game.away_team,
                Game.home_team,
                Game.season_id,
            )
            .join(Game, GamePlayByPlay.game_id == Game.game_id)
            .filter(GamePlayByPlay.id > last_id, GamePlayByPlay.play_description.is_not(None), keyword_filter)
        )
        if season:
            query = query.filter(Game.season_id.in_(_regular_season_ids(season)))
        rows = query.order_by(GamePlayByPlay.id.asc()).limit(_PBP_SCAN_BATCH_SIZE).all()
        if not rows:
            return
        for row in rows:
            (
                pbp_id,
                game_id,
                inning,
                inning_half,
                pitcher_name,
                batter_name,
                desc,
                event_type,
                result,
                player_id,
                source_row_index,
                source_name,
                game_date,
                away_team,
                home_team,
                season_id,
            ) = row
            last_id = int(pbp_id)
            content = (
                f"경기일: {game_date}, 경기: {away_team} vs {home_team}, "
                f"이닝: {inning}회{inning_half or ''}, "
                f"투수: {pitcher_name or '미상'}, 타자: {batter_name or '미상'}, 내용: {desc}"
            )
            yield {
                "source_table": "game_play_by_play",
                "source_row_id": stable_pbp_source_row_id(
                    PbpSourceIdentity(
                        game_id=game_id,
                        source_row_index=source_row_index,
                        inning=inning,
                        inning_half=inning_half,
                        pitcher_name=pitcher_name,
                        batter_name=batter_name,
                        play_description=desc,
                        event_type=event_type,
                        result=result,
                        source_name=source_name,
                    )
                ),
                "title": f"[{game_date}] {away_team} vs {home_team} {inning}회{inning_half or ''} {batter_name or ''}",
                "content": content,
                "team_id": home_team,
                "player_id": str(player_id) if player_id else None,
                "season_year": game_date.year if game_date else _season_year_from_id(season_id),
                "document_type": "game_play_by_play",
                "game_date": str(game_date),
                "published_at": None,
                "source_url": None,
                "language": _DEFAULT_LANGUAGE,
                "league_type_code": _REGULAR_SEASON_CODE,
                "meta": {"game_id": game_id, "inning": inning, "event_type": event_type},
            }
            emitted += 1
            if limit and emitted >= limit:
                return


_MARKDOWN_SOURCE_TABLES = ("markdown_docs", "kbo_definitions", "kbo_regulations")


def _markdown_docs_root() -> Path:
    """Return the local Markdown root, allowing Docker deployments to override it."""
    default_root = Path(__file__).resolve().parents[3] / "Docs" / "baseball"
    return Path(os.getenv("KBO_MARKDOWN_DOCS_DIR", str(default_root)))


def _iter_local_markdown_chunks(
    source_table: str,
    limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks in the shape expected by the vector repository."""
    if source_table not in _MARKDOWN_SOURCE_TABLES:
        return

    transformer = TextTransformer()
    yielded = 0
    for doc in load_local_markdown_docs(_markdown_docs_root()):
        if markdown_source_table(doc) != source_table:
            continue

        for chunk in transformer.chunk_document(doc):
            meta = dict(chunk.get("meta", {}))
            source_path = str(meta.get("source_path") or "").replace("\\", "/")
            chunk_index = meta.get("chunk_index")
            source_row_id = (
                f"{source_path}_{chunk_index - 1}"
                if source_path and isinstance(chunk_index, int)
                else str(meta.get("source_row_id", ""))
            )
            if not source_row_id:
                continue
            meta["source_table"] = source_table
            meta["document_type"] = "markdown_doc"
            yield {
                "source_table": source_table,
                "source_row_id": source_row_id,
                "title": chunk.get("title"),
                "content": chunk["content"],
                "team_id": None,
                "player_id": None,
                "season_year": None,
                "document_type": "markdown_doc",
                "game_date": None,
                "published_at": None,
                "source_url": None,
                "language": _DEFAULT_LANGUAGE,
                "league_type_code": None,
                "meta": meta,
            }
            yielded += 1
            if limit and yielded >= limit:
                return


def _iter_markdown_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield general local Markdown knowledge chunks."""
    yield from _iter_local_markdown_chunks("markdown_docs", limit)


def _iter_definition_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks containing KBO definitions."""
    yield from _iter_local_markdown_chunks("kbo_definitions", limit)


def _iter_regulation_chunks(_session: Session, _season: int | None, limit: int | None) -> Iterator[dict[str, Any]]:
    """Yield local Markdown chunks containing KBO regulations."""
    yield from _iter_local_markdown_chunks("kbo_regulations", limit)


def _row_value(row: Mapping[Any, Any], *keys: str) -> object:
    """Return the first non-null value from a source row."""
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


def _row_metadata(row: Mapping[Any, Any]) -> dict[str, Any]:
    """Normalize metadata from either KBO RAG schema variant."""
    value = _row_value(row, "meta", "metadata")
    if isinstance(value, dict):
        metadata = dict(value)
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = {}
        metadata = dict(parsed) if isinstance(parsed, dict) else {}
    else:
        metadata = {}
    for key in (
        "source_type",
        "source_uri",
        "embedding_model",
        "embedding_dim",
        "embedding_version",
        "chunking_version",
        "quality_score",
    ):
        value = row.get(key)
        if value is not None:
            metadata.setdefault(key, value)
    return metadata


_STAGING_COLUMNS = (
    "id, source_table, source_row_id, title, content, team_id, player_id, season_year, "
    "season_id, league_type_code, game_date, published_at, source_url, language, "
    "content_hash, index_status, is_active, meta"
)

_STAGING_LIGHTWEIGHT_QUERY = f"SELECT {_STAGING_COLUMNS} FROM rag_chunks ORDER BY id"  # noqa: S608


def _staging_query(session: Session, *, skip_populated: bool, lightweight: bool, last_id: int = 0) -> str:
    """Build the staging query for the requested payload mode."""
    cursor_clause = f" AND id > {int(last_id)}" if last_id > 0 else ""
    if skip_populated:
        dialect_name = getattr(getattr(session.get_bind(), "dialect", None), "name", "")
        embedding_column = "embedding_vector" if dialect_name == "oracle" else "embedding"
        return f"SELECT {_STAGING_COLUMNS} FROM rag_chunks WHERE {embedding_column} IS NULL{cursor_clause} ORDER BY id"  # noqa: S608
    if lightweight:
        if last_id > 0:
            return f"SELECT {_STAGING_COLUMNS} FROM rag_chunks WHERE id > {int(last_id)} ORDER BY id"  # noqa: S608
        return _STAGING_LIGHTWEIGHT_QUERY
    if last_id > 0:
        return f"SELECT {_STAGING_COLUMNS} FROM rag_chunks WHERE id > {int(last_id)} ORDER BY id"  # noqa: S608
    return f"SELECT {_STAGING_COLUMNS} FROM rag_chunks ORDER BY id"  # noqa: S608


def _read_staging_cursor() -> int:
    """Read the staging keyset cursor from env or file."""
    cursor_env = os.getenv("RAG_STAGING_LAST_ID")
    if cursor_env:
        try:
            return int(cursor_env)
        except ValueError, TypeError:
            return 0
    cursor_file = os.getenv("RAG_STAGING_CURSOR_FILE", "data/rag/staging_cursor.json")
    try:
        path = Path(cursor_file)
        with path.open(encoding="utf-8") as f:
            cursor_data = json.load(f)
            return int(cursor_data.get("last_id", 0))
    except FileNotFoundError, json.JSONDecodeError, ValueError, TypeError, OSError:
        return 0


def _save_staging_cursor(last_id: int) -> None:
    """Persist the staging keyset cursor to file."""
    cursor_file = os.getenv("RAG_STAGING_CURSOR_FILE", "data/rag/staging_cursor.json")
    try:
        path = Path(cursor_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump({"last_id": last_id}, f)
    except OSError, TypeError, ValueError:
        logger.warning("staging 커서 저장 실패: %s", cursor_file)


def _iter_staging_rag_chunks(
    session: Session,
    _season: int | None,
    limit: int | None,
    *,
    skip_populated: bool = False,
    lightweight: bool = False,
    last_id: int = 0,
) -> Iterator[dict[str, Any]]:
    """Yield staging RAG content while deliberately discarding old vectors."""
    query = _staging_query(session, skip_populated=skip_populated, lightweight=lightweight, last_id=last_id)
    try:
        rows = session.execute(text(query)).mappings()
    except SQLAlchemyError:
        session.rollback()
        raise
    yielded = 0
    max_id_seen = last_id
    for row in rows:
        row_id = row.get("id")
        if isinstance(row_id, int) and row_id > max_id_seen:
            max_id_seen = row_id
        if row.get("is_active") is False or row.get("is_active") == 0:
            continue
        if str(row.get("index_status") or "").upper() in {"DELETED", "TOMBSTONED", "PURGED"}:
            continue
        content = str(row.get("content") or "").strip()
        if not content:
            continue
        metadata = _row_metadata(row)
        source_table = str(_row_value(row, "source_table", "source_type") or "rag_chunks")
        source_row_id = str(_row_value(row, "source_row_id", "chunk_hash", "id") or "")
        if not source_row_id:
            continue
        document_type = _row_value(row, "document_type") or metadata.get("document_type")
        source_url = _row_value(row, "source_url", "source_uri") or metadata.get("source_url")
        yield {
            "source_table": source_table,
            "source_row_id": source_row_id,
            "title": _row_value(row, "title") or "",
            "content": content,
            "team_id": _row_value(row, "team_id") or metadata.get("team_id"),
            "player_id": _row_value(row, "player_id") or metadata.get("player_id"),
            "season_year": _row_value(row, "season_year") or metadata.get("season_year"),
            "season_id": _row_value(row, "season_id") or metadata.get("season_id"),
            "league_type_code": _row_value(row, "league_type_code") or metadata.get("league_type_code"),
            "document_type": document_type,
            "game_date": _row_value(row, "game_date") or metadata.get("game_date"),
            "published_at": _row_value(row, "published_at", "valid_from") or metadata.get("published_at"),
            "source_url": source_url,
            "language": _row_value(row, "language") or metadata.get("language") or _DEFAULT_LANGUAGE,
            "content_hash": row.get("content_hash"),
            "meta": metadata,
        }
        yielded += 1
        if limit and yielded >= limit:
            break
    _STAGING_CURSOR_MAX_ID[0] = max(_STAGING_CURSOR_MAX_ID[0], max_id_seen)


def _iter_staging_missing_rag_chunks(
    session: Session,
    season: int | None,
    limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Yield only staging rows without a populated vector in the same store."""
    last_id = _read_staging_cursor()
    # Re-seed the accumulator from the real cursor on every call. The holder is
    # module-level so a generator can record its high-water mark, which means a
    # second call in the same process would otherwise inherit the first one's
    # value and, after a short run, save that larger number -- skipping the rows
    # in between without ever having processed them.
    _STAGING_CURSOR_MAX_ID[0] = last_id
    return _iter_staging_rag_chunks(session, season, limit, skip_populated=True, last_id=last_id)


def _iter_staging_candidate_chunks(
    session: Session,
    season: int | None,
    limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Yield staging chunks without loading vector payloads during selection."""
    last_id = _read_staging_cursor()
    # See ``_iter_staging_missing_rag_chunks``: the holder is module-level, so it
    # has to be re-seeded per call or a later short run inherits an earlier run's
    # high-water mark and skips the rows it never touched.
    _STAGING_CURSOR_MAX_ID[0] = last_id
    return _iter_staging_rag_chunks(session, season, limit, lightweight=True, last_id=last_id)


# ─── 소스 매핑 ────────────────────────────────────────────────────────────────

_SOURCE_MAP = {
    "players": _iter_player_chunks,
    "batting": _iter_batting_chunks,
    "pitching": _iter_pitching_chunks,
    "games": _iter_game_chunks,
    "standings": _iter_standings_chunks,
    "events": _iter_event_chunks,
    "movements": _iter_movement_chunks,
    "awards": _iter_award_chunks,
    "lineups": _iter_lineup_chunks,
    "rankings": _iter_rankings_chunks,
    "teams": _iter_team_chunks,
    "team_history": _iter_team_history_chunks,
    "highlights": _iter_highlight_chunks,
    "inning_scores": _iter_inning_score_chunks,
    "pbp": _iter_pbp_chunks_keyset,
    "markdown_docs": _iter_markdown_chunks,
    "kbo_definitions": _iter_definition_chunks,
    "kbo_regulations": _iter_regulation_chunks,
    "staging_rag_chunks": _iter_staging_candidate_chunks,
}

_SOURCE_TABLE_BY_SOURCE = {
    "players": "player_basic",
    "batting": "player_season_batting",
    "pitching": "player_season_pitching",
    "games": "game",
    "standings": "team_standings_daily",
    "events": "team_events",
    "movements": "player_movements",
    "awards": "awards",
    "lineups": "game_lineups",
    "rankings": "stat_rankings",
    "teams": "teams",
    "team_history": "team_history",
    "highlights": "game_highlights",
    "inning_scores": "game_inning_scores",
    "pbp": "game_play_by_play",
    "markdown_docs": "markdown_docs",
    "kbo_definitions": "kbo_definitions",
    "kbo_regulations": "kbo_regulations",
}


# ─── 임베딩 + 저장 ─────────────────────────────────────────────────────────────


def _dedupe_batch(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """배치 내 동일 키(source_table + source_row_id) 중복을 제거합니다 (마지막 항목 유지)."""
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for chunk in batch:
        deduped[(chunk["source_table"], chunk["source_row_id"])] = chunk
    return list(deduped.values())


def _state_from_index_row(row: Sequence[object]) -> RagIndexState:
    """Normalize a sparse or vector index projection."""
    return index_state_from_projection(row)


def _load_index_states(index_session: Session, source_table: str | None) -> dict[tuple[str, str], RagIndexState]:
    """Load canonical sparse states used by incremental selection."""
    from src.models.rag_chunk import RagChunk

    statement = select(
        RagChunk.source_table,
        RagChunk.source_row_id,
        RagChunk.content_hash,
        RagChunk.index_version,
        RagChunk.index_status,
        RagChunk.embedding.is_not(None).label("embedding_present"),
        RagChunk.meta,
    )
    if source_table:
        statement = statement.where(RagChunk.source_table == source_table)
    return {
        state.key: state for state in (_state_from_index_row(row) for row in index_session.execute(statement).all())
    }


def _load_vector_play_identities(vector_session: Session, source_table: str) -> frozenset:
    """Load the play identities of ACTIVE vector rows, whatever key or team code they were stored under."""
    from src.models.rag_chunk_vector import RagChunkVector
    from src.services.rag_play_identity import play_identity

    statement = (
        select(RagChunkVector.content)
        .where(RagChunkVector.source_table == source_table)
        .where(RagChunkVector.index_status == "ACTIVE")
    )
    identities = (play_identity(text_value or "") for text_value in vector_session.execute(statement).scalars())
    return frozenset(identity for identity in identities if identity is not None)


def _load_vector_content_hashes(vector_session: Session, source_table: str) -> frozenset[str]:
    """Load the content hashes the vector store holds as ACTIVE rows, across every identity key.

    Keyed by content rather than identity, so a play embedded under a legacy key still counts.
    """
    from src.models.rag_chunk_vector import RagChunkVector

    statement = (
        select(RagChunkVector.content_hash)
        .where(RagChunkVector.source_table == source_table)
        .where(RagChunkVector.index_status == "ACTIVE")
        .where(RagChunkVector.content_hash.is_not(None))
        .distinct()
    )
    return frozenset(vector_session.execute(statement).scalars().all())


def _load_vector_states(vector_session: Session, source_table: str | None) -> dict[tuple[str, str], RagIndexState]:
    """Load vector states used when sparse and vector stores are separated."""
    from src.models.rag_chunk_vector import RagChunkVector

    statement = select(
        RagChunkVector.source_table,
        RagChunkVector.source_row_id,
        RagChunkVector.content_hash,
        RagChunkVector.index_version,
        RagChunkVector.index_status,
        RagChunkVector.embedding.is_not(None).label("embedding_present"),
        RagChunkVector.meta,
    )
    if source_table:
        statement = statement.where(RagChunkVector.source_table == source_table)
    return {
        state.key: state for state in (_state_from_index_row(row) for row in vector_session.execute(statement).all())
    }


def rag_source_table_names() -> frozenset[str]:
    """Return the sources that map to a source table, so the presence filters can key on it."""
    return frozenset(_SOURCE_TABLE_BY_SOURCE)


def rag_all_source_names() -> tuple[str, ...]:
    """Return the sources ``--source all`` runs, in the order it runs them."""
    return tuple(_SOURCE_MAP.keys())


def rag_source_choices() -> tuple[str, ...]:
    """Return the supported source names for read-only planners."""
    return _VALID_SOURCES


def rag_source_map() -> Mapping[str, Callable[[Session, int | None, int | None], Iterator[dict[str, Any]]]]:
    """Return the source iterator registry for planners."""
    return _SOURCE_MAP


def source_table_for_source(source_name: str) -> str | None:
    """Return the canonical table name for one source iterator."""
    return _SOURCE_TABLE_BY_SOURCE.get(source_name)


def load_incremental_index_states(
    index_session: Session,
    source_table: str | None,
) -> dict[tuple[str, str], RagIndexState]:
    """Load canonical states for a read-only planner."""
    return _load_index_states(index_session, source_table)


def load_incremental_vector_states(
    vector_session: Session,
    source_table: str | None,
) -> dict[tuple[str, str], RagIndexState]:
    """Load vector states for a read-only planner."""
    return _load_vector_states(vector_session, source_table)


def _embedding_fingerprint_for_service(embedding_service: object) -> str | None:
    """Return the configured embedding fingerprint when all contract fields exist."""
    model = getattr(embedding_service, "model_name", None)
    if callable(model):
        model = model()
    if not model:
        from src.services.embedding_service import DEFAULT_OPENROUTER_EMBEDDING_MODEL

        model = os.getenv("EMBEDDING_MODEL", DEFAULT_OPENROUTER_EMBEDDING_MODEL)
    dimension = getattr(embedding_service, "dimension", RAG_EMBEDDING_DIMENSION)
    chunking_version = os.getenv("RAG_CHUNKING_VERSION", "rag-v1")
    return embedding_fingerprint(model, dimension, chunking_version)


def _load_presence_sets(selection: IncrementalSelectionOptions) -> tuple[frozenset[str] | None, frozenset | None]:
    """Load the content hashes and play identities the vector store already holds, when requested."""
    if selection.content_presence and (selection.vector_session is None or selection.source_table is None):
        message = "content presence needs a vector session and a single source table"
        raise ValueError(message)
    present_content_hashes = (
        _load_vector_content_hashes(selection.vector_session, selection.source_table)
        if selection.content_presence
        else None
    )
    present_play_identities = (
        _load_vector_play_identities(selection.vector_session, selection.source_table)
        if selection.play_presence
        else None
    )
    return present_content_hashes, present_play_identities


def _skip_existing_index_rows(
    chunk_iter: Iterator[dict[str, Any]],
    index_session: Session,
    options: IncrementalSelectionOptions | None = None,
    *,
    source_table: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield chunks whose content, version, lifecycle, or vector state is stale."""
    if options is not None and source_table is not None:
        message = "source_table cannot be combined with selection options"
        raise ValueError(message)
    selection = options or IncrementalSelectionOptions(source_table=source_table)
    canonical_states = _load_index_states(index_session, selection.source_table)
    vector_states = (
        _load_vector_states(selection.vector_session, selection.source_table)
        if selection.vector_session is not None
        else {}
    )
    present_content_hashes, present_play_identities = _load_presence_sets(selection)
    version = selection.desired_index_version or current_index_version()
    metadata_gap_count = 0
    repaired_count = 0
    blocked_count = 0
    for chunk in chunk_iter:
        key = (str(chunk.get("source_table") or ""), str(chunk.get("source_row_id") or ""))
        decision = decide_incremental_candidate(
            chunk,
            canonical_states.get(key),
            vector_states.get(key),
            IncrementalDecisionOptions(
                desired_index_version=version,
                desired_embedding_fingerprint=selection.desired_embedding_fingerprint,
                require_vector=selection.vector_session is not None,
                require_primary_embedding=selection.vector_session is None,
                vector_key_presence=selection.vector_key_presence,
                present_content_hashes=present_content_hashes,
                present_play_identities=present_play_identities,
            ),
        )
        if decision.blocked:
            blocked_count += 1
            logger.debug("RAG incremental candidate blocked: %s (%s)", decision.source_key, decision.reason)
            continue
        if decision.metadata_repair and not decision.should_reembed:
            if not selection.repair_legacy_fingerprint:
                metadata_gap_count += 1
                continue
            # The approved repair is a re-embed, not a metadata stamp. The row's
            # content, version, and vector are healthy, so the fingerprint is the
            # one thing nobody measured; writing the desired value into meta
            # without producing that embedding would assert a provenance the
            # store never had. Yielding routes the row through the ordinary
            # write path, which is where both stores learn the truth.
            repaired_count += 1
            yield chunk
            continue
        if decision.should_reembed or selection.include_healthy:
            yield chunk
    if metadata_gap_count:
        logger.warning(
            "RAG incremental selection found %d legacy rows without embedding fingerprint; "
            "metadata repair was not applied (pass --repair-legacy-fingerprint to re-embed them)",
            metadata_gap_count,
        )
    if repaired_count:
        logger.info("RAG incremental selection re-embedded %d legacy rows to record their fingerprint", repaired_count)
    if blocked_count:
        logger.info("RAG incremental selection blocked %d terminal identities", blocked_count)


def _session_is_oracle(session: Session) -> bool:
    """Return whether a SQLAlchemy session is bound to Oracle."""
    bind = session.get_bind()
    return str(getattr(getattr(bind, "dialect", None), "name", "")) == "oracle"


def _oracle_vector_schema_available(index_session: Session) -> bool:
    """Check the Oracle vector column on the actual sparse index session."""
    if not _session_is_oracle(index_session):
        return False
    try:
        result = index_session.execute(
            text(
                "SELECT COUNT(*) FROM user_tab_columns "
                "WHERE table_name = 'RAG_CHUNKS' AND column_name = 'EMBEDDING_VECTOR'"
            )
        )
        return int(result.scalar() or 0) == 1
    except SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError:
        return False


def _vector_store_is_the_index_store(index_session: Session) -> bool:
    """Return whether the dense index is the very table this build writes.

    ``PGVECTOR_URL`` may name the operational database, and then ``rag_chunks``
    is one table serving both roles. Publishing that batch through the dense
    row builder fails on the first insert: it writes five display columns
    (``document_type``, ``game_date``, ``published_at``, ``source_url``,
    ``language``) that the shared table keeps in ``meta`` instead.
    """
    from src.db.vector_engine import vector_store_shares_database_with

    return vector_store_shares_database_with(index_session.get_bind())


def _persist_index_batch(batch: list[dict[str, Any]], index_session: Session) -> None:
    """Publish one batch through pending and final sparse/vector states."""
    if _session_is_oracle(index_session) or _vector_store_is_the_index_store(index_session):
        from src.services.rag_index_propagation import publish_single_store_batch

        publish_single_store_batch(index_session, batch)
        return

    from src.db.vector_engine import get_vector_session
    from src.services.rag_index_propagation import publish_index_batch

    with get_vector_session() as vsession:
        publish_index_batch(index_session, vsession, batch)


def _process_source(
    source_name: str,
    chunk_iter: Iterator[dict[str, Any]],
    embedding_service: object,
    index_session: Session,
    *,
    dry_run: bool,
) -> int:
    """단일 소스의 청크를 sparse/vector 인덱스에 동일하게 저장합니다."""
    total = 0
    batch: list[dict[str, Any]] = []
    embedding_fingerprint_value = _embedding_fingerprint_for_service(embedding_service)

    def _flush_batch(batch: list[dict[str, Any]]) -> None:
        nonlocal total
        if not batch:
            return
        batch = _dedupe_batch(batch)
        texts = [c["content"] for c in batch]
        embeddings = embedding_service.get_embeddings_batch(texts)  # type: ignore[attr-defined]
        _validate_embeddings(embeddings, len(batch))
        for chunk, emb in zip(batch, embeddings, strict=False):
            chunk["content_hash"] = chunk_content_hash(chunk.get("title"), str(chunk.get("content") or ""))
            if embedding_fingerprint_value:
                metadata = dict(chunk.get("meta") or {})
                metadata["embedding_fingerprint"] = embedding_fingerprint_value
                chunk["meta"] = metadata
            chunk["embedding"] = emb
            chunk["index_version"] = current_index_version()
            chunk["indexed_at"] = datetime.now(KST)

        if not dry_run:
            _persist_index_batch(batch, index_session)
            total += len(batch)
            if total % _COMMIT_EVERY == 0:
                logger.info("  [%s] %d 청크 저장 완료...", source_name, total)
        else:
            total += len(batch)

    for chunk in chunk_iter:
        batch.append(chunk)
        if len(batch) >= _BATCH_SIZE:
            _flush_batch(batch)
            batch = []

    if batch:
        _flush_batch(batch)

    return total


def _prepare_source_chunks(
    source_name: str,
    chunk_fn: Callable[[Session, int | None, int | None], Iterator[dict[str, Any]]],
    source_session: Session,
    season: int | None,
    limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Materialize long database-backed sources before slow API calls.

    Oracle can close an idle connection while a generator is paused between
    embedding batches. Materializing the large streaming sources first lets
    the source session release its connection before OpenRouter work begins.
    """
    chunk_iter = chunk_fn(source_session, season, limit)
    if source_name in {"markdown_docs", "kbo_definitions", "kbo_regulations", "staging_rag_chunks"}:
        return chunk_iter

    chunks = list(chunk_iter)
    source_session.close()
    return iter(chunks)


class RagIndexUnavailableError(RuntimeError):
    """A build stopped because a store it needs cannot be used.

    ``RuntimeError`` rather than ``SystemExit`` on purpose. ``SystemExit`` derives
    from ``BaseException``, so ``except SCHEDULER_JOB_EXCEPTIONS`` cannot catch
    it and a scheduler job calling :func:`main` in-process would die holding its
    tier lock with no traceback logged and no failure alert. The exit code is
    carried here and translated back to a process exit by :func:`cli_main`, so
    the CLI contract is unchanged.
    """

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        """Keep the CLI exit code alongside the reason."""
        super().__init__(message)
        self.exit_code = exit_code


#: Reasons the build cannot proceed, kept here so the raise sites stay readable.
_VECTOR_STORE_UNREACHABLE = (
    "vector DB에 연결할 수 없습니다. Oracle vector migration 또는 local pgvector setup을 확인하세요."
)
_ORACLE_VECTOR_SCHEMA_MISSING = "Oracle RAG vector schema is unavailable. Apply Oracle migrations before indexing."


def _iter_candidate_chunks(
    source_name: str,
    source_session: Session,
    index_session: Session,
    vector_session: Session | None,
    options: BuildSourceOptions,
) -> Iterator[dict[str, Any]]:
    """Build one source iterator and apply incremental candidate selection."""
    chunk_iter = _prepare_source_chunks(
        source_name,
        _SOURCE_MAP[source_name],
        source_session,
        options.season,
        options.limit,
    )
    if options.offset:
        chunk_iter = islice(chunk_iter, options.offset, None)
    selected = _skip_existing_index_rows(
        chunk_iter,
        index_session,
        IncrementalSelectionOptions(
            source_table=_SOURCE_TABLE_BY_SOURCE.get(source_name),
            vector_session=vector_session,
            desired_index_version=current_index_version(),
            desired_embedding_fingerprint=options.desired_embedding_fingerprint,
            include_healthy=not options.skip_existing,
            repair_legacy_fingerprint=options.repair_legacy_fingerprint,
            vector_key_presence=options.vector_key_presence,
            content_presence=options.content_presence,
            play_presence=options.play_presence,
        ),
    )
    return _drop_excluded(selected, options.exclude_ids)


def _vector_session_context(*, sparse_is_oracle: bool) -> AbstractContextManager[Session | None]:
    """Select the vector session context for the resolved sparse target."""
    if sparse_is_oracle:
        return nullcontext(None)
    from src.db.vector_engine import get_vector_session

    return get_vector_session()


# ─── 메인 CLI ─────────────────────────────────────────────────────────────────


def _read_exclude_ids(path: str | None) -> frozenset[str]:
    """Read source_row_ids to leave out, one per line; ``-`` reads standard input.

    Blank lines and ``#`` comments are ignored. ``None`` excludes nothing.
    """
    if path is None:
        return frozenset()
    text_source = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return frozenset(
        line.strip() for line in text_source.splitlines() if line.strip() and not line.lstrip().startswith("#")
    )


def _drop_excluded(chunk_iter: Iterator[dict[str, Any]], exclude_ids: frozenset[str]) -> Iterator[dict[str, Any]]:
    """Yield chunks whose source_row_id is not in the exclusion list."""
    for chunk in chunk_iter:
        if str(chunk.get("source_row_id") or "") not in exclude_ids:
            yield chunk


def _count_candidates(chunk_iter: Iterator[dict[str, Any]], list_path: Path | None) -> int:
    """Count candidate chunks; with ``list_path``, also write one JSON line per candidate.

    Read-only: no embedding call and no database write. The file is for sampling the candidates
    by hand before an apply run.
    """
    if list_path is None:
        return sum(1 for _ in chunk_iter)
    count = 0
    with list_path.open("w", encoding="utf-8") as handle:
        for chunk in chunk_iter:
            record = {
                "source_table": chunk.get("source_table"),
                "source_row_id": chunk.get("source_row_id"),
                "title": chunk.get("title"),
                "content": str(chunk.get("content") or "")[:300],
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def _add_presence_flags(parser: argparse.ArgumentParser) -> None:
    """Register the presence filters that skip identities the vector store already holds."""
    parser.add_argument(
        "--content-presence",
        action="store_true",
        help="정본에 없어도 벡터에 같은 본문 해시(ACTIVE)가 있으면 갭에서 뺀다 (--skip-existing과 단일 --source 필요)",
    )
    parser.add_argument(
        "--play-presence",
        action="store_true",
        help="벡터에 같은 플레이(날짜·팀쌍·이닝·타자·내용)가 있으면 갭에서 뺀다 (--skip-existing, --source pbp)",
    )


def _prepare_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Apply count-only's dry-run implication and refuse flag combinations that are not meaningful."""
    args.dry_run = args.dry_run or args.count_only
    """Refuse presence filters that only mean something on the skip-existing path."""
    if (args.vector_presence or args.content_presence) and not args.skip_existing:
        parser.error("--vector-presence and --content-presence require --skip-existing")
    if args.list_candidates is not None and not args.count_only:
        parser.error("--list-candidates requires --count-only")
    if args.exclude_file is not None and args.source != "pbp":
        parser.error("--exclude-file is only supported with --source pbp")
    if args.play_presence and (not args.skip_existing or args.source != "pbp"):
        parser.error("--play-presence requires --skip-existing and --source pbp")


def _completion_suffix(args: argparse.Namespace) -> str:
    """Name the mode in the final log line so a count is never mistaken for a write."""
    if args.count_only:
        return " (count-only, 임베딩 호출 없음, 저장 없음)"
    if args.dry_run:
        return " (dry-run, 저장 없음)"
    return ""


def main(argv: list[str] | None = None) -> None:  # noqa: C901, PLR0915
    """KBO RAG 인덱스를 구축합니다."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Oracle KBO 데이터를 Oracle AI Vector RAG 인덱스에 저장")
    parser.add_argument(
        "--source",
        choices=_VALID_SOURCES,
        default="all",
        help="임베딩할 데이터 소스 (기본값: all)",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="특정 시즌만 처리 (예: 2025)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="소스당 최대 처리 행 수 (테스트용)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="소스 iterator에서 건너뛸 행 수 (중단된 staged run 재개용)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="신규·변경·stale version·누락 vector만 처리하고 정상 identity는 건너뜀",
    )
    parser.add_argument(
        "--repair-legacy-fingerprint",
        action="store_true",
        help=(
            "내용·버전·vector는 정상이나 embedding_fingerprint가 없는 legacy 행을 "
            "재임베딩해 fingerprint를 기록 (기본 비활성, 승인 게이트)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="임베딩만 생성하고 DB에 저장하지 않음",
    )
    parser.add_argument(
        "--embedding-mode",
        choices=("configured", "deterministic"),
        default="configured",
        help="임베딩 provider; deterministic는 staging 검증 전용",
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="임베딩 API를 호출하지 않고 처리 대상 청크 수만 센다 (dry-run을 포함한다)",
    )
    parser.add_argument(
        "--vector-presence",
        action="store_true",
        help="정본에 없어도 벡터 저장소에 같은 키가 있으면 갭에서 뺀다 (--skip-existing 필요, 키 존재만 비교)",
    )
    parser.add_argument(
        "--list-candidates",
        type=Path,
        default=None,
        help="--count-only와 함께: 후보를 JSON Lines 파일로 내보낸다 (쓰기·임베딩 없음)",
    )
    parser.add_argument(
        "--exclude-file",
        default=None,
        help="제외할 source_row_id 목록 파일 (한 줄에 하나, '-'는 표준 입력, --source pbp 전용)",
    )
    _add_presence_flags(parser)
    args = parser.parse_args(argv)
    _prepare_args(parser, args)

    from src.db.engine import DATABASE_URL
    from src.db.vector_engine import init_vector_db, is_pgvector_available

    try:
        source_db_url = os.getenv(SOURCE_URL_ENV_VARS[0]) or DATABASE_URL
        targets = _resolve_build_targets(
            source_db_url,
            target_db_url=DATABASE_URL,
            embedding_mode=args.embedding_mode,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        raise RagIndexUnavailableError(str(exc), exit_code=2) from exc

    target_display = targets.display()
    logger.info(
        "RAG targets | source=%s sparse=%s vector=%s environment=%s embedding=%s write_enabled=%s",
        target_display["source_db"],
        target_display["sparse_index_db"],
        target_display["vector_db"],
        targets.target_environment or "unset",
        args.embedding_mode,
        targets.write_enabled,
    )

    sparse_is_oracle = _is_oracle_url(targets.sparse_index_db)
    if not sparse_is_oracle and not is_pgvector_available():
        raise RagIndexUnavailableError(_VECTOR_STORE_UNREACHABLE)

    if not args.dry_run and not sparse_is_oracle:
        logger.info("pgvector 테이블 초기화 확인 중...")
        init_vector_db()

    # 서비스 초기화
    embedding_service = _embedding_service(args.embedding_mode, cache_enabled=not args.dry_run)

    # 소스 선택
    # staging_rag_chunks는 일회성 대규모 백필 성격이므로 야간 --source all에서 제외.
    # 수동 실행: --source staging_rag_chunks (keyset 커서로 재개 가능)
    sources = [s for s in _SOURCE_MAP if s != "staging_rag_chunks"] if args.source == "all" else [args.source]

    logger.info(
        "RAG 인덱스 빌드 시작 | 소스: %s | 시즌: %s | 제한: %s | dry-run: %s | embedding: %s",
        args.source,
        args.season or "전체",
        args.limit or "없음",
        args.dry_run,
        args.embedding_mode,
    )

    # Source와 sparse index는 별도 DB 세션을 사용한다.
    from src.db.engine import get_rag_index_session, get_rag_source_session, init_rag_index_db

    grand_total = 0
    if not args.dry_run:
        init_rag_index_db()
    vector_session_context = _vector_session_context(sparse_is_oracle=sparse_is_oracle)
    build_source_options = BuildSourceOptions(
        season=args.season,
        limit=args.limit,
        offset=args.offset,
        skip_existing=args.skip_existing,
        desired_embedding_fingerprint=_embedding_fingerprint_for_service(embedding_service),
        repair_legacy_fingerprint=args.repair_legacy_fingerprint,
        vector_key_presence=args.vector_presence,
        content_presence=args.content_presence,
        play_presence=args.play_presence,
        exclude_ids=_read_exclude_ids(args.exclude_file),
    )
    with (
        get_rag_source_session() as source_session,
        get_rag_index_session() as index_session,
        vector_session_context as vector_session,
    ):
        if sparse_is_oracle and not _oracle_vector_schema_available(index_session):
            raise RagIndexUnavailableError(_ORACLE_VECTOR_SCHEMA_MISSING)
        failed_sources: list[str] = []
        for source_name in sources:
            logger.info("▶ [%s] 처리 시작...", source_name)
            try:
                chunk_iter = _iter_candidate_chunks(
                    source_name,
                    source_session,
                    index_session,
                    vector_session,
                    build_source_options,
                )
                # count-only는 임베딩 전 단계에서 세기만 한다. 제공자 호출과 저장이 모두 없다.
                count = (
                    _count_candidates(chunk_iter, args.list_candidates)
                    if args.count_only
                    else _process_source(
                        source_name,
                        chunk_iter,
                        embedding_service,
                        index_session,
                        dry_run=args.dry_run,
                    )
                )
            except Exception:
                failed_sources.append(source_name)
                logger.exception("❌ [%s] 처리 실패 — 다음 소스로 계속합니다", source_name)
                continue
            logger.info("✅ [%s] %d 청크 완료", source_name, count)
            grand_total += count
        if failed_sources:
            logger.error("RAG 빌드에서 실패한 소스: %s", ", ".join(failed_sources))

        # staging keyset 커서 저장 (dry-run이 아닌 경우)
        if not args.dry_run and "staging_rag_chunks" in sources:
            _save_staging_cursor(_STAGING_CURSOR_MAX_ID[0])
            logger.info("staging 커서 저장: last_id=%d", _STAGING_CURSOR_MAX_ID[0])

    if failed_sources:
        failed_message = "RAG 빌드 실패 소스: " + ", ".join(failed_sources)
        raise RagIndexUnavailableError(
            failed_message,
            exit_code=1,
        )

    logger.info("🎉 RAG 인덱스 빌드 완료 — 총 %d 청크 처리%s", grand_total, _completion_suffix(args))


def cli_main(argv: list[str] | None = None) -> int:
    """Process entrypoint: translate a store failure back into an exit code.

    :func:`main` raises instead of exiting so an in-process caller -- the
    ``sync_rag_incremental_job`` scheduler job -- can catch it inside its own
    handler. Only this boundary converts it back to the documented exit code.
    """
    try:
        main(argv)
    except RagIndexUnavailableError as exc:
        # Not logging.exception: the traceback adds nothing here, because every
        # raise site is a single documented precondition check with no recovery
        # path to inspect. The reason itself is the diagnostic.
        logger.error("%s", exc)  # noqa: TRY400
        return exc.exit_code
    return 0


if __name__ == "__main__":
    sys.exit(cli_main())
