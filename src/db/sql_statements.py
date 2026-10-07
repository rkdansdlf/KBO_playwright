"""One SQL statement splitter, so a migration means the same thing everywhere.

Splitting a migration on ``;`` alone is wrong, and it was wrong in production.
``060_parking_fee_kinds.sql`` carries a header comment that reads "these kinds
never had a table of their own; they lived only in the raw snapshot text", so a
naive split cut the file inside that comment. Two failures followed from that one
cut: the leading chunk was comment-only and reached the driver as an empty query,
and the orphaned tail of the comment was welded onto the next statement as though
it were code.

This scanner is the fix, and it exists as its own module because there were three
splitters. ``src.cli.sync.apply_postgres_migrations`` had one, ``src.db.migration_engine``
had a second with the same defect plus three more, and the Oracle runner had a
third that is correct for PL/SQL but shares nothing with the other two. Three
interpretations of the same file is three chances for the runners to disagree
about what a migration does, so the scanner lives here and both ``;``-separated
chains call it.

The Oracle chain is deliberately not routed through here. Its migrations are
anonymous PL/SQL blocks whose terminator is a ``/`` on its own line and whose
semicolons belong to the body; a scanner that treated ``;`` as a separator would
tear those in half. See
:func:`src.cli.sync.apply_oracle_migrations._execute_migration` for that chain.

The scanner tracks line comments, block comments and single-quoted strings --
enough to know where a ``;`` is real syntax and where it is text. Dollar quoting
is not tracked: neither the PostgreSQL nor the SQLite chain uses it, and a
migration that introduces it would fail loudly here rather than silently.
"""

from __future__ import annotations

import re

LINE_COMMENT = "--"
BLOCK_COMMENT_OPEN = "/*"
BLOCK_COMMENT_CLOSE = "*/"
QUOTE = "'"


class UnterminatedBlockCommentError(ValueError):
    """Raised when a migration opens a ``/*`` comment it never closes."""


def _scan_quoted(source: str, start: int) -> int:
    """Return the index just past the string literal opening at ``start``.

    A doubled quote (``''``) is an escaped quote inside the literal, not its
    end, so ``'a''; b'`` stays one string. A literal left unterminated runs to
    the end of the file rather than being cut short, which keeps the trailing
    text with the statement it belongs to instead of splitting mid-literal.
    """
    index = start + 1
    length = len(source)
    while index < length:
        if source[index] == QUOTE:
            if source[index + 1 : index + 2] == QUOTE:
                index += 2
                continue
            return index + 1
        index += 1
    return length


def _skip_line_comment(source: str, start: int) -> int:
    """Return the index of the newline ending the comment at ``start``."""
    end = source.find("\n", start)
    return len(source) if end == -1 else end


def _skip_block_comment(source: str, start: int) -> int:
    """Return the index just past the block comment opening at ``start``.

    Raises:
        UnterminatedBlockCommentError: When no closing ``*/`` follows. The scanner
            cannot tell how far an unclosed comment reaches, so it refuses the
            file instead of guessing that everything after it is comment --
            which would leave the migration applying as nothing while still
            recording its version.

    """
    end = source.find(BLOCK_COMMENT_CLOSE, start + 2)
    if end == -1:
        msg = f"unterminated block comment starting at offset {start}"
        raise UnterminatedBlockCommentError(msg)
    return end + 2


SQLPLUS_TERMINATOR = "/"
_TRAILING_TERMINATOR_RE = re.compile(rf"(?m)^[ \t]*{re.escape(SQLPLUS_TERMINATOR)}[ \t]*$")


def _strip_sqlplus_terminator(sql: str) -> str:
    """Drop lines that are nothing but a SQL*Plus block terminator.

    The Oracle chain writes its anonymous PL/SQL blocks as ``END;`` followed by
    ``/`` on its own line, and copies of that style turn up in the other chains
    too. The terminator is not SQL, so leaving it in hands the driver a statement
    ending in ``/`` -- which reads as a divide. The earlier splitter in
    ``migration_engine`` stripped a trailing ``/`` as a side effect of splitting
    on it, which worked by accident; doing it explicitly keeps the safety
    without the accident.

    Only a line consisting solely of ``/`` is removed, so a division expression
    or a path is untouched.
    """
    return _TRAILING_TERMINATOR_RE.sub("", sql).strip()


def _keep_chunk(statements: list[str], chunk: str, *, has_code: bool) -> None:
    """Append a chunk only when it carries SQL rather than comments alone.

    ``has_code`` is tracked by the scanner rather than recomputed here so the two
    cannot disagree about what counts as a comment. A comment is carried along
    with the code it precedes, so a statement keeps the rationale written next to
    it -- dropping comments would lose that, while keeping a comment *only* chunk
    would send the driver an empty query.
    """
    sql = _strip_sqlplus_terminator(chunk)
    if sql and has_code:
        statements.append(sql)


def split_sql_statements(source: str) -> list[str]:
    """Split a migration file into the statements to execute, in file order.

    Args:
        source: The migration file's text.

    Returns:
        Statements ready to execute, without comment-only chunks.

    """
    statements: list[str] = []
    current: list[str] = []
    has_code = False
    index = 0
    length = len(source)

    while index < length:
        pair = source[index : index + 2]

        if pair == LINE_COMMENT:
            end = _skip_line_comment(source, index)
            current.append(source[index:end])
            index = end
            continue

        if pair == BLOCK_COMMENT_OPEN:
            end = _skip_block_comment(source, index)
            current.append(source[index:end])
            index = end
            continue

        char = source[index]

        if char == QUOTE:
            end = _scan_quoted(source, index)
            current.append(source[index:end])
            has_code = True
            index = end
            continue

        if char == ";":
            _keep_chunk(statements, "".join(current), has_code=has_code)
            current = []
            has_code = False
            index += 1
            continue

        current.append(char)
        if not char.isspace():
            has_code = True
        index += 1

    _keep_chunk(statements, "".join(current), has_code=has_code)
    return statements


__all__ = ["UnterminatedBlockCommentError", "split_sql_statements"]
