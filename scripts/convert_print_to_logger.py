import logging

logger = logging.getLogger(__name__)
"""
Convert print() calls to logger calls in Python files.

Heuristics:
  - In except blocks: if logger.error/warning/exception already exists, REMOVE print()
    otherwise, convert print() -> logger.exception()
  - print with ❌ -> logger.error
  - print with ⚠️  -> logger.warning
  - print with ℹ️✅📡🔍📊 -> logger.info
  - else -> logger.info

Usage:
  python scripts/convert_print_to_logger.py <file1.py> [file2.py ...]
"""

import ast
import re
import sys
from pathlib import Path


def _visit_except_handler(blocks: dict, node: ast.ExceptHandler, lines: list[str]) -> None:
    first = node.lineno - 1
    last = node.end_lineno or len(lines)
    has_logger = False
    print_lines: set[int] = set()

    for stmt in node.body:
        stmt_first = stmt.lineno - 1
        stmt_last = stmt.end_lineno or stmt_first
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            if isinstance(stmt.value.func, ast.Attribute):
                if isinstance(stmt.value.func.value, ast.Name) and stmt.value.func.value.id == "logger":
                    has_logger = True
        for ln in range(stmt_first, stmt_last + 1):
            if first <= ln <= last:
                line = lines[ln] if ln < len(lines) else ""
                if re.match(r"^\s*print\(", line):
                    print_lines.add(ln)

    for ln in range(first, last + 1):
        blocks[ln] = {"has_logger": has_logger, "print_lines": print_lines, "first_line": first, "last_line": last}


def _find_except_blocks(source: str) -> dict[int, dict]:
    tree = ast.parse(source)
    blocks = {}
    lines = source.splitlines()

    class ExceptVisitor(ast.NodeVisitor):
        def visit_ExceptHandler(self, node):
            _visit_except_handler(blocks, node, lines)
            self.generic_visit(node)

    ExceptVisitor().visit(tree)
    return blocks


def _convert_print_line(line: str, i: int, except_blocks: dict, delete_lines: set) -> str | None:
    stripped = line.strip()
    if not (stripped.startswith("print(") and stripped.endswith(")")):
        return None
    if i in delete_lines:
        return ""  # signal deletion
    is_except = i in except_blocks
    m = re.match(r"^(\s*)print\((.*)\)\s*$", line)
    if not m:
        return line
    indent, content = m.group(1), m.group(2)
    if is_except:
        return f"{indent}logger.exception({content})"
    if "❌" in content:
        return f"{indent}logger.error({content})"
    if "⚠️" in content:
        return f"{indent}logger.warning({content})"
    return f"{indent}logger.info({content})"


def _add_logging_import(source: str, new_source: str) -> str:
    if "import logging" in source or "logging.getLogger" in source:
        return new_source
    if "from __future__" in source:
        new_source = new_source.replace("from __future__", "import logging\nfrom __future__", 1)
    else:
        new_source = "import logging\n" + new_source
    return re.sub(r"(^(?:import|from)\s.*\n)", r"\1\nlogger = logging.getLogger(__name__)\n", new_source, count=1)


def process_file(path: Path) -> int:
    source = path.read_text()
    lines = source.splitlines()
    except_blocks = _find_except_blocks(source)

    delete_lines = set()
    for _lineno, info in except_blocks.items():
        if info["has_logger"]:
            for pl in info["print_lines"]:
                delete_lines.add(pl)

    new_lines = []
    changed = 0
    deleted = 0

    for i, line in enumerate(lines):
        converted = _convert_print_line(line, i, except_blocks, delete_lines)
        if converted is None:
            new_lines.append(line)
        elif converted == "":
            deleted += 1
        else:
            new_lines.append(converted)
            changed += 1

    if changed or deleted:
        new_source = _add_logging_import(source, "\n".join(new_lines))
        path.write_text(new_source)
        logger.info(f"  {path.name}: {changed} converted, {deleted} removed")

    return changed + deleted


def main():
    files = [Path(a) for a in sys.argv[1:] if a.endswith(".py")]
    total = 0
    for f in files:
        total += process_file(f)
    logger.info(f"\nTotal: {total} print() calls handled across {len(files)} files")


if __name__ == "__main__":
    main()
