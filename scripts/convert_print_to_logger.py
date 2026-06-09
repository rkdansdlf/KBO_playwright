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


def _find_except_blocks(source: str) -> dict[int, dict]:
    """Return dict of lineno(0-indexed) -> {'has_logger': bool, 'print_lines': set, 'first_line': int, 'last_line': int}."""
    tree = ast.parse(source)
    blocks = {}

    class ExceptVisitor(ast.NodeVisitor):
        def visit_ExceptHandler(self, node):
            first = node.lineno - 1
            last = node.end_lineno or len(source.splitlines())
            has_logger = False
            print_lines = set()

            for stmt in node.body:
                stmt_first = stmt.lineno - 1
                stmt_last = stmt.end_lineno or stmt_first

                # Check if this statement is a logger call
                if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                    if isinstance(stmt.value.func, ast.Attribute):
                        if isinstance(stmt.value.func.value, ast.Name) and stmt.value.func.value.id == "logger":
                            has_logger = True

                # Check if it's a bare except: with no type
                if node.type is None and not isinstance(stmt, ast.Raise):
                    pass  # bare except

                # Record print statements in this block
                for ln in range(stmt_first, stmt_last + 1):
                    if ln >= first and ln <= last:
                        line = source.splitlines()[ln] if ln < len(source.splitlines()) else ""
                        if re.match(r"^\s*print\(", line):
                            print_lines.add(ln)

            for ln in range(first, last + 1):
                blocks[ln] = {
                    "has_logger": has_logger,
                    "print_lines": print_lines,
                    "first_line": first,
                    "last_line": last,
                }
            self.generic_visit(node)

    ExceptVisitor().visit(tree)
    return blocks


def process_file(path: Path) -> int:
    source = path.read_text()
    lines = source.splitlines()
    except_blocks = _find_except_blocks(source)

    # Build set of print lines that should be DELETED (already has logger in except block)
    delete_lines = set()
    for _lineno, info in except_blocks.items():
        if info["has_logger"]:
            for pl in info["print_lines"]:
                delete_lines.add(pl)

    new_lines = []
    changed = 0
    deleted = 0

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("print(") and stripped.endswith(")"):
            if i in delete_lines:
                # Remove the print() line entirely
                deleted += 1
                continue

            # Convert
            is_except = i in except_blocks
            m = re.match(r"^(\s*)print\((.*)\)\s*$", line)
            if m:
                indent = m.group(1)
                content = m.group(2)
                if is_except:
                    new_lines.append(f"{indent}logger.exception({content})")
                elif "❌" in content:
                    new_lines.append(f"{indent}logger.error({content})")
                elif "⚠️" in content:
                    new_lines.append(f"{indent}logger.warning({content})")
                else:
                    new_lines.append(f"{indent}logger.info({content})")
                changed += 1
                continue
            # Fallback: keep original if regex didn't match
            new_lines.append(line)
        else:
            new_lines.append(line)

    if changed or deleted:
        new_source = "\n".join(new_lines)
        if "import logging" not in source and "logging.getLogger" not in source:
            if "from __future__" in source:
                new_source = new_source.replace("from __future__", "import logging\nfrom __future__", 1)
            else:
                new_source = "import logging\n" + new_source
            new_source = re.sub(
                r"(^(?:import|from)\s.*\n)",
                r"\1\nlogger = logging.getLogger(__name__)\n",
                new_source,
                count=1,
            )
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
