"""AST-based docstring generator for KBO Playwright codebase."""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

VERB_MAP: dict[str, str] = {
    "save": "Saves",
    "add": "Adds",
    "insert": "Inserts",
    "upsert": "Inserts or updates",
    "update": "Updates",
    "delete": "Deletes",
    "remove": "Removes",
    "get": "Gets",
    "fetch": "Fetches",
    "find": "Finds",
    "lookup": "Looks up",
    "search": "Searches for",
    "retrieve": "Retrieves",
    "crawl": "Crawls",
    "scrape": "Scrapes",
    "parse": "Parses",
    "extract": "Extracts",
    "build": "Builds",
    "create": "Creates",
    "make": "Makes",
    "generate": "Generates",
    "compute": "Computes",
    "calculate": "Calculates",
    "aggregate": "Aggregates",
    "normalize": "Normalizes",
    "format": "Formats",
    "validate": "Validates",
    "verify": "Verifies",
    "check": "Checks",
    "resolve": "Resolves",
    "determine": "Determines",
    "derive": "Derives",
    "classify": "Classifies",
    "filter": "Filters",
    "transform": "Transforms",
    "convert": "Converts",
    "map": "Maps",
    "reduce": "Reduces",
    "load": "Loads",
    "dump": "Dumps",
    "read": "Reads",
    "write": "Writes",
    "send": "Sends",
    "receive": "Receives",
    "sync": "Syncs",
    "merge": "Merges",
    "split": "Splits",
    "run": "Runs",
    "execute": "Executes",
    "process": "Processes",
    "handle": "Handles",
    "dispatch": "Dispatches",
    "emit": "Emits",
    "log": "Logs",
    "print": "Prints",
    "report": "Reports",
    "alert": "Alerts",
    "notify": "Notifies",
    "enrich": "Enriches",
    "augment": "Augments",
    "backfill": "Backfills",
    "repair": "Repairs",
    "fix": "Fixes",
    "heal": "Heals",
    "recalc": "Recalculates",
    "recompute": "Recalculates",
    "seed": "Seeds",
    "initialize": "Initializes",
    "init": "Initializes",
    "setup": "Set up",
    "configure": "Configures",
    "reset": "Resets",
    "clear": "Clears",
    "cleanup": "Cleans up",
    "purge": "Purges",
    "archive": "Archives",
}

MAGIC_DOCSTRINGS: dict[str, str] = {
    "__repr__": "Returns a string representation of this object.",
    "__str__": "Returns a user-friendly string representation.",
    "__len__": "Returns the number of items.",
    "__bool__": "Returns the truth value of this object.",
    "__eq__": "Compares this object with another for equality.",
    "__ne__": "Compares this object with another for inequality.",
    "__lt__": "Returns whether this object is less than another.",
    "__le__": "Returns whether this object is less than or equal to another.",
    "__gt__": "Returns whether this object is greater than another.",
    "__ge__": "Returns whether this object is greater than or equal to another.",
    "__hash__": "Returns the hash value for this object.",
    "__iter__": "Returns an iterator over the items.",
    "__next__": "Returns the next item from the iterator.",
    "__contains__": "Returns whether the collection contains the given item.",
    "__getitem__": "Returns the item at the given key.",
    "__setitem__": "Sets the item at the given key.",
    "__delitem__": "Deletes the item at the given key.",
    "__call__": "Calls the object with the given arguments.",
    "__enter__": "Enters the runtime context.",
    "__exit__": "Exits the runtime context.",
    "__init__": "Initializes a new instance.",
    "__new__": "Creates a new instance.",
    "__del__": "Destroys this object.",
    "__sizeof__": "Returns the size of this object in bytes.",
    "__reversed__": "Returns a reversed iterator.",
}

SKIP_PREFIXES = ("test_",)
CLI_MAIN_RE = re.compile(r"^main(_[a-z0-9_]+)?$")


def _strip_self(args: list[str]) -> list[str]:
    if args and args[0] in ("self", "cls"):
        return args[1:]
    return args


_PARAM_DESCRIPTIONS: dict[str, str] = {
    "db": "Database session.",
    "db session": "Database session.",
    "save": "Whether to persist the results.",
    "headless": "Whether to run the browser in headless mode.",
    "dry run": "If True, performs a dry run without persisting changes.",
    "force": "If True, forces the operation even if data already exists.",
    "verbose": "If True, enables verbose logging output.",
    "year": "Season year.",
    "month": "Month number (1-12).",
    "date": "Target date in YYYYMMDD format.",
    "season": "Season year.",
    "concurrency": "Maximum number of concurrent requests.",
    "timeout": "Timeout in seconds.",
    "output": "Output file path.",
    "input": "Input file or directory path.",
}


def _suffix_desc(readable: str) -> str | None:
    suffixes = [
        (" id", " ID."),
        (" url", " URL."),
        (" dir", " directory path."),
        (" path", " file path."),
    ]
    for suffix, template in suffixes:
        if readable.endswith(suffix):
            base = readable[: -len(suffix)].replace("_", " ").title()
            return f"{base}{template}"
    return None


def _param_desc(name: str) -> str:
    readable = name.replace("_", " ").strip()
    if readable in _PARAM_DESCRIPTIONS:
        return _PARAM_DESCRIPTIONS[readable]
    suffix = _suffix_desc(readable)
    if suffix:
        return suffix
    return f"{readable.replace('_', ' ').title()}."


_NAME_RETURN_DESC: dict[str, str | None] = {
    "None": None,
    "bool": "True if successful, False otherwise.",
    "int": "Integer result.",
    "str": "String result.",
    "dict": "Dictionary result.",
    "list": "List of results.",
    "tuple": "Tuple result.",
    "Path": "Path object.",
}

_SUB_RETURN_DESC: dict[str, str] = {
    "list": "List of results.",
    "dict": "Dictionary result.",
    "tuple": "Tuple result.",
    "Sequence": "Sequence of results.",
    "Optional": "The result if found, None otherwise.",
    "Callable": "Callable object.",
}


def _return_desc(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    returns = node.returns
    if returns is None:
        return None
    if isinstance(returns, ast.Constant):
        if returns.value is None or returns.value is ...:
            return None
    if isinstance(returns, ast.Name):
        name = returns.id
        if name in _NAME_RETURN_DESC:
            return _NAME_RETURN_DESC[name]
        return f"{name} instance."
    if isinstance(returns, ast.Subscript) and isinstance(returns.value, ast.Name):
        name = returns.value.id
        if name in _SUB_RETURN_DESC:
            return _SUB_RETURN_DESC[name]
    return "The result of the operation."


def _infer_verb(name: str) -> str | None:
    parts = name.split("_")
    for part in parts:
        if part in VERB_MAP:
            return VERB_MAP[part]
    return None


def _noun_from_name(name: str) -> str:
    parts = [p for p in name.split("_") if p and p not in VERB_MAP]
    if not parts:
        return name.replace("_", " ").title()
    return " ".join(parts).replace("-", " ").title()


def _generate_function_docstring(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    name = node.name
    if name in MAGIC_DOCSTRINGS:
        return MAGIC_DOCSTRINGS[name]
    args = [a.arg for a in node.args.args]
    args = _strip_self(args)
    if CLI_MAIN_RE.match(name):
        return "Main entry point for this CLI command."
    verb = _infer_verb(name)
    noun = _noun_from_name(name)
    if verb:
        summary = f"{verb} {noun.lower()}."
    elif name.startswith(("is_", "has_")):
        property_name = name.replace("is_", "").replace("has_", "").replace("_", " ")
        summary = f"Returns whether the {property_name}."
    elif name.startswith("can_"):
        property_name = name[4:].replace("_", " ")
        summary = f"Returns whether the {property_name}."
    elif name.startswith("should_"):
        property_name = name[7:].replace("_", " ")
        summary = f"Returns whether the {property_name}."
    else:
        summary = f"Handles the {noun.lower()} operation."
    lines = [summary]
    if args:
        lines.append("")
        lines.append("Args:")
        for arg in args:
            desc = _param_desc(arg)
            lines.append(f"    {arg}: {desc}")
    return_desc = _return_desc(node)
    if return_desc:
        lines.append("")
        lines.append("Returns:")
        lines.append(f"    {return_desc}")
    return "\n".join(lines)


def _generate_class_docstring(node: ast.ClassDef) -> str:
    return f"{node.name} class."


def _has_docstring(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> bool:
    return (
        isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    )


def _get_def_indent(lines: list[str], lineno: int) -> str:
    def_line = lines[lineno - 1]
    return " " * (len(def_line) - len(def_line.lstrip()))


def _find_body_start(lines: list[str], lineno: int, body_lineno: int) -> int:
    if body_lineno <= lineno:
        return lineno
    body_line = lines[body_lineno - 1]
    stripped = body_line.strip()
    if stripped.endswith("...") and not stripped.startswith("def "):
        return body_lineno
    if body_lineno == lineno + 1:
        return body_lineno - 1
    return body_lineno - 1


def _collect_targets(tree: ast.AST) -> list[tuple[int, str, str, int]]:
    targets: list[tuple[int, str, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            if node.name.startswith("_") or _has_docstring(node):
                continue
            docstring = _generate_class_docstring(node)
            targets.append((node.lineno, node.name, f'"""{docstring}"""', node.body[0].lineno))
            continue
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith(SKIP_PREFIXES):
            continue
        if node.name.startswith("_") and not node.name.startswith("__"):
            continue
        if _has_docstring(node):
            continue
        if (
            isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and node.body[0].value.value is ...
        ):
            continue
        docstring = _generate_function_docstring(node)
        targets.append((node.lineno, node.name, f'"""{docstring}"""', node.body[0].lineno))
    return targets


def _apply_changes(source: str, targets: list[tuple[int, str, str, int]]) -> tuple[str, list[str]]:
    if not targets:
        return source, []
    sorted_targets = sorted(targets, key=lambda t: t[0], reverse=True)
    lines = source.splitlines(keepends=True)
    changes: list[str] = []
    for lineno, name, quoted, body_lineno in sorted_targets:
        indent = _get_def_indent(lines, lineno)
        body_indent = indent + "    "
        insert_pos = _find_body_start(lines, lineno, body_lineno)
        if body_lineno <= lineno:
            lines.insert(lineno, f"{body_indent}{quoted}\n")
        else:
            lines.insert(insert_pos, f"{body_indent}{quoted}\n")
        first_line = quoted.split("\n")[0].removeprefix('"""').removesuffix('"""')
        changes.append(f"  L{lineno}: {name} -> {first_line}")
    return "".join(lines), changes


def _process_file(filepath: Path) -> list[str]:
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))
    targets = _collect_targets(tree)
    if not targets:
        return []
    result, changes = _apply_changes(source, targets)
    try:
        ast.parse(result)
    except SyntaxError as e:
        return [f"SKIPPED: syntax error at line {e.lineno}: {e.msg}"]
    filepath.write_text(result, encoding="utf-8")
    return changes


def _process_directory(dirpath: Path) -> list[str]:
    all_changes: list[str] = []
    for pyfile in sorted(dirpath.rglob("*.py")):
        if "investigations" in pyfile.parts:
            continue
        try:
            changes = _process_file(pyfile)
        except (IndentationError, SyntaxError) as e:
            all_changes.append(f"\n{pyfile}: SKIPPED ({type(e).__name__}: {e})")
            continue
        if changes:
            all_changes.append(f"\n{pyfile}:")
            all_changes.extend(changes)
    return all_changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate missing docstrings.")
    parser.add_argument("path", help="File or directory to process")
    args = parser.parse_args()
    target = Path(args.path)
    if not target.exists():
        print(f"Error: {target} not found", file=sys.stderr)
        return 1
    if target.is_file():
        changes = _process_file(target)
    else:
        changes = _process_directory(target)
    if changes:
        print("Generated docstrings:")
        for line in changes:
            print(line)
        print(f"\nTotal: {sum(1 for c in changes if c.startswith('  L'))} functions")
    else:
        print("No missing docstrings found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
