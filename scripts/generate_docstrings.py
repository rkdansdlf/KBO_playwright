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


def _param_desc(name: str) -> str:
    readable = name.replace("_", " ").strip()
    if readable == "db":
        return "Database session."
    if readable == "db session":
        return "Database session."
    if readable.endswith(" id"):
        return f"{readable.replace(' id', '').replace('_', ' ').title()} ID."
    if readable.endswith(" url"):
        return f"{readable.replace(' url', '').replace('_', ' ').title()} URL."
    if readable.endswith(" dir"):
        return f"{readable.replace(' dir', '').replace('_', ' ').title()} directory path."
    if readable.endswith(" path"):
        return f"{readable.replace(' path', '').replace('_', ' ').title()} file path."
    if readable == "save":
        return "Whether to persist the results."
    if readable == "headless":
        return "Whether to run the browser in headless mode."
    if readable == "dry run":
        return "If True, performs a dry run without persisting changes."
    if readable == "force":
        return "If True, forces the operation even if data already exists."
    if readable == "verbose":
        return "If True, enables verbose logging output."
    if readable == "year":
        return "Season year."
    if readable == "month":
        return "Month number (1-12)."
    if readable == "date":
        return "Target date in YYYYMMDD format."
    if readable == "season":
        return "Season year."
    if readable == "concurrency":
        return "Maximum number of concurrent requests."
    if readable == "timeout":
        return "Timeout in seconds."
    if readable == "output":
        return "Output file path."
    if readable == "input":
        return "Input file or directory path."
    return f"{readable.replace('_', ' ').title()}."


def _return_desc(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    returns = node.returns
    if returns is None:
        return None
    if isinstance(returns, ast.Constant) and returns.value is None:
        return None
    if isinstance(returns, ast.Constant) and returns.value is ...:
        return None
    if isinstance(returns, ast.Name):
        name = returns.id
        if name == "None":
            return None
        if name == "bool":
            return "True if successful, False otherwise."
        if name == "int":
            return "Integer result."
        if name == "str":
            return "String result."
        if name == "dict":
            return "Dictionary result."
        if name == "list":
            return "List of results."
        if name == "tuple":
            return "Tuple result."
        if name == "Path":
            return "Path object."
        return f"{name} instance."
    if isinstance(returns, ast.Subscript):
        if isinstance(returns.value, ast.Name):
            if returns.value.id == "list":
                return "List of results."
            if returns.value.id == "dict":
                return "Dictionary result."
            if returns.value.id == "tuple":
                return "Tuple result."
            if returns.value.id == "Sequence":
                return "Sequence of results."
            if returns.value.id == "Optional":
                return "The result if found, None otherwise."
            if returns.value.id == "Callable":
                return "Callable object."
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


def _process_file(filepath: Path) -> list[str]:
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))
    targets: list[tuple[int, str, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            if _has_docstring(node):
                continue
            docstring = _generate_class_docstring(node)
            quoted = f'"""{docstring}"""'
            body_lineno = node.body[0].lineno
            targets.append((node.lineno, node.name, quoted, body_lineno))
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
        quoted = f'"""{docstring}"""'
        body_lineno = node.body[0].lineno
        targets.append((node.lineno, node.name, quoted, body_lineno))
    if not targets:
        return []
    targets.sort(key=lambda t: t[0], reverse=True)
    lines = source.splitlines(keepends=True)
    changes: list[str] = []
    for lineno, name, quoted, body_lineno in targets:
        indent = _get_def_indent(lines, lineno)
        body_indent = indent + "    "
        insert_pos = _find_body_start(lines, lineno, body_lineno)
        if body_lineno <= lineno:
            lines.insert(lineno, f"{body_indent}{quoted}\n")
        else:
            lines.insert(insert_pos, f"{body_indent}{quoted}\n")
        first_line = quoted.split("\n")[0].removeprefix('"""').removesuffix('"""')
        changes.append(f"  L{lineno}: {name} -> {first_line}")
    result = "".join(lines)
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
