"""AST-based docstring generator for KBO Playwright codebase."""

from __future__ import annotations
import argparse
import ast
import re
import sys
from pathlib import Path

VERB_MAP = {
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
    "setup": "Sets up",
    "configure": "Configures",
    "reset": "Resets",
    "clear": "Clears",
    "cleanup": "Cleans up",
    "purge": "Purges",
    "archive": "Archives",
    "collect": "Collects",
    "gather": "Gathers",
    "prepare": "Prepares",
    "ensure": "Ensures",
    "wait": "Waits for",
    "expect": "Expects",
}

MAGIC_DOCSTRINGS = {
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
    "__aenter__": "Enters the async runtime context.",
    "__aexit__": "Exits the async runtime context.",
}

SKIP_PREFIXES = ("test_",)
CLI_MAIN_RE = re.compile(r"^main(_[a-z0-9_]+)?$")


def _strip_self(args):
    if args and args[0] in ("self", "cls"):
        return args[1:]
    return args


def _humanize(name):
    words = name.replace("-", "_").split("_")
    return " ".join(words).lower()


def _param_desc(name):
    readable = _humanize(name)
    if readable == "db":
        return "Database session."
    if readable == "db session":
        return "Database session."
    if readable == "session":
        return "Database session."
    if readable.endswith(" id"):
        return f"{_humanize(name[:-3]).title()} ID."
    if readable.endswith(" url"):
        return f"{_humanize(name[:-4]).title()} URL."
    if readable.endswith(" dir"):
        return f"{_humanize(name[:-4]).title()} directory path."
    if readable.endswith(" path"):
        return f"{_humanize(name[:-5]).title()} file path."
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
    if readable == "html":
        return "Raw HTML content."
    if readable == "text":
        return "Input text content."
    if readable == "data":
        return "Data payload to process."
    if readable == "payload":
        return "Data payload to process."
    if readable == "url":
        return "Target URL."
    if readable == "pool":
        return "AsyncPlaywrightPool instance."
    if readable == "page":
        return "Playwright page object."
    if readable == "logger":
        return "Logger instance."
    if readable == "query":
        return "Database query."
    if readable == "result":
        return "Operation result."
    if readable == "results":
        return "List of operation results."
    if readable == "config":
        return "Configuration object."
    if readable == "options":
        return "Options dictionary."
    if readable == "params":
        return "Parameters dictionary."
    if readable == "kwargs":
        return "Additional keyword arguments."
    if readable == "args":
        return "Additional positional arguments."
    if readable == "player id":
        return "KBO player ID."
    if readable == "game id":
        return "KBO game ID."
    if readable == "team code":
        return "Team code identifier."
    if readable == "league":
        return "League identifier."
    if readable == "series":
        return "Series identifier."
    if readable == "category":
        return "Category identifier."
    if readable == "source":
        return "DataSource instance."
    if readable == "snapshot":
        return "Raw snapshot content."
    if readable == "events":
        return "List of events."
    if readable == "standings":
        return "Standings data."
    if readable == "stats":
        return "Statistics data."
    if readable == "trend":
        return "Trend data."
    if readable == "matrix":
        return "Matrix data."
    if readable == "runners":
        return "Base runners state."
    if readable == "score":
        return "Score value."
    if readable == "inning":
        return "Inning number."
    if readable == "outs":
        return "Number of outs."
    if readable == "wins":
        return "Number of wins."
    if readable == "losses":
        return "Number of losses."
    if readable == "streak":
        return "Streak information."
    if readable == "rankings":
        return "Rankings data."
    if readable == "matchups":
        return "Matchup data."
    if readable == "splits":
        return "Split statistics."
    if readable == "defense":
        return "Defensive statistics."
    return f"{readable.title()}."


def _return_desc(node):
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
            return "True if the condition is met, False otherwise."
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
        if name == "bytes":
            return "Bytes result."
        return f"{name} instance."
    if isinstance(returns, ast.Subscript):
        if isinstance(returns.value, ast.Name):
            if returns.value.id == "list":
                return "List of results."
            if returns.value.id == "dict":
                return "Dictionary mapping."
            if returns.value.id == "tuple":
                return "Tuple result."
            if returns.value.id == "Sequence":
                return "Sequence of results."
            if returns.value.id == "Optional":
                return "The result if found, None otherwise."
            if returns.value.id == "Callable":
                return "Callable object."
            if returns.value.id == "Generator":
                return "Generator yielding results."
            if returns.value.id == "Iterator":
                return "Iterator yielding results."
            if returns.value.id == "Awaitable":
                return "Awaitable result."
    return "The result of the operation."


def _infer_verb(name):
    parts = name.split("_")
    for part in parts:
        if part in VERB_MAP:
            return VERB_MAP[part]
    return None


def _noun_from_name(name, verb):
    verb_parts = set()
    if verb:
        for part in name.split("_"):
            if part in VERB_MAP:
                verb_parts.add(part)
    parts = [p for p in name.split("_") if p and p not in verb_parts]
    if not parts:
        return _humanize(name).title()
    return " ".join(parts).replace("-", " ").title()


def _generate_function_docstring(node):
    name = node.name
    if name in MAGIC_DOCSTRINGS:
        return MAGIC_DOCSTRINGS[name]
    args = [a.arg for a in node.args.args]
    args = _strip_self(args)
    if CLI_MAIN_RE.match(name):
        return "Main entry point for this CLI command."
    verb = _infer_verb(name)
    noun = _noun_from_name(name, verb)
    if verb:
        summary = f"{verb} {noun.lower()}."
    elif name.startswith("is_"):
        property_name = _humanize(name[3:])
        summary = f"Returns whether the {property_name}."
    elif name.startswith("has_") or name.startswith("can_"):
        property_name = _humanize(name[4:])
        summary = f"Returns whether the {property_name}."
    elif name.startswith("should_"):
        property_name = _humanize(name[7:])
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


def _has_docstring(node):
    return (
        isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    )


def _process_file(filepath):
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))
    targets = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith(SKIP_PREFIXES):
            continue
        if _has_docstring(node):
            continue
        docstring = _generate_function_docstring(node)
        quoted = f'"""{docstring}"""'
        body_lineno = node.body[0].lineno
        targets.append((node.lineno, node.name, quoted, body_lineno))
    if not targets:
        return []
    targets.sort(key=lambda t: t[0], reverse=True)
    lines = source.splitlines(keepends=True)
    changes = []
    for lineno, name, quoted, body_lineno in targets:
        if body_lineno <= lineno:
            def_line = lines[lineno - 1]
            def_indent = " " * (len(def_line) - len(def_line.lstrip()))
            body_indent = def_indent + "    "
            lines.insert(lineno, f"{body_indent}{quoted}\n")
        else:
            body_line = lines[body_lineno - 1]
            body_indent = " " * (len(body_line) - len(body_line.lstrip()))
            insert_pos = body_lineno - 1
            lines.insert(insert_pos, f"{body_indent}{quoted}\n")
        first_line = quoted.split("\n")[0].strip('"""')
        changes.append(f"  L{lineno}: {name} -> {first_line}")
    source = "".join(lines)
    filepath.write_text(source, encoding="utf-8")
    return changes


def _process_directory(dirpath):
    all_changes = []
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


def main():
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
