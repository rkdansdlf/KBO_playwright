import logging

logger = logging.getLogger(__name__)
"""Safe print utility for Windows console encoding issues."""


def safe_print(*args, **kwargs):
    """
    Print function that handles Unicode encoding errors on Windows.
    Falls back to ASCII representation if encoding fails.
    """
    try:
        logger.info(*args, **kwargs)
    except UnicodeEncodeError:
        # Convert all args to strings and encode/decode with error handling
        safe_args = []
        for arg in args:
            try:
                safe_args.append(str(arg).encode("ascii", "replace").decode("ascii"))
            except Exception:
                safe_args.append(repr(arg))
        logger.exception(*safe_args, **kwargs)
