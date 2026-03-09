"""Shared validation helpers for the API.

Centralizes path and request validation so router and service layers
use the same rules (e.g. repo_path under allowed_base).
"""

from __future__ import annotations

from pathlib import Path


def resolve_repo_path_under_base(path: str, allowed_base: Path) -> Path:
    """Resolve and validate that repo_path is under the allowed base directory.

    Performs:
    - Resolve the path to absolute.
    - Reject path traversal (no \"..\" in path parts).
    - Require the result to be absolute.
    - Require the result to be under allowed_base (is_relative_to).

    Raises
    ------
    ValueError
        If path contains \"..\", is not absolute after resolve, or is
        outside allowed_base.

    Returns
    ------
    Path
        The resolved absolute path under allowed_base.
    """
    resolved = Path(path).resolve()
    if ".." in Path(path).parts:
        raise ValueError("repo_path must not contain '..' path segments")
    if not resolved.is_absolute():
        raise ValueError("repo_path must be an absolute path")
    allowed = Path(allowed_base).resolve()
    if not resolved.is_relative_to(allowed):
        raise ValueError(
            f"Repository path {resolved} is outside the allowed base directory {allowed}"
        )
    return resolved
