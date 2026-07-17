"""DMR Cap+ Monitor — single source of truth for the project version.

Bump ``__version__`` on every user-visible change. Semver (major.minor.patch):
  * patch — bug fix or internal cleanup, no UI/API change
  * minor — new feature, backwards compatible
  * major — breaking change to the CLI / API / event schema

The number is surfaced at ``/api/version`` and rendered in the dashboard
footer so it's obvious from the UI which build is running.
"""
from __future__ import annotations

__version__ = "0.26.3"
__build_date__ = "2026-07-17"
