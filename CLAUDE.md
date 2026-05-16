# Project instructions for Claude

## Every change must bump the version

Treat this repo as a real shipping project. **No commit should land
without a version bump and a changelog entry.** Specifically, every PR
or push must update:

1. `backend/__init__.py` — bump `__version__` and update `__build_date__`
   to today's date.
2. `CHANGELOG.md` — add a new section at the top following the existing
   format (`## [X.Y.Z] — YYYY-MM-DD` with `### Added` / `### Changed` /
   `### Fixed` / `### Removed` subsections as appropriate).

### Semver rules (already documented in `backend/__init__.py`)

* **patch** (`0.11.0` → `0.11.1`) — bug fix or internal cleanup, no
  UI/API/event-schema change.
* **minor** (`0.11.0` → `0.12.0`) — new feature, backwards compatible.
* **major** (`0.11.0` → `1.0.0`) — breaking change to CLI / HTTP API /
  event schema (also bump `EVENT_SCHEMA_VERSION` in `backend/models.py`).

If a single commit/PR bundles a fix AND a feature, take the higher
bump (minor wins over patch).

### Workflow per change

1. Make the code changes.
2. Bump `__version__` and `__build_date__` in `backend/__init__.py`.
3. Prepend a new section to `CHANGELOG.md` describing what changed and why.
4. Run `.venv/bin/pytest tests/ -q` — must be green before commit.
5. Commit with a message that names the same version (e.g.
   `fix: ... → v0.11.1`).

The dashboard footer + `/api/version` surface the version, so a missing
bump is immediately visible to the operator running the build.
