# Changelog

## [0.2.0] - 2026-04-17

### Security
- **SQL injection protection** — `_validate_identifier()` validates all user-supplied
  database, schema, and table names before interpolation into SHOW/DESCRIBE/SELECT
  statements. Rejects anything outside `[a-zA-Z0-9_.]`.

### Fixed
- **`sandbox=True` now blocked** for Snowflake runs with a clear error message.
  The `__snowflake__` context requires network access to reach Snowflake, which
  is incompatible with Docker's `--network=none`.
- **Type annotation bug** in `context.py` — `database: str = None` corrected to
  `database: str | None = None` (same for `schema`)
- **Duplicated `_inject_pep723_header`** removed from `server.py` — now imported
  from `marimo_sandbox.server`
- **Broken `check_setup` test** — updated to patch `_impl_check_setup` directly
  instead of nonexistent `_ms_server`

### Added
- 10 new unit tests: identifier validation (8), sandbox blocking (2)
- LICENSE file (MIT)
- `pyproject.toml` metadata: description, readme, license, authors, URLs
- `tests/__init__.py`

### Changed
- README URLs updated from placeholder `your-org` to actual repo

## [0.1.0] - 2026-04-15

### Added
- Initial release of snowbox
- `SnowflakeConnector` with lazy connection initialization and key-pair / password auth
- `SNOWFLAKE_CONTEXT_CODE` injected into every `run_python` call, exposing `__snowflake__`
- FastMCP server with 21 tools:
  - 4 Snowflake-specific tools: `snowflake_query`, `snowflake_write`, `list_tables`, `describe_table`
  - Enhanced `run_python` (injects Snowflake context + required packages)
  - Enhanced `check_setup` (includes Snowflake connectivity test)
  - 15 passthrough tools delegating to `marimo-sandbox` `_impl_*` functions
- CI: lint (ruff), typecheck (mypy), test matrix (Python 3.11–3.13), integration-test gate
