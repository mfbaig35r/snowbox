# Changelog

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
