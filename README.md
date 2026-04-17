# snowbox

**snowbox** is a FastMCP server that adds Snowflake connectivity to
[marimo-sandbox](https://github.com/your-org/marimo-sandbox) — giving AI agents
auditable Python execution against Snowflake data at zero Snowflake compute cost.

## Features

- **`__snowflake__` context injection** — every `run_python` call automatically receives a
  pre-configured `__snowflake__` helper with `.query()` and `.write()` methods
- **4 Snowflake-specific tools** — `snowflake_query`, `snowflake_write`, `list_tables`,
  `describe_table`
- **17 marimo-sandbox tools** — all run management, artifact, approval, and environment tools
  pass through unchanged (enhanced `run_python` and `check_setup`)
- **Key-pair and password auth** — choose via env vars; no plaintext secrets in code
- **Zero compute overhead** — queries run through the Snowflake connector locally; no
  Snowflake Virtual Warehouse is spun up for connector operations

## Installation

```bash
pip install snowbox
```

Or for development:

```bash
git clone https://github.com/mfbaig35r/snowbox
cd snowbox
pip install -e ".[dev]"
```

## Quick start

### MCP configuration

Add to your Claude Desktop / MCP client config:

```json
{
  "mcpServers": {
    "snowbox": {
      "command": "snowbox",
      "env": {
        "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
        "SNOWFLAKE_USER": "myuser",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "ANALYTICS",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "SNOWFLAKE_PRIVATE_KEY_PATH": "/path/to/rsa_key.p8"
      }
    }
  }
}
```

### Using `__snowflake__` in code

```python
# In any run_python call, __snowflake__ is pre-injected:
df = __snowflake__.query("SELECT * FROM ORDERS LIMIT 100")
print(df.head())

# Write results back
__snowflake__.write(df, "ORDERS_COPY", mode="overwrite")
```

## Authentication

snowbox supports two auth methods, controlled by environment variables:

| Method | Required env vars |
|--------|------------------|
| Key-pair (preferred) | `SNOWFLAKE_PRIVATE_KEY_PATH`, optionally `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` |
| Password | `SNOWFLAKE_PASSWORD` |

Always required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`

Optional: `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`

## Tools reference

### Snowflake tools

| Tool | Description |
|------|-------------|
| `snowflake_query(sql, limit)` | Execute SQL, preview results, save CSV |
| `snowflake_write(run_id, artifact_path, table, mode)` | Push a run artifact to Snowflake |
| `list_tables(schema, database)` | List accessible tables |
| `describe_table(table, schema, database)` | Column definitions + 5 sample rows |

### Run management (passthrough from marimo-sandbox)

| Tool | Description |
|------|-------------|
| `run_python(code, ...)` | Execute Python with `__snowflake__` pre-injected |
| `check_setup()` | Verify marimo, Docker, and Snowflake connectivity |
| `open_notebook(run_id)` | Open a run in the browser |
| `list_runs` / `get_run` | Browse run history |
| `rerun` / `delete_run` / `purge_runs` | Run lifecycle management |
| `list_artifacts` / `read_artifact` | Access run output files |
| `approve_run` / `list_pending_approvals` | Human-in-the-loop approval flow |
| `cancel_run` | Cancel async runs |
| `diff_runs` | Compare two runs |
| `list_environments` / `clean_environments` | Manage cached virtualenvs |

## Development

```bash
# Lint
ruff check src/ tests/

# Type-check
mypy src/snowbox/ --ignore-missing-imports

# Unit tests (no Snowflake credentials required)
pytest tests/ -m "not slow" --timeout=30 -v

# Integration tests (requires real credentials in env)
pytest tests/ -m slow -v
```

## License

MIT
