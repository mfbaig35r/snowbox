"""snowbox FastMCP server — 21 tools for auditable Snowflake-connected Python execution."""

from __future__ import annotations

import os
import secrets
import socket
import subprocess
import time
from pathlib import Path

import pandas as pd
from fastmcp import FastMCP

from snowbox.connector import SnowflakeConnector
from snowbox.context import SNOWFLAKE_CONTEXT_CODE, SNOWFLAKE_REQUIRED_PACKAGES

# ── Configuration ─────────────────────────────────────────────────────────────

# Uses the same DATA_DIR as marimo-sandbox so runs land in the same store
DATA_DIR = (
    Path(os.environ.get("MARIMO_SANDBOX_DIR", Path.home() / ".marimo-sandbox"))
    .expanduser()
    .resolve()
)

# ── Singletons ────────────────────────────────────────────────────────────────

connector = SnowflakeConnector()  # lazy — no connection opened at import time

# ── Delegate to marimo-sandbox _impl_* functions ──────────────────────────────

import marimo_sandbox.server as _ms_server  # noqa: E402  (for .fn access on tool objects)
from marimo_sandbox.server import (  # noqa: E402
    _impl_approve_run,
    _impl_cancel_run,
    _impl_clean_environments,
    _impl_delete_run,
    _impl_diff_runs,
    _impl_get_run,
    _impl_get_run_outputs,
    _impl_list_artifacts,
    _impl_list_environments,
    _impl_list_pending_approvals,
    _impl_list_runs,
    _impl_purge_runs,
    _impl_read_artifact,
    _impl_rerun,
    _impl_run_python,
)

# ── Server ────────────────────────────────────────────────────────────────────

mcp = FastMCP(
    "snowbox",
    instructions=(
        "Execute Python code with Snowflake connectivity in auditable Marimo notebooks. "
        "Every run is saved as a human-readable .py notebook file. "
        "Use run_python to execute code — __snowflake__ is pre-injected so you can "
        "query Snowflake directly with __snowflake__.query(sql). "
        "Use snowflake_query to run SQL and preview results. "
        "Use snowflake_write to push a run artifact back to Snowflake. "
        "Use list_tables / describe_table to explore the Snowflake schema. "
        "Use check_setup to verify both the sandbox and Snowflake connectivity. "
        "Use open_notebook to open a run in the browser for interactive editing."
    ),
)

# ── Group A: Snowflake-specific tools ─────────────────────────────────────────


def _impl_snowflake_query(sql: str, limit: int = 1000) -> dict:
    try:
        df = connector.query(sql, limit)
        if df is None:
            # DDL / DML / SHOW-style statement — no result set
            return {"row_count": 0, "columns": [], "preview": [], "saved_to": None, "ddl": True}
        results_dir = DATA_DIR / "query_results"
        results_dir.mkdir(parents=True, exist_ok=True)
        result_path = results_dir / f"query_{secrets.token_hex(4)}.csv"
        df.to_csv(result_path, index=False)
        return {
            "row_count": len(df),
            "columns": list(df.columns),
            "preview": df.head(min(50, len(df))).to_dict(orient="records"),
            "saved_to": str(result_path),
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _impl_snowflake_write(
    run_id: str,
    artifact_path: str,
    table: str,
    mode: str = "append",
    database: str | None = None,
    schema: str | None = None,
) -> dict:
    full_path = DATA_DIR / "notebooks" / run_id / artifact_path
    if not full_path.exists():
        return {"error": f"Artifact not found: {full_path}"}
    suffix = full_path.suffix.lower()
    try:
        if suffix == ".csv":
            df: pd.DataFrame = pd.read_csv(full_path)
        elif suffix in (".parquet", ".pq"):
            df = pd.read_parquet(full_path)
        else:
            return {"error": f"Unsupported file type: {suffix}"}
        connector.write(df, table, mode, database=database, schema=schema)
        return {"status": "ok", "rows_written": len(df), "table": table}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _impl_list_tables(
    schema: str | None = None,
    database: str | None = None,
) -> dict:
    try:
        rows = connector.list_tables(schema=schema, database=database)
        return {"tables": rows}
    except Exception as exc:
        return {"error": str(exc)}


def _impl_describe_table(
    table: str,
    schema: str | None = None,
    database: str | None = None,
) -> dict:
    try:
        result = connector.describe_table(table, schema=schema, database=database)
        return {"table": table, "columns": result["columns"], "sample_rows": result["sample_rows"]}
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
def snowflake_query(sql: str, limit: int = 1000) -> dict:
    """
    Execute a SQL query against Snowflake and return the results.

    Saves the full result set as a CSV file in the shared data directory.

    Args:
        sql:    SQL statement to execute.
        limit:  Maximum rows to return (default 1000, 0 = no limit).

    Returns:
        row_count, columns, preview (first 50 rows), saved_to path — or error.
    """
    return _impl_snowflake_query(sql, limit)


@mcp.tool()
def snowflake_write(
    run_id: str,
    artifact_path: str,
    table: str,
    mode: str = "append",
    database: str | None = None,
    schema: str | None = None,
) -> dict:
    """
    Write a run artifact (CSV or Parquet) from a previous run back to Snowflake.

    Args:
        run_id:         The run_id whose artifact you want to upload.
        artifact_path:  Relative path within that run's notebook directory (e.g. "output.csv").
        table:          Destination Snowflake table name.
        mode:           "append" (default) or "overwrite".
        database:       Target database (overrides session default).
        schema:         Target schema (overrides session default).

    Returns:
        status, rows_written, table — or error.
    """
    return _impl_snowflake_write(run_id, artifact_path, table, mode, database=database, schema=schema)  # noqa: E501


@mcp.tool()
def list_tables(
    schema: str | None = None,
    database: str | None = None,
) -> dict:
    """
    List Snowflake tables visible to the configured role.

    Args:
        schema:    Filter to a specific schema (requires database when set).
        database:  Database to scope the listing.

    Returns:
        tables list with name, database_name, schema_name, kind — or error.
    """
    return _impl_list_tables(schema=schema, database=database)


@mcp.tool()
def describe_table(
    table: str,
    schema: str | None = None,
    database: str | None = None,
) -> dict:
    """
    Describe a Snowflake table: column definitions and 5 sample rows.

    Args:
        table:     Table name (may be fully qualified: db.schema.table).
        schema:    Schema override.
        database:  Database override.

    Returns:
        table, columns, sample_rows — or error.
    """
    return _impl_describe_table(table, schema=schema, database=database)


# ── Group B: Enhanced marimo-sandbox tools ────────────────────────────────────


def _inject_pep723_header(notebook_path: str, packages: list[str]) -> None:
    """Prepend PEP 723 inline script metadata to *notebook_path*.

    This tells ``marimo edit`` (via uv) which packages to install so the
    notebook opens with a working kernel instead of showing "kernel not found".
    """
    path = Path(notebook_path)
    if not path.exists():
        return
    content = path.read_text(encoding="utf-8")
    # Don't double-inject
    if "# /// script" in content:
        return
    # Pin marimo to the installed version so --sandbox doesn't upgrade it and
    # cause a notebook-format mismatch ("kernel not found" in the browser).
    try:
        import marimo as _marimo
        marimo_pin = f"marimo=={_marimo.__version__}"
    except Exception:
        marimo_pin = "marimo"
    pinned = [marimo_pin if p == "marimo" else p for p in packages]
    dep_lines = "\n".join(f'#     "{pkg}",' for pkg in pinned)
    header = (
        "# /// script\n"
        "# requires-python = \">=3.11\"\n"
        "# dependencies = [\n"
        f"{dep_lines}\n"
        "# ]\n"
        "# ///\n"
    )
    path.write_text(header + content, encoding="utf-8")


def _impl_sf_run_python(
    code: str,
    description: str = "Snowflake Python run",
    packages: list[str] | None = None,
    timeout_seconds: int = 60,
    sandbox: bool = False,
    async_mode: bool = False,
    dry_run: bool = False,
    require_approval: bool = False,
    parent_run_id: str | None = None,
) -> dict:
    packages = list(packages or [])
    for pkg in SNOWFLAKE_REQUIRED_PACKAGES:
        if pkg not in packages:
            packages.append(pkg)
    injected_code = SNOWFLAKE_CONTEXT_CODE + code
    result = _impl_run_python(
        code=injected_code,
        description=description,
        packages=packages,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
        async_mode=async_mode,
        dry_run=dry_run,
        require_approval=require_approval,
        parent_run_id=parent_run_id,
    )
    # Inject PEP 723 metadata so `marimo edit` can find the right kernel
    if notebook_path := result.get("notebook_path"):
        _inject_pep723_header(notebook_path, packages)
    return result


def _impl_sf_check_setup() -> dict:
    base: dict = _ms_server.check_setup.fn()
    sf_test = connector.test()
    sf_info: dict = {**sf_test}
    # Warn when no default database is configured — unqualified DDL will fail
    if not connector._database:
        sf_info["warning"] = (
            "SNOWFLAKE_DATABASE is not set. Unqualified DDL (CREATE TABLE, etc.) will fail. "
            "Use fully qualified names (database.schema.table) or set SNOWFLAKE_DATABASE."
        )
    else:
        sf_info["database"] = connector._database
    if connector._schema:
        sf_info["schema"] = connector._schema
    base["snowflake"] = sf_info
    return base


@mcp.tool()
def run_python(
    code: str,
    description: str = "Snowflake Python run",
    packages: list[str] | None = None,
    timeout_seconds: int = 60,
    sandbox: bool = False,
    async_mode: bool = False,
    dry_run: bool = False,
    require_approval: bool = False,
    parent_run_id: str | None = None,
) -> dict:
    """
    Execute Python code with Snowflake connectivity pre-injected.

    The ``__snowflake__`` helper is automatically available in your code::

        df = __snowflake__.query("SELECT * FROM MY_TABLE LIMIT 10")
        __snowflake__.write(df, "OUTPUT_TABLE")

    The required Snowflake packages (snowflake-connector-python, pyarrow, pandas)
    are automatically added to the packages list.

    Args:
        code:             Python source code to run.
        description:      Short label for this run.
        packages:         Additional PyPI packages to install before running.
        timeout_seconds:  Max execution time in seconds (default 60).
        sandbox:          Run inside Docker with --network=none (requires Docker).
        async_mode:       Launch in background; poll with get_run().
        dry_run:          Return static risk analysis only — do not execute.
        require_approval: Block on critical risk patterns; confirm via approve_run().
        parent_run_id:    Link this run to a parent for lineage tracking.

    Returns:
        run_id, status, stdout, stderr, error, duration_ms, notebook_path,
        view_command, code_hash, artifacts, risk_findings, packages_installed.
    """
    return _impl_sf_run_python(
        code=code,
        description=description,
        packages=packages,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
        async_mode=async_mode,
        dry_run=dry_run,
        require_approval=require_approval,
        parent_run_id=parent_run_id,
    )


@mcp.tool()
def check_setup() -> dict:
    """
    Check that the sandbox environment and Snowflake connectivity are ready.

    Returns the data directory, marimo/Docker availability, run count, setup
    notes, and a Snowflake connectivity test result under the "snowflake" key.
    """
    return _impl_sf_check_setup()


# ── Group B passthroughs ──────────────────────────────────────────────────────


def _port_is_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _free_port(port: int) -> None:
    """Kill any process bound to *port* so marimo can claim it."""
    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        for pid_str in result.stdout.strip().split():
            try:
                os.kill(int(pid_str), 15)  # SIGTERM
            except (ProcessLookupError, ValueError):
                pass
        if result.stdout.strip():
            time.sleep(0.75)  # give the process time to die
    except Exception:
        pass


@mcp.tool()
def open_notebook(run_id: str, port: int = 2718) -> dict:
    """
    Open a run's Marimo notebook in the interactive editor.

    Activates the run's cached virtualenv (which already has all required
    packages installed) before launching ``marimo edit --no-sandbox``.

    ``--no-sandbox`` is required: notebooks contain a PEP 723 ``# /// script``
    header that marimo auto-detects and uses to trigger uv sandbox mode.
    That uv-managed environment conflicts with our pre-activated venv and
    causes "kernel not found". ``--no-sandbox`` overrides the auto-detection
    and uses the venv's Python for the kernel.

    Any existing process already occupying *port* is killed first so the new
    server always serves the requested run.

    Args:
        run_id:  The run ID returned by run_python.
        port:    Local port for the Marimo server (default 2718).

    Returns:
        success, url, pid, notebook_path, message — or success=False + error.
    """
    run_result = _impl_get_run(run_id, include_code=False)
    if run_result.get("error"):
        return {"success": False, "error": run_result["error"]}

    notebook_path_str = run_result.get("notebook_path") or ""
    if not notebook_path_str:
        return {"success": False, "error": f"Run {run_id!r} not found or has no notebook path"}
    notebook_path = Path(notebook_path_str)
    if not notebook_path.exists():
        return {"success": False, "error": f"Notebook file not found at {notebook_path}"}

    # Kill any existing server on this port so we don't accidentally return
    # success against a stale process serving a different notebook.
    _free_port(port)

    # Activate the run's cached virtualenv so the kernel has all packages.
    # Falls back to the current environment if the venv doesn't exist.
    env = os.environ.copy()
    env_hash = run_result.get("env_hash") or ""
    if env_hash:
        venv_dir = DATA_DIR / "envs" / env_hash
        venv_bin = venv_dir / "bin"
        if venv_bin.exists():
            env["VIRTUAL_ENV"] = str(venv_dir)
            env["PATH"] = str(venv_bin) + os.pathsep + env.get("PATH", "")

    process = subprocess.Popen(
        ["marimo", "edit", str(notebook_path), "--port", str(port), "--no-token", "--no-sandbox"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raw_err = process.stderr.read() if process.stderr else b""
            stderr_text = raw_err.decode(errors="replace")[:400]
            return {"success": False, "error": f"marimo exited: {stderr_text}"}
        if _port_is_open(port):
            return {
                "success": True,
                "url": f"http://127.0.0.1:{port}",
                "pid": process.pid,
                "notebook_path": str(notebook_path),
                "message": "Notebook is open. Navigate to the URL to view it.",
            }
        time.sleep(0.25)

    process.terminate()
    return {"success": False, "error": "marimo did not become ready within 15 seconds"}


@mcp.tool()
def list_runs(limit: int = 20, status: str | None = None, offset: int = 0) -> dict:
    """List recent runs with status, description, and timing."""
    return _impl_list_runs(limit=limit, status=status, offset=offset)


@mcp.tool()
def get_run(
    run_id: str,
    include_code: bool = True,
    include_notebook_source: bool = False,
) -> dict:
    """Full details of a specific run, including code and output."""
    return _impl_get_run(
        run_id=run_id,
        include_code=include_code,
        include_notebook_source=include_notebook_source,
    )


@mcp.tool()
def delete_run(run_id: str, delete_files: bool = True) -> dict:
    """Remove a run's record and notebook files."""
    return _impl_delete_run(run_id=run_id, delete_files=delete_files)


@mcp.tool()
def rerun(
    run_id: str,
    code: str | None = None,
    description: str | None = None,
    timeout_seconds: int = 60,
    sandbox: bool = False,
    packages: list[str] | None = None,
) -> dict:
    """Re-execute a previous run's code, optionally with modifications."""
    return _impl_rerun(
        run_id=run_id,
        code=code,
        description=description,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
        packages=packages,
    )


@mcp.tool()
def purge_runs(
    older_than_days: int,
    delete_files: bool = True,
    dry_run: bool = False,
) -> dict:
    """Bulk-delete runs older than N days."""
    return _impl_purge_runs(
        older_than_days=older_than_days,
        delete_files=delete_files,
        dry_run=dry_run,
    )


@mcp.tool()
def list_artifacts(run_id: str) -> dict:
    """List user-created files in a run's notebook directory."""
    return _impl_list_artifacts(run_id=run_id)


@mcp.tool()
def read_artifact(
    run_id: str,
    artifact_path: str,
    max_size_bytes: int = 5_000_000,
) -> dict:
    """Read the content of an artifact file from a run."""
    return _impl_read_artifact(
        run_id=run_id,
        artifact_path=artifact_path,
        max_size_bytes=max_size_bytes,
    )


@mcp.tool()
def get_run_outputs(run_id: str) -> dict:
    """Retrieve the structured __outputs__ dict from a run."""
    return _impl_get_run_outputs(run_id=run_id)


@mcp.tool()
def approve_run(token: str, reason: str = "") -> dict:
    """Confirm a blocked run and execute it."""
    return _impl_approve_run(token=token, reason=reason)


@mcp.tool()
def list_pending_approvals() -> dict:
    """List runs awaiting approval."""
    return _impl_list_pending_approvals()


@mcp.tool()
def cancel_run(run_id: str) -> dict:
    """Cancel a running async run."""
    return _impl_cancel_run(run_id=run_id)


@mcp.tool()
def diff_runs(run_id: str, compare_to: str | None = None) -> dict:
    """Compare two runs: code, env, status, artifacts, and outputs."""
    return _impl_diff_runs(run_id=run_id, compare_to=compare_to)


@mcp.tool()
def list_environments() -> dict:
    """List cached virtual environments."""
    return _impl_list_environments()


@mcp.tool()
def clean_environments(older_than_days: int = 90) -> dict:
    """Delete old cached virtual environments."""
    return _impl_clean_environments(older_than_days=older_than_days)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()
