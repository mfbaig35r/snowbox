"""SnowflakeConnector — lazy singleton wrapping snowflake-connector-python."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd


def _validate_identifier(name: str) -> None:
    """Raise ValueError if *name* contains characters unsafe for SQL identifiers.

    Snowflake SHOW/DESCRIBE don't support parameterized identifiers, so we
    validate that user-supplied database/schema/table names contain only safe
    characters before interpolating them into SQL.
    """
    import re
    if not re.fullmatch(r'[a-zA-Z0-9_.]+', name):
        raise ValueError(
            f"Invalid identifier: {name!r}. "
            "Only alphanumeric characters, underscores, and dots are allowed."
        )


class SnowflakeConnector:
    """Lazy Snowflake connection.  Reads env vars at construction time but
    does NOT open a connection until the first query is issued."""

    def __init__(self) -> None:
        self._account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        self._user = os.environ.get("SNOWFLAKE_USER", "")
        self._warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
        self._database = os.environ.get("SNOWFLAKE_DATABASE")
        self._schema = os.environ.get("SNOWFLAKE_SCHEMA")
        self._role = os.environ.get("SNOWFLAKE_ROLE")
        self._private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
        self._private_key_passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        self._password = os.environ.get("SNOWFLAKE_PASSWORD")
        self._conn: Any = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "account": self._account,
            "user": self._user,
        }
        for attr, key in [
            (self._warehouse, "warehouse"),
            (self._database, "database"),
            (self._schema, "schema"),
            (self._role, "role"),
        ]:
            if attr:
                kwargs[key] = attr

        if self._private_key_path:
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
            kwargs["private_key_file"] = self._private_key_path
            if self._private_key_passphrase:
                kwargs["private_key_file_pwd"] = self._private_key_passphrase
        elif self._password:
            kwargs["password"] = self._password
        else:
            raise RuntimeError(
                "No Snowflake auth configured. "
                "Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD."
            )
        return kwargs

    def _connect(self) -> None:
        import snowflake.connector  # lazy import — driver not needed at module load

        self._conn = snowflake.connector.connect(**self._build_connect_kwargs())

    def _get_conn(self) -> Any:
        if self._conn is None or self._conn.is_closed():
            self._connect()
        return self._conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def test(self) -> dict[str, str]:
        """Return ``{"status": "ok"}`` or ``{"status": "error", "detail": ...}``."""
        try:
            cur = self._get_conn().cursor()
            cur.execute("SELECT 1")
            return {"status": "ok"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}

    def query(self, sql: str, limit: int = 1000) -> pd.DataFrame | None:
        """Execute *sql* and return results as a DataFrame (capped at *limit* rows).

        Returns ``None`` for DDL / DML statements (CREATE, INSERT, USE, etc.) that
        produce no result set.  Falls back to ``fetchall()`` for SHOW / DESCRIBE
        commands that have a description but don't support ``fetch_pandas_all()``.
        """
        cur = self._get_conn().cursor()
        cur.execute(sql)
        if cur.description is None:
            cur.close()
            return None
        try:
            df: pd.DataFrame = cur.fetch_pandas_all()
        except Exception:
            # SHOW / DESCRIBE commands have a description but don't support
            # fetch_pandas_all() — fall back to plain fetchall()
            rows = cur.fetchall()
            col_names = [d[0] for d in (cur.description or [])]
            df = pd.DataFrame(rows, columns=col_names)
        return df if limit == 0 else df.head(limit)

    def list_tables(
        self,
        schema: str | None = None,
        database: str | None = None,
    ) -> list[dict[str, str]]:
        """Return a list of table dicts with keys: name, database_name, schema_name, kind."""
        if database and schema:
            _validate_identifier(database)
            _validate_identifier(schema)
            sql = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        else:
            sql = "SHOW TABLES"
        cur = self._get_conn().cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        desc = cur.description or []
        col_names = [d[0].lower() for d in desc]
        results: list[dict[str, str]] = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            results.append(
                {
                    "name": str(row_dict.get("name", "")),
                    "database_name": str(row_dict.get("database_name", "")),
                    "schema_name": str(row_dict.get("schema_name", "")),
                    "kind": str(row_dict.get("kind", "")),
                }
            )
        return results

    def describe_table(
        self,
        table: str,
        schema: str | None = None,
        database: str | None = None,
    ) -> dict[str, Any]:
        """Return column info and 5 sample rows for *table*."""
        parts = []
        if database:
            _validate_identifier(database)
            parts.append(database)
        if schema:
            _validate_identifier(schema)
            parts.append(schema)
        _validate_identifier(table)
        parts.append(table)
        fqn = ".".join(parts)

        conn = self._get_conn()

        # Column info
        cur = conn.cursor()
        cur.execute(f"DESCRIBE TABLE {fqn}")
        col_rows = cur.fetchall()
        desc = cur.description or []
        col_names = [d[0].lower() for d in desc]
        columns = [dict(zip(col_names, row)) for row in col_rows]

        # Sample data
        cur2 = conn.cursor()
        cur2.execute(f"SELECT * FROM {fqn} LIMIT 5")
        sample_rows_raw = cur2.fetchall()
        sample_desc = cur2.description or []
        sample_col_names = [d[0] for d in sample_desc]
        sample_rows = [dict(zip(sample_col_names, row)) for row in sample_rows_raw]

        return {"columns": columns, "sample_rows": sample_rows}

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        mode: str = "append",
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        """Write *df* to Snowflake *table*.

        Pass *database* and/or *schema* to target a specific location when the
        session has no default database configured.
        """
        from snowflake.connector.pandas_tools import write_pandas  # lazy import

        kwargs: dict[str, Any] = {
            "auto_create_table": True,
            "overwrite": (mode == "overwrite"),
        }
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema

        write_pandas(self._get_conn(), df, table.upper(), **kwargs)

    def close(self) -> None:
        """Close the underlying connection if open."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
