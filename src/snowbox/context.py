"""Context code injected into every snowbox run_python call."""

SNOWFLAKE_CONTEXT_CODE: str = """\
# ── __snowflake__ context (injected by snowflake-sandbox) ──────────────────
import os as _sf_os
import snowflake.connector as _sf_connector
from snowflake.connector.pandas_tools import write_pandas as _sf_write_pandas
import pandas as _sf_pd

class _SnowflakeContext:
    def __init__(self) -> None:
        self._conn = None

    def _connect(self) -> None:
        kwargs: dict = dict(
            account=_sf_os.environ["SNOWFLAKE_ACCOUNT"],
            user=_sf_os.environ["SNOWFLAKE_USER"],
        )
        for k, env in [("warehouse","SNOWFLAKE_WAREHOUSE"),("database","SNOWFLAKE_DATABASE"),
                       ("schema","SNOWFLAKE_SCHEMA"),("role","SNOWFLAKE_ROLE")]:
            v = _sf_os.environ.get(env)
            if v:
                kwargs[k] = v
        pk = _sf_os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
        if pk:
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
            kwargs["private_key_file"] = pk
            pp = _sf_os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            if pp:
                kwargs["private_key_file_pwd"] = pp
        else:
            kwargs["password"] = _sf_os.environ["SNOWFLAKE_PASSWORD"]
        self._conn = _sf_connector.connect(**kwargs)

    def _get_conn(self):
        if self._conn is None or self._conn.is_closed():
            self._connect()
        return self._conn

    def query(self, sql: str, limit: int = 10000):
        cur = self._get_conn().cursor()
        cur.execute(sql)
        if cur.description is None:
            cur.close()
            return None
        return cur.fetch_pandas_all()

    def write(self, df: "_sf_pd.DataFrame", table: str, mode: str = "append",
              database: str | None = None, schema: str | None = None) -> None:
        kwargs = dict(auto_create_table=True, overwrite=(mode == "overwrite"))
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema
        _sf_write_pandas(self._get_conn(), df, table.upper(), **kwargs)

__snowflake__ = _SnowflakeContext()
# ───────────────────────────────────────────────────────────────────────────
"""

# Packages automatically added to every snowflake run's package list
SNOWFLAKE_REQUIRED_PACKAGES: list[str] = [
    "marimo",
    "snowflake-connector-python[pandas]",
    "pyarrow",
    "pandas",
]
