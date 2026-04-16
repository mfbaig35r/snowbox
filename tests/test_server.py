"""Unit tests for snowbox server _impl_* functions (all I/O mocked)."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from snowbox.context import SNOWFLAKE_CONTEXT_CODE, SNOWFLAKE_REQUIRED_PACKAGES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_impl_run_python() -> MagicMock:
    mock = MagicMock(return_value={"run_id": "test-run-id", "status": "success"})
    return mock


# ---------------------------------------------------------------------------
# run_python — enhanced tool
# ---------------------------------------------------------------------------

class TestSfRunPython:
    def test_run_python_prepends_context(self) -> None:
        from snowbox.server import _impl_sf_run_python

        user_code = "print('hello')"
        captured: dict = {}

        def fake_run_python(code: str, **kwargs: object) -> dict:
            captured["code"] = code
            return {"run_id": "x", "status": "success"}

        with patch("snowbox.server._impl_run_python", side_effect=fake_run_python):
            _impl_sf_run_python(code=user_code)

        assert captured["code"].startswith(SNOWFLAKE_CONTEXT_CODE)
        assert user_code in captured["code"]

    def test_run_python_merges_packages(self) -> None:
        from snowbox.server import _impl_sf_run_python

        captured: dict = {}

        def fake_run_python(**kwargs: object) -> dict:
            captured["packages"] = kwargs.get("packages", [])
            return {"run_id": "x", "status": "success"}

        with patch("snowbox.server._impl_run_python", side_effect=fake_run_python):
            _impl_sf_run_python(code="x", packages=["requests"])

        pkgs = captured["packages"]
        # User package preserved
        assert "requests" in pkgs
        # Required Snowflake packages added
        for required in SNOWFLAKE_REQUIRED_PACKAGES:
            assert required in pkgs
        # No duplicates
        assert len(pkgs) == len(set(pkgs))

    def test_run_python_no_duplicates_when_user_provides_required_package(self) -> None:
        from snowbox.server import _impl_sf_run_python

        captured: dict = {}

        def fake_run_python(**kwargs: object) -> dict:
            captured["packages"] = kwargs.get("packages", [])
            return {"run_id": "x", "status": "success"}

        # User explicitly includes a required package
        with patch("snowbox.server._impl_run_python", side_effect=fake_run_python):
            _impl_sf_run_python(code="x", packages=["snowflake-connector-python[pandas]"])

        pkgs = captured["packages"]
        assert pkgs.count("snowflake-connector-python[pandas]") == 1

    def test_run_python_forwards_kwargs(self) -> None:
        from snowbox.server import _impl_sf_run_python

        captured: dict = {}

        def fake_run_python(**kwargs: object) -> dict:
            captured.update(kwargs)
            return {"run_id": "x", "status": "success"}

        with patch("snowbox.server._impl_run_python", side_effect=fake_run_python):
            _impl_sf_run_python(
                code="x",
                timeout_seconds=120,
                sandbox=True,
                async_mode=True,
            )

        assert captured["timeout_seconds"] == 120
        assert captured["sandbox"] is True
        assert captured["async_mode"] is True


# ---------------------------------------------------------------------------
# check_setup — enhanced tool
# ---------------------------------------------------------------------------

class TestSfCheckSetup:
    def test_check_setup_includes_snowflake_key(self) -> None:
        from snowbox.server import _impl_sf_check_setup

        fake_base = {"data_dir": "/tmp", "marimo_available": True, "ready": True}
        mock_tool = MagicMock()
        mock_tool.fn.return_value = fake_base

        with (
            patch("snowbox.server._ms_server") as mock_ms,
            patch("snowbox.server.connector") as mock_connector,
        ):
            mock_ms.check_setup = mock_tool
            mock_connector.test.return_value = {"status": "ok"}
            mock_connector._database = "ANALYTICS"
            mock_connector._schema = "PUBLIC"
            result = _impl_sf_check_setup()

        assert "snowflake" in result
        assert result["snowflake"]["status"] == "ok"
        assert result["snowflake"]["database"] == "ANALYTICS"


# ---------------------------------------------------------------------------
# snowflake_query
# ---------------------------------------------------------------------------

class TestSnowflakeQuery:
    def _run_query(self, df: pd.DataFrame) -> dict:
        from snowbox.server import _impl_snowflake_query

        with tempfile.TemporaryDirectory() as tmp:
            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                with patch("snowbox.server.connector") as mock_conn:
                    mock_conn.query.return_value = df
                    return _impl_snowflake_query("SELECT 1")

    def test_snowflake_query_returns_expected_shape(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
        result = self._run_query(df)

        assert result["row_count"] == 3
        assert result["columns"] == ["A", "B"]
        assert len(result["preview"]) == 3
        assert "saved_to" in result

    def test_snowflake_query_saves_csv_file(self) -> None:
        from snowbox.server import _impl_snowflake_query

        df = pd.DataFrame({"X": [10, 20]})

        # Keep temp dir alive for the assertion
        with tempfile.TemporaryDirectory() as tmp:
            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                with patch("snowbox.server.connector") as mock_conn:
                    mock_conn.query.return_value = df
                    result = _impl_snowflake_query("SELECT 1")

            saved = Path(result["saved_to"])
            assert saved.exists()
            loaded = pd.read_csv(saved)
            assert list(loaded["X"]) == [10, 20]

    def test_snowflake_query_preview_capped_at_50(self) -> None:
        df = pd.DataFrame({"N": range(200)})
        result = self._run_query(df)

        assert result["row_count"] == 200
        assert len(result["preview"]) == 50

    def test_snowflake_query_error_handling(self) -> None:
        from snowbox.server import _impl_snowflake_query

        with patch("snowbox.server.connector") as mock_conn:
            mock_conn.query.side_effect = RuntimeError("connection refused")
            result = _impl_snowflake_query("SELECT 1")

        assert "error" in result
        assert "connection refused" in result["error"]


# ---------------------------------------------------------------------------
# snowflake_write
# ---------------------------------------------------------------------------

class TestSnowflakeWrite:
    def test_snowflake_write_reads_csv_and_calls_write(self) -> None:
        from snowbox.server import _impl_snowflake_write

        df = pd.DataFrame({"col": [1, 2, 3]})

        with tempfile.TemporaryDirectory() as tmp:
            run_id = "run-abc"
            artifact = "output.csv"
            artifact_dir = Path(tmp) / "notebooks" / run_id
            artifact_dir.mkdir(parents=True)
            df.to_csv(artifact_dir / artifact, index=False)

            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                with patch("snowbox.server.connector") as mock_conn:
                    mock_conn.write.return_value = None
                    result = _impl_snowflake_write(run_id, artifact, "MY_TABLE", "append")

        assert result["status"] == "ok"
        assert result["rows_written"] == 3
        assert result["table"] == "MY_TABLE"
        # Verify write called with a DataFrame of the right shape
        called_df = mock_conn.write.call_args[0][0]
        assert len(called_df) == 3

    def test_snowflake_write_reads_parquet(self) -> None:
        from snowbox.server import _impl_snowflake_write

        df = pd.DataFrame({"val": [10, 20]})

        with tempfile.TemporaryDirectory() as tmp:
            run_id = "run-pq"
            artifact = "data.parquet"
            artifact_dir = Path(tmp) / "notebooks" / run_id
            artifact_dir.mkdir(parents=True)
            df.to_parquet(artifact_dir / artifact, index=False)

            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                with patch("snowbox.server.connector") as mock_conn:
                    mock_conn.write.return_value = None
                    result = _impl_snowflake_write(run_id, artifact, "PARQUET_TABLE")

        assert result["status"] == "ok"
        assert result["rows_written"] == 2

    def test_snowflake_write_missing_file(self) -> None:
        from snowbox.server import _impl_snowflake_write

        with tempfile.TemporaryDirectory() as tmp:
            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                result = _impl_snowflake_write("no-such-run", "missing.csv", "TABLE")

        assert "error" in result
        assert "Artifact not found" in result["error"]

    def test_snowflake_write_bad_extension(self) -> None:
        from snowbox.server import _impl_snowflake_write

        with tempfile.TemporaryDirectory() as tmp:
            run_id = "run-bad"
            artifact = "data.xlsx"
            artifact_dir = Path(tmp) / "notebooks" / run_id
            artifact_dir.mkdir(parents=True)
            (artifact_dir / artifact).write_bytes(b"fake xlsx")

            with patch("snowbox.server.DATA_DIR", Path(tmp)):
                result = _impl_snowflake_write(run_id, artifact, "TABLE")

        assert "error" in result
        assert "Unsupported file type" in result["error"]


# ---------------------------------------------------------------------------
# list_tables / describe_table
# ---------------------------------------------------------------------------

class TestListTables:
    def test_list_tables_delegates_to_connector(self) -> None:
        from snowbox.server import _impl_list_tables

        fake_tables = [
            {"name": "ORDERS", "database_name": "DB", "schema_name": "SCH", "kind": "TABLE"}
        ]
        with patch("snowbox.server.connector") as mock_conn:
            mock_conn.list_tables.return_value = fake_tables
            result = _impl_list_tables(schema="SCH", database="DB")

        assert result == {"tables": fake_tables}
        mock_conn.list_tables.assert_called_once_with(schema="SCH", database="DB")

    def test_list_tables_error_handling(self) -> None:
        from snowbox.server import _impl_list_tables

        with patch("snowbox.server.connector") as mock_conn:
            mock_conn.list_tables.side_effect = RuntimeError("auth failed")
            result = _impl_list_tables()

        assert "error" in result
        assert "auth failed" in result["error"]


class TestDescribeTable:
    def test_describe_table_delegates_to_connector(self) -> None:
        from snowbox.server import _impl_describe_table

        fake_result = {
            "columns": [{"name": "ID", "type": "NUMBER"}],
            "sample_rows": [{"ID": 1}],
        }
        with patch("snowbox.server.connector") as mock_conn:
            mock_conn.describe_table.return_value = fake_result
            result = _impl_describe_table("ORDERS", schema="SCH", database="DB")

        assert result["table"] == "ORDERS"
        assert result["columns"] == fake_result["columns"]
        assert result["sample_rows"] == fake_result["sample_rows"]
        mock_conn.describe_table.assert_called_once_with("ORDERS", schema="SCH", database="DB")

    def test_describe_table_error_handling(self) -> None:
        from snowbox.server import _impl_describe_table

        with patch("snowbox.server.connector") as mock_conn:
            mock_conn.describe_table.side_effect = RuntimeError("table not found")
            result = _impl_describe_table("MISSING")

        assert "error" in result
        assert "table not found" in result["error"]
