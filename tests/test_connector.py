"""Unit tests for SnowflakeConnector (all Snowflake calls mocked)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(env: dict[str, str]) -> "SnowflakeConnector":  # noqa: F821
    """Build a SnowflakeConnector with the given env vars patched in."""
    with patch.dict(os.environ, env, clear=False):
        from snowbox.connector import SnowflakeConnector
        return SnowflakeConnector()


def _sf_modules() -> dict:
    """Return a sys.modules patch dict that stubs out snowflake.connector."""
    mock_connector = MagicMock()
    mock_pandas_tools = MagicMock()
    return {
        "snowflake": MagicMock(connector=mock_connector),
        "snowflake.connector": mock_connector,
        "snowflake.connector.pandas_tools": mock_pandas_tools,
    }


# ---------------------------------------------------------------------------
# Auth kwargs
# ---------------------------------------------------------------------------

class TestBuildConnectKwargs:
    def test_key_pair_auth_kwargs(self) -> None:
        env = {
            "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
            "SNOWFLAKE_USER": "myuser",
            "SNOWFLAKE_PRIVATE_KEY_PATH": "/path/to/key.p8",
        }
        conn = _make_connector(env)
        kwargs = conn._build_connect_kwargs()

        assert kwargs["authenticator"] == "SNOWFLAKE_JWT"
        assert kwargs["private_key_file"] == "/path/to/key.p8"
        assert "password" not in kwargs

    def test_key_pair_with_passphrase(self) -> None:
        env = {
            "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
            "SNOWFLAKE_USER": "myuser",
            "SNOWFLAKE_PRIVATE_KEY_PATH": "/path/to/key.p8",
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "s3cr3t",
        }
        conn = _make_connector(env)
        kwargs = conn._build_connect_kwargs()

        assert kwargs["private_key_file_pwd"] == "s3cr3t"

    def test_password_auth_kwargs(self) -> None:
        env = {
            "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
            "SNOWFLAKE_USER": "myuser",
            "SNOWFLAKE_PASSWORD": "hunter2",
        }
        conn = _make_connector(env)
        kwargs = conn._build_connect_kwargs()

        assert kwargs["password"] == "hunter2"
        assert "authenticator" not in kwargs
        assert "private_key_file" not in kwargs

    def test_optional_fields_included_when_set(self) -> None:
        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "u",
            "SNOWFLAKE_PASSWORD": "p",
            "SNOWFLAKE_WAREHOUSE": "WH",
            "SNOWFLAKE_DATABASE": "DB",
            "SNOWFLAKE_SCHEMA": "SCH",
            "SNOWFLAKE_ROLE": "ROLE",
        }
        conn = _make_connector(env)
        kwargs = conn._build_connect_kwargs()

        assert kwargs["warehouse"] == "WH"
        assert kwargs["database"] == "DB"
        assert kwargs["schema"] == "SCH"
        assert kwargs["role"] == "ROLE"

    def test_raises_if_no_auth(self) -> None:
        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "u",
        }
        # Ensure neither SNOWFLAKE_PASSWORD nor SNOWFLAKE_PRIVATE_KEY_PATH is set
        clean_env = {k: v for k, v in env.items()}
        for key in ("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PRIVATE_KEY_PATH"):
            os.environ.pop(key, None)
        with patch.dict(os.environ, clean_env, clear=False):
            from snowbox.connector import SnowflakeConnector
            conn = SnowflakeConnector()
        with pytest.raises(RuntimeError, match="No Snowflake auth configured"):
            conn._build_connect_kwargs()


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

class TestConnectivityTest:
    def test_test_ok(self) -> None:
        mocks = _sf_modules()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.is_closed.return_value = False
        mock_conn.cursor.return_value = mock_cursor
        mocks["snowflake.connector"].connect.return_value = mock_conn

        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "u",
            "SNOWFLAKE_PASSWORD": "p",
        }
        with patch.dict("sys.modules", mocks), patch.dict(os.environ, env):
            from snowbox.connector import SnowflakeConnector
            conn = SnowflakeConnector()
            conn._conn = mock_conn  # bypass lazy connect
            result = conn.test()

        assert result == {"status": "ok"}
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_test_error(self) -> None:
        mocks = _sf_modules()
        mock_conn = MagicMock()
        mock_conn.is_closed.return_value = False
        mock_conn.cursor.side_effect = RuntimeError("boom")
        mocks["snowflake.connector"].connect.return_value = mock_conn

        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "u",
            "SNOWFLAKE_PASSWORD": "p",
        }
        with patch.dict("sys.modules", mocks), patch.dict(os.environ, env):
            from snowbox.connector import SnowflakeConnector
            conn = SnowflakeConnector()
            conn._conn = mock_conn
            result = conn.test()

        assert result["status"] == "error"
        assert "boom" in result["detail"]


# ---------------------------------------------------------------------------
# Reconnect behaviour
# ---------------------------------------------------------------------------

class TestReconnect:
    def test_reconnects_if_closed(self) -> None:
        mocks = _sf_modules()
        mock_conn = MagicMock()
        # First call: closed; second call: open
        mock_conn.is_closed.side_effect = [True, False]
        fresh_conn = MagicMock()
        fresh_conn.is_closed.return_value = False
        mocks["snowflake.connector"].connect.return_value = fresh_conn

        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "u",
            "SNOWFLAKE_PASSWORD": "p",
        }
        with patch.dict("sys.modules", mocks), patch.dict(os.environ, env):
            from snowbox.connector import SnowflakeConnector
            conn = SnowflakeConnector()
            conn._conn = mock_conn  # simulate previously-opened (now closed) connection
            live = conn._get_conn()

        # Should have called connect() once to get a fresh connection
        mocks["snowflake.connector"].connect.assert_called_once()
        assert live is fresh_conn
