"""Unit tests for SNOWFLAKE_CONTEXT_CODE and SNOWFLAKE_REQUIRED_PACKAGES."""

from __future__ import annotations

from snowbox.context import SNOWFLAKE_CONTEXT_CODE, SNOWFLAKE_REQUIRED_PACKAGES


class TestContextCode:
    def test_context_code_is_valid_python(self) -> None:
        """The injected boilerplate must be syntactically valid Python."""
        compile(SNOWFLAKE_CONTEXT_CODE, "<context>", "exec")

    def test_context_defines_snowflake_symbol(self) -> None:
        """The injected code must define the __snowflake__ helper."""
        assert "__snowflake__" in SNOWFLAKE_CONTEXT_CODE

    def test_context_defines_snowflake_context_class(self) -> None:
        """The _SnowflakeContext class must be present."""
        assert "_SnowflakeContext" in SNOWFLAKE_CONTEXT_CODE

    def test_context_includes_query_method(self) -> None:
        """The context must expose a query() method."""
        assert "def query" in SNOWFLAKE_CONTEXT_CODE

    def test_context_includes_write_method(self) -> None:
        """The context must expose a write() method."""
        assert "def write" in SNOWFLAKE_CONTEXT_CODE


class TestRequiredPackages:
    def test_required_packages_nonempty(self) -> None:
        assert isinstance(SNOWFLAKE_REQUIRED_PACKAGES, list)
        assert len(SNOWFLAKE_REQUIRED_PACKAGES) > 0

    def test_required_packages_include_connector(self) -> None:
        """At least one package string must reference snowflake."""
        assert any("snowflake" in pkg for pkg in SNOWFLAKE_REQUIRED_PACKAGES)

    def test_required_packages_are_strings(self) -> None:
        assert all(isinstance(pkg, str) for pkg in SNOWFLAKE_REQUIRED_PACKAGES)
