"""Unit tests for utils/tables.py - Table name parsing and constraint validation."""

import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint

from master_instrument.etl.loading.utils.tables import (
    parse_table_name,
    build_qualified_name,
    build_qualified_table_name,
    quote_identifier,
    is_internal_id_pattern,
    find_columns_by_pattern,
    validate_single_column,
    get_pk_columns,
    get_unique_constraint_columns,
    get_unique_columns,
    is_pk_match,
    is_unique_constraint_match,
    is_single_unique_column_match,
    matches_constraint,
    format_constraint_error,
    validate_unique_key,
)


class TestParseTableName:
    """Tests for parse_table_name()."""
    
    def test_parses_qualified_name(self):
        """Should parse schema.table format."""
        schema, table = parse_table_name("master.country")
        assert schema == "master"
        assert table == "country"
    
    def test_raises_on_invalid_format(self):
        """Should raise ValueError if not schema.table format."""
        with pytest.raises(ValueError, match="must be in format"):
            parse_table_name("invalidname")
    
    def test_raises_on_too_many_parts(self):
        """Should raise ValueError if more than 2 parts."""
        with pytest.raises(ValueError, match="must be in format"):
            parse_table_name("db.schema.table")


class TestBuildQualifiedName:
    """Tests for build_qualified_name()."""
    
    def test_builds_qualified_name(self):
        """Should build schema.table format."""
        result = build_qualified_name("master", "country")
        assert result == "master.country"
    
    def test_handles_special_chars(self):
        """Should handle tables with underscores."""
        result = build_qualified_name("test_schema", "test_table")
        assert result == "test_schema.test_table"


class TestBuildQualifiedTableName:
    """Tests for build_qualified_table_name()."""
    
    def test_uses_override_schema(self):
        """Should use override schema when provided."""
        result = build_qualified_table_name("override", "model_schema", "table")
        assert result == "override.table"
    
    def test_uses_table_schema(self):
        """Should use table schema when override not provided."""
        result = build_qualified_table_name(None, "model_schema", "table")
        assert result == "model_schema.table"
    
    def test_defaults_to_public(self):
        """Should default to public when no schema provided."""
        result = build_qualified_table_name(None, None, "table")
        assert result == "public.table"


class TestQuoteIdentifier:
    """Tests for quote_identifier()."""
    
    def test_quotes_identifier(self):
        """Should wrap identifier in double quotes."""
        result = quote_identifier("column_name")
        assert result == '"column_name"'
    
    def test_quotes_with_spaces(self):
        """Should quote identifiers with spaces."""
        result = quote_identifier("my column")
        assert result == '"my column"'


class TestIsInternalIdPattern:
    """Tests for is_internal_id_pattern()."""
    
    def test_matches_internal_id(self):
        """Should match internal_*_id pattern."""
        assert is_internal_id_pattern("internal_company_id") is True
        assert is_internal_id_pattern("internal_venue_id") is True
    
    def test_rejects_non_internal(self):
        """Should reject non-internal columns."""
        assert is_internal_id_pattern("company_id") is False
        assert is_internal_id_pattern("external_id") is False
        assert is_internal_id_pattern("name") is False  # No internal_ prefix


class TestFindColumnsByPattern:
    """Tests for find_columns_by_pattern()."""
    
    def test_finds_columns_containing_pattern(self):
        """Should find all columns containing pattern."""
        cols = ["external_id", "internal_company_id", "company_name"]
        result = find_columns_by_pattern(cols, "external")
        assert result == ["external_id"]
    
    def test_finds_columns_by_prefix(self):
        """Should find columns with prefix when prefix=True."""
        cols = ["internal_company_id", "internal_venue_id", "company_id"]
        result = find_columns_by_pattern(cols, "internal_", prefix=True)
        assert result == ["internal_company_id", "internal_venue_id"]
    
    def test_returns_empty_when_no_match(self):
        """Should return empty list when no match."""
        cols = ["id", "name", "value"]
        result = find_columns_by_pattern(cols, "external")
        assert result == []


class TestValidateSingleColumn:
    """Tests for validate_single_column()."""
    
    def test_returns_single_column(self):
        """Should return the single column."""
        result = validate_single_column(["id"], "primary key")
        assert result == "id"
    
    def test_raises_on_multiple_columns(self):
        """Should raise ValueError if multiple columns."""
        with pytest.raises(ValueError, match="Expected exactly 1"):
            validate_single_column(["id", "name"], "primary key")
    
    def test_raises_on_no_columns(self):
        """Should raise ValueError if no columns."""
        with pytest.raises(ValueError, match="Expected exactly 1"):
            validate_single_column([], "primary key")


class TestGetPkColumns:
    """Tests for get_pk_columns()."""
    
    def test_extracts_single_pk(self):
        """Should extract single primary key."""
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer, primary_key=True))
        result = get_pk_columns(table)
        assert result == {"id"}
    
    def test_extracts_composite_pk(self):
        """Should extract composite primary key."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True)
        )
        result = get_pk_columns(table)
        assert result == {"id1", "id2"}


class TestGetUniqueConstraintColumns:
    """Tests for get_unique_constraint_columns()."""
    
    def test_extracts_unique_constraints(self):
        """Should extract unique constraint columns."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String),
            UniqueConstraint("code")
        )
        result = get_unique_constraint_columns(table)
        assert {"code"} in result
    
    def test_extracts_composite_unique_constraint(self):
        """Should extract composite unique constraint."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("col1", String),
            Column("col2", String),
            UniqueConstraint("col1", "col2")
        )
        result = get_unique_constraint_columns(table)
        assert {"col1", "col2"} in result


class TestGetUniqueColumns:
    """Tests for get_unique_columns()."""
    
    def test_extracts_unique_columns(self):
        """Should extract columns marked as unique."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String, unique=True)
        )
        result = get_unique_columns(table)
        assert result == {"code"}


class TestIsPkMatch:
    """Tests for is_pk_match()."""
    
    def test_matches_single_pk(self):
        """Should match single primary key."""
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer, primary_key=True))
        assert is_pk_match({"id"}, table) is True
    
    def test_matches_composite_pk(self):
        """Should match composite primary key."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True)
        )
        assert is_pk_match({"id1", "id2"}, table) is True
    
    def test_rejects_partial_pk(self):
        """Should reject partial primary key."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True)
        )
        assert is_pk_match({"id1"}, table) is False


class TestIsUniqueConstraintMatch:
    """Tests for is_unique_constraint_match()."""
    
    def test_matches_unique_constraint(self):
        """Should match unique constraint."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String),
            UniqueConstraint("code")
        )
        assert is_unique_constraint_match({"code"}, table) is True


class TestIsSingleUniqueColumnMatch:
    """Tests for is_single_unique_column_match()."""
    
    def test_matches_unique_column(self):
        """Should match single unique column."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String, unique=True)
        )
        assert is_single_unique_column_match(["code"], table) is True
    
    def test_rejects_multiple_columns(self):
        """Should reject multiple columns."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("code", String, unique=True)
        )
        assert is_single_unique_column_match(["code", "name"], table) is False


class TestMatchesConstraint:
    """Tests for matches_constraint()."""
    
    def test_matches_pk(self):
        """Should match primary key."""
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer, primary_key=True))
        assert matches_constraint(["id"], table) is True
    
    def test_matches_unique_constraint(self):
        """Should match unique constraint."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String),
            UniqueConstraint("code")
        )
        assert matches_constraint(["code"], table) is True
    
    def test_matches_unique_column(self):
        """Should match unique column."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String, unique=True)
        )
        assert matches_constraint(["code"], table) is True
    
    def test_rejects_non_constraint(self):
        """Should reject non-constraint column."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String)
        )
        assert matches_constraint(["name"], table) is False


class TestFormatConstraintError:
    """Tests for format_constraint_error()."""
    
    def test_formats_error_message(self):
        """Should format error message with available constraints."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String, unique=True)
        )
        result = format_constraint_error(table, ["name"])
        assert "unique_key ['name']" in result
        assert "table 'test'" in result
        assert "PK=" in result
        assert "Unique columns=" in result


class TestValidateUniqueKey:
    """Tests for validate_unique_key()."""
    
    def test_validates_valid_pk(self):
        """Should not raise for valid primary key."""
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer, primary_key=True))
        validate_unique_key(table, ["id"])  # Should not raise
    
    def test_validates_valid_unique_constraint(self):
        """Should not raise for valid unique constraint."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String),
            UniqueConstraint("code")
        )
        validate_unique_key(table, ["code"])  # Should not raise
    
    def test_raises_for_invalid_key(self):
        """Should raise ValueError for invalid key."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String)
        )
        with pytest.raises(ValueError, match="does not match any constraint"):
            validate_unique_key(table, ["name"])
    
    def test_raises_for_partial_composite_key(self):
        """Should raise ValueError for partial composite key."""
        metadata = MetaData()
        table = Table(
            "test", metadata,
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True)
        )
        with pytest.raises(ValueError, match="does not match any constraint"):
            validate_unique_key(table, ["id1"])
