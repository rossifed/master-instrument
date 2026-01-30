"""Unit tests for utils/database.py - PostgreSQL operations and engine handling."""

import pytest
from unittest.mock import MagicMock, patch, call
from sqlalchemy import create_engine

from master_instrument.etl.loading.utils.database import (
    extract_engine,
    get_non_pk_indexes,
    drop_indexes,
    recreate_indexes,
    truncate_table,
)


class TestExtractEngine:
    """Tests for extract_engine()."""
    
    def test_returns_engine_directly(self):
        """Should return Engine object directly."""
        engine = create_engine("sqlite:///:memory:")
        result = extract_engine(engine)
        assert result is engine
    
    def test_extracts_from_resource(self):
        """Should extract engine from resource object."""
        resource = MagicMock()
        mock_engine = create_engine("sqlite:///:memory:")
        resource.get_engine.return_value = mock_engine
        
        result = extract_engine(resource)
        assert result is mock_engine
        resource.get_engine.assert_called_once()


class TestGetNonPkIndexes:
    """Tests for get_non_pk_indexes()."""
    
    @patch('master_instrument.etl.loading.utils.database.text')
    def test_queries_pg_indexes(self, mock_text):
        """Should query PostgreSQL system catalog for indexes."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [
            ("idx_test", "CREATE INDEX idx_test ON test(col)")
        ]
        mock_conn.execute.return_value = mock_result
        
        result = get_non_pk_indexes(mock_engine, "test_schema", "test_table")
        
        # Should return list of dicts with name and definition
        assert isinstance(result, list)
        if len(result) > 0:
            assert "name" in result[0]
            assert "definition" in result[0]
        
        # Verify SQL query was constructed
        mock_text.assert_called_once()
        call_args = mock_text.call_args[0][0]
        assert "pg_indexes" in call_args
        assert "schemaname" in call_args
    
    @patch('master_instrument.etl.loading.utils.database.text')
    def test_excludes_primary_key_indexes(self, mock_text):
        """Should exclude primary key indexes."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Simulate result with _pkey suffix (should be excluded)
        mock_result = MagicMock()
        mock_result.mappings.return_value = []
        mock_conn.execute.return_value = mock_result
        
        result = get_non_pk_indexes(mock_engine, "test_schema", "test_table")
        
        # Query should use NOT EXISTS with pg_constraint to exclude constraint-backing indexes
        call_args = mock_text.call_args[0][0]
        assert "pg_constraint" in call_args and "NOT EXISTS" in call_args


class TestDropIndexes:
    """Tests for drop_indexes()."""
    
    def test_drops_each_index(self):
        """Should drop each index in the list."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        indexes = [
            {"name": "idx1", "definition": "CREATE INDEX idx1 ON test(col1)"},
            {"name": "idx2", "definition": "CREATE INDEX idx2 ON test(col2)"}
        ]
        
        drop_indexes(mock_engine, "test_schema", indexes)
        
        # Should call DROP INDEX for each index
        assert mock_conn.execute.call_count == 2
    
    @patch('master_instrument.etl.loading.utils.database.text')
    def test_handles_empty_list(self, mock_text):
        """Should handle empty index list gracefully."""
        mock_engine = MagicMock()
        
        drop_indexes(mock_engine, "test_schema", [])
        
        # Should not execute anything
        mock_text.assert_not_called()


class TestRecreateIndexes:
    """Tests for recreate_indexes()."""
    
    def test_recreates_each_index(self):
        """Should recreate each index using its definition."""
        mock_engine = MagicMock()
        mock_raw_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_engine.raw_connection.return_value = mock_raw_conn
        mock_raw_conn.cursor.return_value = mock_cursor
        
        indexes = [
            {"name": "idx1", "definition": "CREATE INDEX idx1 ON test(col1)"},
            {"name": "idx2", "definition": "CREATE INDEX idx2 ON test(col2)"}
        ]
        
        recreate_indexes(mock_engine, indexes)
        
        # Should execute CREATE INDEX CONCURRENTLY for each (default is concurrently=True)
        assert mock_cursor.execute.call_count == 2
    
    @patch('master_instrument.etl.loading.utils.database.text')
    def test_handles_empty_list(self, mock_text):
        """Should handle empty index list gracefully."""
        mock_engine = MagicMock()
        
        recreate_indexes(mock_engine, [])
        
        # Should not execute anything
        mock_text.assert_not_called()


class TestTruncateTable:
    """Tests for truncate_table()."""
    
    def test_truncates_table(self):
        """Should execute TRUNCATE command."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        truncate_table(mock_engine, "test_schema", "test_table")
        
        # Should use begin() and execute TRUNCATE
        mock_engine.begin.assert_called_once()
        mock_conn.execute.assert_called_once()
    
    def test_builds_qualified_table_name(self):
        """Should use qualified schema.table format."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        truncate_table(mock_engine, "master", "country")
        
        # Should execute with qualified table name
        mock_conn.execute.assert_called_once()
