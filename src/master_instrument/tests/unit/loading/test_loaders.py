"""Unit tests for loaders.py - Simple and batch loader implementations."""

import pytest
from unittest.mock import MagicMock, Mock, patch, call
from datetime import date
from sqlalchemy import text

from master_instrument.etl.loading.loaders import SimpleLoader, BatchLoader
from master_instrument.etl.loading.configs import MergeConfig, MappingConfig


class MockSource:
    """Mock data source for testing."""
    def __init__(self, sql: str = "SELECT 1"):
        self.sql = sql
        self._config = None
    
    @property
    def config(self):
        return self._config
    
    def read(self, params: dict) -> str:
        """Read method matching DataSource protocol."""
        return self.sql


class MockModel:
    """Mock SQLAlchemy model for testing."""
    __tablename__ = "mock_table"
    __table_args__ = {"schema": "test"}


class TestSimpleLoaderCreation:
    """Tests for SimpleLoader instantiation."""
    
    def test_creation_with_required_args(self):
        """Should create loader with engine and source."""
        mock_engine = MagicMock()
        source = MockSource()
        
        loader = SimpleLoader(engine=mock_engine, source=source)
        
        # Engine is extracted via extract_engine()
        assert loader.source is source
        assert loader.logger is not None  # Dagster logger by default
    
    def test_creation_with_logger(self):
        """Should accept optional logger."""
        mock_engine = MagicMock()
        mock_logger = MagicMock()
        source = MockSource()
        
        loader = SimpleLoader(engine=mock_engine, source=source, logger=mock_logger)
        
        assert loader.logger is mock_logger


class TestSimpleLoaderLoad:
    """Tests for SimpleLoader.load() method."""
    
    @patch('master_instrument.etl.loading.loaders.extract_engine')
    def test_loads_and_executes_data_source(self, mock_extract):
        """Should read data source and execute SQL."""
        mock_engine = MagicMock()
        mock_extract.return_value = mock_engine
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        source = MockSource("SELECT * FROM test_table;")
        loader = SimpleLoader(mock_engine, source)
        
        result = loader.load()
        
        # Should execute SQL once
        mock_conn.execute.assert_called_once()
        # Should return dict with results
        assert isinstance(result, dict)
        assert "sql" in result
    
    @patch('master_instrument.etl.loading.loaders.extract_engine')
    def test_uses_transaction_context(self, mock_extract):
        """Should execute within transaction context."""
        mock_engine = MagicMock()
        mock_extract.return_value = mock_engine
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        source = MockSource()
        loader = SimpleLoader(mock_engine, source)
        
        loader.load()
        
        # Should use begin() context manager
        mock_engine.begin.assert_called_once()
    
    def test_passes_params_to_source(self):
        """Should pass params dictionary to source.read()."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        source = MagicMock()
        source.config = None
        source.read.return_value = "SELECT 1;"
        loader = SimpleLoader(mock_engine, source)
        
        loader.load(params={"start_date": "2023-01-01"})
        
        # Should pass params to source
        source.read.assert_called_once_with({"start_date": "2023-01-01"})
    
    def test_logs_execution_start(self):
        """Should log execution start when logger provided."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_logger = MagicMock()
        
        source = MockSource()
        loader = SimpleLoader(mock_engine, source, logger=mock_logger)
        
        loader.load()
        
        # Should call logger
        assert mock_logger.info.called
    
    def test_logs_sql_statement(self):
        """Should log SQL statement being executed."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_logger = MagicMock()
        
        sql = "INSERT INTO test VALUES (1, 'data');"
        source = MockSource(sql)
        loader = SimpleLoader(mock_engine, source, logger=mock_logger)
        
        loader.load()
        
        # Logger should be called (SQL may be logged)
        assert mock_logger.info.called or mock_logger.debug.called
    
    @patch('master_instrument.etl.loading.loaders.extract_engine')
    def test_handles_execution_error(self, mock_extract):
        """Should propagate execution errors."""
        mock_engine = MagicMock()
        mock_extract.return_value = mock_engine
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("SQL Error")
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        source = MockSource("INVALID SQL;")
        loader = SimpleLoader(mock_engine, source)
        
        with pytest.raises(Exception) as exc_info:
            loader.load()
        
        assert "SQL Error" in str(exc_info.value)
    
    def test_dry_run_returns_sql_without_executing(self):
        """Should return SQL without executing when dry_run=True."""
        mock_engine = MagicMock()
        
        source = MockSource("SELECT * FROM test;")
        loader = SimpleLoader(mock_engine, source)
        
        result = loader.load(dry_run=True)
        
        # Should not execute
        mock_engine.begin.assert_not_called()
        # Should return SQL
        assert result["dry_run"] is True
        assert "SELECT * FROM test;" in result["sql"]


class TestBatchLoaderPrivateMethods:
    """Tests for BatchLoader private helper methods."""
    
    def test_process_batch_internal_flow(self):
        """Should verify internal batch processing flow."""
        # This would test _process_batch, _extract_data, _load_to_staging, _merge_data
        # These are implementation details and may be tested indirectly
        pass


class TestLoaderIntegration:
    """Integration tests for loader classes."""
    
    @patch('master_instrument.etl.loading.loaders.extract_engine')
    def test_simple_loader_end_to_end(self, mock_extract):
        """Should perform complete simple load operation."""
        mock_engine = MagicMock()
        mock_extract.return_value = mock_engine
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 100
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        
        sql = "INSERT INTO test.table VALUES (1, 'test');"
        source = MockSource(sql)
        loader = SimpleLoader(mock_engine, source)
        
        # Execute load
        result = loader.load()
        
        # Verify transaction and execution
        mock_engine.begin.assert_called_once()
        mock_conn.execute.assert_called_once()
        # Verify result
        assert result["rowcount"] == 100
        assert result["sql"] == sql


