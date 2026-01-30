"""Unit tests for src/master_instrument/loading/sources.py"""
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from master_instrument.etl.loading.configs import LoadingScheme, MergeConfig
from master_instrument.etl.loading.sources import SqlFileSource, TemplateSource


class TestSqlFileSourceCreation:
    """Tests for SqlFileSource instantiation."""
    
    def test_creation_with_file_path(self):
        """Should create source with sql_file parameter."""
        source = SqlFileSource(sql_file="path/to/query.sql")
        
        assert source.sql_file == "path/to/query.sql"
        assert source.config is None


class TestSqlFileSourceRead:
    """Tests for SqlFileSource.read() method."""
    
    @patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM test;")
    def test_read_file_content(self, mock_file):
        """Should read and return SQL from file."""
        source = SqlFileSource(sql_file="query.sql")
        
        result = source.read(params={})
        
        # Should call open with the resolved path
        assert mock_file.called
        assert result == "SELECT * FROM test;"
    
    @patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM {{ table }};")
    def test_read_with_params(self, mock_file):
        """Should accept params parameter."""
        source = SqlFileSource(sql_file="query.sql")
        
        result = source.read(params={"table": "my_table"})
        
        assert isinstance(result, str)
    
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_read_raises_on_missing_file(self, mock_file):
        """Should raise FileNotFoundError for missing files."""
        source = SqlFileSource(sql_file="nonexistent.sql")
        
        with pytest.raises(FileNotFoundError):
            source.read(params={})
    
    @patch("builtins.open", new_callable=mock_open, read_data="-- Comment\nSELECT * FROM test;\n")
    def test_read_includes_comments(self, mock_file):
        """Should include SQL comments in result."""
        source = SqlFileSource(sql_file="query.sql")
        
        result = source.read(params={})
        
        assert "-- Comment" in result
        assert "SELECT * FROM test;" in result


class TestTemplateSourceCreation:
    """Tests for TemplateSource instantiation."""
    
    def test_creation_with_config(self):
        """Should create source with config."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        source = TemplateSource(config=config)
        
        assert source.config is config
    
    def test_config_property(self):
        """Should expose config property."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        source = TemplateSource(config=config)
        
        assert source.config == config


class TestTemplateSourceRead:
    """Tests for TemplateSource.read() method."""
    
    def test_read_with_params(self):
        """Should return SQL string."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        source = TemplateSource(config=config)
        
        result = source.read(params={})
        
        assert isinstance(result, str)
        assert len(result) > 0
    
    def test_read_with_empty_params(self):
        """Should work with empty params."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        source = TemplateSource(config=config)
        
        result = source.read(params={})
        
        assert isinstance(result, str)


class TestSourcePolymorphism:
    """Tests for source polymorphism/interchangeability."""
    
    def test_both_sources_have_read_method(self):
        """Both sources should have read() method."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        
        sql_source = SqlFileSource(sql_file="test.sql")
        template_source = TemplateSource(config=config)
        
        assert hasattr(sql_source, "read")
        assert hasattr(template_source, "read")
        assert callable(sql_source.read)
        assert callable(template_source.read)
    
    @patch("builtins.open", new_callable=mock_open, read_data="SELECT 1;")
    def test_sources_can_be_used_interchangeably(self, mock_file):
        """Both sources should return string from read()."""
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()
        class TestModel(Base):
            __tablename__ = "test"
            __table_args__ = {"schema": "test"}
            id = Column(Integer, primary_key=True)
        
        config = MergeConfig.from_model(
            model=TestModel,
            source_table="staging.test",
            unique_key="id",
            exclude_mixins=False,
            auto_exclude_server_defaults=False
        )
        
        sql_source = SqlFileSource(sql_file="test.sql")
        template_source = TemplateSource(config=config)
        
        sql_result = sql_source.read(params={})
        template_result = template_source.read(params={})
        
        assert isinstance(sql_result, str)
        assert isinstance(template_result, str)
