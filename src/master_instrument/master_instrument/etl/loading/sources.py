"""Data source abstractions for loading operations."""

import re
from pathlib import Path
from typing import Protocol, Any, Dict, Union
from jinja2 import Environment, FileSystemLoader

from master_instrument.etl.loading.configs import (
    MergeConfig, 
    UpsertConfig, 
    CDCConfig, 
    InsertConfig
)

ConfigType = Union[MergeConfig, UpsertConfig, CDCConfig, InsertConfig]


class DataSource(Protocol):
    """Protocol for data sources - defines how to get SQL/data."""
    
    @property
    def config(self) -> ConfigType | None:
        """Optional config attribute for template-based sources."""
        ...
    
    def read(self, params: dict[str, Any]) -> str:
        """Read and return SQL query or data.
        
        Args:
            params: Parameters that may be used to customize the query
            
        Returns:
            SQL query string or data content
        """
        ...


class SqlFileSource:
    """Read SQL queries from files in the sql/ directory."""
    
    def __init__(self, sql_file: str):
        """Initialize SQL file source.
        
        Args:
            sql_file: Name of SQL file relative to loading/sql/ directory
        """
        self.sql_file = sql_file
        self._config: ConfigType | None = None
    
    @property
    def config(self) -> ConfigType | None:
        """No config for file sources."""
        return self._config
    
    def read(self, params: dict[str, Any]) -> str:
        """Read SQL file content.
        
        Args:
            params: Not used for file reading, but kept for protocol consistency
            
        Returns:
            SQL file content as string
        """
        sql_path = Path(__file__).parent / "sql" / self.sql_file
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()


class TemplateSource:
    """DataSource implementation that generates SQL from Jinja2 templates.
    
    Implements the DataSource protocol (read method) for compatibility with 
    existing loading infrastructure (BatchLoader, BatchLoadPipeline, etc.).
    """
    
    def __init__(self, config: ConfigType):
        """Initialize template source.
        
        Args:
            config: Template configuration (MergeConfig, UpsertConfig, CDCConfig, or InsertConfig)
        """
        self._config: ConfigType = config
        
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))
    
    @property
    def config(self) -> ConfigType:
        """Get the config."""
        return self._config
    
    def read(self, params: Dict[str, Any]) -> str:
        """Generate SQL from template with given parameters.
        
        Implements DataSource.read() protocol method.
        
        Args:
            params: Runtime parameters to merge with config 
                   (e.g., :start_date, :end_date for batch loading)
            
        Returns:
            Generated SQL query string, cleaned and validated
        """
        template_params = self._config.to_template_params()
        template_params.update(params)
        
        template = self.jinja_env.get_template(f"{self.config.scheme.value}.sql.j2")
        raw_sql = template.render(**template_params)
        
        return self._clean_sql(raw_sql)
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize generated SQL.
        
        Fixes common template generation issues:
        - Double commas (, ,)
        - Orphan commas on their own line
        - Comma before SELECT/MERGE/INSERT statements
        - Excessive whitespace
        
        This makes templates more robust - snippets don't need to worry
        about comma placement, the cleanup handles edge cases.
        
        Args:
            sql: Raw SQL from template rendering
            
        Returns:
            Cleaned SQL string
        """
        # 1. Remove orphan commas (comma alone on a line)
        sql = re.sub(r'^\s*,\s*$', '', sql, flags=re.MULTILINE)
        
        # 2. Remove double/multiple commas with whitespace between
        sql = re.sub(r',(\s*,)+', ',', sql)
        
        # 3. Remove comma before statement keywords (SELECT, MERGE, INSERT, UPDATE, DELETE)
        sql = re.sub(r',(\s*)(SELECT|MERGE|INSERT|UPDATE|DELETE)\b', r'\1\2', sql, flags=re.IGNORECASE)
        
        # 4. Remove comma after WITH (WITH , -> WITH)
        sql = re.sub(r'\bWITH\s*,', 'WITH ', sql, flags=re.IGNORECASE)
        
        # 5. Normalize multiple blank lines to single blank line
        sql = re.sub(r'\n\s*\n\s*\n', '\n\n', sql)
        
        # 6. Remove trailing whitespace on each line
        sql = re.sub(r'[ \t]+$', '', sql, flags=re.MULTILINE)
        
        return sql.strip()
