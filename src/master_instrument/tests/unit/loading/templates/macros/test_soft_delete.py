"""Tests for soft_delete macro.

Tests soft_delete_update and hard_delete_statement macros.
"""
import pytest
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import os


@pytest.fixture
def jinja_env():
    """Create Jinja2 environment for template rendering."""
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = current_dir.parent.parent.parent.parent.parent
    template_dir = project_root / "master_instrument" / "etl" / "loading" / "templates"
    return Environment(loader=FileSystemLoader(str(template_dir)))


class TestSoftDeleteUpdate:
    """Test soft_delete_update macro."""
    
    def test_basic_soft_delete(self, jinja_env):
        """Test soft delete UPDATE statement."""
        template = jinja_env.from_string("""
{%- from 'macros/soft_delete.j2' import soft_delete_update -%}
{{ soft_delete_update('master.company', 'company_id', 'stg_source') }}
""")
        sql = template.render()
        
        assert "UPDATE master.company" in sql
        assert "SET" in sql
        assert "deleted_at = CURRENT_TIMESTAMP" in sql
        assert "WHERE" in sql
        assert "NOT IN" in sql
    
    def test_with_exclude_condition(self, jinja_env):
        """Test soft delete with exclusion WHERE clause."""
        template = jinja_env.from_string("""
{%- from 'macros/soft_delete.j2' import soft_delete_update -%}
{{ soft_delete_update('master.company', 'company_id', 'stg_source', ['is_system = TRUE']) }}
""")
        sql = template.render()
        
        assert "AND NOT" in sql
        assert "is_system = TRUE" in sql


class TestHardDeleteStatement:
    """Test hard_delete_statement macro."""
    
    def test_basic_hard_delete(self, jinja_env):
        """Test hard DELETE statement."""
        template = jinja_env.from_string("""
{%- from 'macros/soft_delete.j2' import hard_delete_statement -%}
{{ hard_delete_statement('master.company', 'stg_source', ['company_id']) }}
""")
        sql = template.render()
        
        assert "DELETE FROM master.company" in sql
        assert "WHERE" in sql
        assert "NOT EXISTS" in sql
        assert "src.company_id = tgt.company_id" in sql
    
    def test_with_exclusion(self, jinja_env):
        """Test hard delete with exclusion condition."""
        template = jinja_env.from_string("""
{%- from 'macros/soft_delete.j2' import hard_delete_statement -%}
{{ hard_delete_statement('master.company', 'stg_source', ['company_id'], exclude_conditions='is_protected = TRUE') }}
""")
        sql = template.render()
        
        assert "AND NOT" in sql
        assert "is_protected = TRUE" in sql
    
    def test_composite_key(self, jinja_env):
        """Test hard delete with composite key."""
        template = jinja_env.from_string("""
{%- from 'macros/soft_delete.j2' import hard_delete_statement -%}
{{ hard_delete_statement('master.price', 'stg_source', ['instrument_id', 'date']) }}
""")
        sql = template.render()
        
        assert "DELETE FROM master.price" in sql
        assert "src.instrument_id = tgt.instrument_id" in sql
        assert "src.date = tgt.date" in sql
