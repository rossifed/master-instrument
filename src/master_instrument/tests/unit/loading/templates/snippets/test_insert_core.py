"""Tests for insert_core snippet.

Tests snippets/insert_core.j2 which generates the core INSERT statement.
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


class TestInsertCoreSnippet:
    """Test snippets/insert_core.j2 snippet."""
    
    def test_simple_insert(self, jinja_env):
        """Test basic INSERT."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set columns = ['code', 'name'] -%}
{%- set audit = None -%}
{% include 'snippets/insert_core.j2' %}
""")
        sql = template.render()
        
        assert "INSERT INTO master.currency" in sql
        assert "code" in sql
        assert "name" in sql
        assert "SELECT" in sql
        assert "FROM stg_source" in sql
    
    def test_with_returning(self, jinja_env):
        """Test INSERT with RETURNING clause."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.entity' -%}
{%- set source_cte = 'stg_source' -%}
{%- set columns = ['entity_id', 'name'] -%}
{%- set audit = None -%}
{%- set returning_clause = 'entity_id, external_id' -%}
{% include 'snippets/insert_core.j2' %}
""")
        sql = template.render()
        
        assert "RETURNING entity_id, external_id" in sql

    def test_with_audit_columns(self, jinja_env):
        """Test INSERT with audit columns."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set columns = ['code', 'name'] -%}
{%- set audit = {'with_created_at': True, 'with_updated_at': True, 'with_deleted_at': False} -%}
{% include 'snippets/insert_core.j2' %}
""")
        sql = template.render()
        
        assert ",created_at" in sql
        assert ",updated_at" in sql
        assert "CURRENT_TIMESTAMP" in sql

    def test_with_source_column_mapping(self, jinja_env):
        """Test INSERT with column mapping (renaming)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set columns = ['code', 'name'] -%}
{%- set audit = None -%}
{%- set source_column_mapping = {'code': 'currency_code', 'name': 'currency_name'} -%}
{% include 'snippets/insert_core.j2' %}
""")
        sql = template.render()
        
        # SELECT should use mapped column names
        assert "currency_code" in sql
        assert "currency_name" in sql
