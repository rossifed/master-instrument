"""Tests for upsert_core CTE snippet.

Tests snippets/upsert_core.j2 snippet which generates the core UPSERT statement.
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


class TestUpsertCoreSnippet:
    """Test snippets/upsert_core.j2 snippet."""
    
    def test_simple_upsert(self, jinja_env):
        """Test basic UPSERT (INSERT ON CONFLICT)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        assert "INSERT INTO master.currency" in sql
        assert "SELECT" in sql
        assert "FROM stg_source" in sql
        assert "ON CONFLICT (code)" in sql
        assert "DO UPDATE SET" in sql
        assert "name = EXCLUDED.name" in sql
    
    def test_composite_key(self, jinja_env):
        """Test UPSERT with composite key."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.price' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = ['instrument_id', 'date'] -%}
{%- set columns = ['instrument_id', 'date', 'price'] -%}
{%- set data_columns = ['price'] -%}
{%- set audit = None -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        assert "ON CONFLICT (instrument_id, date)" in sql
    
    def test_with_returning(self, jinja_env):
        """Test UPSERT with RETURNING clause."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.entity' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'entity_id' -%}
{%- set columns = ['entity_id', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{%- set returning_clause = 'entity_id, external_id' -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        assert "RETURNING entity_id, external_id" in sql

    def test_do_nothing_when_no_data_columns(self, jinja_env):
        """Test UPSERT uses DO NOTHING when no data columns to update."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code'] -%}
{%- set data_columns = [] -%}
{%- set audit = None -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        assert "DO NOTHING" in sql
        assert "DO UPDATE SET" not in sql

    def test_with_audit_columns(self, jinja_env):
        """Test UPSERT with audit columns."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = {'with_created_at': True, 'with_updated_at': True, 'with_deleted_at': False} -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        assert ",created_at" in sql
        assert ",updated_at" in sql
        assert "CURRENT_TIMESTAMP" in sql
        assert ",updated_at = CURRENT_TIMESTAMP" in sql

    def test_with_source_unique_key(self, jinja_env):
        """Test UPSERT with different source key (inheritance pattern)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.entity' -%}
{%- set source_cte = 'enriched_parent' -%}
{%- set unique_key = 'entity_id' -%}
{%- set source_unique_key = 'parent_id' -%}
{%- set columns = ['entity_id', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{% include 'snippets/upsert_core.j2' %}
""")
        sql = template.render()
        
        # The SELECT should use source_unique_key for PK column
        assert "parent_id" in sql
