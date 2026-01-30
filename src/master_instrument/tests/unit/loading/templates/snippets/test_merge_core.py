"""Tests for merge_core CTE snippet.

Tests snippets/merge_core.j2 snippet which generates the core MERGE statement.
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


class TestMergeCoreSnippet:
    """Test snippets/merge_core.j2 snippet."""
    
    def test_simple_merge(self, jinja_env):
        """Test basic MERGE without audit or soft delete."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        assert "MERGE INTO master.currency AS tgt" in sql
        assert "USING stg_source AS src" in sql
        assert "ON src.code = tgt.code" in sql
        assert "WHEN MATCHED" in sql
        assert "THEN" in sql
        assert "UPDATE SET" in sql
        assert "name = src.name" in sql
        assert "WHEN NOT MATCHED THEN" in sql
        assert "INSERT" in sql
        assert "VALUES" in sql
    
    def test_composite_key(self, jinja_env):
        """Test MERGE with composite key."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.price' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = ['instrument_id', 'date'] -%}
{%- set columns = ['instrument_id', 'date', 'price'] -%}
{%- set data_columns = ['price'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        # Composite key should have AND on new line for readability
        assert "ON src.instrument_id = tgt.instrument_id" in sql
        assert "AND src.date = tgt.date" in sql
    
    def test_with_source_unique_key(self, jinja_env):
        """Test MERGE with different source key (inheritance pattern)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.entity' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'entity_id' -%}
{%- set source_unique_key = 'internal_company_id' -%}
{%- set columns = ['name', 'description'] -%}
{%- set data_columns = ['name', 'description'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        # Critical: source key different from target key
        assert "ON src.internal_company_id = tgt.entity_id" in sql
        assert "ON src.entity_id = tgt.entity_id" not in sql
    
    def test_with_returning(self, jinja_env):
        """Test MERGE with RETURNING clause."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'company_id' -%}
{%- set columns = ['company_id', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{%- set returning_clause = 'company_id, src.external_id' -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        assert "RETURNING company_id, src.external_id" in sql


class TestMergeCoreWithSoftDelete:
    """Test merge_core snippet with soft delete."""
    
    def test_soft_delete_flag(self, jinja_env):
        """Test MERGE with soft delete modifies WHEN MATCHED."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'company_id' -%}
{%- set columns = ['company_id', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = True -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        assert "WHEN MATCHED" in sql
        assert "tgt.deleted_at IS NULL" in sql
        assert "WHEN NOT MATCHED BY SOURCE" in sql or "WHEN NOT MATCHED" in sql
        assert "UPDATE SET" in sql
        assert "deleted_at = CURRENT_TIMESTAMP" in sql or "deleted_at" in sql

    def test_hard_delete(self, jinja_env):
        """Test MERGE with hard delete uses DELETE clause."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'company_id' -%}
{%- set columns = ['company_id', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = None -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = True -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        assert "WHEN NOT MATCHED BY SOURCE" in sql
        assert "DELETE" in sql


class TestMergeCoreWithAudit:
    """Test merge_core snippet with audit columns."""
    
    def test_audit_columns_in_insert(self, jinja_env):
        """Test MERGE adds audit columns in INSERT."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = {'with_created_at': True, 'with_updated_at': True, 'with_deleted_at': False} -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        assert "created_at" in sql
        assert "updated_at" in sql
        assert "CURRENT_TIMESTAMP" in sql

    def test_audit_updated_at_in_update(self, jinja_env):
        """Test MERGE adds updated_at in UPDATE clause."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.currency' -%}
{%- set source_cte = 'stg_source' -%}
{%- set unique_key = 'code' -%}
{%- set columns = ['code', 'name'] -%}
{%- set data_columns = ['name'] -%}
{%- set audit = {'with_created_at': True, 'with_updated_at': True, 'with_deleted_at': False} -%}
{%- set with_soft_delete = False -%}
{%- set hard_delete = False -%}
{% include 'snippets/merge_core.j2' %}
""")
        sql = template.render()
        
        # Check UPDATE clause has updated_at
        assert "updated_at = CURRENT_TIMESTAMP" in sql

