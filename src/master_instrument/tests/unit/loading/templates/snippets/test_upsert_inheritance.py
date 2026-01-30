"""Tests for upsert inheritance snippets.

Tests snippets/upsert_inheritance_parent.j2 and upsert_inheritance_child.j2.
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


class TestUpsertInheritanceParentSnippet:
    """Test snippets/upsert_inheritance_parent.j2."""
    
    def test_basic_parent_upsert(self, jinja_env):
        """Test parent UPSERT generates correct SQL."""
        template = jinja_env.from_string("""
{%- set inheritance = {
    'parent_table': 'master.entity',
    'parent_unique_key': 'entity_id',
    'parent_columns': ['entity_id', 'name', 'code'],
    'source_parent_key': 'natural_key'
} -%}
{%- set source_cte = 'stg_source' -%}
{%- set audit = None -%}
{%- set mapping = None -%}
{% include 'snippets/upsert_inheritance_parent.j2' %}
""")
        sql = template.render()
        
        assert "upsert_parent AS" in sql
        assert "INSERT INTO master.entity" in sql
        assert "ON CONFLICT (natural_key)" in sql
        assert "DO UPDATE SET" in sql
        assert "RETURNING entity_id" in sql

    def test_parent_with_audit(self, jinja_env):
        """Test parent UPSERT with audit columns."""
        template = jinja_env.from_string("""
{%- set inheritance = {
    'parent_table': 'master.entity',
    'parent_unique_key': 'entity_id',
    'parent_columns': ['entity_id', 'name'],
    'source_parent_key': 'nat_key'
} -%}
{%- set source_cte = 'stg_source' -%}
{%- set audit = {'with_created_at': True, 'with_updated_at': True} -%}
{%- set mapping = None -%}
{% include 'snippets/upsert_inheritance_parent.j2' %}
""")
        sql = template.render()
        
        assert ",created_at" in sql
        assert ",updated_at" in sql


class TestUpsertInheritanceChildSnippet:
    """Test snippets/upsert_inheritance_child.j2."""
    
    def test_basic_child_upsert(self, jinja_env):
        """Test child UPSERT generates correct SQL."""
        template = jinja_env.from_string("""
{%- set inheritance = {
    'parent_table': 'master.entity',
    'parent_unique_key': 'entity_id',
    'child_table': 'master.company',
    'child_unique_key': 'company_id',
    'child_columns': ['company_id', 'ticker', 'sector'],
    'source_parent_key': 'natural_key'
} -%}
{%- set source_cte = 'stg_source' -%}
{%- set audit = None -%}
{%- set mapping = None -%}
{% include 'snippets/upsert_inheritance_child.j2' %}
""")
        sql = template.render()
        
        assert "enriched_parent AS" in sql
        assert "upsert_child AS" in sql
        assert "INSERT INTO master.company" in sql
        assert "ON CONFLICT (company_id)" in sql
        assert "FROM upsert_parent" in sql

    def test_child_with_mapping(self, jinja_env):
        """Test child UPSERT with mapping returns correct columns."""
        template = jinja_env.from_string("""
{%- set inheritance = {
    'parent_table': 'master.entity',
    'parent_unique_key': 'entity_id',
    'child_table': 'master.company',
    'child_unique_key': 'company_id',
    'child_columns': ['company_id', 'name'],
    'source_parent_key': 'natural_key'
} -%}
{%- set source_cte = 'stg_source' -%}
{%- set audit = None -%}
{%- set mapping = {
    'source_external_id_column': 'external_id',
    'data_source_column': 'data_source_id'
} -%}
{% include 'snippets/upsert_inheritance_child.j2' %}
""")
        sql = template.render()
        
        # UPSERT uses different alias (no src available in RETURNING for INSERT..ON CONFLICT)
        assert "RETURNING company_id" in sql

    def test_child_do_nothing_when_no_data_columns(self, jinja_env):
        """Test child UPSERT uses DO NOTHING when only PK column."""
        template = jinja_env.from_string("""
{%- set inheritance = {
    'parent_table': 'master.entity',
    'parent_unique_key': 'entity_id',
    'child_table': 'master.company',
    'child_unique_key': 'company_id',
    'child_columns': ['company_id'],
    'source_parent_key': 'natural_key'
} -%}
{%- set source_cte = 'stg_source' -%}
{%- set audit = None -%}
{%- set mapping = None -%}
{% include 'snippets/upsert_inheritance_child.j2' %}
""")
        sql = template.render()
        
        assert "DO NOTHING" in sql
