"""Tests for inheritance snippets.

Tests snippets/inheritance_parent.j2 and inheritance_child.j2 for MERGE pattern.
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


class TestInheritanceParentSnippet:
    """Test snippets/inheritance_parent.j2."""
    
    def test_basic_parent_merge(self, jinja_env):
        """Test parent MERGE generates correct SQL."""
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
{% include 'snippets/inheritance_parent.j2' %}
""")
        sql = template.render()
        
        assert "merge_parent AS" in sql
        assert "MERGE INTO master.entity" in sql
        assert "ON src.natural_key = tgt.entity_id" in sql
        assert "WHEN MATCHED" in sql
        assert "THEN" in sql
        assert "WHEN NOT MATCHED THEN" in sql
        assert "RETURNING tgt.entity_id" in sql

    def test_parent_with_audit(self, jinja_env):
        """Test parent MERGE with audit columns."""
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
{% include 'snippets/inheritance_parent.j2' %}
""")
        sql = template.render()
        
        assert "created_at" in sql
        assert "updated_at" in sql
        assert "CURRENT_TIMESTAMP" in sql


class TestInheritanceChildSnippet:
    """Test snippets/inheritance_child.j2."""
    
    def test_basic_child_merge(self, jinja_env):
        """Test child MERGE generates correct SQL."""
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
{% include 'snippets/inheritance_child.j2' %}
""")
        sql = template.render()
        
        assert "enriched_parent AS" in sql
        assert "merge_child AS" in sql
        assert "MERGE INTO master.company" in sql
        assert "FROM merge_parent" in sql
        assert "CROSS JOIN" in sql  # Sans mapping: CROSS JOIN
        # Child data columns (non-PK)
        assert "ticker" in sql
        assert "sector" in sql

    def test_child_with_mapping(self, jinja_env):
        """Test child MERGE with mapping uses COALESCE for existing entities."""
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
    'mapping_table': 'master.entity_mapping',
    'source_external_id_column': 'external_id',
    'data_source_column': 'data_source_id',
    'mapping_external_column': 'external_entity_id',
    'mapping_internal_column': 'internal_entity_id',
    'mapping_type_column': 'entity_type_id',
    'source_type_column': 'entity_type_id'
} -%}
{% include 'snippets/inheritance_child.j2' %}
""")
        sql = template.render()
        
        # With mapping: COALESCE allows updating child even if parent unchanged
        assert "COALESCE(mp.entity_id, em.internal_entity_id)" in sql
        assert "FROM stg_source src" in sql
        assert "LEFT JOIN merge_parent mp" in sql
        assert "LEFT JOIN master.entity_mapping em" in sql
        assert "RETURNING tgt.company_id, src.external_id, src.data_source_id" in sql
