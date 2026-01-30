"""Exhaustive tests for upsert.sql.j2"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestUpsertSimpleBasic:
    """Basic tests."""
    
    def test_basic_upsert(self):
        """Basic upsert test."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'ticker'],
            'data_columns': ['name', 'ticker'],
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.company' in sql
        assert 'ON CONFLICT' in sql
        assert 'company_id' in sql
    
    @pytest.mark.parametrize("num_keys", [1, 2, 3])
    def test_composite_keys(self, num_keys):
        """Test with composite keys."""
        if num_keys == 1:
            unique_key = ['id']
            columns = ['id', 'name']
        elif num_keys == 2:
            unique_key = ['id1', 'id2']
            columns = ['id1', 'id2', 'name']
        else:
            unique_key = ['id1', 'id2', 'id3']
            columns = ['id1', 'id2', 'id3', 'name']
        
        params = {
            'target_table': 'master.test',
            'source_table': 'intermediate.int_test',
            'unique_key': unique_key,
            'columns': columns,
            'data_columns': ['name'],
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'ON CONFLICT' in sql
        for key in unique_key:
            assert key in sql


class TestUpsertSimpleWithSelfReference:
    """Tests with self-reference."""
    
    def test_self_reference(self):
        """Test upsert with self-reference."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'parent_company_id'],
            'data_columns': ['name', 'parent_company_id'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id',
                'source_external_id_column': 'external_id'
            },
            'self_reference': type('SelfRef', (), {
                'columns': {'parent_company_id': 'parent_company_id'},
                'requires_mapping': True
            })(),
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'ON CONFLICT' in sql
        assert 'parent_company_id = src.parent_company_id' in sql
        assert 'self_map.external_entity_id = src.external_id' in sql


class TestUpsertSimpleDoNothing:
    """Tests DO NOTHING vs DO UPDATE."""
    
    def test_do_nothing_when_no_data_columns(self):
        """Test DO NOTHING quand pas de data_columns."""
        params = {
            'target_table': 'master.mapping',
            'source_table': 'intermediate.int_mapping',
            'unique_key': ['id'],
            'columns': ['id'],
            'data_columns': [],
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'DO NOTHING' in sql
    
    def test_do_update_when_has_data_columns(self):
        """Test DO UPDATE quand il y a data_columns."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['id'],
            'columns': ['id', 'name'],
            'data_columns': ['name'],
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'DO UPDATE SET' in sql


class TestUpsertSimpleWithAudit:
    """Tests with audit."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("has_data_columns", [True, False])
    def test_audit_combinations(self, with_audit, has_data_columns):
        """Test audit combinations."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name'],
            'data_columns': ['name'] if has_data_columns else [],
            'with_audit': with_audit
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'ON CONFLICT' in sql
        
        if has_data_columns or with_audit:
            assert 'DO UPDATE SET' in sql
        else:
            assert 'DO NOTHING' in sql
        
        if with_audit:
            assert 'created_at' in sql or 'updated_at' in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
