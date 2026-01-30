"""Exhaustive tests for merge.sql.j2"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestMergeSimpleBasic:
    """Basic tests without advanced features."""
    
    def test_basic_merge(self):
        """Basic merge test."""
        params = {
            'target_table': 'test.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'description'],
            'data_columns': ['name', 'description'],
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'MERGE INTO test.company' in sql
        assert 'FROM intermediate.int_company' in sql
        assert len(sql) > 0
    
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
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # All keys must be in ON clause
        for key in unique_key:
            assert f'src.{key} = tgt.{key}' in sql


class TestMergeSimpleWithMapping:
    """Tests with mapping (external_id → internal_id)."""
    
    def test_merge_with_mapping(self):
        """Test merge with mapping."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'ticker'],
            'data_columns': ['name', 'ticker'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id',
                'source_external_id_column': 'external_id'
            },
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.entity_mapping' in sql
        assert 'external_entity_id' in sql
        assert 'ON CONFLICT' in sql


class TestMergeSimpleWithSelfReference:
    """Tests with self-reference FK (critical!)."""
    
    def test_self_reference_single(self):
        """Test with a single self-reference column."""
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
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Must have self-reference UPDATE
        assert 'Update self-referencing columns' in sql or 'self-referencing' in sql
        assert 'parent_company_id = src.parent_company_id' in sql
        assert 'JOIN master.entity_mapping self_map' in sql
        # JOIN must use external_id (general mapping)
        assert 'self_map.external_entity_id = src.external_id' in sql
    
    def test_self_reference_multiple(self):
        """Test with multiple self-reference columns."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'parent_company_id', 'ultimate_organization_id'],
            'data_columns': ['name', 'parent_company_id', 'ultimate_organization_id'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id',
                'source_external_id_column': 'external_id'
            },
            'self_reference': type('SelfRef', (), {
                'columns': {
                    'parent_company_id': 'parent_company_id',
                    'ultimate_organization_id': 'ultimate_organization_id'
                },
                'requires_mapping': True
            })(),
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Must have both columns in SET
        assert 'parent_company_id = src.parent_company_id' in sql
        assert 'ultimate_organization_id = src.ultimate_organization_id' in sql
        # JOIN must use external_id (general mapping), not specific columns
        assert 'self_map.external_entity_id = src.external_id' in sql


class TestMergeSimpleWithAudit:
    """Tests with audit columns."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    def test_audit_columns(self, with_audit):
        """Test with and without audit."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name'],
            'data_columns': ['name'],
            'with_audit': with_audit,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        if with_audit:
            assert 'created_at' in sql
            assert 'updated_at' in sql
            assert 'CURRENT_TIMESTAMP' in sql
    
    def test_soft_delete(self):
        """Test with soft delete."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name'],
            'data_columns': ['name'],
            'with_audit': True,
            'audit': {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': True
            },
            'with_soft_delete': True,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'deleted_at' in sql
        assert 'deleted_at IS NULL' in sql


class TestMergeSimpleWithBatchDate:
    """Tests with batch_date_column filtering."""
    
    def test_batch_date_filtering(self):
        """Test with batch_date_column."""
        params = {
            'target_table': 'master.price',
            'source_table': 'intermediate.int_price',
            'unique_key': ['price_id'],
            'columns': ['price_id', 'trade_date', 'close_price'],
            'data_columns': ['trade_date', 'close_price'],
            'batch': type('Batch', (), {'batch_date_column': 'trade_date'})(),
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'trade_date' in sql
        assert ':start_date' in sql
        assert ':end_date' in sql


class TestMergeSimpleCombinations:
    """Tests of feature combinations."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("with_soft_delete", [True, False])
    @pytest.mark.parametrize("with_mapping", [True, False])
    def test_all_combinations(self, with_audit, with_soft_delete, with_mapping):
        """Test all valid combinations."""
        if with_soft_delete and not with_audit:
            pytest.skip("soft_delete requires audit")
        
        params = {
            'target_table': 'test.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'description'],
            'data_columns': ['name', 'description'],
            'with_audit': with_audit,
            'with_soft_delete': with_soft_delete,
            'hard_delete': False
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': with_soft_delete
            }
        
        if with_mapping:
            params['source_external_id_column'] = 'external_id'
            params['data_source_column'] = 'data_source_id'
            params['mapping'] = {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id'
            }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'MERGE INTO test.company' in sql
        assert len(sql) > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
