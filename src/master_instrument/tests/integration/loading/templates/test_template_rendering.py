"""Exhaustive tests for rendering all templates with all possible configurations.

This file ensures that no config combination causes a Jinja2 error.
"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from itertools import product

# Setup Jinja2 environment
TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestMergeTemplateExhaustive:
    """Test all config combinations for merge.sql.j2"""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("with_soft_delete", [True, False])
    @pytest.mark.parametrize("hard_delete", [True, False])
    def test_merge_all_combinations(self, with_audit, with_soft_delete, hard_delete):
        """Test all basic merge combinations."""
        # Skip invalid: soft_delete requires audit
        if with_soft_delete and not with_audit:
            pytest.skip("soft_delete requires audit")
        
        # Skip invalid: soft_delete and hard_delete are mutually exclusive
        if with_soft_delete and hard_delete:
            pytest.skip("soft_delete and hard_delete are mutually exclusive")
        
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': ['id', 'name', 'value'],
            'data_columns': ['name', 'value'],
            'with_audit': with_audit,
            'with_soft_delete': with_soft_delete,
            'hard_delete': hard_delete,
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': with_soft_delete
            }
        
        template = jinja_env.get_template('merge.sql.j2')
        # Should not raise any Jinja2 error
        sql = template.render(**params)
        
        # Basic validations
        assert 'MERGE INTO master.test' in sql or 'INSERT INTO master.test' in sql
        assert len(sql) > 0
    
    @pytest.mark.parametrize("created,updated,deleted", [
        (True, True, True),
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, True, True),
        (False, True, False),
        (False, False, True),
    ])
    def test_merge_all_audit_combinations(self, created, updated, deleted):
        """Test all audit column combinations."""
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': ['id', 'name'],
            'data_columns': ['name'],
            'with_audit': True,
            'audit': {
                'with_created_at': created,
                'with_updated_at': updated,
                'with_deleted_at': deleted
            },
            'with_soft_delete': deleted,  # soft_delete requires deleted_at
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Verify audit columns presence
        if created:
            assert 'created_at' in sql.lower()
        if updated:
            assert 'updated_at' in sql.lower()
        if deleted:
            assert 'deleted_at' in sql.lower()


class TestInsertTemplateExhaustive:
    """Test all combinations for insert.sql.j2"""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("with_batch", [True, False])
    def test_insert_all_combinations(self, with_audit, with_batch):
        """Test all basic insert combinations."""
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'columns': ['id', 'name'],
            'with_audit': with_audit
        }
        
        if with_batch:
            params['batch'] = type('Batch', (), {'batch_date_column': 'trade_date'})()
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.test' in sql
        if with_batch:
            assert ':start_date' in sql
            assert ':end_date' in sql


class TestUpsertTemplateExhaustive:
    """Test all combinations for upsert.sql.j2"""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("with_data_columns", [True, False])
    def test_upsert_all_combinations(self, with_audit, with_data_columns):
        """Test all upsert combinations."""
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': ['id', 'name'],
            'data_columns': ['name'] if with_data_columns else [],
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
        
        assert 'ON CONFLICT' in sql.upper()
        
        if with_data_columns or with_audit:
            assert 'DO UPDATE SET' in sql
        else:
            assert 'DO NOTHING' in sql


class TestInheritanceMergeTemplateExhaustive:
    """Test inheritance in merge.sql.j2 (no separate inheritance_merge.sql.j2 template)"""
    
    @pytest.mark.parametrize("with_mapping", [True, False])
    @pytest.mark.parametrize("with_audit", [True, False])
    def test_inheritance_all_combinations(self, with_mapping, with_audit):
        """Test merge with inheritance config."""
        params = {
            'target_table': 'master.child',
            'source_table': 'staging.child',
            'unique_key': 'child_id',
            'columns': ['child_id', 'name'],
            'data_columns': ['name'],
            'with_audit': with_audit,
            'data_source_column': 'source_id',
            'inheritance': {
                'parent_table': 'master.parent',
                'parent_unique_key': 'parent_id',
                'child_table': 'master.child',
                'child_unique_key': 'child_id',
                'source_parent_key': 'parent_id',
                'parent_columns': ['parent_id', 'common_name'],
                'child_columns': ['child_id', 'name']  # All child columns (same as root 'columns')
            }
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        if with_mapping:
            params['mapping'] = {
                'mapping_table': 'master.test_mapping',
                'mapping_external_column': 'external_id',
                'data_source_column': 'source_id',
                'mapping_internal_column': 'internal_id'
            }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Verify both parent and child operations
        assert 'master.parent' in sql.lower()
        assert 'master.child' in sql.lower()


class TestCDCTemplateExhaustive:
    """Test all combinations for cdc_changes.sql.j2"""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    @pytest.mark.parametrize("with_soft_delete", [True, False])
    def test_cdc_all_combinations(self, with_audit, with_soft_delete):
        """Test all CDC combinations."""
        if with_soft_delete and not with_audit:
            pytest.skip("soft_delete requires audit")
            
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test_changes',
            'unique_key': ['id'],
            'columns': ['id', 'name', 'value'],
            'data_columns': ['name', 'value'],
            'with_audit': with_audit,
            'with_soft_delete': with_soft_delete,
            'hard_delete': not with_soft_delete
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': True
            }
        
        template = jinja_env.get_template('cdc_changes.sql.j2')
        sql = template.render(**params)
        
        assert 'sys_change_operation' in sql.lower()
        assert 'INSERT INTO master.test' in sql


class TestBatchDateRangeTemplate:
    """Test batch date filtering in templates"""
    
    @pytest.mark.parametrize("date_column", ["trade_date", "created_at", "updated_at"])
    def test_batch_date_range_renders(self, date_column):
        """Test that merge renders with batch_date_column."""
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': ['id', 'name'],
            'data_columns': ['name'],
            'batch': type('Batch', (), {'batch_date_column': date_column})(),
            'with_audit': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert date_column in sql
        assert ':start_date' in sql
        assert ':end_date' in sql


class TestEdgeCases:
    """Test edge cases and extreme configurations."""
    
    def test_merge_with_composite_key(self):
        """Test merge with composite key."""
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': ['id1', 'id2', 'id3'],
            'columns': ['id1', 'id2', 'id3', 'name'],
            'data_columns': ['name'],
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        assert 'id1' in sql and 'id2' in sql and 'id3' in sql
    
    def test_merge_with_many_columns(self):
        """Test merge with many columns (performance)."""
        columns = ['id'] + [f'col_{i}' for i in range(100)]
        params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': columns,
            'data_columns': [c for c in columns if c != 'id'],
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
        
        assert 'col_0' in sql
        assert 'col_99' in sql
        assert len(sql) > 1000  # Should be quite long
    
    def test_all_templates_exist_and_render_basic(self):
        """Smoke test: all main templates exist and render."""
        templates_to_test = [
            'merge.sql.j2',
            'insert.sql.j2',
            'upsert.sql.j2',
            'cdc_changes.sql.j2'
        ]
        
        basic_params = {
            'target_table': 'master.test',
            'source_table': 'staging.test',
            'unique_key': 'id',
            'columns': ['id', 'name'],
            'data_columns': ['name'],
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        for template_name in templates_to_test:
            template = jinja_env.get_template(template_name)
            
            # Adjust params for specific templates
            if template_name == 'cdc_changes.sql.j2':
                params = dict(basic_params)
                params['cdc'] = {
                    'detect_changes': False,
                    'hash_columns': None
                }
            else:
                params = basic_params
            
            # Should not raise
            sql = template.render(**params)
            assert len(sql) > 0, f"Template {template_name} rendered empty SQL"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
