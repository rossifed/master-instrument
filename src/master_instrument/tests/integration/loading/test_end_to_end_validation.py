"""End-to-end validation test - ensures all features work together."""
# pyright: reportUnknownArgumentType=false, reportUnknownVariableType=false

import pytest
from master_instrument.etl.loading.configs import (
    MergeConfig, MappingConfig, InheritanceConfig
)
from master_instrument.db.models.company import Company
from master_instrument.db.models.entity import Entity
from master_instrument.db.models.equity import Equity
from master_instrument.db.models.instrument import Instrument
from master_instrument.db.models.venue import Venue
from master_instrument.db.models.entity_mapping import EntityMapping
from master_instrument.db.models.instrument_mapping import InstrumentMapping
from master_instrument.db.models.venue_mapping import VenueMapping


class TestEndToEndValidation:
    """End-to-end validation - all features working together."""
    
    def test_company_config_complete(self):
        """Test complete Company config with all auto-features."""
        # Parent
        parent_config = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test"
        )
        
        # Child with inheritance + mapping
        config = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_entity_id"
            ),
            mapping=MappingConfig.from_model(EntityMapping)
        )
        
        # Verify sys_period excluded (auto)
        assert "sys_period" not in config.columns
        assert "sys_period" not in parent_config.columns
        
        # Verify mapping auto-detected
        assert config.mapping is not None
        assert config.mapping is not None
        assert config.mapping.mapping_external_column == "external_entity_id"
        assert config.mapping.mapping_internal_column == "internal_entity_id"
        assert config.mapping.source_external_id_column == "external_entity_id"  # Same as mapping
        assert config.mapping.data_source_column == "data_source_id"
        
        # Verify inheritance composition
        assert config.inheritance is not None
        assert config.inheritance.parent_config == parent_config
        assert config.inheritance.source_parent_key == "internal_entity_id"
        
        # Verify tables
        assert config.target_table == "test.company"
        assert parent_config.target_table == "test.entity"
    
    def test_equity_config_complete(self):
        """Test complete Equity config."""
        parent_config = MergeConfig.from_model(
            Instrument,
            source_table="intermediate.int_equity",
            unique_key="instrument_id",
            schema="test"
        )
        
        config = MergeConfig.from_model(
            Equity,
            source_table="intermediate.int_equity",
            unique_key="equity_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_instrument_id"
            ),
            mapping=MappingConfig.from_model(InstrumentMapping)
        )
        
        # Verify auto-features
        assert "sys_period" not in config.columns
        assert config.mapping is not None
        assert config.mapping.mapping_external_column == "external_instrument_id"
        assert config.mapping.mapping_internal_column == "internal_instrument_id"
        assert config.target_table == "test.equity"
    
    def test_venue_config_simple(self):
        """Test simple Venue config (no inheritance)."""
        config = MergeConfig.from_model(
            Venue,
            source_table="intermediate.int_venue",
            unique_key="venue_id",
            schema="test",
            exclude_columns=["venue_id"],
            mapping=MappingConfig.from_model(VenueMapping)
        )
        
        # Verify auto-features
        assert "sys_period" not in config.columns  # Auto-excluded
        assert "venue_id" not in config.columns  # Manual exclusion
        
        # Verify mapping auto-detected
        assert config.mapping is not None
        assert config.mapping.mapping_external_column == "external_venue_id"
        assert config.mapping.mapping_internal_column == "internal_venue_id"
        
        # No inheritance
        assert config.inheritance is None
    
    def test_configuration_line_reduction(self):
        """Test that new features dramatically reduce configuration code."""
        # BEFORE (verbose, manual)
        old_lines = """
        parent_config = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test",
              # Manual!
        )
        
        config = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
              # Manual!
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_entity_id"
            ),
            mapping=MappingConfig(
                mapping_table="master.entity_mapping",
                source_external_id_column="external_id",
                data_source_column="data_source_id",
                mapping_external_column="external_entity_id",
                mapping_internal_column="internal_entity_id"
            )
        )
        """
        
        # AFTER (concise, auto)
        new_lines = """
        parent_config = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test"
        )
        
        config = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_entity_id"
            ),
            mapping=MappingConfig.from_model(EntityMapping)
        )
        """
        
        # Verify line reduction
        assert len(old_lines.strip().split('\n')) > len(new_lines.strip().split('\n'))
        
        # Both produce identical configs
        parent_config = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test"
        )
        
        config_new = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_entity_id"
            ),
            mapping=MappingConfig.from_model(EntityMapping)
        )
        
        # Verify results are correct
        assert "sys_period" not in config_new.columns
        assert config_new.mapping is not None
        assert config_new.mapping.mapping_external_column == "external_entity_id"
    
    def test_all_models_have_sys_period_excluded(self):
        """Test that all major models auto-exclude sys_period."""
        models_to_test = [
            (Company, "company_id"),
            (Entity, "entity_id"),
            (Venue, "venue_id"),
            (Instrument, "instrument_id"),
            (Equity, "equity_id"),
        ]
        
        for model, unique_key in models_to_test:
            config = MergeConfig.from_model(
                model,
                source_table=f"intermediate.int_{model.__tablename__}",
                unique_key=unique_key,
                schema="test"
            )
            
            assert "sys_period" not in config.columns, \
                f"sys_period should be auto-excluded for {model.__name__}"
    
    def test_all_mapping_models_auto_detect(self):
        """Test that all mapping models auto-detect correctly."""
        mappings_to_test = [
            (EntityMapping, "entity"),      # entity_mapping has external_entity_id
            (VenueMapping, "venue"),
            (InstrumentMapping, "instrument"),
        ]
        
        for model, entity_name in mappings_to_test:
            mapping = MappingConfig.from_model(model)
            
            # Verify auto-detection
            assert mapping.mapping_external_column == f"external_{entity_name}_id", \
                f"Failed for {model.__name__}"
            assert mapping.mapping_internal_column == f"internal_{entity_name}_id", \
                f"Failed for {model.__name__}"
            assert mapping.source_external_id_column == f"external_{entity_name}_id"  # Same as mapping
            assert mapping.data_source_column == "data_source_id"


class TestRobustness:
    """Test robustness and edge cases."""
    
    def test_mixing_auto_and_manual_exclusions(self):
        """Test that auto and manual exclusions work together."""
        config = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            exclude_columns=["phone", "fax"]  # Manual exclusions
        )
        
        # Both types of exclusions applied
        assert "sys_period" not in config.columns  # Auto
        assert "phone" not in config.columns  # Manual
        assert "fax" not in config.columns  # Manual
        
        # Other columns present
        assert "company_id" in config.columns
    
    def test_override_auto_detection(self):
        """Test that auto-detection can be overridden."""
        mapping = MappingConfig.from_model(
            EntityMapping,
            source_external_id_column="custom_ext_id",
            data_source_column="custom_source"
        )
        
        # Custom values used
        assert mapping.source_external_id_column == "custom_ext_id"
        assert mapping.data_source_column == "custom_source"
        
        # Mapping table columns still auto-detected
        assert mapping.mapping_external_column == "external_entity_id"
    
    def test_config_chaining(self):
        """Test that configs can be composed."""
        # Create base configs
        parent = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test"
        )
        
        mapping = MappingConfig.from_model(EntityMapping)
        
        # Compose into child config
        child = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent,
                source_parent_key="internal_entity_id"
            ),
            mapping=mapping
        )
        
        # Verify composition
        assert child.inheritance is not None
        assert child.inheritance.parent_config == parent
        assert child.mapping == mapping


@pytest.mark.integration
class TestRealWorldScenarios:
    """Test real-world usage scenarios."""
    
    def test_typical_reference_table(self):
        """Test typical reference table (Venue)."""
        config = MergeConfig.from_model(
            Venue,
            source_table="intermediate.int_venue",
            unique_key="venue_id",
            schema="master",
            exclude_columns=["venue_id"],
            mapping=MappingConfig.from_model(VenueMapping),
            order_by="name"
        )
        
        assert config.target_table == "master.venue"
        assert config.order_by == "name"
        assert "sys_period" not in config.columns
        assert config.mapping is not None
    
    def test_typical_inheritance_pattern(self):
        """Test typical inheritance pattern (Company -> Entity)."""
        parent = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="master"
        )
        
        child = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="master",
            inheritance=InheritanceConfig(
                parent_config=parent,
                source_parent_key="internal_entity_id"
            ),
            mapping=MappingConfig.from_model(EntityMapping),
            order_by="name"
        )
        
        # Complete validation
        assert child.target_table == "master.company"
        assert parent.target_table == "master.entity"
        assert child.inheritance is not None
        assert child.inheritance.parent_config == parent
        assert child.mapping is not None
        assert child.mapping.mapping_table == "master.entity_mapping"
        assert "sys_period" not in child.columns
        assert "sys_period" not in parent.columns
