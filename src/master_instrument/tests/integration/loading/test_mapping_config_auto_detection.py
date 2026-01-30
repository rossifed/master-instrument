"""Unit tests for MappingConfig.from_model() auto-detection features."""

import pytest
from master_instrument.etl.loading.configs import MappingConfig
from master_instrument.db.models.entity_mapping import EntityMapping
from master_instrument.db.models.venue_mapping import VenueMapping
from master_instrument.db.models.instrument_mapping import InstrumentMapping


class TestMappingConfigAutoDetection:
    """Test MappingConfig.from_model() auto-detection logic."""
    
    def test_minimal_usage_all_defaults(self):
        """Test minimal usage with all parameters auto-detected."""
        mapping = MappingConfig.from_model(EntityMapping)
        
        # Verify auto-detected SOURCE columns (defaults to same as mapping columns)
        assert mapping.source_external_id_column == "external_entity_id"
        assert mapping.data_source_column == "data_source_id"
        
        # Verify auto-detected MAPPING TABLE columns (from model)
        assert mapping.mapping_table == "master.entity_mapping"
        assert mapping.mapping_external_column == "external_entity_id"
        assert mapping.mapping_internal_column == "internal_entity_id"
        
        # Verify type column auto-detection
        assert mapping.mapping_type_column == "entity_type_id"
        assert mapping.source_type_column == "entity_type_id"
    
    def test_custom_source_columns(self):
        """Test that custom source column names override defaults."""
        mapping = MappingConfig.from_model(
            EntityMapping,
            source_external_id_column="custom_ext_id",
            data_source_column="custom_source_id"
        )
        
        # Custom source columns
        assert mapping.source_external_id_column == "custom_ext_id"
        assert mapping.data_source_column == "custom_source_id"
        
        # Mapping table columns still auto-detected
        assert mapping.mapping_external_column == "external_entity_id"
        assert mapping.mapping_internal_column == "internal_entity_id"
    
    def test_venue_mapping_detection(self):
        """Test auto-detection works for VenueMapping."""
        mapping = MappingConfig.from_model(VenueMapping)
        
        assert mapping.mapping_table == "master.venue_mapping"
        assert mapping.mapping_external_column == "external_venue_id"
        assert mapping.mapping_internal_column == "internal_venue_id"
        assert mapping.source_external_id_column == "external_venue_id"  # Same as mapping
        assert mapping.data_source_column == "data_source_id"
        assert mapping.mapping_type_column == "venue_type_id"
    
    def test_instrument_mapping_detection(self):
        """Test auto-detection works for InstrumentMapping."""
        mapping = MappingConfig.from_model(InstrumentMapping)
        
        assert mapping.mapping_table == "master.instrument_mapping"
        assert mapping.mapping_external_column == "external_instrument_id"
        assert mapping.mapping_internal_column == "internal_instrument_id"
        assert mapping.mapping_type_column == "instrument_type_id"
    
    def test_schema_override(self):
        """Test schema override works."""
        mapping = MappingConfig.from_model(
            EntityMapping,
            schema="custom_schema"
        )
        
        assert mapping.mapping_table == "custom_schema.entity_mapping"
    
    def test_invalid_model_raises_error(self):
        """Test that non-SQLAlchemy model raises error."""
        class NotAModel:
            pass
        
        with pytest.raises(ValueError, match="must be a SQLAlchemy model"):
            MappingConfig.from_model(NotAModel)


class TestMappingConfigBackwardCompatibility:
    """Test that manual MappingConfig creation still works."""
    
    def test_manual_config_still_works(self):
        """Test backward compatibility with manual config."""
        mapping = MappingConfig(
            mapping_table="master.entity_mapping",
            source_external_id_column="external_id",
            data_source_column="data_source_id",
            mapping_external_column="external_entity_id",
            mapping_internal_column="internal_entity_id"
        )
        
        assert mapping.mapping_table == "master.entity_mapping"
        assert mapping.source_external_id_column == "external_id"
        assert mapping.mapping_external_column == "external_entity_id"


class TestMappingConfigFKDetection:
    """Test FK-based detection logic with fallback."""
    
    def test_data_source_fk_detection(self):
        """Test that data_source_id is detected from FK to data_source table."""
        mapping = MappingConfig.from_model(EntityMapping)
        
        # Should auto-detect data_source_id from FK
        assert mapping.data_source_column == "data_source_id"
    
    def test_internal_column_fk_detection(self):
        """Test that internal_*_id is detected from FK to target table."""
        mapping = MappingConfig.from_model(EntityMapping)
        
        # Should detect internal_entity_id from FK to company table
        assert mapping.mapping_internal_column == "internal_entity_id"
    
    def test_external_column_pattern_detection(self):
        """Test that external_*_id is detected by pattern (contains 'external')."""
        mapping = MappingConfig.from_model(EntityMapping)
        
        # Should detect by pattern
        assert mapping.mapping_external_column == "external_entity_id"
