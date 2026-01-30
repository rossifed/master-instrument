"""Data loading infrastructure - framework-agnostic loading components.

This module provides:
- DataSource: Protocol for reading data (SQL files, templates, etc.)
- Sources: SqlFileSource, TemplateSource for different data sources
- Loaders: SimpleLoader, BatchLoader for executing loads
- Strategies: Batch generation strategies (FixedInterval, VolumeBased)
- Configs: Configuration classes for template-based loading
"""

from master_instrument.etl.loading.sources import DataSource, SqlFileSource, TemplateSource
from master_instrument.etl.loading.loaders import (
    LoaderProtocol, 
    Logger, 
    SimpleLoader, 
    BatchLoader, 
    BatchResult
)
from master_instrument.etl.loading.batching import (
    BatchStrategy, 
    FixedIntervalStrategy, 
    VolumeBasedStrategy,
    DateRange,
    DateRangeWithCount,
    IntervalUnit
)
from master_instrument.etl.loading.configs import (
    LoadingScheme,
    MergeConfig,
    UpsertConfig,
    CDCConfig,
    InsertConfig,
    InheritanceConfig,
    MappingConfig,
    SelfReferenceConfig,
    AuditConfig,
    BatchConfig
)

__all__ = [
    # Sources
    "DataSource",
    "SqlFileSource",
    "TemplateSource",
    # Loaders
    "LoaderProtocol",
    "Logger",
    "SimpleLoader",
    "BatchLoader",
    "BatchResult",
    # Batching strategies
    "BatchStrategy",
    "FixedIntervalStrategy",
    "VolumeBasedStrategy",
    "DateRange",
    "DateRangeWithCount",
    "IntervalUnit",
    # Configs
    "LoadingScheme",
    "MergeConfig",
    "UpsertConfig",
    "CDCConfig",
    "InsertConfig",
    "InheritanceConfig",
    "MappingConfig",
    "SelfReferenceConfig",
    "AuditConfig",
    "BatchConfig",
]
