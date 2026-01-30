"""Assets for referential data loading."""

from typing import Any

from dagster import asset, AssetKey

from master_instrument.etl import loading as ld
from master_instrument import models as m
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def countries(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load countries from seed data."""
    config = ld.MergeConfig.from_model(
        m.Country,
        source_table="seed.country",
        unique_key="code_alpha2",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[AssetKey(["intermediate", "reference", "int_currency"])]
)
def currencies(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load currencies from intermediate view."""
    config = ld.MergeConfig.from_model(
        m.Currency,
        source_table="intermediate.int_currency",
        unique_key="code",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def regions(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load regions from seed data."""
    config = ld.MergeConfig.from_model(
        m.Region,
        source_table="seed.region",
        unique_key="code",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def venue_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load venue types from seed data."""
    config = ld.MergeConfig.from_model(
        m.VenueType,
        source_table="seed.venue_type",
        unique_key="mnemonic",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def instrument_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load instrument types from seed data."""
    config = ld.MergeConfig.from_model(
        m.InstrumentType,
        source_table="seed.instrument_type",
        unique_key="mnemonic",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[AssetKey(["staging", "qa", "reference", "stg_qa_equity_type"])]
)
def equity_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load equity types from staging."""
    config = ld.MergeConfig.from_model(
        m.EquityType,
        source_table="staging.stg_qa_equity_type",
        unique_key="mnemonic",
        order_by="description"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def data_sources(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load data sources from seed data."""
    config = ld.MergeConfig.from_model(
        m.DataSource,
        source_table="seed.data_source",
        unique_key="mnemonic",
        order_by="description"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[AssetKey(["staging", "qa", "reference", "stg_qa_dividend_type"])]
)
def dividend_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load dividend types from staging."""
    config = ld.MergeConfig.from_model(
        m.DividendType,
        source_table="staging.stg_qa_dividend_type",
        unique_key="code",
        order_by="code"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[AssetKey(["staging", "qa", "reference", "stg_qa_corpact_type"])]
)
def corpact_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load corporate action types from staging."""
    config = ld.MergeConfig.from_model(
        m.CorpactType,
        source_table="staging.stg_qa_corpact_type",
        unique_key="code",
        order_by="code"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[AssetKey(["staging", "qa", "reference", "stg_qa_weblink_type"])]
)
def weblink_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load weblink types from staging."""
    config = ld.MergeConfig.from_model(
        m.WeblinkType,
        source_table="staging.stg_qa_weblink_type",
        source_columns=["weblink_type_id", "description"],
        order_by="description"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()





@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        countries,
        regions,
        AssetKey(["intermediate", "reference", "int_country_region"])
    ]
)
def country_regions(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load country-region associations (junction table)."""
    config = ld.MergeConfig.from_model(
        m.CountryRegion,
        source_table="intermediate.int_country_region",
        unique_key=["country_id", "region_id"],
        order_by="country_id, region_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def entity_types(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load entity types from seed data."""
    config = ld.MergeConfig.from_model(
        m.EntityType,
        source_table="seed.entity_type",
        unique_key="mnemonic",
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        countries,
        entity_types,
        data_sources,
        AssetKey(["intermediate", "reference", "int_company"])
    ]
)
def companies(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load companies with Entity inheritance and self-references.
    
    Uses inheritance pattern (Entity -> Company) with:
    - Self-references: primary_company_id, ultimate_organization_id
    - Mapping table: entity_mapping (external_id -> internal_entity_id)
    """
    
    # 1. Define parent merge config (m.Entity)
    parent_config = ld.MergeConfig.from_model(
        m.Entity,
        source_table="intermediate.int_company",
        unique_key="entity_id"
    )
    
    # 2. Define child config (m.Company) with inheritance
    config = ld.MergeConfig.from_model(
        m.Company,
        source_table="intermediate.int_company",  # Same source as parent
        unique_key="company_id",
        exclude_columns=["latest_interim_financial_date"],  # Child exclusions
        inheritance=ld.InheritanceConfig(
            parent_config=parent_config,  # Complete parent config
            source_parent_key="internal_entity_id"  # Aligned with EntityMapping column names
        ),
        mapping=ld.MappingConfig.from_model(m.EntityMapping),  # All defaults!,
        self_reference=ld.SelfReferenceConfig(
            columns={
                "primary_company_id": "primary_company_id",
                "ultimate_organization_id": "ultimate_organization_id"
            },
            requires_mapping=True  # Use entity_mapping to resolve external IDs
        ),
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        companies,
        countries,
        instrument_types,
        equity_types,
        data_sources,
        AssetKey(["intermediate", "reference", "int_equity"])
    ]
)
def equities(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load equities with Instrument inheritance.
    
    Uses inheritance pattern (Instrument -> Equity) with:
    - Mapping table: instrument_mapping (external_instrument_id -> internal_instrument_id)
    """
    
    # 1. Define parent config (m.Instrument)
    parent_config = ld.MergeConfig.from_model(
        m.Instrument,
        source_table="intermediate.int_equity",
        unique_key="instrument_id"
    )
    
    # 2. Define child config (m.Equity) with inheritance
    config = ld.MergeConfig.from_model(
        m.Equity,
        source_table="intermediate.int_equity",  # Same source as parent
        unique_key="equity_id",
        inheritance=ld.InheritanceConfig(
            parent_config=parent_config,  # Complete parent config
            source_parent_key="internal_instrument_id"
        ),
        mapping=ld.MappingConfig.from_model(m.InstrumentMapping),  # All defaults!
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        equities,
        AssetKey(["intermediate", "reference", "int_corpact_adjustment"])
    ]
)
def corpact_adjustments(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load corporate action adjustments (split/spinoff factors)."""
    config = ld.MergeConfig.from_model(
        m.CorpactAdjustment,
        source_table="intermediate.int_corpact_adjustment",
        unique_key=["equity_id", "adj_date", "adj_type"]
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        equities,
        currencies,
        AssetKey(["intermediate", "reference", "int_dividend"])
    ]
)
def dividends(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load dividends."""
    config = ld.MergeConfig.from_model(
        m.Dividend,
        source_table="intermediate.int_dividend",
        unique_key=["equity_id", "event_sequence"]
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()

@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        weblink_types,
        companies,
        AssetKey(["intermediate", "reference", "int_company_weblink"])
    ]
)
def company_weblinks(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load company weblinks."""
    config = ld.MergeConfig.from_model(
        m.CompanyWeblink,
        source_table="intermediate.int_company_weblink",
        unique_key=["company_id", "weblink_type_id"],
        order_by="company_id, weblink_type_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()

@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        venue_types,
        countries,
        data_sources,
        AssetKey(["intermediate", "reference", "int_venue"])
    ]
)
def venues(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load trading venues with mapping."""
    config = ld.MergeConfig.from_model(
        m.Venue,
        source_table="intermediate.int_venue",
        unique_key="venue_id",  # Target PK
        source_unique_key="internal_venue_id",  # Source column name (from mapping)
        mapping=ld.MappingConfig.from_model(m.VenueMapping),  # All defaults!
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        equities,
        venues,
        currencies,
        AssetKey(["intermediate", "reference", "int_quote"])
    ]
)
def quotes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load instrument quotes (listings) with mapping."""
    config = ld.MergeConfig.from_model(
        m.Quote,
        source_table="intermediate.int_quote",
        unique_key="quote_id",
        source_unique_key="internal_quote_id",  # From mapping table
        mapping=ld.MappingConfig.from_model(m.QuoteMapping),  # All defaults!
        order_by="instrument_id, venue_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference"
)
def classification_schemes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load classification schemes (GICS, ICB, etc.) from seed data."""
    
    config = ld.MergeConfig.from_model(
        m.ClassificationScheme,
        source_table="seed.classification_scheme",
        unique_key="mnemonic",
        exclude_columns=["classification_scheme_id"],
        order_by="name"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        classification_schemes,
        AssetKey(["intermediate", "reference", "int_classification_level"])
    ]
)
def classification_levels(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load classification hierarchy levels."""
    
    config = ld.MergeConfig.from_model(
        m.ClassificationLevel,
        source_table="intermediate.int_classification_level",
        unique_key=["classification_scheme_id", "level_number"],
        order_by="classification_scheme_id, level_number"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        classification_schemes,
        classification_levels,
        AssetKey(["intermediate", "reference", "int_classification_node"])
    ]
)
def classification_nodes(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load classification nodes (sectors, industries, etc.)."""
    
    config = ld.MergeConfig.from_model(
        m.ClassificationNode,
        source_table="intermediate.int_classification_node",
        unique_key=["classification_scheme_id", "code"],
        order_by="classification_scheme_id, code"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        companies,
        classification_nodes,
        AssetKey(["intermediate", "reference", "int_entity_classification"])
    ]
)
def entity_classifications(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load entity classifications (company -> GICS sector mapping)."""
    
    config = ld.MergeConfig.from_model(
        m.EntityClassification,
        source_table="intermediate.int_entity_classification",
        unique_key=["entity_id", "classification_scheme_id"],
        order_by="entity_id, classification_scheme_id"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        equities,
        AssetKey(["intermediate", "reference", "int_share_outstanding"])
    ]
)
def share_outstandings(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load shares outstanding history."""
    
    config = ld.MergeConfig.from_model(
        m.ShareOutstanding,
        source_table="intermediate.int_share_outstanding",
        unique_key=["equity_id", "date"],
        order_by="equity_id, date"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


@asset(
    key_prefix=["master", "reference"],
    group_name="reference",
    deps=[
        equities,
        corpact_types,
        currencies,
        AssetKey(["intermediate", "reference", "int_corpact_event"])
    ]
)
def corpact_events(engine: SqlAlchemyEngineResource) -> dict[str, Any]:
    """Load corporate action events using from_model() with CorpactEvent model.
    
    Uses composite unique key: (equity_id, event_sequence, corpact_type_id, effective_date)
    """
    config = ld.MergeConfig.from_model(
        m.CorpactEvent,
        source_table="intermediate.int_corpact_event",
        unique_key=["equity_id", "event_sequence", "corpact_type_id", "effective_date"],
        exclude_columns=["corpact_event_id"],
        order_by="equity_id, event_sequence"
    )
    
    return ld.SimpleLoader(engine, ld.TemplateSource(config)).load()


