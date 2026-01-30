from master_instrument.etl.assets.infrastructure.raw_indexes import (
    # Market
    raw_idx_ds2primqtprc,
    raw_idx_ds2primqtprc_changes,
    raw_idx_ds2mktval,
    raw_idx_ds2mktval_changes,
    raw_idx_ds2fxcode,
    raw_idx_ds2fxrate,
    raw_idx_ds2div,
    raw_idx_ds2adj,
    raw_idx_ds2capevent,
    raw_idx_ds2numshares,
    # Fundamental
    raw_idx_rkdfndstdfinval,
    raw_idx_rkdfndstdfinval_changes,
    raw_idx_rkdfndstdperfiling,
    raw_idx_rkdfndstdperiod,
    raw_idx_rkdfndstdstmt,
    raw_idx_rkdfndstditem,
    raw_idx_rkdfndcode,
    # Reference
    raw_idx_ds2security,
    raw_idx_ds2ctryqtinfo,
    raw_idx_ds2exchange,
    raw_idx_ds2exchqtinfo,
    raw_idx_rkdfndcmpref,
    raw_idx_rkdfndcmprefissue,
    raw_idx_rkdfndcmpdet,
    raw_idx_rkdfndcmpfiling,
    raw_idx_rkdfndinfo,
    raw_idx_permorgref,
    raw_idx_permquoteref,
    raw_idx_s3_gics_classification,
)

from master_instrument.etl.assets.infrastructure.seed_indexes import (
    seed_idx_data_source,
    seed_idx_entity_type,
    seed_idx_financial_period_type_mapping,
)

from master_instrument.etl.assets.infrastructure.analyze import (
    analyze_raw_tables,
    analyze_seed_tables,
    analyze_master_tables,
    analyze_all_schemas,
)

__all__ = [
    # Market
    "raw_idx_ds2primqtprc",
    "raw_idx_ds2primqtprc_changes",
    "raw_idx_ds2mktval",
    "raw_idx_ds2mktval_changes",
    "raw_idx_ds2fxcode",
    "raw_idx_ds2fxrate",
    "raw_idx_ds2div",
    "raw_idx_ds2adj",
    "raw_idx_ds2capevent",
    "raw_idx_ds2numshares",
    # Fundamental
    "raw_idx_rkdfndstdfinval",
    "raw_idx_rkdfndstdfinval_changes",
    "raw_idx_rkdfndstdperfiling",
    "raw_idx_rkdfndstdperiod",
    "raw_idx_rkdfndstdstmt",
    "raw_idx_rkdfndstditem",
    "raw_idx_rkdfndcode",
    # Reference
    "raw_idx_ds2security",
    "raw_idx_ds2ctryqtinfo",
    "raw_idx_ds2exchange",
    "raw_idx_ds2exchqtinfo",
    "raw_idx_rkdfndcmpref",
    "raw_idx_rkdfndcmprefissue",
    "raw_idx_rkdfndcmpdet",
    "raw_idx_rkdfndcmpfiling",
    "raw_idx_rkdfndinfo",
    "raw_idx_permorgref",
    "raw_idx_permquoteref",
    "raw_idx_s3_gics_classification",
    # Seed indexes
    "seed_idx_data_source",
    "seed_idx_entity_type",
    "seed_idx_financial_period_type_mapping",
    # Analyze/Maintenance
    "analyze_raw_tables",
    "analyze_seed_tables",
    "analyze_master_tables",
    "analyze_all_schemas",
]
