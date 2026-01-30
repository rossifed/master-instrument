"""
Raw indexes infrastructure assets.

Split by domain (fundamental, market, reference) with one asset per table for:
- Granular re-runs (single table index creation)
- Parallel execution where possible
- Better observability with detailed logging
- Open/Close principle (add new tables without modifying existing)

Asset key structure: ["infrastructure", "raw_indexes", "{domain}", "{table}"]
"""

# Market domain assets
from .market import (
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
)

# Fundamental domain assets
from .fundamental import (
    raw_idx_rkdfndstdfinval,
    raw_idx_rkdfndstdfinval_changes,
    raw_idx_rkdfndstdperfiling,
    raw_idx_rkdfndstdperiod,
    raw_idx_rkdfndstdstmt,
    raw_idx_rkdfndstditem,
    raw_idx_rkdfndcode,
)

# Reference domain assets
from .reference import (
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
]
