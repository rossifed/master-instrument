from sqlalchemy import Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, CDCLoadMixin


class CompanyMarketCapLoad(CDCLoadMixin, Base):
    """Tracks CDC load operations for company_market_cap table."""
    
    __tablename__ = "company_market_cap_load"
    __table_args__ = (
        Index("idx_company_market_cap_load_time", "loaded_at"),
        {"schema": "master"},
    )

    company_market_cap_load_id: Mapped[int] = mapped_column(
        Integer, 
        primary_key=True, 
        autoincrement=True
    )

    def __repr__(self) -> str:
        return (
            f"<CompanyMarketCapLoad(id={self.company_market_cap_load_id}, "
            f"version={self.last_source_version}, "
            f"at={self.loaded_at})>"
        )
