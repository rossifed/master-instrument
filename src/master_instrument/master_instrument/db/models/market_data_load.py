from sqlalchemy import Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, CDCLoadMixin


class MarketDataLoad(CDCLoadMixin, Base):
    """Tracks CDC load operations for market_data table."""
    
    __tablename__ = "market_data_load"
    __table_args__ = (
        Index("idx_market_data_load_time", "loaded_at"),
        {"schema": "master"},
    )

    market_data_load_id: Mapped[int] = mapped_column(
        Integer, 
        primary_key=True, 
        autoincrement=True
    )

    def __repr__(self) -> str:
        return (
            f"<MarketDataLoad(id={self.market_data_load_id}, "
            f"version={self.last_source_version}, "
            f"at={self.loaded_at})>"
        )
