from sqlalchemy import Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, CDCLoadMixin


class TotalReturnLoad(CDCLoadMixin, Base):
    """Tracks CDC load operations for total_return table."""
    
    __tablename__ = "total_return_load"
    __table_args__ = (
        Index("idx_total_return_load_time", "loaded_at"),
        {"schema": "master"},
    )

    total_return_load_id: Mapped[int] = mapped_column(
        Integer, 
        primary_key=True, 
        autoincrement=True
    )

    def __repr__(self) -> str:
        return (
            f"<TotalReturnLoad(id={self.total_return_load_id}, "
            f"version={self.last_source_version}, "
            f"at={self.loaded_at})>"
        )
