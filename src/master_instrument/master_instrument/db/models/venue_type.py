from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import text
from .base import Base

class VenueType(Base):
    __tablename__ = "venue_type"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_venue_type_mnemonic"),
        {"schema": "master"},
    )

    venue_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self):
        return f"<VenueType(id={self.venue_type_id}, mnemonic={self.mnemonic})>"
