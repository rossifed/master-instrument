from typing import TYPE_CHECKING
from sqlalchemy import ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from .country import Country
    from .region import Region

class CountryRegion(Base):
    __tablename__ = "country_region"
    __table_args__ = (
        PrimaryKeyConstraint("country_id", "region_id", name="pk_country_region"),
        {"schema": "master"},
    )

    country_id: Mapped[int] = mapped_column(
        ForeignKey("master.country.country_id", ondelete="CASCADE"),
        nullable=False
    )

    region_id: Mapped[int] = mapped_column(
        ForeignKey("master.region.region_id", ondelete="RESTRICT"),
        nullable=False
    )

    country: Mapped["Country"] = relationship(lazy="joined")
    region: Mapped["Region"] = relationship(lazy="joined")


    def __repr__(self):
        return f"<CountryRegion(country_id={self.country_id}, region_id={self.region_id})>"
