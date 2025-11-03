from sqlalchemy import String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class EntityType(Base):
    __tablename__ = "entity_type"
    __table_args__ = (UniqueConstraint("mnemonic"),
                      {"schema": "ref_data"})

    entity_type_id: Mapped[int] = mapped_column(primary_key=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    venues: Mapped[list["Entity"]] = relationship(back_populates="enity_type")
