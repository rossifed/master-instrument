from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import text
from .base import Base

class EntityType(Base):
    __tablename__ = "entity_type"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_entity_type_mnemonic"),
        {"schema": "master"},
    )

    entity_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self) -> str:
        return f"<EntityType(id={self.entity_type_id}, mnemonic={self.mnemonic})>"
