from sqlalchemy import SmallInteger, String, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text

class DataSource(Base):
    __tablename__ = "data_source"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_data_source_mnemonic"),
        {"schema": "master"},
    )

    data_source_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    mnemonic: Mapped[str] = mapped_column(String(20), nullable=False)
    description: Mapped[str] = mapped_column(String(200), nullable=False)


    def __repr__(self) -> str:
        return f"<DataSource(id={self.data_source_id}, mnemonic={self.mnemonic})>"
