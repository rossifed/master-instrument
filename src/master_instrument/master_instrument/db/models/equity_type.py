from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import text
from .base import Base

class EquityType(Base):
    __tablename__ = "equity_type"
    __table_args__ = (
        UniqueConstraint("mnemonic", name="uq_equity_type_mnemonic"),
        UniqueConstraint("description", name="uq_equity_type_description"),
        {"schema": "master"},
    )

    equity_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    mnemonic: Mapped[str] = mapped_column(String(10), nullable=False)
    description: Mapped[str] = mapped_column(String(50), nullable=False)


    def __repr__(self):
        return f"<EquityType(id={self.equity_type_id}, mnemonic={self.mnemonic})>"
