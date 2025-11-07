from typing import List, TYPE_CHECKING
from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .equity import Equity

class EquityType(Base):
    __tablename__ = "equity_type"
    __table_args__ = (
        UniqueConstraint("mnemonic"),
        {"schema": "ref_data"},
    )

    equity_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    mnemonic: Mapped[str] = mapped_column(String(5), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)

    instruments: Mapped[List["Equity"]] = relationship(back_populates="equity_type", lazy="selectin")

    def __repr__(self) -> str:
        return f"<EquityType(id={self.equity_type_id}, mnemonic={self.mnemonic})>"
