from typing import TYPE_CHECKING
from sqlalchemy import Integer, SmallInteger, UniqueConstraint, Index, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from .currency import Currency

class CurrencyPair(Base):
    __tablename__ = "currency_pair"
    __table_args__ = (
        UniqueConstraint("base_currency_id", "quote_currency_id", name="uq_currency_pair_base_quote"),
        Index("idx_currency_pair_base", "base_currency_id"),
        Index("idx_currency_pair_quote", "quote_currency_id"),
        {"schema": "master"},
    )

    currency_pair_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    base_currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )
    
    quote_currency_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("master.currency.currency_id", ondelete="RESTRICT"),
        nullable=False
    )


    # Relationships
    base_currency: Mapped["Currency"] = relationship(
        "Currency",
        foreign_keys=[base_currency_id],
        lazy="selectin"
    )
    
    quote_currency: Mapped["Currency"] = relationship(
        "Currency",
        foreign_keys=[quote_currency_id],
        lazy="selectin"
    )

    def __repr__(self):
        return f"<CurrencyPair(id={self.currency_pair_id}, base={self.base_currency_id}, quote={self.quote_currency_id})>"
