from typing import Optional, TYPE_CHECKING
from datetime import date
from sqlalchemy import String, Text, BigInteger, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity
    from .country import Country
    from .currency import Currency

class Company(Base):
    __tablename__ = "company"
    __table_args__ = {"schema": "ref_data"}

    company_id: Mapped[int] = mapped_column(ForeignKey("ref_data.entity.entity_id"), primary_key=True)

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    employee_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    employee_count_date: Mapped[Optional[date]] = mapped_column(nullable=True)

    st_address_1: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    st_address_2: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    country_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.country.country_id"), nullable=True)

    public_since: Mapped[Optional[date]] = mapped_column(nullable=True)

    common_shareholders: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    common_shareholders_date: Mapped[Optional[date]] = mapped_column(nullable=True)

    total_shares_outstanding: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    shares_outstanding_updated_at: Mapped[Optional[date]] = mapped_column(nullable=True)
    total_float_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    estimates_currency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.currency.currency_id"), nullable=True)
    statements_currency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ref_data.currency.currency_id"), nullable=True)

    last_modified_financial_at: Mapped[Optional[date]] = mapped_column(nullable=True)
    last_modified_non_financial_at: Mapped[Optional[date]] = mapped_column(nullable=True)
    latest_annual_financial_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    latest_interim_financial_date: Mapped[Optional[date]] = mapped_column(nullable=True)

    entity: Mapped["Entity"] = relationship(back_populates="company", lazy="selectin")
    country: Mapped[Optional["Country"]] = relationship(lazy="selectin")
    estimates_currency: Mapped[Optional["Currency"]] = relationship(
        foreign_keys=[estimates_currency_id], lazy="selectin"
    )
    statements_currency: Mapped[Optional["Currency"]] = relationship(
        foreign_keys=[statements_currency_id], lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<Company(id={self.company_id}, name={self.name})>"
