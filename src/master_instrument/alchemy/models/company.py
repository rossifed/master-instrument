from sqlalchemy import ForeignKey, String,Text,BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from .base import Base

class Company(Base):
    __tablename__ = "company"
    __table_args__ = {"schema": "ref_data"}

    company_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.entity.entity_id"), primary_key=True
    )

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)

    employee_count: Mapped[int] = mapped_column(nullable=True)
    employee_count_date: Mapped[date] = mapped_column(nullable=True)

    st_address_1: Mapped[str] = mapped_column(String(100), nullable=True)
    st_address_2: Mapped[str] = mapped_column(String(100), nullable=True)
    city: Mapped[str] = mapped_column(String(50), nullable=True)
    state: Mapped[str] = mapped_column(String(50), nullable=True)
    postal_code: Mapped[str] = mapped_column(String(20), nullable=True)

    country_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.country.country_id"), nullable=True
    )

    public_since: Mapped[date] = mapped_column(nullable=True)

    common_shareholders: Mapped[int] = mapped_column(BigInteger,nullable=True)
    common_shareholders_date: Mapped[date] = mapped_column(nullable=True)

    total_shares_outstanding: Mapped[int] = mapped_column(BigInteger,nullable=True)
    shares_outstanding_updated_at: Mapped[date] = mapped_column(nullable=True)
    total_float_shares: Mapped[int] = mapped_column(BigInteger,nullable=True)

    estimates_currency_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.currency.currency_id"), nullable=True
    )
    statements_currency_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.currency.currency_id"), nullable=True
    )

    last_modified_financial_at: Mapped[date] = mapped_column(nullable=True)
    last_modified_non_financial_at: Mapped[date] = mapped_column(nullable=True)
    latest_annual_financial_date: Mapped[date] = mapped_column(nullable=True)
    latest_interim_financial_date: Mapped[date] = mapped_column(nullable=True)

    entity: Mapped["Entity"] = relationship(back_populates="company")
    country: Mapped["Country"] = relationship()
    estimates_currency: Mapped["Currency"] = relationship()
