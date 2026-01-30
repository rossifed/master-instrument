from typing import Optional, TYPE_CHECKING, List
from datetime import date
from sqlalchemy import String, BigInteger, Integer, ForeignKey, CHAR
from sqlalchemy.orm import Mapped, mapped_column, relationship, backref
from sqlalchemy.sql import text
from .base import Base

if TYPE_CHECKING:
    from .entity import Entity
    from .country import Country
    from .currency import Currency
    from .company_weblink import CompanyWeblink
    from .std_financial_filing import StdFinancialFiling

class Company(Base):
    __tablename__ = "company"
    __table_args__ = {"schema": "master"}

    company_id: Mapped[int] = mapped_column(
        ForeignKey("master.entity.entity_id", ondelete="CASCADE"),
        primary_key=True
    )

    employee_count: Mapped[Optional[int]] = mapped_column(Integer)
    employee_count_date: Mapped[Optional[date]]

    st_address_1: Mapped[Optional[str]] = mapped_column(String(100))
    st_address_2: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(50))
    state: Mapped[Optional[str]] = mapped_column(String(50))
    postal_code: Mapped[Optional[str]] = mapped_column(String(20))
    phone: Mapped[Optional[str]] = mapped_column(String(50))

    country_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.country.country_id")
    )

    industry_code: Mapped[Optional[str]] = mapped_column(CHAR(3))

    public_since: Mapped[Optional[date]]

    common_shareholders: Mapped[Optional[int]] = mapped_column(BigInteger)
    common_shareholders_date: Mapped[Optional[date]]

    total_shares_outstanding: Mapped[Optional[int]] = mapped_column(BigInteger)
    shares_outstanding_updated_at: Mapped[Optional[date]]
    total_float_shares: Mapped[Optional[int]] = mapped_column(BigInteger)

    estimates_currency_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.currency.currency_id")
    )
    statements_currency_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.currency.currency_id")
    )
    ultimate_organization_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.company.company_id", ondelete="SET NULL")
    )
    primary_company_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("master.company.company_id", ondelete="SET NULL")
    )
    last_modified_financial_at: Mapped[Optional[date]]
    last_modified_non_financial_at: Mapped[Optional[date]]
    latest_annual_financial_date: Mapped[Optional[date]]
    latest_interim_financial_date: Mapped[Optional[date]]
    organization_id: Mapped[Optional[int]] = mapped_column(Integer)
    entity: Mapped["Entity"] = relationship(
        lazy="selectin",
        uselist=False,
        passive_deletes=True,
    )

    country: Mapped[Optional["Country"]] = relationship(
        lazy="selectin"
    )

    estimates_currency: Mapped[Optional["Currency"]] = relationship(
        foreign_keys=[estimates_currency_id],
        lazy="selectin"
    )

    statements_currency: Mapped[Optional["Currency"]] = relationship(
        foreign_keys=[statements_currency_id],
        lazy="selectin"
    )

    weblinks: Mapped[List["CompanyWeblink"]] = relationship(
        lazy="noload"
    )

    std_financial_filings: Mapped[List["StdFinancialFiling"]] = relationship(
        "StdFinancialFiling",
        back_populates="company",
        lazy="noload"
    )

    ultimate_organization: Mapped[Optional["Company"]] = relationship(
        "Company",
        remote_side="Company.company_id",
        foreign_keys=[ultimate_organization_id],
        backref=backref("ultimate_children", lazy="selectin"),
        lazy="selectin",
    )

    primary_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        remote_side="Company.company_id",
        foreign_keys=[primary_company_id],
        backref=backref("primary_children", lazy="selectin"),
        lazy="selectin",
    )


    @property
    def name(self) -> str:
        return self.entity.name

    @property
    def description(self) -> Optional[str]:
        return self.entity.description

    def __repr__(self) -> str:
        return f"<Company(id={self.company_id}, name={self.name})>"
