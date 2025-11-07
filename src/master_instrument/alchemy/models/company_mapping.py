from sqlalchemy import String, Integer, UniqueConstraint, Index, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class CompanyMapping(Base):
    __tablename__ = "company_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_company_id"),
        Index("idx_source_external_company_id", "source", "external_company_id"),
        {"schema": "ref_data"},
    )

    internal_company_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ref_data.company.company_id"),
        primary_key=True
    )
    external_company_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)

    def __repr__(self) -> str:
        return f"<CompanyMapping(id={self.internal_company_id}, source={self.source}, external_id={self.external_company_id})>"
