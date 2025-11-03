from sqlalchemy import TIMESTAMP, String, UniqueConstraint,Index
from sqlalchemy.orm import Mapped, mapped_column
import datetime
from .base import Base


class CompanyMapping(Base):
    __tablename__ = "company_mapping"
    __table_args__ = (
        UniqueConstraint("source", "external_company_id"),
        Index("idx_company_mapping_source_external", "source", "external_company_id"),
        {"schema": "ref_data"},
    )

    internal_company_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_company_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)


