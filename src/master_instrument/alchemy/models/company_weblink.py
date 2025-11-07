from typing import TYPE_CHECKING
from datetime import date
from sqlalchemy import Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .company import Company
    from .weblink_type import WeblinkType

class CompanyWeblink(Base):
    __tablename__ = "company_weblink"
    __table_args__ = (
        UniqueConstraint("company_id", "weblink_type_id", name="uq_company_weblink_type_id"),
        {"schema": "ref_data"},
    )

    company_weblink_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("ref_data.company.company_id"), nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    weblink_type_id: Mapped[int] = mapped_column(ForeignKey("ref_data.weblink_type.weblink_type_id"), nullable=False)
    last_updated: Mapped[date] = mapped_column(nullable=False)

    company: Mapped["Company"] = relationship(back_populates="weblinks", lazy="selectin")
    weblink_type: Mapped["WeblinkType"] = relationship(back_populates="weblinks", lazy="selectin")

    def __repr__(self) -> str:
        return f"<CompanyWeblink(id={self.company_weblink_id}, url={self.url})>"
