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
        UniqueConstraint("company_id", "weblink_type_id", name="uq_company_weblink_company_type"),
        {"schema": "master"},
    )

    company_weblink_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    company_id: Mapped[int] = mapped_column(
        ForeignKey("master.company.company_id", ondelete="CASCADE"),
        nullable=False
    )

    url: Mapped[str] = mapped_column(String(500), nullable=False)

    weblink_type_id: Mapped[int] = mapped_column(
        ForeignKey("master.weblink_type.weblink_type_id", ondelete="RESTRICT"),
        nullable=False
    )

    last_updated: Mapped[date] = mapped_column(nullable=False)

    company: Mapped["Company"] = relationship(
        back_populates="weblinks",
        lazy="selectin"
    )

    weblink_type: Mapped["WeblinkType"] = relationship(
        lazy="selectin"
    )


    def __repr__(self) -> str:
        return f"<CompanyWeblink(id={self.company_weblink_id}, url={self.url})>"
