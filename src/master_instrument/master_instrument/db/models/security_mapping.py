from sqlalchemy import String, UniqueConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text

class SecurityMapping(Base):
    __tablename__ = "security_mapping"
    __table_args__ = (
        UniqueConstraint("data_source_id", "external_security_id", name="uq_security_mapping_source_ext"),
        {"schema": "master"},
    )

    internal_security_id: Mapped[int] = mapped_column(
        ForeignKey("master.security.security_id", ondelete="RESTRICT"),
        primary_key=True
    )

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("master.data_source.data_source_id", ondelete="RESTRICT"),
        nullable=False
    )

    external_security_id: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self) -> str:
        return (
            f"<SecurityMapping(security_id={self.internal_security_id}, "
            f"data_source_id={self.data_source_id}, external_id={self.external_security_id})>"
        )
