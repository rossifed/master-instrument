from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text


class CorpactType(Base):
    __tablename__ = "corpact_type"
    __table_args__ = (
        UniqueConstraint("code", name="uq_corpact_type_code"),
        {"schema": "master"},
    )

    corpact_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(5), nullable=False)
    description: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self):
        return f"<CorpactType(id={self.corpact_type_id}, code={self.code}, description={self.description})>"
