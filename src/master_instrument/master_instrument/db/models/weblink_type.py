from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text


class WeblinkType(Base):
    __tablename__ = "weblink_type"
    __table_args__ = (
        UniqueConstraint("description", name="uq_weblink_type_description"),
        {"schema": "master"},
    )

    weblink_type_id: Mapped[int] = mapped_column(
        SmallInteger,
        primary_key=True,
        autoincrement=False
    )

    description: Mapped[str] = mapped_column(
        String(50),
        nullable=False
    )


    def __repr__(self) -> str:
        return f"<WeblinkType(id={self.weblink_type_id}, description={self.description})>"
