from sqlalchemy import String, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text

class Region(Base):
    __tablename__ = "region"
    __table_args__ = (
        UniqueConstraint("code", name="uq_region_code"),
        {"schema": "master"},
    )

    region_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(5), nullable=False)
    name: Mapped[str] = mapped_column(String(30), nullable=False)


    def __repr__(self):
        return f"<Region(id={self.region_id}, code={self.code}, name={self.name})>"
