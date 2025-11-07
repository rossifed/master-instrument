from typing import Optional
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class ClassificationNode(Base):
    __tablename__ = "classification_node"
    __table_args__ = {"schema": "ref_data"}

    classification_node_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_node_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("ref_data.classification_node.classification_node_id"), nullable=True
    )
    classification_level_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.classification_level.classification_level_id"), nullable=False
    )
    classification_system_id: Mapped[int] = mapped_column(
        ForeignKey("ref_data.classification_system.classification_system_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    code: Mapped[str] = mapped_column(String(10), nullable=True)

    def __repr__(self) -> str:
        return f"<ClassificationNode(id={self.classification_node_id}, code={self.code}, name={self.name})>"
