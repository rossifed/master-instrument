from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class EntityClassification(Base):
    __tablename__ = "entity_classification"
    __table_args__ = {"schema": "ref_data"}

    entity_classification_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    classification_node_id: Mapped[int] = mapped_column(ForeignKey("ref_data.classification_node.classification_node_id"), nullable=False)
    entity_id: Mapped[int] = mapped_column(ForeignKey("ref_data.entity.entity_id"), nullable=False)
    classification_system_id: Mapped[int] = mapped_column(ForeignKey("ref_data.classification_system.classification_system_id"), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    code: Mapped[str] = mapped_column(String(10), nullable=True)

    def __repr__(self) -> str:
        return f"<EntityClassification(id={self.entity_classification_id}, code={self.code}, name={self.name})>"
