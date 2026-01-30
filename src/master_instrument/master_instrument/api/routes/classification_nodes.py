"""ClassificationNode API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.classification import ClassificationNodeResponse
from master_instrument.services.classification import ClassificationNodeService
from master_instrument.dependencies import get_classification_node_service

router = APIRouter()


@router.get("", response_model=list[ClassificationNodeResponse])
def list_classification_nodes(
    service: Annotated[ClassificationNodeService, Depends(get_classification_node_service)],
    scheme_id: int | None = Query(None, description="Filter by classification scheme"),
    level_number: int | None = Query(None, description="Filter by level number"),
):
    """List classification nodes with optional filters."""
    if scheme_id and level_number:
        return service.get_by_level(scheme_id, level_number)
    elif scheme_id:
        return service.get_by_scheme(scheme_id)
    return service.get_all()


@router.get("/{node_id}", response_model=ClassificationNodeResponse)
def get_classification_node(
    node_id: int,
    service: Annotated[ClassificationNodeService, Depends(get_classification_node_service)],
):
    """Get a classification node by ID."""
    node = service.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Classification node not found")
    return node


@router.get("/{node_id}/children", response_model=list[ClassificationNodeResponse])
def list_node_children(
    node_id: int,
    service: Annotated[ClassificationNodeService, Depends(get_classification_node_service)],
):
    """List child nodes of a parent node."""
    return service.get_children_by_id(node_id)
