"""CorpactType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.corpact_type import CorpactTypeResponse
from master_instrument.services.corpact_type import CorpactTypeService
from master_instrument.dependencies import get_corpact_type_service

router = APIRouter()


@router.get("", response_model=list[CorpactTypeResponse])
def list_corpact_types(
    service: Annotated[CorpactTypeService, Depends(get_corpact_type_service)],
):
    """List all corporate action types."""
    return service.get_all()


@router.get("/{corpact_type_id}", response_model=CorpactTypeResponse)
def get_corpact_type(
    corpact_type_id: int,
    service: Annotated[CorpactTypeService, Depends(get_corpact_type_service)],
):
    """Get a corporate action type by ID."""
    item = service.get(corpact_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="CorpactType not found")
    return item


@router.get("/code/{code}", response_model=CorpactTypeResponse)
def get_corpact_type_by_code(
    code: str,
    service: Annotated[CorpactTypeService, Depends(get_corpact_type_service)],
):
    """Get a corporate action type by code."""
    item = service.get_by_code(code)
    if not item:
        raise HTTPException(status_code=404, detail="CorpactType not found")
    return item
