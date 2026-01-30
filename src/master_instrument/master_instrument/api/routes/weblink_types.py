"""WeblinkType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.weblink_type import WeblinkTypeResponse
from master_instrument.services.weblink_type import WeblinkTypeService
from master_instrument.dependencies import get_weblink_type_service

router = APIRouter()


@router.get("", response_model=list[WeblinkTypeResponse])
def list_weblink_types(
    service: Annotated[WeblinkTypeService, Depends(get_weblink_type_service)],
):
    """List all weblink types."""
    return service.get_all()


@router.get("/{weblink_type_id}", response_model=WeblinkTypeResponse)
def get_weblink_type(
    weblink_type_id: int,
    service: Annotated[WeblinkTypeService, Depends(get_weblink_type_service)],
):
    """Get a weblink type by ID."""
    item = service.get(weblink_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="WeblinkType not found")
    return item
