"""EntityType API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.entity_type import EntityTypeResponse
from master_instrument.services.entity_type import EntityTypeService
from master_instrument.dependencies import get_entity_type_service

router = APIRouter()


@router.get("", response_model=list[EntityTypeResponse])
def list_entity_types(
    service: Annotated[EntityTypeService, Depends(get_entity_type_service)],
):
    """List all entity types."""
    return service.get_all()


@router.get("/{entity_type_id}", response_model=EntityTypeResponse)
def get_entity_type(
    entity_type_id: int,
    service: Annotated[EntityTypeService, Depends(get_entity_type_service)],
):
    """Get an entity type by ID."""
    item = service.get(entity_type_id)
    if not item:
        raise HTTPException(status_code=404, detail="EntityType not found")
    return item


@router.get("/mnemonic/{mnemonic}", response_model=EntityTypeResponse)
def get_entity_type_by_mnemonic(
    mnemonic: str,
    service: Annotated[EntityTypeService, Depends(get_entity_type_service)],
):
    """Get an entity type by mnemonic."""
    item = service.get_by_mnemonic(mnemonic)
    if not item:
        raise HTTPException(status_code=404, detail="EntityType not found")
    return item
