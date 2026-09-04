from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.schemas.document import (
    DocumentListResponse,
    DocumentStatus,
)
from app.services.document_service import DocumentService


router = APIRouter(
    prefix="/users",
    tags=["Users"],
)


def get_document_service() -> DocumentService:
    return DocumentService()


@router.get(
    "/{user_id}/documents",
    response_model=DocumentListResponse,
)
async def get_user_documents(
    user_id: str,
    page: int = Query(
        default=1,
        ge=1,
        description="Page number",
    ),
    page_size: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Number of documents per page",
    ),
    status: Optional[DocumentStatus] = Query(
        default=None,
        description="Filter by document status",
    ),
    service: DocumentService = Depends(
        get_document_service
    ),
):
    """
    Get paginated documents for a user.
    """

    return await service.get_user_documents(
        user_id=user_id,
        page=page,
        page_size=page_size,
        status=status,
    )