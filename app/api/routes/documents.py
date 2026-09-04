from fastapi import APIRouter, Depends, HTTPException, status

from app.schemas.document import (
    DocumentCreate,
    DocumentCreateResponse,
    DocumentResponse,
)
from app.services.document_service import DocumentService


router = APIRouter(
    prefix="/documents",
    tags=["Documents"]
)


def get_document_service() -> DocumentService:
    return DocumentService()


@router.post(
    "",
    response_model=DocumentCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_document(
    document: DocumentCreate,
    service: DocumentService = Depends(get_document_service),
):
    """
    Submit a document for background processing.
    """

    return await service.create_document(document)


@router.get(
    "/{document_id}",
    response_model=DocumentResponse,
)
async def get_document(
    document_id: str,
    service: DocumentService = Depends(get_document_service),
):
    """
    Get document status and summary.
    """

    document = await service.get_document(document_id)

    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found",
        )

    return document 