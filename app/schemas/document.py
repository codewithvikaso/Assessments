from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentCreate(BaseModel):
    user_id: str = Field(
        ...,
        min_length=1,
        max_length=100
    )

    title: str = Field(
        ...,
        min_length=1,
        max_length=200
    )

    content: str = Field(
        ...,
        min_length=1
    )


class SummaryResponse(BaseModel):
    text: str
    word_count: int


class DocumentCreateResponse(BaseModel):
    document_id: str
    status: DocumentStatus


class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    document_id: str
    user_id: str
    title: str
    status: DocumentStatus
    summary: Optional[SummaryResponse] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class DocumentListResponse(BaseModel):
    documents: list[DocumentResponse]
    page: int
    page_size: int
    total: int