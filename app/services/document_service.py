import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from app.core.config import settings
from app.db.mongodb import mongodb
from app.db.redis import get_redis
from app.models.document import DocumentModel
from app.schemas.document import DocumentCreate, DocumentStatus


class DocumentService:

    COLLECTION_NAME = DocumentModel.COLLECTION_NAME
    QUEUE_NAME = "document_processing_queue"

    def __init__(self):
        self.collection = mongodb.get_collection(
            self.COLLECTION_NAME
        )
        self.redis = get_redis()

    # ---------------------------------------------------------
    # Create document
    # ---------------------------------------------------------

    async def create_document(
        self,
        document: DocumentCreate,
    ) -> dict[str, Any]:

        # 1. Generate content hash
        content_hash = self._generate_content_hash(
            document.content
        )

        # 2. Check Redis cache
        cached_summary = await self._get_cached_summary(
            content_hash
        )

        if cached_summary:
            return await self._create_cached_document(
                document=document,
                content_hash=content_hash,
                summary=cached_summary,
            )

        # 3. Check per-user active job limit
        await self._check_rate_limit(
            document.user_id
        )

        # 4. Reserve one active job
        await self._increment_active_jobs(
            document.user_id
        )

        try:

            # 5. Create MongoDB document
            mongo_document = DocumentModel.create_document(
                user_id=document.user_id,
                title=document.title,
                content=document.content,
                content_hash=content_hash,
            )

            result = await self.collection.insert_one(
                mongo_document
            )

            document_id = str(result.inserted_id)

            # 6. Add job to Redis queue
            job = {
                "document_id": document_id,
            }

            await self.redis.rpush(
                self.QUEUE_NAME,
                json.dumps(job),
            )

            return {
                "document_id": document_id,
                "status": "queued",
            }

        except Exception:
            # Important:
            # If MongoDB/queue operation fails after incrementing
            # the active count, release the reserved slot.

            await self._decrement_active_jobs(
                document.user_id
            )

            raise

    # ---------------------------------------------------------
    # Get document
    # ---------------------------------------------------------

    async def get_document(
        self,
        document_id: str,
    ) -> dict[str, Any] | None:

        if not ObjectId.is_valid(document_id):
            return None

        document = await self.collection.find_one(
            {
                "_id": ObjectId(document_id)
            }
        )

        if not document:
            return None

        return self._serialize_document(document)

    # ---------------------------------------------------------
    # Content hash
    # ---------------------------------------------------------

    @staticmethod
    def _generate_content_hash(
        content: str,
    ) -> str:

        return hashlib.sha256(
            content.encode("utf-8")
        ).hexdigest()

    # ---------------------------------------------------------
    # Redis cache
    # ---------------------------------------------------------

    async def _get_cached_summary(
        self,
        content_hash: str,
    ) -> dict[str, Any] | None:

        cache_key = f"document:summary:{content_hash}"

        cached_data = await self.redis.get(cache_key)

        if not cached_data:
            return None

        return json.loads(cached_data)

    async def _set_cached_summary(
        self,
        content_hash: str,
        summary: dict[str, Any],
    ) -> None:

        cache_key = f"document:summary:{content_hash}"

        await self.redis.set(
            cache_key,
            json.dumps(summary),
            ex=settings.CACHE_TTL,
        )
    # ---------------------------------------------------------
    # Cached document
    # ---------------------------------------------------------

    async def _create_cached_document(
        self,
        document: DocumentCreate,
        content_hash: str,
        summary: dict[str, Any],
    ) -> dict[str, Any]:

        now = datetime.now(timezone.utc)

        mongo_document = {
            "user_id": document.user_id,
            "title": document.title,
            "content": document.content,
            "content_hash": content_hash,
            "status": "completed",
            "summary": summary,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }

        result = await self.collection.insert_one(
            mongo_document
        )

        return {
            "document_id": str(result.inserted_id),
            "status": "completed",
        }

    # ---------------------------------------------------------
    # Rate limiting
    # ---------------------------------------------------------

    async def _check_rate_limit(
        self,
        user_id: str,
    ) -> None:

        key = self._active_jobs_key(user_id)

        current_count = await self.redis.get(key)

        current_count = int(current_count or 0)

        if current_count >= settings.MAX_ACTIVE_JOBS:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=429,
                detail="Maximum 3 active documents allowed per user",
            )

    async def _increment_active_jobs(
        self,
        user_id: str,
    ) -> None:

        key = self._active_jobs_key(user_id)

        await self.redis.incr(key)

    async def _decrement_active_jobs(
        self,
        user_id: str,
    ) -> None:

        key = self._active_jobs_key(user_id)

        count = await self.redis.decr(key)

        if count <= 0:
            await self.redis.delete(key)

    @staticmethod
    def _active_jobs_key(
        user_id: str,
    ) -> str:

        return f"user:{user_id}:active_jobs"

    # ---------------------------------------------------------
    # Serialization
    # ---------------------------------------------------------

    @staticmethod
    def _serialize_document(
        document: dict[str, Any],
    ) -> dict[str, Any]:

        return {
            "document_id": str(
                document["_id"]
            ),
            "user_id": document["user_id"],
            "title": document["title"],
            "status": document["status"],
            "summary": document.get("summary"),
            "error": document.get("error"),
            "created_at": document["created_at"],
            "updated_at": document["updated_at"],
        }
        
    async def get_user_documents(
        self,
        user_id: str,
        page: int,
        page_size: int,
        status: DocumentStatus | None = None,
    ) -> dict:

        query = {
            "user_id": user_id
        }

        if status:
            query["status"] = status.value

        # Total documents
        total = await self.collection.count_documents(
            query
        )

        # Pagination
        skip = (page - 1) * page_size

        cursor = (
            self.collection
            .find(query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(page_size)
        )

        documents = []

        async for document in cursor:
            documents.append(
                self._serialize_document(document)
            )

        return {
            "documents": documents,
            "page": page,
            "page_size": page_size,
            "total": total,
        }