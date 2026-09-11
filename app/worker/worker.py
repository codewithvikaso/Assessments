import asyncio
import json
import logging
import random
from datetime import datetime, timezone

from bson import ObjectId
from pymongo import ReturnDocument

from app.core.config import settings
from app.db.mongodb import mongodb
from app.db.redis import redis_manager
from app.models.document import DocumentModel


"""
Document processing flow:

1. Redis queue provides the document job.
2. MongoDB changes status: queued -> processing.
3. Worker processes the document.
4. Success:
   - MongoDB status -> completed
   - Summary stored in MongoDB
   - Successful summary cached in Redis
5. Failure:
   - MongoDB status -> failed
   - Error stored in MongoDB
   - Failed result is not cached
6. Active-job count is decremented in Redis in both cases.
"""
# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Constants
# ---------------------------------------------------------

QUEUE_NAME = "document_processing_queue"
COLLECTION_NAME = DocumentModel.COLLECTION_NAME


# ---------------------------------------------------------
# Worker
# ---------------------------------------------------------

class DocumentWorker:

    def __init__(self):
        self.collection = None
        self.redis = None

    async def initialize(self):
        """
        Initialize MongoDB and Redis connections.
        """

        await mongodb.connect()
        await redis_manager.connect()

        self.collection = mongodb.get_collection(
            COLLECTION_NAME
        )

        self.redis = redis_manager.get_client()

        logger.info("Worker initialized")

    async def shutdown(self):
        """
        Close connections.
        """

        await redis_manager.disconnect()
        await mongodb.disconnect()

        logger.info("Worker shutdown")

    # -----------------------------------------------------
    # Main loop
    # -----------------------------------------------------

    async def run(self):
        """
        Continuously consume jobs from Redis.
        """

        logger.info(
            "Worker started. Waiting for jobs..."
        )

        while True:

            try:
                job = await self.get_next_job()

                if job is None:
                    continue

                await self.process_job(job)

            except asyncio.CancelledError:
                logger.info(
                    "Worker cancellation received"
                )
                break

            except Exception:
                logger.exception(
                    "Unexpected worker error"
                )

                # Avoid a tight retry loop
                await asyncio.sleep(1)

    # -----------------------------------------------------
    # Get job from Redis
    # -----------------------------------------------------

    async def get_next_job(self):
        try:
            result = await self.redis.blpop(
                QUEUE_NAME,
                timeout=1
            )

            if result is None:
                return None

            _, job_data = result
            return json.loads(job_data)

        except Exception:
            logger.exception("Error while reading job from Redis")
            await asyncio.sleep(1)
            return None

    # -----------------------------------------------------
    # Process job
    # -----------------------------------------------------

    async def process_job(
        self,
        job: dict,
    ):
        """
        Process one document.
        """

        document_id = job.get("document_id")

        if not document_id:
            logger.error(
                "Job does not contain document_id"
            )
            return

        logger.info(
            "Received job for document %s",
            document_id,
        )

        # Atomically claim the document
        document = await self.claim_document(
            document_id
        )

        if document is None:
            logger.warning(
                "Document %s could not be claimed",
                document_id,
            )
            return

        user_id = document["user_id"]

        try:

            # Simulate AI processing
            processing_time = random.randint(
                10,
                30,
            )

            logger.info(
                "Processing document %s for %s seconds",
                document_id,
                processing_time,
            )

            await asyncio.sleep(
                processing_time
            )
            # Simulate a 10% processing failure as required by the assignment.
            # Failed documents are marked as "failed" in MongoDB and are not
            # stored in the Redis summary cache.
            if random.random() < 0.10:
                raise RuntimeError(
                    "Simulated document processing failure"
                )

            # Generate mock summary
            summary = self.generate_summary(
                document
            )

            # Update MongoDB
            await self.mark_completed(
                document_id,
                summary,
            )

            # Cache only successful summaries. This allows future requests
            # with the same content hash to reuse the generated summary.
            await self.cache_summary(
                document["content_hash"],
                summary,
            )

            logger.info(
                "Document %s completed successfully",
                document_id,
            )

        except Exception as exc:
            # MongoDB is the source of truth for document status.
            # When processing fails, update the document to "failed"
            # and store the error message for the GET /documents/{id} API.

            logger.exception(
                "Document %s processing failed",
                document_id,
            )

            await self.mark_failed(
                document_id,
                str(exc),
            )

        finally:

            # Release the user's active-job slot whether processing
            # succeeds or fails, so a failed job does not consume
            # the user's rate-limit capacity permanently.
            await self.decrement_active_jobs(
                user_id
            )

    # -----------------------------------------------------
    # Atomically claim document
    # -----------------------------------------------------

    async def claim_document(
        self,
        document_id: str,
    ):
        """
        Atomically change queued -> processing.

        This prevents two workers from processing
        the same document.
        """

        if not ObjectId.is_valid(document_id):
            logger.error(
                "Invalid document ID: %s",
                document_id,
            )
            return None

        document = (
            await self.collection.find_one_and_update(
                {
                    "_id": ObjectId(document_id),
                    "status": "queued",
                },
                {
                    "$set": {
                        "status": "processing",
                        "updated_at": datetime.now(
                            timezone.utc
                        ),
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
        )

        return document

    # -----------------------------------------------------
    # Generate mock summary
    # -----------------------------------------------------

    @staticmethod
    def generate_summary(
        document: dict,
    ) -> dict:

        content = document["content"]

        words = content.split()

        # Simple mock summary
        summary_text = " ".join(words[:30])

        if len(words) > 30:
            summary_text += "..."

        return {
            "text": summary_text,
            "word_count": len(words),
        }

    # -----------------------------------------------------
    # Mark completed
    # -----------------------------------------------------

    async def mark_completed(
        self,
        document_id: str,
        summary: dict,
    ):

        await self.collection.update_one(
            {
                "_id": ObjectId(document_id),
                "status": "processing",
            },
            {
                "$set": {
                    "status": "completed",
                    "summary": summary,
                    "error": None,
                    "updated_at": datetime.now(
                        timezone.utc
                    ),
                }
            },
        )

    # -----------------------------------------------------
    # Mark failed
    # -----------------------------------------------------

    async def mark_failed(
        self,
        document_id: str,
        error: str,
    ):
        # Document status is maintained in MongoDB because MongoDB
        # is the source of truth for the document lifecycle.

        await self.collection.update_one(
            {
                "_id": ObjectId(document_id),
                "status": "processing",
            },
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "updated_at": datetime.now(
                        timezone.utc
                    ),
                }
            },
        )

    # -----------------------------------------------------
    # Redis cache
    # -----------------------------------------------------

    async def cache_summary(
        self,
        content_hash: str,
        summary: dict,
    ):
        # Redis is used only as a temporary cache for successful summaries.
        # Failed processing results are intentionally not cached.

        cache_key = (
            f"document:summary:{content_hash}"
        )

        await self.redis.set(
            cache_key,
            json.dumps(summary),
            ex=settings.CACHE_TTL,
        )

        logger.info(
            "Summary cached: %s",
            cache_key,
        )

    # -----------------------------------------------------
    # Active job counter
    # -----------------------------------------------------

    async def decrement_active_jobs(
        self,
        user_id: str,
    ):

        key = (
            f"user:{user_id}:active_jobs"
        )

        try:

            count = await self.redis.decr(
                key
            )

            # Prevent negative values
            if count <= 0:
                await self.redis.delete(key)

            logger.info(
                "Active jobs for user %s: %s",
                user_id,
                max(count, 0),
            )

        except Exception:
            logger.exception(
                "Failed to decrement active job count "
                "for user %s",
                user_id,
            )


# ---------------------------------------------------------
# Application entry point
# ---------------------------------------------------------

async def main():

    worker = DocumentWorker()

    await worker.initialize()

    try:
        await worker.run()

    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())