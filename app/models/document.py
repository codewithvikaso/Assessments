from datetime import datetime, timezone
from typing import Optional, Dict, Any


class DocumentModel:
    """
    MongoDB document structure for the documents collection.
    """

    COLLECTION_NAME = "documents"

    @staticmethod
    def create_document(
        user_id: str,
        title: str,
        content: str,
        content_hash: str,
    ) -> Dict[str, Any]:
        """
        Create a MongoDB document ready for insertion.
        """

        now = datetime.now(timezone.utc)

        return {
            "user_id": user_id,
            "title": title,
            "content": content,
            "content_hash": content_hash,

            # queued -> processing -> completed / failed
            "status": "queued",

            # Will be populated after processing
            "summary": None,

            # Store error information if processing fails
            "error": None,

            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def update_processing() -> Dict[str, Any]:
        """
        Fields to update when worker starts processing.
        """

        return {
            "$set": {
                "status": "processing",
                "updated_at": datetime.now(timezone.utc),
            }
        }

    @staticmethod
    def update_completed(
        summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Fields to update when processing succeeds.
        """

        return {
            "$set": {
                "status": "completed",
                "summary": summary,
                "error": None,
                "updated_at": datetime.now(timezone.utc),
            }
        }

    @staticmethod
    def update_failed(
        error: str,
    ) -> Dict[str, Any]:
        """
        Fields to update when processing fails.
        """

        return {
            "$set": {
                "status": "failed",
                "error": error,
                "updated_at": datetime.now(timezone.utc),
            }
        }