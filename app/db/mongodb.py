from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import settings


class MongoDB:

    def __init__(self):
        self.client: AsyncIOMotorClient | None = None
        self.database: AsyncIOMotorDatabase | None = None

    async def connect(self):
        """
        Connect to MongoDB.
        """

        self.client = AsyncIOMotorClient(
            settings.MONGO_URL
        )

        self.database = self.client[
            settings.MONGO_DB
        ]

        # Verify connection
        await self.client.admin.command("ping")

        # Create indexes
        await self.create_indexes()

        print("MongoDB connected successfully")

    async def disconnect(self):
        """
        Close MongoDB connection.
        """

        if self.client:
            self.client.close()

        print("MongoDB connection closed")

    def get_collection(self, collection_name: str):
        """
        Get MongoDB collection.
        """

        if self.database is None:
            raise RuntimeError(
                "MongoDB is not connected"
            )

        return self.database[collection_name]

    async def create_indexes(self):
        """
        Create indexes required by the application.
        """

        collection = self.get_collection(
            "documents"
        )

        # User + status
        await collection.create_index(
            [
                ("user_id", 1),
                ("status", 1)
            ]
        )

        # User document pagination
        await collection.create_index(
            [
                ("user_id", 1),
                ("created_at", -1)
            ]
        )

        # Content hash lookup
        await collection.create_index(
            "content_hash"
        )


mongodb = MongoDB()