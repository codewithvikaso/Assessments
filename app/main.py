from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.mongodb import mongodb

from app.api.routes.documents import router as document_router
from app.api.routes.users import router as user_router
from app.db.redis import redis_manager
@asynccontextmanager
async def lifespan(app: FastAPI):

    # Startup
    await mongodb.connect()
    await redis_manager.connect()

    yield

    # Shutdown
    await redis_manager.disconnect()
    await mongodb.disconnect()


app = FastAPI(
    title="Document Insights API",
    lifespan=lifespan
)
app.include_router(document_router)
app.include_router(user_router)