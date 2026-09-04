from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    MONGO_URL: str = "mongodb://mongodb:27017"
    MONGO_DB: str = "document_insights"

    REDIS_URL: str = "redis://redis:6379"

    CACHE_TTL: int = 3600
    MAX_ACTIVE_JOBS: int = 3

    class Config:
        env_file = ".env"


settings = Settings()