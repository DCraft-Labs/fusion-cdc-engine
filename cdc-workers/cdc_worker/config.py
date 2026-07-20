"""
WorkerConfig — pydantic-settings based configuration.
All values can be overridden via environment variables.
"""

from pydantic_settings import BaseSettings
from pydantic import model_validator
from typing import Optional


class WorkerConfig(BaseSettings):
    # Control Plane
    CONTROL_PLANE_URL: str = "http://localhost:8000"
    WORKER_TOKEN: str = "change-me-worker-secret-token"
    WORKER_ID: str = "worker-1"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: str = ""
    REDIS_STREAM_MAXLEN: int = 100_000

    # SQLite fallback / checkpoint
    CHECKPOINT_DB_PATH: str = "/var/lib/cdc/checkpoints.db"
    FALLBACK_DB_PATH: str = "/var/lib/cdc/fallback.db"

    # Concurrency
    MAX_CONCURRENT_TABLES: int = 20

    # Heartbeat interval (seconds)
    HEARTBEAT_INTERVAL: int = 30

    # Central checkpoint sync interval (seconds)
    CHECKPOINT_SYNC_INTERVAL: int = 300

    # Fallback drain interval (seconds)
    FALLBACK_DRAIN_INTERVAL: int = 5

    # WAL feedback — DO NOT increase beyond 10
    WAL_FEEDBACK_INTERVAL: int = 10

    # Encryption key (must match control plane)
    ENCRYPTION_KEY: str = "your-32-byte-encryption-key-for-aes256"

    # HTTP port for worker internal API (start-streaming, health)
    HTTP_PORT: int = 8081

    @model_validator(mode="after")
    def inject_redis_password(self) -> "WorkerConfig":
        """Inject REDIS_PASSWORD into REDIS_URL if password is provided but URL lacks auth."""
        if self.REDIS_PASSWORD and "@" not in self.REDIS_URL:
            # Insert :password@ before the host
            # redis://host:port  →  redis://:password@host:port
            url = self.REDIS_URL
            scheme = url.split("://")[0]
            rest = url.split("://", 1)[1]
            object.__setattr__(self, "REDIS_URL", f"{scheme}://:{self.REDIS_PASSWORD}@{rest}")
        return self

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = WorkerConfig()
