"""Application configuration"""

from pydantic_settings import BaseSettings
from pydantic import model_validator
from typing import List


class Settings(BaseSettings):
    """Application settings"""
    
    # Database — can be set directly OR constructed from individual components.
    # Kubernetes envFrom ConfigMaps do NOT interpolate $(VAR) syntax, so we
    # read POSTGRES_* vars injected individually via common_secret / config.
    DATABASE_URL: str = ""
    POSTGRES_DB_USERNAME: str = ""
    POSTGRES_DB_PASSWORD: str = ""
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_DB: str = "fusion_cdc_metadata"

    @model_validator(mode="after")
    def build_database_url(self) -> "Settings":
        """Construct DATABASE_URL from individual Postgres vars when not explicitly set."""
        if (
            not self.DATABASE_URL
            or "localhost" in self.DATABASE_URL
            or "$(POSTGRES_DB_USERNAME)" in self.DATABASE_URL
        ):
            if self.POSTGRES_DB_USERNAME and self.POSTGRES_DB_PASSWORD:
                object.__setattr__(
                    self,
                    "DATABASE_URL",
                    f"postgresql://{self.POSTGRES_DB_USERNAME}:{self.POSTGRES_DB_PASSWORD}"
                    f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}",
                )
            elif not self.DATABASE_URL:
                # Fall back to dev default
                object.__setattr__(
                    self,
                    "DATABASE_URL",
                    "postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata",
                )
        return self

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: str = ""

    @model_validator(mode="after")
    def inject_redis_password(self) -> "Settings":
        """Inject REDIS_PASSWORD into REDIS_URL when not already embedded."""
        if self.REDIS_PASSWORD and "@" not in self.REDIS_URL:
            url = self.REDIS_URL
            scheme = url.split("://")[0]
            rest = url.split("://", 1)[1]
            object.__setattr__(self, "REDIS_URL", f"{scheme}://:{self.REDIS_PASSWORD}@{rest}")
        return self
    
    # Keycloak
    KEYCLOAK_SERVER_URL: str = "http://localhost:8080"
    KEYCLOAK_REALM: str = "fusion"
    KEYCLOAK_CLIENT_ID: str = "fusion-cdc"
    KEYCLOAK_CLIENT_SECRET: str = ""
    
    # JWT
    JWT_SECRET_KEY: str = "your-super-secret-jwt-key-change-this-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 30
    
    # Encryption (for sensitive credentials)
    ENCRYPTION_KEY: str = "your-32-byte-encryption-key-for-aes256"

    # Internal worker API auth (shared secret, empty = disabled)
    WORKER_SHARED_SECRET: str = ""

    # CDC Worker HTTP URL for direct start-streaming notification
    WORKER_CONTROL_URL: str = ""
    CDC_WORKER_URL: str = "http://localhost:8081"

    # Airflow REST API for BATCH/SCHEDULED DAG triggers
    AIRFLOW_API_URL: str = "http://localhost:8080"
    AIRFLOW_USER: str = "admin"
    AIRFLOW_PASSWORD: str = "admin"
    
    # Application
    APP_ENV: str = "development"
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # Spark consumer webhook URL for schema reload notifications (spec §3)
    SPARK_CONSUMER_URL: str = ""

    # Periodic re-introspection interval (spec §3: "e.g. daily")
    SCHEMA_REINTROSPECT_INTERVAL_HOURS: int = 24
    
    # CORS
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:5173,http://localhost:5174"
    
    @property
    def cors_origins_list(self) -> List[str]:
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Allow extra fields from .env


settings = Settings()

# Module-level aliases used by test suites and legacy imports
JWT_SECRET_KEY = settings.JWT_SECRET_KEY
JWT_ALGORITHM = settings.JWT_ALGORITHM
JWT_EXPIRATION_MINUTES = settings.JWT_EXPIRATION_MINUTES
