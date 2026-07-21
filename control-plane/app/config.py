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

    # JWT — the default is a known public string. In any non-dev APP_ENV we
    # fail fast at startup if the operator has not overridden it. This
    # prevents anyone with the source from minting valid tokens.
    JWT_SECRET_KEY: str = "your-super-secret-jwt-key-change-this-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 30

    # Encryption (for sensitive credentials at rest). Default is a known
    # public string — fail fast in non-dev if not overridden so credentials
    # cannot be decrypted by anyone with the source.
    ENCRYPTION_KEY: str = "your-32-byte-encryption-key-for-aes256"

    # Internal worker API auth (shared secret). Empty = disabled, which is
    # fine for dev/test but means any pod can call /internal/heartbeat,
    # /internal/checkpoint, /internal/event-failed in production. Fail fast
    # in non-dev if unset.
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

    # Kafka bootstrap servers — probed by the /api/v1/monitoring/health
    # endpoint. Empty string means "not configured" (the health check will
    # report kafka: not_configured instead of unhealthy).
    KAFKA_BOOTSTRAP_SERVERS: str = ""

    # Periodic re-introspection interval (spec §3: "e.g. daily")
    SCHEMA_REINTROSPECT_INTERVAL_HOURS: int = 24

    # CORS
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:5173,http://localhost:5174"

    @property
    def cors_origins_list(self) -> List[str]:
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]

    @model_validator(mode="after")
    def fail_fast_on_default_secrets_in_production(self) -> "Settings":
        """Fail fast if production secrets are unset or still at default."""
        dev_envs = {"dev", "development", "local", "test", "testing", "ci"}
        env = (self.APP_ENV or "").strip().lower()
        if env in dev_envs:
            return self

        if (
            not self.JWT_SECRET_KEY
            or self.JWT_SECRET_KEY
            == "your-super-secret-jwt-key-change-this-in-production"
        ):
            raise RuntimeError(
                "JWT_SECRET_KEY must be set to a non-default value in "
                f"production (APP_ENV={env})"
            )

        if (
            not self.ENCRYPTION_KEY
            or self.ENCRYPTION_KEY == "your-32-byte-encryption-key-for-aes256"
        ):
            raise RuntimeError(
                "ENCRYPTION_KEY must be set to a non-default value in "
                f"production (APP_ENV={env})"
            )

        if not self.WORKER_SHARED_SECRET:
            raise RuntimeError(
                "WORKER_SHARED_SECRET must be set in production to protect "
                "internal APIs (/internal/heartbeat, /internal/checkpoint, "
                "/internal/event-failed)"
            )

        return self

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Allow extra fields from .env


settings = Settings()

# Module-level aliases used by test suites and legacy imports
JWT_SECRET_KEY = settings.JWT_SECRET_KEY
JWT_ALGORITHM = settings.JWT_ALGORITHM
JWT_EXPIRATION_MINUTES = settings.JWT_EXPIRATION_MINUTES
