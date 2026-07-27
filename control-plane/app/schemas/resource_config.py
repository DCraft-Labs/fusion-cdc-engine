"""Pydantic schemas for the resource-aware admission-control system:
resource pool configuration + per-connection admission preview/confirm.

Kept in its own module (mirroring ``schemas/connection.py``) so the
frontend agent building the Resource Config page + mode picker has a single
file to read for the exact request/response shapes.
"""
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ===========================
# Resource Config (pool sizing) schemas
# ===========================

class ResourceConfigBase(BaseModel):
    total_cpu_min_millis: int = Field(..., gt=0, description="Total pool CPU floor, in millicores (e.g. 2000 = 2 cores)")
    total_cpu_max_millis: int = Field(..., gt=0, description="Total pool CPU ceiling, in millicores")
    total_memory_min_mi: int = Field(..., gt=0, description="Total pool memory floor, in MiB")
    total_memory_max_mi: int = Field(..., gt=0, description="Total pool memory ceiling, in MiB")
    instance_type: str = Field(..., min_length=1, max_length=100, description="e.g. 'm5.2xlarge'")
    instance_count: int = Field(..., gt=0, description="Number of instances in the pool")
    pool_scope: str = Field("dedicated", description="'dedicated' (fully owned by fusion-cdc) or 'shared' (slice of a bigger cluster)")

    @field_validator("pool_scope")
    @classmethod
    def validate_pool_scope(cls, v: str) -> str:
        v = (v or "").lower()
        if v not in ("dedicated", "shared"):
            raise ValueError("pool_scope must be 'dedicated' or 'shared'")
        return v

    @field_validator("total_cpu_max_millis")
    @classmethod
    def validate_cpu_max(cls, v: int, info) -> int:
        mn = info.data.get("total_cpu_min_millis")
        if mn is not None and v < mn:
            raise ValueError("total_cpu_max_millis must be >= total_cpu_min_millis")
        return v

    @field_validator("total_memory_max_mi")
    @classmethod
    def validate_memory_max(cls, v: int, info) -> int:
        mn = info.data.get("total_memory_min_mi")
        if mn is not None and v < mn:
            raise ValueError("total_memory_max_mi must be >= total_memory_min_mi")
        return v


class ResourceConfigUpsert(ResourceConfigBase):
    """Body for ``PUT /api/v1/resource-config`` — create-or-update the
    calling user's bank-scoped singleton. Setting this always flips
    ``resource_configured`` to true server-side."""
    pass


class ResourceConfigResponse(BaseModel):
    resource_config_id: Optional[UUID] = None
    bank_id: Optional[UUID] = None
    total_cpu_min_millis: Optional[int] = None
    total_cpu_max_millis: Optional[int] = None
    total_memory_min_mi: Optional[int] = None
    total_memory_max_mi: Optional[int] = None
    instance_type: Optional[str] = None
    instance_count: Optional[int] = None
    pool_scope: Optional[str] = None
    resource_configured: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ===========================
# Admission preview / confirm schemas
# ===========================

class AdmissionPreviewRequest(BaseModel):
    stream_ids: Optional[List[UUID]] = Field(
        None,
        description="Stream IDs to size for. Defaults to all is_enabled=true streams on the connection "
                    "(i.e. the tables the user just selected in the wizard).",
    )


class AdmissionModeOption(BaseModel):
    mode: str = Field(..., description="'aggressive' | 'normal' | 'saver'")
    fits: bool = Field(..., description="Whether this mode currently fits available ledger capacity")
    reason: Optional[str] = Field(None, description="Why it doesn't fit, e.g. 'insufficient_cpu' / 'insufficient_memory' — null when fits=true")
    eta_seconds: int = Field(..., description="Estimated initial-load wall-clock time for this mode")
    parallelism: int = Field(..., description="K — partition parallelism this mode would use (1 for saver)")
    cpu_millis: int = Field(..., description="CPU (millicores) this mode would reserve for the initial load")
    memory_mi: int = Field(..., description="Memory (MiB) this mode would reserve for the initial load")
    bulk_mode: Optional[str] = Field(None, description="'duckdb' | 'python' — resolved per AUTO_BULK_MODE_ROW_THRESHOLD, informational")


class AdmissionPreviewResponse(BaseModel):
    connection_id: UUID
    rows_estimated_total: Optional[int] = None
    tier: str = Field(..., description="'S' | 'M' | 'L' | 'XL'")
    modes: List[AdmissionModeOption]
    available_modes: List[str] = Field(..., description="Subset of mode names where fits=true — the only ones the UI should let the user pick")
    available_cpu_millis: int
    available_memory_mi: int


class AdmissionConfirmRequest(BaseModel):
    mode: str = Field(..., description="'aggressive' | 'normal' | 'saver' — must be one of the fitting modes from admission-preview")
    stream_ids: Optional[List[UUID]] = Field(None, description="Must match (or be a subset sized the same as) the preview call")

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        v = (v or "").lower()
        if v not in ("aggressive", "normal", "saver"):
            raise ValueError("mode must be one of: aggressive, normal, saver")
        return v


class AdmissionConfirmResponse(BaseModel):
    connection_id: UUID
    reserved: bool = Field(..., description="False means the reservation lost the atomic capacity race — retry preview")
    reason: Optional[str] = Field(None, description="Set when reserved=false, e.g. 'insufficient_capacity'")
    mode: Optional[str] = None
    tier: Optional[str] = None
    parallelism: Optional[int] = None
    cpu_reserved_millis: Optional[int] = None
    memory_reserved_mi: Optional[int] = None
    eta_seconds: Optional[int] = None
    available_cpu_millis: int
    available_memory_mi: int
