"""Pydantic schemas for Transformation Pipelines and UDF Catalog"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ===========================
# Transformation Pipeline
# ===========================

VALID_PIPELINE_TYPES = {"sql", "python", "spark"}
VALID_LANGUAGES = {"sql", "python", "scala"}
VALID_EXECUTION_MODES = {"batch", "streaming"}


class TransformPipelineCreate(BaseModel):
    """Schema for creating a new transformation pipeline"""

    pipeline_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None)
    pipeline_type: str = Field(..., description="sql | python | spark")
    transformation_code: str = Field(..., min_length=1)
    language: str = Field(..., description="sql | python | scala")
    input_streams: List[str] = Field(default_factory=list)
    output_stream: str = Field(..., min_length=1, max_length=255)
    execution_mode: str = Field(..., description="batch | streaming")
    spark_config: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("pipeline_type")
    @classmethod
    def validate_pipeline_type(cls, v: str) -> str:
        if v not in VALID_PIPELINE_TYPES:
            raise ValueError(f"pipeline_type must be one of: {', '.join(sorted(VALID_PIPELINE_TYPES))}")
        return v

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        if v not in VALID_LANGUAGES:
            raise ValueError(f"language must be one of: {', '.join(sorted(VALID_LANGUAGES))}")
        return v

    @field_validator("execution_mode")
    @classmethod
    def validate_execution_mode(cls, v: str) -> str:
        if v not in VALID_EXECUTION_MODES:
            raise ValueError(f"execution_mode must be one of: {', '.join(sorted(VALID_EXECUTION_MODES))}")
        return v


class TransformPipelineUpdate(BaseModel):
    """Schema for updating an existing transformation pipeline"""

    pipeline_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    pipeline_type: Optional[str] = None
    transformation_code: Optional[str] = Field(None, min_length=1)
    language: Optional[str] = None
    input_streams: Optional[List[str]] = None
    output_stream: Optional[str] = Field(None, min_length=1, max_length=255)
    execution_mode: Optional[str] = None
    spark_config: Optional[Dict[str, Any]] = None
    is_published: Optional[bool] = None
    is_active: Optional[bool] = None

    @field_validator("pipeline_type")
    @classmethod
    def validate_pipeline_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_PIPELINE_TYPES:
            raise ValueError(f"pipeline_type must be one of: {', '.join(sorted(VALID_PIPELINE_TYPES))}")
        return v

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_LANGUAGES:
            raise ValueError(f"language must be one of: {', '.join(sorted(VALID_LANGUAGES))}")
        return v

    @field_validator("execution_mode")
    @classmethod
    def validate_execution_mode(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_EXECUTION_MODES:
            raise ValueError(f"execution_mode must be one of: {', '.join(sorted(VALID_EXECUTION_MODES))}")
        return v


class TransformPipelineResponse(BaseModel):
    """Schema for transformation pipeline response"""

    pipeline_id: UUID
    pipeline_name: str
    description: Optional[str]
    pipeline_type: str
    transformation_code: str
    language: str
    input_streams: List[Any]
    output_stream: str
    execution_mode: str
    spark_config: Dict[str, Any]
    version: int
    is_published: bool
    is_validated: bool
    validation_errors: Optional[Any]
    validated_at: Optional[datetime]
    is_active: bool
    is_deleted: bool
    last_executed_at: Optional[datetime]
    sub_tenant_id: UUID
    bank_id: UUID
    created_by: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TransformPipelineListResponse(BaseModel):
    """Paginated list of transformation pipelines"""

    pipelines: List[TransformPipelineResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class TransformValidateResponse(BaseModel):
    """Schema for transformation validation result"""

    pipeline_id: UUID
    valid: bool
    errors: List[str]
    validated_at: datetime


# ===========================
# UDF Catalog
# ===========================

VALID_UDF_LANGUAGES = {"python", "scala", "java"}


class UDFCreate(BaseModel):
    """Schema for registering a new UDF"""

    udf_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    function_code: str = Field(..., min_length=1)
    language: str = Field(..., description="python | scala | java")
    return_type: str = Field(..., min_length=1, max_length=100)
    parameters: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of {name, type, description} dicts",
    )
    category: Optional[str] = Field(None, max_length=100)
    tags: List[str] = Field(default_factory=list)

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        if v not in VALID_UDF_LANGUAGES:
            raise ValueError(f"language must be one of: {', '.join(sorted(VALID_UDF_LANGUAGES))}")
        return v


class UDFUpdate(BaseModel):
    """Schema for updating a UDF"""

    description: Optional[str] = None
    function_code: Optional[str] = Field(None, min_length=1)
    language: Optional[str] = None
    return_type: Optional[str] = Field(None, min_length=1, max_length=100)
    parameters: Optional[List[Dict[str, Any]]] = None
    category: Optional[str] = Field(None, max_length=100)
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_UDF_LANGUAGES:
            raise ValueError(f"language must be one of: {', '.join(sorted(VALID_UDF_LANGUAGES))}")
        return v


class UDFResponse(BaseModel):
    """Schema for UDF response"""

    udf_id: UUID
    udf_name: str
    description: Optional[str]
    function_code: str
    language: str
    return_type: str
    parameters: List[Any]
    category: Optional[str]
    tags: List[Any]
    is_validated: bool
    validation_errors: Optional[Any]
    is_active: bool
    sub_tenant_id: UUID
    bank_id: UUID
    created_by: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UDFListResponse(BaseModel):
    """Paginated list of UDFs"""

    udfs: List[UDFResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
