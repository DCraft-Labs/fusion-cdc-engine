"""
Settings API — system config and Spark configuration management.

Endpoints:
  GET    /settings/system-config             list all config keys
  POST   /settings/system-config             create a config key
  PUT    /settings/system-config/{key}       update a config key
  DELETE /settings/system-config/{key}       delete a config key

  GET    /settings/spark-config              get all spark.* keys as structured object
  PUT    /settings/spark-config              bulk-upsert spark config
"""
from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import User
from app.models.system import SystemConfig

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ──────────────────────────────────────────────────────────────────────────────

class ConfigEntryResponse(BaseModel):
    key: str
    value: str
    value_type: str
    description: Optional[str] = None
    category: Optional[str] = None
    is_sensitive: bool = False

    class Config:
        from_attributes = True


class ConfigCreateRequest(BaseModel):
    key: str
    value: str
    value_type: str = "string"
    description: Optional[str] = None
    category: Optional[str] = None
    is_sensitive: bool = False


class ConfigUpdateRequest(BaseModel):
    value: str
    description: Optional[str] = None


class SparkConfigRequest(BaseModel):
    master: str = "k8s://https://kubernetes.default.svc.cluster.local:443"
    deploy_mode: str = "cluster"
    namespace: str = "spark"
    image_pull_policy: str = "IfNotPresent"
    driver_cores: str = "1"
    driver_memory: str = "1g"
    executor_cores: str = "1"
    executor_memory: str = "1g"
    executor_instances: str = "2"
    dynamic_allocation_enabled: bool = False
    dynamic_allocation_min: str = "1"
    dynamic_allocation_max: str = "5"
    checkpoint_dir: str = "/tmp/spark-checkpoints"
    extra_conf: Dict[str, str] = {}
    service_account: str = "spark"
    image_registry: str = ""
    image_tag: str = "3.4.1"


class SparkConfigResponse(SparkConfigRequest):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_SPARK_PREFIX = "spark."

_SPARK_FIELD_MAP = {
    "master":                      f"{_SPARK_PREFIX}master",
    "deploy_mode":                 f"{_SPARK_PREFIX}deploy_mode",
    "namespace":                   f"{_SPARK_PREFIX}kubernetes.namespace",
    "image_pull_policy":           f"{_SPARK_PREFIX}kubernetes.container.image.pullPolicy",
    "driver_cores":                f"{_SPARK_PREFIX}driver.cores",
    "driver_memory":               f"{_SPARK_PREFIX}driver.memory",
    "executor_cores":              f"{_SPARK_PREFIX}executor.cores",
    "executor_memory":             f"{_SPARK_PREFIX}executor.memory",
    "executor_instances":          f"{_SPARK_PREFIX}executor.instances",
    "dynamic_allocation_enabled":  f"{_SPARK_PREFIX}dynamicAllocation.enabled",
    "dynamic_allocation_min":      f"{_SPARK_PREFIX}dynamicAllocation.minExecutors",
    "dynamic_allocation_max":      f"{_SPARK_PREFIX}dynamicAllocation.maxExecutors",
    "checkpoint_dir":              f"{_SPARK_PREFIX}streaming.checkpointLocation",
    "service_account":             f"{_SPARK_PREFIX}kubernetes.authenticate.driver.serviceAccountName",
    "image_registry":              f"{_SPARK_PREFIX}kubernetes.container.image.registry",
    "image_tag":                   f"{_SPARK_PREFIX}kubernetes.container.image.tag",
}


def _upsert_config(db: Session, key: str, value: str, category: str, description: str, user_id: Any) -> None:
    row = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
    if row:
        row.config_value = value
        row.updated_by = user_id
    else:
        db.add(SystemConfig(
            config_key=key,
            config_value=value,
            value_type="string",
            category=category,
            description=description,
            is_sensitive=False,
            updated_by=user_id,
        ))


# ──────────────────────────────────────────────────────────────────────────────
# Generic system-config CRUD
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/system-config", response_model=List[ConfigEntryResponse])
def list_system_config(
    category: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return all system config entries. Sensitive values are masked."""
    q = db.query(SystemConfig)
    if category:
        q = q.filter(SystemConfig.category == category)
    rows = q.order_by(SystemConfig.category, SystemConfig.config_key).all()
    result = []
    for r in rows:
        result.append(ConfigEntryResponse(
            key=r.config_key,
            value="***" if r.is_sensitive else r.config_value,
            value_type=r.value_type,
            description=r.description,
            category=r.category,
            is_sensitive=r.is_sensitive,
        ))
    return result


@router.post("/system-config", response_model=ConfigEntryResponse, status_code=status.HTTP_201_CREATED)
def create_system_config(
    payload: ConfigCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("admin")),
):
    existing = db.query(SystemConfig).filter(SystemConfig.config_key == payload.key).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Config key '{payload.key}' already exists")
    row = SystemConfig(
        config_key=payload.key,
        config_value=payload.value,
        value_type=payload.value_type,
        description=payload.description,
        category=payload.category,
        is_sensitive=payload.is_sensitive,
        updated_by=current_user.user_id,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return ConfigEntryResponse(
        key=row.config_key,
        value="***" if row.is_sensitive else row.config_value,
        value_type=row.value_type,
        description=row.description,
        category=row.category,
        is_sensitive=row.is_sensitive,
    )


@router.put("/system-config/{key}", response_model=ConfigEntryResponse)
def update_system_config(
    key: str,
    payload: ConfigUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("admin")),
):
    row = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Config key '{key}' not found")
    row.config_value = payload.value
    if payload.description is not None:
        row.description = payload.description
    row.updated_by = current_user.user_id
    db.commit()
    db.refresh(row)
    return ConfigEntryResponse(
        key=row.config_key,
        value="***" if row.is_sensitive else row.config_value,
        value_type=row.value_type,
        description=row.description,
        category=row.category,
        is_sensitive=row.is_sensitive,
    )


@router.delete("/system-config/{key}", status_code=status.HTTP_204_NO_CONTENT)
def delete_system_config(
    key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("superadmin")),
):
    row = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Config key '{key}' not found")
    db.delete(row)
    db.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Spark configuration — structured read/write
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/spark-config", response_model=SparkConfigResponse)
def get_spark_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return structured Spark configuration from system_config table."""
    rows = {
        r.config_key: r.config_value
        for r in db.query(SystemConfig).filter(SystemConfig.category == "spark").all()
    }

    def _get(field: str, default: str) -> str:
        return rows.get(_SPARK_FIELD_MAP.get(field, ""), default)

    # Collect extra_conf — any spark.* key not in the known map
    known_values = set(_SPARK_FIELD_MAP.values())
    extra_conf = {k: v for k, v in rows.items() if k not in known_values and k.startswith(_SPARK_PREFIX)}

    return SparkConfigResponse(
        master=_get("master", "k8s://https://kubernetes.default.svc.cluster.local:443"),
        deploy_mode=_get("deploy_mode", "cluster"),
        namespace=_get("namespace", "spark"),
        image_pull_policy=_get("image_pull_policy", "IfNotPresent"),
        driver_cores=_get("driver_cores", "1"),
        driver_memory=_get("driver_memory", "1g"),
        executor_cores=_get("executor_cores", "1"),
        executor_memory=_get("executor_memory", "1g"),
        executor_instances=_get("executor_instances", "2"),
        dynamic_allocation_enabled=_get("dynamic_allocation_enabled", "false").lower() == "true",
        dynamic_allocation_min=_get("dynamic_allocation_min", "1"),
        dynamic_allocation_max=_get("dynamic_allocation_max", "5"),
        checkpoint_dir=_get("checkpoint_dir", "/tmp/spark-checkpoints"),
        service_account=_get("service_account", "spark"),
        image_registry=_get("image_registry", ""),
        image_tag=_get("image_tag", "3.4.1"),
        extra_conf=extra_conf,
    )


@router.put("/spark-config", response_model=SparkConfigResponse)
def update_spark_config(
    payload: SparkConfigRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("admin")),
):
    """Bulk-upsert all Spark config keys into system_config table."""
    field_values = {
        "master": payload.master,
        "deploy_mode": payload.deploy_mode,
        "namespace": payload.namespace,
        "image_pull_policy": payload.image_pull_policy,
        "driver_cores": payload.driver_cores,
        "driver_memory": payload.driver_memory,
        "executor_cores": payload.executor_cores,
        "executor_memory": payload.executor_memory,
        "executor_instances": payload.executor_instances,
        "dynamic_allocation_enabled": str(payload.dynamic_allocation_enabled).lower(),
        "dynamic_allocation_min": payload.dynamic_allocation_min,
        "dynamic_allocation_max": payload.dynamic_allocation_max,
        "checkpoint_dir": payload.checkpoint_dir,
        "service_account": payload.service_account,
        "image_registry": payload.image_registry,
        "image_tag": payload.image_tag,
    }

    for field, value in field_values.items():
        db_key = _SPARK_FIELD_MAP[field]
        _upsert_config(db, db_key, value, "spark", field.replace("_", " ").title(), current_user.user_id)

    # Handle extra_conf
    for k, v in payload.extra_conf.items():
        _upsert_config(db, k, v, "spark", "Custom Spark property", current_user.user_id)

    db.commit()
    return get_spark_config(db=db, current_user=current_user)
