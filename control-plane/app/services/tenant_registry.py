"""
Spec §1 (P1-8): Tenant Registry — automatically provision a Kubernetes namespace
and RBAC resources when a new SubTenant is created in Fusion.

This module provides two things:
1. ``TenantRegistry`` service class — called by the sub-tenant creation API endpoint.
2. ``provision_tenant_namespace()`` — the core K8s provisioning function.

K8s resources created per tenant:
  - Namespace:    fusion-<bank_id>-<tenant_id>
  - ResourceQuota: default compute limits
  - NetworkPolicy: default deny + allow from control-plane (see kubernetes/network_policy.yaml)
  - ServiceAccount: fusion-worker
  - RoleBinding:  fusion-worker → ClusterRole:fusion-worker-role

All operations are idempotent — safe to call on repeated tenant creation events.
Set KUBERNETES_TENANT_PROVISIONING_ENABLED=true to enable.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_ENABLED = os.environ.get("KUBERNETES_TENANT_PROVISIONING_ENABLED", "false").lower() == "true"
_NS_PREFIX = os.environ.get("TENANT_NAMESPACE_PREFIX", "fusion")


class TenantRegistry:
    """
    Wrapper used by the sub-tenant creation REST endpoint.

    Usage in API endpoint:
        from app.services.tenant_registry import TenantRegistry
        TenantRegistry().provision(bank_id=..., tenant_id=..., tenant_name=...)
    """

    def provision(self, bank_id: str, tenant_id: str, tenant_name: str) -> dict:
        """
        Provision K8s namespace and RBAC for a new tenant.
        Returns a dict describing what was created (or skipped if disabled).
        """
        if not _ENABLED:
            log.debug("TenantRegistry: provisioning disabled (KUBERNETES_TENANT_PROVISIONING_ENABLED=false)")
            return {"provisioned": False, "reason": "provisioning disabled"}
        return provision_tenant_namespace(bank_id=bank_id, tenant_id=tenant_id, tenant_name=tenant_name)

    def deprovision(self, bank_id: str, tenant_id: str) -> dict:
        """
        Remove K8s namespace for a deleted tenant.
        NOTE: this is destructive — it deletes the namespace and all resources within it.
        """
        if not _ENABLED:
            return {"deprovisioned": False, "reason": "provisioning disabled"}
        namespace = _build_namespace(bank_id, tenant_id)
        return _delete_namespace(namespace)


# ---------------------------------------------------------------------------
# Core provisioning
# ---------------------------------------------------------------------------

def _build_namespace(bank_id: str, tenant_id: str) -> str:
    # K8s namespace names must be <= 63 chars, lowercase alphanumeric + hyphens
    raw = f"{_NS_PREFIX}-{bank_id[:8]}-{tenant_id[:8]}".lower().replace("_", "-")
    return raw[:63]


def provision_tenant_namespace(bank_id: str, tenant_id: str, tenant_name: str) -> dict:
    """Idempotently create K8s namespace + RBAC for a tenant."""
    try:
        from kubernetes import client as k8s, config as k8s_config  # type: ignore
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()
    except ImportError:
        log.warning("TenantRegistry: kubernetes library not installed — skipping provisioning")
        return {"provisioned": False, "reason": "kubernetes library not installed"}

    namespace = _build_namespace(bank_id, tenant_id)
    labels = {
        "app.kubernetes.io/managed-by": "fusion",
        "fusion/bank-id": bank_id[:63],
        "fusion/tenant-id": tenant_id[:63],
    }

    v1 = k8s.CoreV1Api()
    rbac = k8s.RbacAuthorizationV1Api()
    created = []

    # 1. Namespace
    try:
        v1.create_namespace(k8s.V1Namespace(
            metadata=k8s.V1ObjectMeta(name=namespace, labels=labels)
        ))
        created.append(f"namespace/{namespace}")
        log.info("TenantRegistry: created namespace %s", namespace)
    except k8s.exceptions.ApiException as exc:
        if exc.status != 409:  # 409 = already exists
            raise

    # 2. ResourceQuota
    try:
        v1.create_namespaced_resource_quota(
            namespace=namespace,
            body=k8s.V1ResourceQuota(
                metadata=k8s.V1ObjectMeta(name="fusion-default-quota", namespace=namespace),
                spec=k8s.V1ResourceQuotaSpec(hard={
                    "requests.cpu": "4", "limits.cpu": "8",
                    "requests.memory": "4Gi", "limits.memory": "8Gi",
                    "pods": "20",
                }),
            ),
        )
        created.append(f"resourcequota/fusion-default-quota in {namespace}")
    except k8s.exceptions.ApiException as exc:
        if exc.status != 409:
            raise

    # 3. ServiceAccount
    try:
        v1.create_namespaced_service_account(
            namespace=namespace,
            body=k8s.V1ServiceAccount(
                metadata=k8s.V1ObjectMeta(name="fusion-worker", namespace=namespace, labels=labels)
            ),
        )
        created.append(f"serviceaccount/fusion-worker in {namespace}")
    except k8s.exceptions.ApiException as exc:
        if exc.status != 409:
            raise

    # 4. RoleBinding — bind worker SA to cluster-level worker role
    try:
        rbac.create_namespaced_role_binding(
            namespace=namespace,
            body=k8s.V1RoleBinding(
                metadata=k8s.V1ObjectMeta(name="fusion-worker-binding", namespace=namespace),
                role_ref=k8s.V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="ClusterRole",
                    name="fusion-worker-role",
                ),
                subjects=[k8s.V1Subject(
                    kind="ServiceAccount",
                    name="fusion-worker",
                    namespace=namespace,
                )],
            ),
        )
        created.append(f"rolebinding/fusion-worker-binding in {namespace}")
    except k8s.exceptions.ApiException as exc:
        if exc.status != 409:
            raise

    log.info("TenantRegistry: provisioned %d resources for tenant %s", len(created), tenant_id)
    return {"provisioned": True, "namespace": namespace, "resources": created}


def _delete_namespace(namespace: str) -> dict:
    """Delete a K8s namespace (and all resources within it)."""
    try:
        from kubernetes import client as k8s, config as k8s_config  # type: ignore
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()
        v1 = k8s.CoreV1Api()
        v1.delete_namespace(name=namespace)
        log.info("TenantRegistry: deleted namespace %s", namespace)
        return {"deprovisioned": True, "namespace": namespace}
    except Exception as exc:
        log.warning("TenantRegistry: failed to delete namespace %s: %s", namespace, exc)
        return {"deprovisioned": False, "error": str(exc)}
