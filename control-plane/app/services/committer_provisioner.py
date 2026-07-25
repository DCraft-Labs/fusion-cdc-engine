"""
Dynamic Iceberg committer provisioning.

Previously the per-(connection, table) Iceberg committer Deployment had to be
hand-added to ``helm/fusion-cdc/values.yaml`` under ``committer.targets`` and
rolled out with a manual ``helm upgrade`` — nothing in the control-plane ever
created or updated it automatically, so every new Iceberg connection needed an
operator to notice and go do that by hand.

This module replaces that manual step: ``ensure_committer()`` is called from
the initial-load producer (``app/api/connections.py::_enqueue_initial_load_tasks``,
the single choke point both connection-creation-with-immediate-activation and
retry-initial-load already funnel through) whenever a stream's destination
needs the transform-worker/committer path. It builds the exact same
Deployment + Secret + headless Service that the Helm template
(``templates/iceberg-committer.yaml``) renders — same naming scheme
(``sha256(connectionId + "-" + table)[:10]``), same labels, same env vars,
same probes — but does it directly against the Kubernetes API, idempotently
(create if missing, patch if the resolved config changed).

Set ``COMMITTER_AUTO_PROVISION_ENABLED=false`` to disable (falls back to the
old manual Helm-values workflow). All operations are no-ops (with a clear log
message) if the ``kubernetes`` package isn't installed, mirroring
``app/services/tenant_registry.py``'s defensive-import pattern — control-plane
still boots without it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Optional

log = logging.getLogger(__name__)

_ENABLED = os.environ.get("COMMITTER_AUTO_PROVISION_ENABLED", "true").lower() == "true"

# Mirrors helm/fusion-cdc/values.yaml's committer.* defaults exactly, so a
# dynamically-provisioned committer behaves the same as the old manually
# Helm-templated one when no per-connection override is set.
_LOCK_TTL_S = int(os.environ.get("COMMITTER_LOCK_TTL_S", "30"))
_DRAIN_TIMEOUT_MS = int(os.environ.get("COMMITTER_DRAIN_TIMEOUT_MS", "5000"))
_IDLE_SLEEP_S = os.environ.get("COMMITTER_IDLE_SLEEP_S", "5.0")
_METRICS_PORT = int(os.environ.get("COMMITTER_METRICS_PORT", "8081"))
_READINESS_LAG_THRESHOLD = int(os.environ.get("COMMITTER_READINESS_LAG_THRESHOLD", "1000"))
_LIVENESS_LAG_THRESHOLD = int(os.environ.get("COMMITTER_LIVENESS_LAG_THRESHOLD", "5000"))
_CPU_REQUEST = os.environ.get("COMMITTER_CPU_REQUEST", "250m")
_MEM_REQUEST = os.environ.get("COMMITTER_MEM_REQUEST", "512Mi")
_CPU_LIMIT = os.environ.get("COMMITTER_CPU_LIMIT", "2000m")
_MEM_LIMIT = os.environ.get("COMMITTER_MEM_LIMIT", "2048Mi")

# Auto drain_batch heuristic (used when resource_limits.drain_batch is unset).
# Anchored on the throughput-investigation session's own validated result:
# K=6 parallelism + drainBatch=1000 sustained ~74k-97k rows/sec end-to-end on
# a 35.86M-row table with zero backlog growth. Scaling linearly with K
# reflects the real mechanism — more concurrent workers stage more pending
# files per drain cycle — and a size bump for very large tables gives the
# committer more headroom before the pending list can grow. Clamped to a
# sane range so a pathological K or estimate can't produce something silly.
_AUTO_DRAIN_BATCH_ANCHOR_K = 6
_AUTO_DRAIN_BATCH_ANCHOR_VALUE = 1000
_AUTO_DRAIN_BATCH_MIN = 100
_AUTO_DRAIN_BATCH_MAX = 5000
_AUTO_DRAIN_BATCH_LARGE_TABLE_ROWS = 50_000_000
_AUTO_DRAIN_BATCH_LARGE_TABLE_MULTIPLIER = 1.5


def resolve_drain_batch(resource_limits: dict, k: int, rows_estimated_total: Optional[int]) -> int:
    """Resolve the committer's per-cycle Redis drain batch size.

    An explicit ``resource_limits.drain_batch`` always wins. Otherwise
    ("auto", the default) scale the validated K=6/1000 baseline by actual
    parallelism, with a bump for very large tables.
    """
    explicit = (resource_limits or {}).get("drain_batch")
    if explicit:
        try:
            return max(1, int(explicit))
        except (TypeError, ValueError):
            pass
    k = max(1, int(k or 1))
    scaled = _AUTO_DRAIN_BATCH_ANCHOR_VALUE * (k / _AUTO_DRAIN_BATCH_ANCHOR_K)
    if rows_estimated_total and rows_estimated_total >= _AUTO_DRAIN_BATCH_LARGE_TABLE_ROWS:
        scaled *= _AUTO_DRAIN_BATCH_LARGE_TABLE_MULTIPLIER
    return int(max(_AUTO_DRAIN_BATCH_MIN, min(_AUTO_DRAIN_BATCH_MAX, scaled)))


def _committer_name(connection_id: str, table: str, release_name: str) -> str:
    """Matches the Helm template's naming EXACTLY:
    ``printf "%s-%s" $tgt.connectionId $tgt.table | sha256sum | trunc 10``
    so provisioning a connection that was previously added by hand under
    ``committer.targets`` finds and updates the SAME object instead of
    creating a duplicate.
    """
    digest = hashlib.sha256(f"{connection_id}-{table}".encode()).hexdigest()[:10]
    return f"{release_name}-committer-{digest}"


def _current_namespace() -> str:
    ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    try:
        with open(ns_file) as f:
            return f.read().strip()
    except OSError:
        return os.environ.get("NAMESPACE", "default")


def _catalog_readiness_check_url(catalog_config: dict) -> Optional[str]:
    """Best-effort HTTP endpoint to probe before starting the committer's
    main loop, derived from the destination's OWN configured catalog
    connection details (never guessed from a naming convention — see the
    caller's comment for why that was a real bug). Returns None when the
    catalog type has no natural HTTP readiness endpoint to check (e.g.
    glue/dynamodb/sql) — the caller skips the startup probe entirely in
    that case rather than checking something meaningless.
    """
    catalog_type = str(catalog_config.get("catalog_type") or "").lower()
    if catalog_type == "nessie":
        uri = catalog_config.get("nessie_uri")
    elif catalog_type in ("rest", "hive"):
        uri = catalog_config.get("catalog_uri") or catalog_config.get("hive_uri")
    else:
        return None
    if not uri:
        return None
    from urllib.parse import urlparse
    parsed = urlparse(uri)
    if not parsed.hostname:
        return None
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    # Nessie's REST catalog root is under /iceberg or similar per nessie_uri;
    # /api/v1/config is Nessie's own health/config endpoint, reachable at
    # the same host:port regardless of the configured catalog sub-path.
    scheme = parsed.scheme or "http"
    return f"{scheme}://{parsed.hostname}:{port}/api/v1/config"


def _labels(committer_name: str, connection_id: str, table: str) -> dict:
    return {
        "app.kubernetes.io/component": "iceberg-committer",
        "app.kubernetes.io/name": committer_name,
        "app.kubernetes.io/managed-by": "fusion-cdc-control-plane",
        "cdc.dcraftfusion.io/connection-id": connection_id,
        "cdc.dcraftfusion.io/table": table,
    }


def _load_k8s():
    try:
        from kubernetes import client as k8s, config as k8s_config  # type: ignore
    except ImportError:
        return None, None
    try:
        k8s_config.load_incluster_config()
    except Exception:
        try:
            k8s_config.load_kube_config()
        except Exception:
            log.warning("committer_provisioner: no in-cluster or kubeconfig credentials available")
            return None, None
    return k8s, k8s_config


def ensure_committer(connection_id: str, table: str, catalog_config: dict,
                      resource_limits: dict, k: int,
                      rows_estimated_total: Optional[int] = None,
                      dest_namespace: str = "fusion") -> dict:
    """Idempotently create or update the committer Deployment/Secret/Service
    for this (connection_id, table) pair. Returns a dict describing what
    happened. Never raises — a provisioning failure logs loudly but does not
    block the initial-load producer (the connection can still be manually
    provisioned via the legacy Helm-values path as a fallback).
    """
    if not _ENABLED:
        log.debug("committer_provisioner: auto-provisioning disabled "
                  "(COMMITTER_AUTO_PROVISION_ENABLED=false)")
        return {"provisioned": False, "reason": "auto-provisioning disabled"}

    k8s, _ = _load_k8s()
    if k8s is None:
        log.warning("committer_provisioner: kubernetes library/credentials unavailable — "
                    "skipping auto-provisioning for connection=%s table=%s "
                    "(fall back to the manual helm committer.targets workflow)",
                    connection_id, table)
        return {"provisioned": False, "reason": "kubernetes client unavailable"}

    release_name = os.environ.get("RELEASE_NAME")
    transform_worker_image = os.environ.get("TRANSFORM_WORKER_IMAGE")
    if not release_name or not transform_worker_image:
        log.warning("committer_provisioner: RELEASE_NAME/TRANSFORM_WORKER_IMAGE env vars "
                    "not set — skipping auto-provisioning for connection=%s table=%s",
                    connection_id, table)
        return {"provisioned": False, "reason": "RELEASE_NAME/TRANSFORM_WORKER_IMAGE not set"}

    namespace = _current_namespace()
    committer_name = _committer_name(connection_id, table, release_name)
    labels = _labels(committer_name, connection_id, table)
    drain_batch = resolve_drain_batch(resource_limits, k, rows_estimated_total)
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    # Bug (found via live test): this used to GUESS the catalog's readiness
    # endpoint from a Helm release-name convention
    # (f"{release_name}-nessie...") — wrong whenever the actual catalog
    # service isn't deployed under that exact naming scheme (confirmed live:
    # this cluster's real Nessie service is just "nessie", not
    # "fusion-cdc-nessie" — a second instance of chart/release naming drift,
    # beyond the ServiceAccount one already found). Derive the real
    # host:port straight from the destination's OWN configured catalog URI
    # instead — the same value the committer itself will use to connect —
    # so the probe can never point somewhere different from the real catalog.
    _catalog_readiness_url = _catalog_readiness_check_url(catalog_config or {})
    # Prefer an explicit env (public image-only chart SA names omit the
    # `-sa` suffix) and fall back to the private-chart convention.
    service_account_name = os.environ.get(
        "COMMITTER_SERVICE_ACCOUNT",
        f"{release_name}-cdc-worker-sa",
    )
    secret_name = f"{committer_name}-catalog"

    try:
        apps_v1 = k8s.AppsV1Api()
        core_v1 = k8s.CoreV1Api()

        # 1. Secret (destination connection_config, so the committer can
        # build a PyIceberg Catalog without an operator manually supplying it).
        secret_body = k8s.V1Secret(
            metadata=k8s.V1ObjectMeta(name=secret_name, namespace=namespace, labels=labels),
            string_data={"ICEBERG_CATALOG_CONFIG": json.dumps(catalog_config or {})},
        )
        _apply_secret(core_v1, namespace, secret_name, secret_body)

        # 2. Deployment
        env = [
            k8s.V1EnvVar(name="WORKER_ID", value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(field_path="metadata.name"))),
            k8s.V1EnvVar(name="REDIS_URL", value=redis_url),
            k8s.V1EnvVar(name="ICEBERG_COMMITTER_LOCK_TTL_S", value=str(_LOCK_TTL_S)),
            k8s.V1EnvVar(name="ICEBERG_COMMITTER_DRAIN_BATCH", value=str(drain_batch)),
            k8s.V1EnvVar(name="ICEBERG_COMMITTER_DRAIN_TIMEOUT_MS", value=str(_DRAIN_TIMEOUT_MS)),
            k8s.V1EnvVar(name="ICEBERG_COMMITTER_IDLE_SLEEP_S", value=str(_IDLE_SLEEP_S)),
            k8s.V1EnvVar(name="ICEBERG_CATALOG_CONFIG", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(name=secret_name, key="ICEBERG_CATALOG_CONFIG"))),
        ]
        args = [
            "--connection-id", str(connection_id),
            "--table", str(table),
            "--namespace", str(dest_namespace or "fusion"),
            "--redis-url", redis_url,
            "--drain-batch", str(drain_batch),
        ]
        lag_check = (
            "import os, sys, redis\n"
            "r = redis.from_url(os.environ['REDIS_URL'])\n"
            f"k = 'fusion:iceberg-pending-files:{connection_id}:{table}'\n"
            "lag = r.llen(k) if r.exists(k) else 0\n"
            "sys.exit(0 if lag < {threshold} else 1)\n"
        )
        startup_probe = None
        if _catalog_readiness_url:
            catalog_check = (
                "import urllib.request\n"
                f"urllib.request.urlopen({_catalog_readiness_url!r}, timeout=2).read()\n"
            )
            startup_probe = k8s.V1Probe(
                _exec=k8s.V1ExecAction(command=["python", "-c", catalog_check]),
                failure_threshold=30, period_seconds=5, timeout_seconds=3,
            )
        container = k8s.V1Container(
            name="iceberg-committer",
            image=transform_worker_image,
            image_pull_policy="IfNotPresent",
            command=["python", "-u", "iceberg_committer.py"],
            args=args,
            env=env,
            startup_probe=startup_probe,
            liveness_probe=k8s.V1Probe(
                _exec=k8s.V1ExecAction(command=["python", "-c", lag_check.format(threshold=_LIVENESS_LAG_THRESHOLD)]),
                initial_delay_seconds=120, period_seconds=30, failure_threshold=5, timeout_seconds=10,
            ),
            readiness_probe=k8s.V1Probe(
                _exec=k8s.V1ExecAction(command=["python", "-c", lag_check.format(threshold=_READINESS_LAG_THRESHOLD)]),
                initial_delay_seconds=30, period_seconds=15, failure_threshold=3, timeout_seconds=10,
            ),
            resources=k8s.V1ResourceRequirements(
                requests={"cpu": _CPU_REQUEST, "memory": _MEM_REQUEST},
                limits={"cpu": _CPU_LIMIT, "memory": _MEM_LIMIT},
            ),
            volume_mounts=[k8s.V1VolumeMount(name="tmp", mount_path="/tmp")],
        )
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account_name,
            automount_service_account_token=False,
            termination_grace_period_seconds=60,
            containers=[container],
            volumes=[k8s.V1Volume(name="tmp", empty_dir=k8s.V1EmptyDirVolumeSource())],
        )
        deployment_body = k8s.V1Deployment(
            metadata=k8s.V1ObjectMeta(name=committer_name, namespace=namespace, labels=labels),
            spec=k8s.V1DeploymentSpec(
                replicas=1,
                selector=k8s.V1LabelSelector(match_labels={"app.kubernetes.io/name": committer_name}),
                strategy=k8s.V1DeploymentStrategy(type="Recreate"),
                template=k8s.V1PodTemplateSpec(
                    metadata=k8s.V1ObjectMeta(labels=labels, annotations={
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": str(_METRICS_PORT),
                        "prometheus.io/path": "/metrics",
                    }),
                    spec=pod_spec,
                ),
            ),
        )
        action = _apply_deployment(apps_v1, namespace, committer_name, deployment_body)

        # 3. Headless Service (metrics scraping)
        service_body = k8s.V1Service(
            metadata=k8s.V1ObjectMeta(name=f"{committer_name}-headless", namespace=namespace, labels=labels),
            spec=k8s.V1ServiceSpec(
                cluster_ip="None",
                selector={"app.kubernetes.io/name": committer_name},
                ports=[k8s.V1ServicePort(name="metrics", port=_METRICS_PORT, target_port=_METRICS_PORT)],
            ),
        )
        _apply_service(core_v1, namespace, f"{committer_name}-headless", service_body)

        log.info("committer_provisioner: %s committer=%s connection=%s table=%s drain_batch=%d",
                  action, committer_name, connection_id, table, drain_batch)
        return {"provisioned": True, "action": action, "committer_name": committer_name,
                "drain_batch": drain_batch}
    except Exception:
        log.exception("committer_provisioner: failed to provision committer for "
                      "connection=%s table=%s — connection will still enqueue tasks, "
                      "but no committer may be running to drain them",
                      connection_id, table)
        return {"provisioned": False, "reason": "provisioning error (see logs)"}


def _apply_secret(core_v1, namespace: str, name: str, body) -> str:
    try:
        core_v1.read_namespaced_secret(name, namespace)
        core_v1.replace_namespaced_secret(name, namespace, body)
        return "updated"
    except Exception:
        core_v1.create_namespaced_secret(namespace, body)
        return "created"


def _apply_deployment(apps_v1, namespace: str, name: str, body) -> str:
    try:
        apps_v1.read_namespaced_deployment(name, namespace)
        apps_v1.patch_namespaced_deployment(name, namespace, body)
        return "updated"
    except Exception:
        apps_v1.create_namespaced_deployment(namespace, body)
        return "created"


def _apply_service(core_v1, namespace: str, name: str, body) -> str:
    try:
        core_v1.read_namespaced_service(name, namespace)
        # Services are mostly immutable (clusterIP etc.) — nothing here
        # changes across re-provisioning calls, so leave an existing one alone.
        return "unchanged"
    except Exception:
        core_v1.create_namespaced_service(namespace, body)
        return "created"


def teardown_committer(connection_id: str, table: str) -> dict:
    """Delete the committer Deployment/Secret/Service for this
    (connection_id, table) pair. Called on connection deletion. Never raises.
    """
    if not _ENABLED:
        return {"deprovisioned": False, "reason": "auto-provisioning disabled"}
    k8s, _ = _load_k8s()
    if k8s is None:
        return {"deprovisioned": False, "reason": "kubernetes client unavailable"}
    release_name = os.environ.get("RELEASE_NAME")
    if not release_name:
        return {"deprovisioned": False, "reason": "RELEASE_NAME not set"}
    namespace = _current_namespace()
    committer_name = _committer_name(connection_id, table, release_name)
    removed = []
    try:
        apps_v1 = k8s.AppsV1Api()
        core_v1 = k8s.CoreV1Api()
        for kind, api, name in (
            ("deployment", apps_v1.delete_namespaced_deployment, committer_name),
            ("secret", core_v1.delete_namespaced_secret, f"{committer_name}-catalog"),
            ("service", core_v1.delete_namespaced_service, f"{committer_name}-headless"),
        ):
            try:
                api(name, namespace)
                removed.append(f"{kind}/{name}")
            except Exception:
                pass  # already gone — fine
        log.info("committer_provisioner: torn down committer=%s connection=%s table=%s (%s)",
                  committer_name, connection_id, table, ", ".join(removed) or "nothing to remove")
        return {"deprovisioned": True, "removed": removed}
    except Exception:
        log.exception("committer_provisioner: failed to tear down committer for "
                      "connection=%s table=%s", connection_id, table)
        return {"deprovisioned": False, "reason": "teardown error (see logs)"}
