"""Resource Config model — admission-control resource pool sizing.

Captures the ONE-TIME admin setup ("first login") of the compute pool that
fusion-cdc's control-plane, cdc-workers, transform-workers and
iceberg-committers draw from: total CPU/memory min-max, instance
type/count, and whether the pool is "dedicated" to fusion-cdc or a "shared"
slice of a bigger cluster.

Scoping: this codebase's existing multi-tenancy pattern (see
``app.models.base.MultiTenancyMixin`` and ``Connection``/``AlertRule``/etc.)
scopes tenant-owned rows by ``bank_id`` (top-level tenant; null only for
super-admin/global rows) with an optional ``sub_tenant_id`` for a bank's
sub-tenants. A compute pool is provisioned per-bank (each bank gets its own
K8s node pool / instance fleet), not per-sub-tenant, so ``ResourceConfig`` is
a **bank-scoped singleton**: one row per ``bank_id`` (enforced by a unique
constraint), sharing the pool across all of that bank's sub-tenants and
connections. This mirrors how ``committer_provisioner.py`` and the CDC
baseline reservations are already tracked per-connection rather than
per-sub-tenant elsewhere in this admission-control feature.

``resource_configured`` is false until the admin completes the one-time
setup; a later frontend gate (not part of this change) blocks every other
screen until it flips to true.
"""
from sqlalchemy import Column, String, Integer, Boolean, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from app.models.base import BaseModel, TimestampMixin, MultiTenancyMixin


class ResourceConfig(BaseModel, TimestampMixin, MultiTenancyMixin):
    """Bank-scoped singleton describing the total compute pool available for
    admission-control decisions (see ``app.services.resource_ledger`` and
    ``app.services.resource_admission``)."""

    __tablename__ = "resource_configs"

    resource_config_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )

    # Total pool sizing. CPU stored in millicores, memory in MiB — matches
    # the units already used by committer_provisioner.py's
    # _CPU_REQUEST/_MEM_REQUEST/_CPU_LIMIT/_MEM_LIMIT constants (e.g.
    # "250m" / "512Mi"), stored here as plain integers for cheap arithmetic
    # in the Redis ledger's Lua script (no k8s-quantity string parsing at
    # reservation time).
    total_cpu_min_millis = Column(Integer, nullable=False)
    total_cpu_max_millis = Column(Integer, nullable=False)
    total_memory_min_mi = Column(Integer, nullable=False)
    total_memory_max_mi = Column(Integer, nullable=False)

    # Fleet description (informational — not used for capacity math today).
    instance_type = Column(String(100), nullable=False)
    instance_count = Column(Integer, nullable=False)

    # "dedicated" -> pool belongs entirely to fusion-cdc; the ledger trusts
    # total_cpu_max_millis/total_memory_max_mi as-is.
    # "shared" -> pool is a slice of a bigger cluster; a later phase will
    # reconcile against live K8s state instead of trusting the static max.
    # Just captured/stored for now, per the product spec.
    pool_scope = Column(String(20), nullable=False, server_default=text("'dedicated'::character varying"))

    resource_configured = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        # One resource pool config per bank. bank_id is nullable (global /
        # single-tenant deployments use NULL) — Postgres treats multiple
        # NULLs as distinct under a unique constraint, so the API layer
        # additionally guards single-row-per-bank with a get-before-insert
        # check for the bank_id IS NULL case.
        UniqueConstraint("bank_id", name="uq_resource_config_bank_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<ResourceConfig(bank_id={self.bank_id}, "
            f"pool_scope={self.pool_scope}, configured={self.resource_configured})>"
        )
