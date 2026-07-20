"""
Spec §2 (P2-3): Redis-based leader election for the control-plane scheduler.

Only one control-plane pod should run the background scheduler at a time.
Leader election uses a Redis key with a TTL; the current leader renews it on
every heartbeat. If the leader pod dies, the key expires and a standby pod
acquires leadership on its next iteration.

Usage example (in main.py lifespan):
    from app.utils.leader_election import RedisLeaderElection
    import redis.asyncio as aioredis

    redis_client = aioredis.from_url(settings.REDIS_URL)
    election = RedisLeaderElection(redis_client)

    async def scheduler_loop():
        while True:
            if await election.is_leader():
                await run_scheduler_tick()
            await asyncio.sleep(10)
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid

log = logging.getLogger(__name__)

# Default key & TTL constants
_DEFAULT_KEY = "fusion:scheduler:leader"
_DEFAULT_TTL = 30   # seconds — must be > scheduler tick interval
_DEFAULT_RENEWAL_INTERVAL = 10  # seconds


def _instance_id() -> str:
    """Stable-but-unique identifier for this pod / process."""
    return os.environ.get("POD_NAME") or f"{socket.gethostname()}-{os.getpid()}"


class RedisLeaderElection:
    """
    Distributed leader election backed by a single Redis key.

    The algorithm:
    1. ``try_acquire()`` — SET key value NX EX ttl (atomic)
       Returns True if this instance acquired the lock.
    2. ``renew()`` — If already leader, extend the TTL (SET key value XX EX ttl).
       Returns True if still leader after renewal.
    3. ``release()`` — DEL key only when the stored value matches this instance.
    4. ``is_leader()`` — Combine try_acquire + renew in one call; safe to call
       on every tick.
    """

    def __init__(
        self,
        redis,            # redis.asyncio.Redis or redis.Redis instance
        key: str = _DEFAULT_KEY,
        ttl: int = _DEFAULT_TTL,
        instance_id: str | None = None,
    ) -> None:
        self._redis = redis
        self._key = key
        self._ttl = ttl
        self._instance_id = instance_id or _instance_id()
        self._is_leader = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def try_acquire(self) -> bool:
        """
        Attempt to acquire leadership.
        Uses SET NX EX — guaranteed atomic in Redis.
        """
        result = await self._redis.set(
            self._key,
            self._instance_id,
            nx=True,      # Only set if key does NOT exist
            ex=self._ttl,
        )
        if result:
            self._is_leader = True
            log.info("Leader election: %s acquired leadership (key=%s)", self._instance_id, self._key)
        return bool(result)

    async def renew(self) -> bool:
        """
        Renew the leader TTL.  Only succeeds if the stored value still matches
        this instance (uses a Lua script for atomicity).
        """
        _lua_renew = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('expire', KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = await self._redis.eval(_lua_renew, 1, self._key, self._instance_id, self._ttl)
        if not result:
            self._is_leader = False
            log.warning("Leader election: %s lost leadership (key=%s)", self._instance_id, self._key)
        return bool(result)

    async def release(self) -> None:
        """
        Release leadership.  Only deletes the key if the stored value is ours
        (Lua script to avoid race conditions).
        """
        _lua_release = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        await self._redis.eval(_lua_release, 1, self._key, self._instance_id)
        self._is_leader = False
        log.info("Leader election: %s released leadership (key=%s)", self._instance_id, self._key)

    async def is_leader(self) -> bool:
        """
        Convenience: try to acquire if not currently leader, or renew if we are.
        Safe to call on every scheduler tick.
        """
        if self._is_leader:
            return await self.renew()
        return await self.try_acquire()

    # ------------------------------------------------------------------
    # Background renewal task (optional)
    # ------------------------------------------------------------------

    async def start_renewal_task(self, interval: int = _DEFAULT_RENEWAL_INTERVAL) -> asyncio.Task:
        """
        Spawn a background asyncio.Task that continuously renews leadership.
        Use this if the scheduler tick is longer than the TTL.
        Cancel the task when the application shuts down.
        """
        async def _renew_loop():
            while True:
                await asyncio.sleep(interval)
                if self._is_leader:
                    await self.renew()

        task = asyncio.create_task(_renew_loop(), name="leader-election-renewal")
        return task
