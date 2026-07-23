"""v1.2.31 regression test: verify _atomic_dequeue's blmove/brpoplpush calls
match the real redis-py signature. Catches the v1.2.30 bug where blmove was
called with the wrong argument order (timeout bound to src positionally),
causing TypeError: got multiple values for argument 'timeout' on every pod.
"""
import inspect
import redis


def test_blmove_signature_high_queue_call_binds():
    """The HIGH_QUEUE blmove call in _atomic_dequeue must bind to the real
    redis.Redis.blmove signature without raising TypeError."""
    sig = inspect.signature(redis.Redis.blmove)
    # Mirror the exact call from worker.py _atomic_dequeue (HIGH_QUEUE branch)
    HIGH_QUEUE = "fusion:transforms:high"
    IN_FLIGHT_QUEUE = "fusion:transforms:in-flight"
    sig.bind(HIGH_QUEUE, IN_FLIGHT_QUEUE, timeout=1, src="RIGHT", dest="LEFT")


def test_blmove_signature_normal_queue_call_binds():
    """The NORMAL_QUEUE blmove call in _atomic_dequeue must bind to the real
    redis.Redis.blmove signature without raising TypeError."""
    sig = inspect.signature(redis.Redis.blmove)
    NORMAL_QUEUE = "fusion:transforms:normal"
    IN_FLIGHT_QUEUE = "fusion:transforms:in-flight"
    timeout = 5
    sig.bind(NORMAL_QUEUE, IN_FLIGHT_QUEUE, timeout=max(1, timeout - 1), src="RIGHT", dest="LEFT")


def test_blmove_timeout_is_third_positional_param():
    """Guard against future redis-py upgrades silently changing the signature.
    Documents the expected argument order so a future maintainer doesn't
    reintroduce the v1.2.30 bug."""
    sig = inspect.signature(redis.Redis.blmove)
    params = list(sig.parameters.keys())
    # Expected: self, first_list, second_list, timeout, src, dest
    assert params[0] == "self"
    assert params[1] == "first_list"
    assert params[2] == "second_list"
    assert params[3] == "timeout", (
        f"redis-py blmove signature changed: param 3 is now {params[3]!r}, "
        f"expected 'timeout'. The v1.2.31 fix assumes timeout is the 3rd "
        f"positional param. Update _atomic_dequeue if this changes."
    )


def test_brpoplpush_fallback_signature_binds():
    """The brpoplpush fallback (for Redis <6.2) must also bind correctly."""
    sig = inspect.signature(redis.Redis.brpoplpush)
    HIGH_QUEUE = "fusion:transforms:high"
    IN_FLIGHT_QUEUE = "fusion:transforms:in-flight"
    sig.bind(HIGH_QUEUE, IN_FLIGHT_QUEUE, timeout=1)
