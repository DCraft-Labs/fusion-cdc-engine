"""
Password hashing and verification utilities.

Uses the bcrypt library directly (bypasses passlib's wrap-bug detection probe
which is incompatible with bcrypt 3.2+).  bcrypt produces the standard $2b$
Blowfish hash format used by virtually all bcrypt implementations.
"""
import bcrypt

# bcrypt enforces a 72-byte limit on passwords.  We truncate explicitly to
# ensure consistent behaviour and prevent library-version-dependent errors.
_BCRYPT_MAX_BYTES = 72


def hash_password(password: str) -> str:
    """Hash a plain-text password with bcrypt (produces $2b$… format)."""
    secret = password.encode("utf-8")[:_BCRYPT_MAX_BYTES]
    return bcrypt.hashpw(secret, bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain-text password against a stored bcrypt hash."""
    secret = plain_password.encode("utf-8")[:_BCRYPT_MAX_BYTES]
    hashed_bytes = hashed_password.encode("utf-8") if isinstance(hashed_password, str) else hashed_password
    return bcrypt.checkpw(secret, hashed_bytes)


def needs_rehash(hashed_password: str) -> bool:
    """Return True if the hash was made with fewer rounds than the current default."""
    # bcrypt library doesn't have a direct rehash check; we approximate by
    # comparing rounds.  Return False unless rounds are clearly below modern default.
    try:
        rounds = int(hashed_password.split("$")[2])
        return rounds < 12
    except (IndexError, ValueError):
        return True
