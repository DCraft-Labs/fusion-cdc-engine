"""
Fernet symmetric encryption for database credentials stored at rest.

The ENCRYPTION_KEY setting is an arbitrary string; we derive a valid 32-byte
Fernet key from it via SHA-256 so operators aren't forced to pre-base64-encode
a 32-byte value.

Backward compatibility:
  - Ciphertext that starts with "encrypted_" (the old stub format) is still
    accepted by decrypt_secret() and the raw value is returned.
"""

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from app.config import settings


def _get_fernet() -> Fernet:
    """Derive a stable Fernet key from the configured ENCRYPTION_KEY."""
    key_bytes = hashlib.sha256(settings.ENCRYPTION_KEY.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key_bytes))


def encrypt_secret(plaintext: str) -> str:
    """Return Fernet-encrypted ciphertext (URL-safe base64 string)."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    """
    Decrypt a Fernet ciphertext.

    Raises:
        cryptography.fernet.InvalidToken: if the ciphertext is corrupt or was
            encrypted with a different key.
    """
    # Backward-compat: legacy stub values stored as "encrypted_<plaintext>"
    if ciphertext.startswith("encrypted_"):
        return ciphertext[len("encrypted_"):]
    return _get_fernet().decrypt(ciphertext.encode()).decode()
