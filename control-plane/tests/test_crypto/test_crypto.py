"""
Unit tests for app/utils/crypto.py (P1.15 — Fernet encryption).

No database or external services required — pure unit tests.
"""

import os
import pytest
from cryptography.fernet import InvalidToken

# Ensure settings pick up a deterministic key before the module is imported
os.environ.setdefault("ENCRYPTION_KEY", "test-encryption-key-for-unit-tests-only")


class TestEncryptSecret:
    """Tests for encrypt_secret()"""

    def test_encrypt_returns_non_empty_string(self):
        from app.utils.crypto import encrypt_secret

        ciphertext = encrypt_secret("super_secret_password")
        assert isinstance(ciphertext, str)
        assert len(ciphertext) > 0

    def test_encrypt_does_not_return_plaintext(self):
        from app.utils.crypto import encrypt_secret

        plaintext = "my_database_password"
        ciphertext = encrypt_secret(plaintext)
        assert plaintext not in ciphertext

    def test_encrypt_produces_different_ciphertext_each_call(self):
        """Fernet uses a random IV, so the same plaintext gives different ciphertexts."""
        from app.utils.crypto import encrypt_secret

        ct1 = encrypt_secret("same_password")
        ct2 = encrypt_secret("same_password")
        assert ct1 != ct2


class TestDecryptSecret:
    """Tests for decrypt_secret()"""

    def test_roundtrip_encrypt_decrypt(self):
        from app.utils.crypto import encrypt_secret, decrypt_secret

        plaintext = "correct_horse_battery_staple"
        assert decrypt_secret(encrypt_secret(plaintext)) == plaintext

    def test_decrypt_empty_string_roundtrip(self):
        from app.utils.crypto import encrypt_secret, decrypt_secret

        assert decrypt_secret(encrypt_secret("")) == ""

    def test_decrypt_legacy_stub_format(self):
        """Values stored as 'encrypted_<plaintext>' by the old stub are readable."""
        from app.utils.crypto import decrypt_secret

        legacy = "encrypted_my_old_password"
        assert decrypt_secret(legacy) == "my_old_password"

    def test_decrypt_invalid_ciphertext_raises(self):
        from app.utils.crypto import decrypt_secret

        with pytest.raises(InvalidToken):
            decrypt_secret("not_a_valid_fernet_token")


class TestKeyDerivation:
    """Tests for the deterministic key derivation."""

    def test_same_key_decrypts_across_instances(self):
        """Two calls to _get_fernet() with the same ENCRYPTION_KEY must produce
        the same key (otherwise the stored ciphertext becomes unreadable after restart)."""
        from app.utils.crypto import _get_fernet

        f1 = _get_fernet()
        f2 = _get_fernet()
        plaintext = "consistency_check"
        ct = f1.encrypt(plaintext.encode())
        assert f2.decrypt(ct).decode() == plaintext
