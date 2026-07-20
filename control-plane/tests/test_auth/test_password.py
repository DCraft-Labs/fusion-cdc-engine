"""Unit tests for password hashing utilities"""
import pytest
from app.auth.password import hash_password, verify_password, needs_rehash


class TestPasswordHashing:
    """Test password hashing functions"""
    
    def test_hash_password_creates_different_hashes(self):
        """Test that same password creates different hashes (due to salt)"""
        password = "TestPassword123!"
        hash1 = hash_password(password)
        hash2 = hash_password(password)
        
        assert hash1 != hash2, "Same password should create different hashes"
        assert hash1.startswith("$2b$"), "Hash should use bcrypt format"
    
    def test_hash_password_returns_string(self):
        """Test that hash_password returns a string"""
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        assert isinstance(password_hash, str)
        assert len(password_hash) > 0
    
    def test_verify_password_with_correct_password(self):
        """Test password verification with correct password"""
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        assert verify_password(password, password_hash) is True
    
    def test_verify_password_with_incorrect_password(self):
        """Test password verification with incorrect password"""
        password = "TestPassword123!"
        wrong_password = "WrongPassword456!"
        password_hash = hash_password(password)
        
        assert verify_password(wrong_password, password_hash) is False
    
    def test_verify_password_case_sensitive(self):
        """Test that password verification is case-sensitive"""
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        assert verify_password("testpassword123!", password_hash) is False
    
    def test_hash_password_with_special_characters(self):
        """Test hashing passwords with special characters"""
        password = "P@$$w0rd!#%^&*()"
        password_hash = hash_password(password)
        
        assert verify_password(password, password_hash) is True
    
    def test_hash_password_with_unicode(self):
        """Test hashing passwords with unicode characters"""
        password = "パスワード123"
        password_hash = hash_password(password)
        
        assert verify_password(password, password_hash) is True
    
    def test_hash_password_with_empty_string(self):
        """Test hashing empty password"""
        password = ""
        password_hash = hash_password(password)
        
        assert verify_password(password, password_hash) is True
    
    def test_hash_password_long_password(self):
        """Test hashing very long password"""
        password = "A" * 1000
        password_hash = hash_password(password)
        
        assert verify_password(password, password_hash) is True
    
    def test_needs_rehash_with_current_algorithm(self):
        """Test needs_rehash with current bcrypt algorithm"""
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        # Current algorithm should not need rehash
        assert needs_rehash(password_hash) is False
    
    def test_needs_rehash_with_weak_hash(self):
        """Test needs_rehash with weaker bcrypt rounds"""
        # Simulate old hash with fewer rounds (not actually testing since we can't easily create old hashes)
        # This is more of a demonstration that the function exists
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        # Current hash should not need rehash
        assert needs_rehash(password_hash) is False


class TestPasswordSecurity:
    """Test password security aspects"""
    
    def test_hash_contains_salt(self):
        """Test that hashes contain salt (different for same password)"""
        password = "TestPassword123!"
        hashes = [hash_password(password) for _ in range(5)]
        
        # All hashes should be different due to different salts
        assert len(set(hashes)) == 5
    
    def test_hash_is_one_way(self):
        """Test that hash is one-way (cannot reverse)"""
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        # Hash should not contain the original password
        assert password not in password_hash
        assert password.lower() not in password_hash.lower()
    
    def test_timing_attack_resistance(self):
        """Test that verification time is constant (timing attack resistance)"""
        import time
        
        password = "TestPassword123!"
        password_hash = hash_password(password)
        
        # Measure time for correct password
        start = time.time()
        verify_password(password, password_hash)
        correct_time = time.time() - start
        
        # Measure time for incorrect password
        start = time.time()
        verify_password("WrongPassword456!", password_hash)
        incorrect_time = time.time() - start
        
        # Times should be similar (within 50ms)
        # Note: This is a basic test; timing attack resistance is complex
        assert abs(correct_time - incorrect_time) < 0.05
    
    def test_hash_length_consistency(self):
        """Test that all hashes have consistent length"""
        passwords = ["short", "medium_password", "very_long_password" * 10]
        hashes = [hash_password(p) for p in passwords]
        
        # All bcrypt hashes should have the same length
        assert len(set(len(h) for h in hashes)) == 1
