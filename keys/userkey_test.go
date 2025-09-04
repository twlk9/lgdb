package keys

import (
	"testing"
)

func TestUserKey_Compare(t *testing.T) {
	uk1 := UserKey("aaa")
	uk2 := UserKey("bbb")
	uk3 := UserKey("aaa")

	if uk1.Compare(uk2) >= 0 {
		t.Errorf("Expected uk1 < uk2")
	}

	if uk2.Compare(uk1) <= 0 {
		t.Errorf("Expected uk2 > uk1")
	}

	if uk1.Compare(uk3) != 0 {
		t.Errorf("Expected uk1 == uk3")
	}
}

func TestNewUserKey(t *testing.T) {
	original := []byte("test_key")
	uk := UserKey(original)

	if string(uk) != "test_key" {
		t.Errorf("Expected 'test_key', got %s", string(uk))
	}

	// Test that it's the same underlying bytes
	if len(uk) != len(original) {
		t.Errorf("Length mismatch")
	}
}
