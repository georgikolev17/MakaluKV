package statemachine

import (
	"bytes"
	"errors"
	"testing"
)

func TestPebbleStorePutAndGet(t *testing.T) {
	store := openTestStore(t)

	key := []byte("name")
	expectedValue := []byte("MakaluKV")

	if err := store.Put(key, expectedValue); err != nil {
		t.Fatalf("failed to put value: %v", err)
	}

	value, found, err := store.Get(key)
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}
	if !found {
		t.Fatalf("value not found for key: %s", key)
	}
	if !bytes.Equal(value, expectedValue) {
		t.Errorf("expected value %s, got %s", expectedValue, value)
	}
}

func TestPebbleStoreGetMissingKey(t *testing.T) {
	store := openTestStore(t)

	key := []byte("missing")
	value, found, err := store.Get(key)
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}
	if found {
		t.Errorf("unexpected value found for key: %s", key)
	}
	if len(value) > 0 {
		t.Errorf("expected empty value for missing key: %s", key)
	}
}

func TestPebbleStoreDelete(t *testing.T) {
	store := openTestStore(t)
	key := []byte("name")
	value := []byte("MakaluKV")

	if err := store.Put(key, value); err != nil {
		t.Fatalf("failed to put value: %v", err)
	}

	if err := store.Delete(key); err != nil {
		t.Fatalf("failed to delete value: %v", err)
	}

	value, found, err := store.Get(key)
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}
	if found {
		t.Errorf("unexpected value found for key after deletion: %s", key)
	}
	if len(value) > 0 {
		t.Errorf("expected empty value for deleted key: %s", key)
	}
}

func TestPebbleStoreCompareAndSwap(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store := openTestStore(t)

		store.Put([]byte("name"), []byte("MakaluKV"))

		result, err := store.CompareAndSwap([]byte("name"), []byte("MakaluKV"), []byte("NewName"))

		if err != nil {
			t.Fatalf("failed to compare and swap: %v", err)
		}
		if result != SwapResultSwapped {
			t.Errorf("expected SwapResultSwapped, got %v", result)
		}

		value, found, err := store.Get([]byte("name"))
		if err != nil {
			t.Fatalf("failed to get value after compare and swap: %v", err)
		}
		if !found {
			t.Fatalf("value not found after compare and swap")
		}
		if !bytes.Equal(value, []byte("NewName")) {
			t.Errorf("expected value 'NewName', got %s", value)
		}
	})

	t.Run("value mismatch", func(t *testing.T) {
		store := openTestStore(t)

		store.Put([]byte("name"), []byte("MakaluKV"))

		result, err := store.CompareAndSwap([]byte("name"), []byte("Makalu"), []byte("NewName"))

		if err != nil {
			t.Fatalf("failed to compare and swap: %v", err)
		}
		if result != SwapResultValueMismatch {
			t.Errorf("expected values to mismatch but they match, got: %v", result)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		store := openTestStore(t)

		result, err := store.CompareAndSwap([]byte("name"), []byte("Makalu"), []byte("NewName"))

		if err != nil {
			t.Fatalf("failed to compare and swap: %v", err)
		}
		if result != SwapResultKeyNotFound {
			t.Errorf("expected SwapResultKeyNotFound, got: %v", result)
		}
	})
}

func TestPebbleStoreEmptyKey(t *testing.T) {
	t.Run("put empty key", func(t *testing.T) {
		store := openTestStore(t)
		if err := store.Put([]byte(""), []byte("value")); !errors.Is(err, ErrEmptyKey) {
			t.Errorf("expected ErrEmptyKey, got: %v", err)
		}
	})

	t.Run("get empty key", func(t *testing.T) {
		store := openTestStore(t)
		_, _, err := store.Get([]byte(""))
		if !errors.Is(err, ErrEmptyKey) {
			t.Errorf("expected ErrEmptyKey, got: %v", err)
		}
	})

	t.Run("delete empty key", func(t *testing.T) {
		store := openTestStore(t)
		if err := store.Delete([]byte("")); !errors.Is(err, ErrEmptyKey) {
			t.Errorf("expected ErrEmptyKey, got: %v", err)
		}
	})

	t.Run("compare and swap empty key", func(t *testing.T) {
		store := openTestStore(t)
		result, err := store.CompareAndSwap([]byte(""), []byte("expected"), []byte("new"))
		if !errors.Is(err, ErrEmptyKey) {
			t.Errorf("expected ErrEmptyKey, got: %v", err)
		}
		if result != SwapResultUnspecified {
			t.Errorf("expected SwapResultKeyNotFound, got: %v", result)
		}
	})
}

func openTestStore(t *testing.T) *PebbleStore {
	t.Helper()

	store, err := OpenPebbleStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open Pebble store: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("failed to close Pebble store: %v", err)
		}
	})

	return store
}
