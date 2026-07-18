package storage

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

func OpenPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db}, nil
}

type PebbleStore struct {
	db    *pebble.DB
	mutex sync.Mutex
}

// Get retrieves the value for a given key.
func (s *PebbleStore) Get(key []byte) (value []byte, found bool, err error) {
	if len(key) == 0 {
		return nil, false, ErrEmptyKey
	}
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	copyValue := bytes.Clone(value)
	if err := closer.Close(); err != nil {
		return nil, false, err
	}
	return copyValue, true, nil
}

// Put sets the value for a given key.
func (s *PebbleStore) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Set(key, value, pebble.Sync)
}

// Delete removes the value for a given key.
func (s *PebbleStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Delete(key, pebble.Sync)
}

// CompareAndSwap atomically swaps the value for a given key if the current value matches the expected value.
func (s *PebbleStore) CompareAndSwap(key []byte, expectedValue []byte, newValue []byte) (result SwapResult, err error) {
	if len(key) == 0 {
		return SwapResultUnspecified, ErrEmptyKey
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return SwapResultKeyNotFound, nil
		}
		return SwapResultUnspecified, err
	}

	matches := bytes.Equal(value, expectedValue)

	if err := closer.Close(); err != nil {
		return SwapResultUnspecified, err
	}

	if !matches {
		return SwapResultValueMismatch, nil
	}

	err = s.db.Set(key, newValue, pebble.Sync)
	if err != nil {
		return SwapResultUnspecified, err
	}

	return SwapResultSwapped, nil
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}
