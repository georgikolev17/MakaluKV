package service

import (
	"github.com/georgikolev17/MakaluKV/internal/storage/statemachine"
)

// Service contains the client-facing application logic.
type Service struct {
	store statemachine.Store
}

// NewService creates a new service with the given state machine store
func NewService(store statemachine.Store) *Service {
	return &Service{store: store}
}

// Get returns the value for the given key
func (s *Service) Get(key []byte) ([]byte, bool, error) {
	if err := validateKeyLength(key); err != nil {
		return nil, false, err
	}

	return s.store.Get(key)
}

// Put replaces the old with the given value for the given key
func (s *Service) Put(key []byte, value []byte) error {
	if err := validateKeyLength(key); err != nil {
		return err
	}
	if err := validateValueLength(value); err != nil {
		return err
	}

	return s.store.Put(key, value)
}

// Delete deletes the value for the given key
func (s *Service) Delete(key []byte) error {
	if err := validateKeyLength(key); err != nil {
		return err
	}

	return s.store.Delete(key)
}

// CompareAndSwap compares the old value to the given expected value for the given key and replaces it with the new value, if they match
func (s *Service) CompareAndSwap(key []byte, expectedValue []byte, newValue []byte) (statemachine.SwapResult, error) {
	if err := validateKeyLength(key); err != nil {
		return statemachine.SwapResultUnspecified, err
	}
	if err := validateValueLength(expectedValue); err != nil {
		return statemachine.SwapResultUnspecified, err
	}
	if err := validateValueLength(newValue); err != nil {
		return statemachine.SwapResultUnspecified, err
	}

	return s.store.CompareAndSwap(key, expectedValue, newValue)
}
