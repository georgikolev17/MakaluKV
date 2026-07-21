package service

import (
	"github.com/georgikolev17/MakaluKV/internal/storage"
)

type Service struct {
	store storage.Store
}

func NewService(store storage.Store) *Service {
	return &Service{store: store}
}

func (s *Service) Get(key []byte) ([]byte, bool, error) {
	if err := validateKeyLength(key); err != nil {
		return nil, false, err
	}

	return s.store.Get(key)
}

func (s *Service) Put(key []byte, value []byte) error {
	if err := validateKeyLength(key); err != nil {
		return err
	}
	if err := validateValueLength(value); err != nil {
		return err
	}

	return s.store.Put(key, value)
}

func (s *Service) Delete(key []byte) error {
	if err := validateKeyLength(key); err != nil {
		return err
	}

	return s.store.Delete(key)
}

func (s *Service) CompareAndSwap(key []byte, expectedValue []byte, newValue []byte) (storage.SwapResult, error) {
	if err := validateKeyLength(key); err != nil {
		return storage.SwapResultUnspecified, err
	}
	if err := validateValueLength(expectedValue); err != nil {
		return storage.SwapResultUnspecified, err
	}
	if err := validateValueLength(newValue); err != nil {
		return storage.SwapResultUnspecified, err
	}

	return s.store.CompareAndSwap(key, expectedValue, newValue)
}
