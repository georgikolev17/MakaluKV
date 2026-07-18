package storage

import (
	"errors"
)

type SwapResult uint8

const (
	SwapResultUnspecified SwapResult = iota
	SwapResultSwapped
	SwapResultKeyNotFound
	SwapResultValueMismatch
)

var ErrEmptyKey = errors.New("storage: key cannot be empty")

type Store interface {
	// Get retrieves the value for a given key.
	Get(key []byte) (value []byte, found bool, err error)

	// Put sets the value for a given key.
	Put(key []byte, value []byte) error

	// Delete removes the value for a given key.
	Delete(key []byte) error

	// CompareAndSwap atomically swaps the value for a given key if the current value matches the expected value.
	CompareAndSwap(key []byte, expectedValue []byte, newValue []byte) (result SwapResult, err error)
}
