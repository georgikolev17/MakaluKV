package service

import "errors"

const (
	MinKeyLength   = 1
	MaxKeyLength   = 1024
	MaxValueLength = 1 << 20
)

var ErrInvalidKeyLength = errors.New(
	"service: key length must be between 1 and 1024 bytes",
)

var ErrInvalidValueLength = errors.New(
	"service: value length must not exceed 1MiB",
)

func validateKeyLength(key []byte) error {
	if len(key) < MinKeyLength || len(key) > MaxKeyLength {
		return ErrInvalidKeyLength
	}
	return nil
}

func validateValueLength(value []byte) error {
	if len(value) > MaxValueLength {
		return ErrInvalidValueLength
	}
	return nil
}
