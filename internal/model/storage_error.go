package model

import "errors"

type StorageError struct {
	Err       error
	Retryable bool
}

func (e *StorageError) Error() string {
	return e.Err.Error()
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

func IsRetryable(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Retryable
}
