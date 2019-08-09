package check

import (
	"errors"
)

// Key returns an error if k == ""
func Key(k string) error {
	if k == "" {
		return errors.New("the provided key is an empty string")
	}
	return nil
}

// KeyAndValue returns an error if k == "" or if v == nil
func KeyAndValue(k string, v interface{}) error {
	if err := Key(k); err != nil {
		return err
	}
	if v == nil {
		return errors.New("the provided value is nil")
	}
	return nil
}
