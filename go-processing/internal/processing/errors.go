package processing

import (
	"errors"
	"fmt"
)

var ErrResourceLimit = errors.New("resource_limit")

type FatalProcessingError struct {
	Code string
}

func (e FatalProcessingError) Error() string {
	return fmt.Sprintf("article processing rejected: %s", e.Code)
}

func fatal(code string) error { return FatalProcessingError{Code: code} }

func isFatal(err error) bool {
	var target FatalProcessingError
	return errors.As(err, &target)
}
