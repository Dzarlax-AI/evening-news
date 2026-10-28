// Package envbool reads the legacy settings boolean spellings.
package envbool

import (
	"errors"
	"os"
	"strings"
)

func Read(name string, defaultValue bool) (bool, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on", "y", "t":
		return true, nil
	case "0", "false", "no", "off", "n", "f":
		return false, nil
	default:
		return false, errors.New("invalid boolean configuration")
	}
}
