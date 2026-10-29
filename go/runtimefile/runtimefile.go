// Package runtimefile reads standalone extractor/egress configuration files.
// The versioned wire format matches News without importing its application modules.
// It never mutates the process environment.
package runtimefile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const Version = 1
const maxSize = 64 << 10

// Manifest is private state. Values must never be logged or returned to a client.
type Manifest struct {
	Version        int               `json:"version"`
	Service        string            `json:"service"`
	InstallationID string            `json:"installation_id"`
	Values         map[string]string `json:"values"`
}

// Load reads and validates a manifest for exactly one service. Only keys in
// allowed may occur, and all required keys must be nonempty. Errors never
// contain file contents or values. Callers must not fall back to environment
// values after opting into managed mode if this function fails.
func Load(path, service string, allowed, required []string) (Manifest, error) {
	var zero Manifest
	if path == "" || service == "" {
		return zero, errors.New("runtime manifest path and service are required")
	}
	f, err := os.Open(path)
	if err != nil {
		return zero, errors.New("runtime manifest unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return zero, errors.New("runtime manifest must be a private regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSize+1))
	if err != nil || len(data) > maxSize {
		return zero, errors.New("runtime manifest unreadable or too large")
	}
	return Decode(data, service, allowed, required)
}

// Decode applies the same validation as Load to a manifest already in memory.
func Decode(data []byte, service string, allowed, required []string) (Manifest, error) {
	var zero Manifest
	if len(data) > maxSize || service == "" {
		return zero, errors.New("invalid runtime manifest")
	}
	if err := rejectDuplicates(data); err != nil {
		return zero, errors.New("invalid runtime manifest JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return zero, errors.New("invalid runtime manifest JSON")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return zero, errors.New("invalid runtime manifest JSON")
	}
	if m.Version != Version || m.Service != service || strings.TrimSpace(m.InstallationID) == "" || m.Values == nil {
		return zero, errors.New("invalid runtime manifest identity")
	}
	allow := make(map[string]bool, len(allowed))
	for _, k := range allowed {
		allow[k] = true
	}
	for k := range m.Values {
		if !allow[k] {
			return zero, errors.New("runtime manifest contains a disallowed key")
		}
	}
	for _, k := range required {
		if !allow[k] {
			return zero, errors.New("invalid runtime manifest policy")
		}
		if strings.TrimSpace(m.Values[k]) == "" {
			return zero, fmt.Errorf("runtime manifest missing required key %s", k)
		}
	}
	return m, nil
}

// rejectDuplicates also checks the complete JSON stream, avoiding last-key-wins
// interpretation differences between provisioning and runtime readers.
func rejectDuplicates(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	var value func() error
	value = func() error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return errors.New("duplicate")
				}
				seen[key] = true
				if err := value(); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("object")
			}
		case '[':
			for d.More() {
				if err := value(); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("array")
			}
		default:
			return errors.New("delimiter")
		}
		return nil
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing")
	}
	return nil
}
