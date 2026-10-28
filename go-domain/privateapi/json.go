// Package privateapi implements the private domain HTTP contract.
package privateapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

var ErrInvalidJSON = errors.New("invalid JSON")

// DecodeJSON rejects duplicate keys at every nesting depth, invalid UTF-8,
// non-standard numbers, trailing values and unknown struct fields. Callers
// must still validate required fields, types and resource limits.
func DecodeJSON(data []byte, out any) error {
	if !utf8.Valid(data) || !validEscapedSurrogates(data) {
		return ErrInvalidJSON
	}
	scan := json.NewDecoder(bytes.NewReader(data))
	scan.UseNumber()
	if err := scanValue(scan, 0); err != nil {
		return ErrInvalidJSON
	}
	if _, err := scan.Token(); err != io.EOF {
		return ErrInvalidJSON
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return ErrInvalidJSON
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return ErrInvalidJSON
	}
	return nil
}

// encoding/json otherwise silently replaces lone UTF-16 surrogates with U+FFFD,
// changing callback fingerprints and identities supplied by a caller.
func validEscapedSurrogates(data []byte) bool {
	inString := false
	for i := 0; i < len(data); i++ {
		if !inString {
			if data[i] == '"' {
				inString = true
			}
			continue
		}
		if data[i] == '"' {
			inString = false
			continue
		}
		if data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
func scanValue(dec *json.Decoder, depth int) error {
	if depth > 128 {
		return ErrInvalidJSON
	}
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			token, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return ErrInvalidJSON
			}
			if _, ok := seen[key]; ok {
				return ErrInvalidJSON
			}
			seen[key] = struct{}{}
			if err = scanValue(dec, depth+1); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidJSON
		}
	case '[':
		for dec.More() {
			if err := scanValue(dec, depth+1); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidJSON
		}
	default:
		return ErrInvalidJSON
	}
	return nil
}
