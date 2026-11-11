package jsonsingle

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// DecodeObject decodes an already bounded object whose fields are all required.
// Unlike encoding/json, it rejects duplicate keys, nulls and case aliases.
func DecodeObject(raw []byte, dst any, fields ...string) error {
	invalid := errors.New("invalid JSON object")
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return invalid
	}
	wanted := make(map[string]bool, len(fields))
	for _, field := range fields {
		wanted[field] = false
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return invalid
		}
		key, ok := token.(string)
		seen, known := wanted[key]
		if !ok || !known || seen {
			return invalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return invalid
		}
		wanted[key] = true
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return invalid
	}
	if decoder.Decode(new(any)) != io.EOF {
		return invalid
	}
	for _, seen := range wanted {
		if !seen {
			return invalid
		}
	}
	return json.Unmarshal(raw, dst)
}
