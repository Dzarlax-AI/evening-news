package transport

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

const MaxSchema = 120_000

// Schema converts a deliberately bounded JSON Schema subset, rejecting constructs we cannot preserve.
func Schema(raw json.RawMessage, strict bool) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > MaxSchema {
		return nil, errors.New("schema too large")
	}
	var root map[string]any
	if json.Unmarshal(raw, &root) != nil || root == nil {
		return nil, errors.New("invalid schema")
	}
	count := 0
	if err := normalize(root, strict, 0, &count); err != nil {
		return nil, err
	}
	if strict && root["type"] != "object" {
		return nil, errors.New("root must be object")
	}
	return root, nil
}
func normalize(m map[string]any, strict bool, depth int, count *int) error {
	*count++
	if depth > 10 || *count > 5000 {
		return errors.New("schema limit")
	}
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "items": true, "enum": true, "description": true, "nullable": true, "additionalProperties": true, "anyOf": true}
	for k := range m {
		if !allowed[k] {
			return errors.New("unsupported schema keyword")
		}
	}
	if d, ok := m["description"]; ok {
		if _, ok := d.(string); !ok {
			return errors.New("invalid description")
		}
	}
	if alternatives, ok := m["anyOf"]; ok {
		if depth == 0 && strict {
			return errors.New("root must be object")
		}
		for k := range m {
			if k != "anyOf" && k != "description" {
				return errors.New("unsupported anyOf combination")
			}
		}
		a, ok := alternatives.([]any)
		if !ok || len(a) < 2 || len(a) > 500 {
			return errors.New("invalid anyOf")
		}
		for _, v := range a {
			child, ok := v.(map[string]any)
			if !ok {
				return errors.New("invalid anyOf variant")
			}
			if err := normalize(child, strict, depth+1, count); err != nil {
				return err
			}
		}
		return nil
	}
	if v, ok := m["type"].(string); ok {
		m["type"] = strings.ToLower(v)
	} else if a, ok := m["type"].([]any); ok {
		for i, v := range a {
			s, ok := v.(string)
			if !ok {
				return errors.New("invalid type")
			}
			a[i] = strings.ToLower(s)
		}
	} else {
		return errors.New("missing type")
	}
	nullable, hasNullable := m["nullable"].(bool)
	if _, exists := m["nullable"]; exists && !hasNullable {
		return errors.New("invalid nullable")
	}
	delete(m, "nullable")
	if nullable {
		makeNullable(m)
	}
	typ, _ := m["type"].(string)
	types, _ := m["type"].([]any)
	if types != nil {
		if len(types) != 2 {
			return errors.New("unsupported union")
		}
		nullCount := 0
		for _, v := range types {
			s := v.(string)
			if s == "null" {
				nullCount++
			}
			if s != "null" {
				typ = s
			}
		}
		if nullCount != 1 {
			return errors.New("unsupported union")
		}
	}
	if e, ok := m["enum"]; ok {
		if a, ok := e.([]any); !ok || len(a) == 0 || len(a) > 500 {
			return errors.New("invalid enum")
		}
	}
	switch typ {
	case "object", "array", "string", "number", "integer", "boolean", "null":
	default:
		return errors.New("unsupported type")
	}
	for _, k := range []string{"properties", "required", "additionalProperties"} {
		if _, ok := m[k]; ok && typ != "object" {
			return errors.New("object keyword on non-object")
		}
	}
	if _, ok := m["items"]; ok && typ != "array" {
		return errors.New("items on non-array")
	}
	if typ == "object" {
		p, ok := m["properties"].(map[string]any)
		if !ok {
			return errors.New("object needs properties")
		}
		required := map[string]bool{}
		if r, ok := m["required"]; ok {
			arr, ok := r.([]any)
			if !ok {
				return errors.New("invalid required")
			}
			for _, v := range arr {
				s, ok := v.(string)
				if !ok || p[s] == nil || required[s] {
					return errors.New("invalid required")
				}
				required[s] = true
			}
		}
		if ap, ok := m["additionalProperties"]; ok {
			b, ok := ap.(bool)
			if !ok {
				return errors.New("unsupported additionalProperties")
			}
			if !strict {
				m["additionalProperties"] = b
			}
		}
		keys := make([]string, 0, len(p))
		for k, v := range p {
			child, ok := v.(map[string]any)
			if !ok {
				return errors.New("invalid property")
			}
			if err := normalize(child, strict, depth+1, count); err != nil {
				return err
			}
			if strict && !required[k] {
				makeNullable(child)
			}
			keys = append(keys, k)
		}
		if strict {
			sort.Strings(keys)
			m["required"] = keys
			m["additionalProperties"] = false
		}
	} else if typ == "array" {
		child, ok := m["items"].(map[string]any)
		if !ok {
			return errors.New("array needs items")
		}
		if err := normalize(child, strict, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}
func makeNullable(m map[string]any) {
	if variants, ok := m["anyOf"].([]any); ok {
		for _, v := range variants {
			if child, ok := v.(map[string]any); ok && isNullType(child["type"]) {
				return
			}
		}
		m["anyOf"] = append(variants, map[string]any{"type": "null"})
		return
	}
	if e, ok := m["enum"].([]any); ok {
		found := false
		for _, v := range e {
			if v == nil {
				found = true
			}
		}
		if !found {
			m["enum"] = append(e, nil)
		}
	}
	switch t := m["type"].(type) {
	case string:
		if t != "null" {
			m["type"] = []any{t, "null"}
		}
	case []any:
		for _, x := range t {
			if x == "null" {
				return
			}
		}
		m["type"] = append(t, "null")
	}
}

func isNullType(v any) bool { s, ok := v.(string); return ok && s == "null" }
