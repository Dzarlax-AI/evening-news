package transport

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSchemaNormalization(t *testing.T) {
	raw := json.RawMessage(`{"type":"OBJECT","properties":{"required":{"type":"STRING"},"optional":{"type":"ARRAY","items":{"type":"INTEGER"}}},"required":["required"],"additionalProperties":true}`)
	s, err := Schema(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	if s["additionalProperties"] != false {
		t.Fatal("object not closed")
	}
	required := s["required"].([]string)
	if len(required) != 2 || required[0] != "optional" {
		t.Fatalf("required: %v", required)
	}
	p := s["properties"].(map[string]any)
	if p["required"].(map[string]any)["type"] != "string" {
		t.Fatal("incorrect required type")
	}
	optional := p["optional"].(map[string]any)
	if optional["type"].([]any)[1] != "null" {
		t.Fatal("optional field must be nullable")
	}
	if optional["items"].(map[string]any)["type"] != "integer" {
		t.Fatal("uppercase nested type")
	}
	for _, raw := range []string{`{"type":"object","properties":{},"$ref":"x"}`, `{"type":"array"}`, `null`, strings.Repeat("x", MaxSchema+1)} {
		if _, err := Schema(json.RawMessage(raw), true); err == nil {
			t.Fatalf("accepted invalid schema %q", raw[:min(len(raw), 50)])
		}
	}
}

func TestNestedAnyOfHealthShape(t *testing.T) {
	raw := json.RawMessage(`{"type":"object","properties":{"insight":{"anyOf":[{"type":"null"},{"type":"object","properties":{"text":{"type":"string"},"fact_ids":{"type":"array","items":{"type":"string"}},"stance":{"type":"string","enum":["agree","qualify","disagree"]}},"required":["text","fact_ids","stance"],"additionalProperties":false}]}},"required":["insight"],"additionalProperties":false}`)
	for _, strict := range []bool{false, true} {
		s, err := Schema(raw, strict)
		if err != nil {
			t.Fatal(err)
		}
		p := s["properties"].(map[string]any)["insight"].(map[string]any)
		a := p["anyOf"].([]any)
		if len(a) != 2 || a[0].(map[string]any)["type"] != "null" {
			t.Fatalf("lost alternatives: %+v", a)
		}
		obj := a[1].(map[string]any)
		if obj["additionalProperties"] != false {
			t.Fatal("open object")
		}
		if strict && len(obj["required"].([]string)) != 3 {
			t.Fatal("lost required")
		}
	}
	bad := []string{`{"anyOf":[{"type":"object","properties":{}},{"type":"null"}]}`, `{"type":"object","properties":{"a":{"type":"number","minimum":2}}}`, `{"type":"object","properties":{"a":{"anyOf":[{"type":"string"},{"type":"null"}],"minLength":2}}}`, `{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}}}`}
	for _, schema := range bad {
		if _, err := Schema(json.RawMessage(schema), true); err == nil {
			t.Fatalf("accepted %s", schema)
		}
	}
}
