// Package runtimeenv creates a private Compose environment candidate.
package runtimeenv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type field struct {
	Key   string
	Value json.RawMessage
}

func fields(raw json.RawMessage) ([]field, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	t, e := d.Token()
	if e != nil || t != json.Delim('{') {
		return nil, errors.New("invalid_object")
	}
	var out []field
	for d.More() {
		k, e := d.Token()
		if e != nil {
			return nil, e
		}
		var v json.RawMessage
		if e = d.Decode(&v); e != nil {
			return nil, e
		}
		out = append(out, field{k.(string), v})
	}
	_, e = d.Token()
	return out, e
}
func lookup(fs []field, k string) json.RawMessage {
	for _, f := range fs {
		if f.Key == k {
			return f.Value
		}
	}
	return nil
}

var projectPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
var keyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var rolePattern = regexp.MustCompile(`[^A-Za-z0-9]+`)

func role(s string) string {
	n := strings.ToUpper(strings.Trim(rolePattern.ReplaceAllString(s, "_"), "_"))
	switch n {
	case "APP", "WEB":
		return "WEB"
	case "EXECUTOR", "MIGRATE":
		return "EXECUTOR"
	}
	return ""
}
func Quote(s string) (string, error) {
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			continue
		}
		j := i
		for j < len(s) && s[j] == '\\' {
			j++
		}
		if (j-i)%2 == 1 && (j == len(s) || s[j] == '\'') {
			return "", errors.New("unsupported_environment_value")
		}
		i = j - 1
	}
	return "'" + strings.ReplaceAll(s, "'", `\'`) + "'", nil
}

type ServiceAudit struct {
	Count   int               `json:"environment_key_count"`
	Mapping map[string]string `json:"key_mapping"`
}
type Audit struct {
	Format        string                  `json:"format"`
	Project       string                  `json:"project"`
	Count         int                     `json:"service_count"`
	Services      map[string]ServiceAudit `json:"services"`
	References    map[string][]any        `json:"env_file_references"`
	Values        bool                    `json:"secret_values_written_to_audit"`
	Hashes        bool                    `json:"secret_hashes_written_to_audit"`
	SourceCompose string                  `json:"source_compose,omitempty"`
	SourceEnv     *string                 `json:"source_interpolation_file,omitempty"`
}

func Prepare(raw []byte, project string) (map[string]any, string, Audit, error) {
	var audit Audit
	fail := func(s string) (map[string]any, string, Audit, error) { return nil, "", audit, errors.New(s) }
	if !projectPattern.MatchString(project) {
		return fail("invalid_project_name")
	}
	top, e := fields(raw)
	if e != nil {
		return fail("invalid_compose_config")
	}
	services, e := fields(lookup(top, "services"))
	if e != nil || len(services) == 0 {
		return fail("invalid_compose_config")
	}
	var candidate map[string]any
	if json.Unmarshal(raw, &candidate) != nil {
		return fail("invalid_compose_config")
	}
	updated, ok := candidate["services"].(map[string]any)
	if !ok {
		return fail("invalid_compose_config")
	}
	audit = Audit{Format: "evening-news-runtime-env-audit-v1", Project: project, Count: len(services), Services: map[string]ServiceAudit{}, References: map[string][]any{}}
	lines := []string{"# Generated privately by prepare_runtime_env.py"}
	canonicalValues := map[string]string{}
	emitted := map[string]bool{}
	for _, s := range services {
		config, e := fields(s.Value)
		if e != nil {
			return fail("invalid_service_config")
		}
		envRaw := lookup(config, "environment")
		var env []field
		if len(envRaw) > 0 && string(envRaw) != "null" {
			env, e = fields(envRaw)
			if e != nil {
				return fail("unsupported_environment_shape")
			}
		}
		if len(env) > 0 {
			if len(lines) > 1 {
				lines = append(lines, "")
			}
			lines = append(lines, "# "+s.Key+" responsibility")
		}
		mapped := map[string]string{}
		mapping := map[string]string{}
		local := map[string]bool{}
		for _, v := range env {
			if !keyPattern.MatchString(v.Key) {
				return fail("invalid_environment_key")
			}
			var val string
			if json.Unmarshal(v.Value, &val) != nil || string(v.Value) == "null" {
				return fail("unresolved_environment_value")
			}
			key := v.Key
			if key == "DATABASE_URL" && role(s.Key) != "" {
				key = role(s.Key) + "_DATABASE_URL"
			}
			val = strings.ReplaceAll(val, "$$", "$")
			if old, exists := canonicalValues[key]; exists && old != val {
				return fail("canonical_key_collision")
			}
			if local[key] {
				return fail("canonical_key_collision")
			}
			local[key] = true
			canonicalValues[key] = val
			mapping[v.Key] = key
			mapped[v.Key] = "${" + key + "?Required by generated runtime environment}"
			if !emitted[key] {
				q, e := Quote(val)
				if e != nil {
					return fail(e.Error())
				}
				lines = append(lines, key+"="+q)
				emitted[key] = true
			}
		}
		u, ok := updated[s.Key].(map[string]any)
		if !ok {
			return fail("invalid_service_config")
		}
		delete(u, "env_file")
		u["environment"] = mapped
		audit.Services[s.Key] = ServiceAudit{len(mapped), mapping}
		if v := lookup(config, "env_file"); len(v) > 0 && string(v) != "null" {
			var arr []any
			if json.Unmarshal(v, &arr) != nil {
				return fail("invalid_service_config")
			}
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					item = m["path"]
				}
				audit.References[s.Key] = append(audit.References[s.Key], item)
			}
		}
	}
	return candidate, strings.Join(lines, "\n") + "\n", audit, nil
}
func Error(code string) error { return fmt.Errorf("%s", code) }
