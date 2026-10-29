package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5/pgxpool"
)

type expectedGrant struct {
	privilege string
	kind      string
	object    string
	columns   []string
}

var grantStatementPattern = regexp.MustCompile(`(?is)^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+(.+?)\s*$`)
var columnGrantPattern = regexp.MustCompile(`(?is)^([A-Z]+)\s*\((.*)\)$`)

var externalPolicyRole = map[string]string{
	"news_web_runtime": "web", "news_domain_runtime": "domain", "news_worker_runtime": "worker",
	"news_execution_runtime": "execution", "news_ingestion_runtime": "ingestion", "news_processing_runtime": "processing",
}

// verifyExternalGrantPolicy checks the complete checked-in runtime GRANT policy
// through each configured login. Fence-owner grants are intentionally omitted:
// those roles are NOLOGIN owners used only by SECURITY DEFINER functions.
func verifyExternalGrantPolicy(ctx context.Context, input externalInput) error {
	expected, err := parseExternalGrantPolicy(managedsetup.GrantPolicy)
	if err != nil {
		return errors.New("managed runtime grant policy is unsupported")
	}
	pools := make(map[string]*pgxpool.Pool, len(input.RuntimeDSNs))
	defer func() {
		for _, pool := range pools {
			pool.Close()
		}
	}()
	for _, role := range externalPolicyRole {
		pool, err := poolForDSN(ctx, input.RuntimeDSNs[role])
		if err != nil {
			return fmt.Errorf("%s runtime grants are unavailable", role)
		}
		pools[role] = pool
	}
	for role, grants := range expected {
		pool := pools[role]
		for _, grant := range grants {
			allowed, checkErr := hasExpectedGrant(ctx, pool, grant)
			if checkErr != nil || !allowed {
				return fmt.Errorf("%s runtime grants are incomplete; install reviewed least-privilege grants before activation", externalPolicyRole[role])
			}
		}
	}
	return nil
}

func parseExternalGrantPolicy(policy string) (map[string][]expectedGrant, error) {
	clean := make([]string, 0, 8)
	for _, line := range strings.Split(policy, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		clean = append(clean, line)
	}
	statements := strings.Split(strings.Join(clean, "\n"), ";")
	out := map[string][]expectedGrant{}
	seenRuntimeRoles := map[string]bool{}
	for _, raw := range statements {
		s := strings.TrimSpace(raw)
		if s == "" || !strings.HasPrefix(strings.ToUpper(s), "GRANT ") {
			continue
		}
		m := grantStatementPattern.FindStringSubmatch(s)
		if m == nil {
			return nil, fmt.Errorf("unsupported grant statement")
		}
		privs := splitSQLList(m[1])
		objects := splitSQLList(m[2])
		if len(privs) == 0 || len(objects) == 0 {
			return nil, errors.New("empty grant")
		}
		kind, _ := grantObjectKind(objects[0])
		for _, object := range objects[1:] {
			upper := strings.ToUpper(strings.TrimSpace(object))
			explicitKind := false
			for _, candidate := range []string{"TABLE", "SEQUENCE", "FUNCTION", "SCHEMA"} {
				if strings.HasPrefix(upper, candidate+" ") {
					explicitKind = candidate == kind
					break
				}
			}
			if !explicitKind && (strings.HasPrefix(upper, "TABLE ") || strings.HasPrefix(upper, "SEQUENCE ") || strings.HasPrefix(upper, "FUNCTION ") || strings.HasPrefix(upper, "SCHEMA ")) {
				return nil, errors.New("mixed grant object kinds")
			}
		}
		for _, roleName := range splitSQLList(m[3]) {
			roleName = strings.Trim(strings.TrimSpace(roleName), `"`)
			role, runtime := externalPolicyRole[roleName]
			if !runtime {
				continue
			}
			seenRuntimeRoles[role] = true
			for _, privilegeText := range privs {
				privilegeText = strings.TrimSpace(privilegeText)
				privilege := strings.ToUpper(privilegeText)
				columns := []string(nil)
				if cm := columnGrantPattern.FindStringSubmatch(privilegeText); cm != nil {
					privilege = strings.ToUpper(cm[1])
					columns = splitSQLList(cm[2])
				}
				for _, object := range objects {
					k, name := grantObjectKind(object)
					if k == "TABLE" && kind != "TABLE" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(object)), "TABLE ") {
						k = kind
					}
					if strings.HasPrefix(strings.ToLower(name), "news.") {
						name = name[len("news."):]
					} else if k != "SCHEMA" {
						return nil, errors.New("grant object outside news schema")
					}
					if k == "FUNCTION" {
						name = strings.TrimSpace(name)
					} else {
						name = strings.Trim(strings.TrimSpace(name), `"`)
					}
					out[role] = append(out[role], expectedGrant{privilege: privilege, kind: k, object: name, columns: columns})
				}
			}
		}
	}
	for _, role := range []string{"web", "domain", "worker", "execution", "ingestion", "processing"} {
		if !seenRuntimeRoles[role] {
			return nil, fmt.Errorf("runtime role missing from policy")
		}
	}
	return out, nil
}

func grantObjectKind(text string) (kind, object string) {
	text = strings.TrimSpace(text)
	upper := strings.ToUpper(text)
	for _, candidate := range []string{"TABLE", "SEQUENCE", "FUNCTION", "SCHEMA"} {
		prefix := candidate + " "
		if strings.HasPrefix(upper, prefix) {
			return candidate, strings.TrimSpace(text[len(prefix):])
		}
	}
	return "TABLE", text
}

func splitSQLList(text string) []string {
	var result []string
	depth := 0
	quoted := false
	start := 0
	for i, r := range text {
		switch r {
		case '"':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted && depth > 0 {
				depth--
			}
		case ',':
			if !quoted && depth == 0 {
				if value := strings.TrimSpace(text[start:i]); value != "" {
					result = append(result, value)
				}
				start = i + 1
			}
		}
	}
	if value := strings.TrimSpace(text[start:]); value != "" {
		result = append(result, value)
	}
	return result
}

func hasExpectedGrant(ctx context.Context, p *pgxpool.Pool, grant expectedGrant) (bool, error) {
	switch grant.kind {
	case "SCHEMA":
		var ok bool
		err := p.QueryRow(ctx, `SELECT has_schema_privilege(current_user,$1,$2)`, grant.object, grant.privilege).Scan(&ok)
		return ok, err
	case "SEQUENCE":
		var ok bool
		err := p.QueryRow(ctx, `SELECT has_sequence_privilege(current_user,$1,$2)`, "news."+grant.object, grant.privilege).Scan(&ok)
		return ok, err
	case "FUNCTION":
		var ok bool
		err := p.QueryRow(ctx, `SELECT has_function_privilege(current_user,to_regprocedure($1),$2)`, "news."+grant.object, grant.privilege).Scan(&ok)
		return ok, err
	case "TABLE":
		if len(grant.columns) == 0 {
			var ok bool
			err := p.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,$2)`, "news."+grant.object, grant.privilege).Scan(&ok)
			return ok, err
		}
		for _, column := range grant.columns {
			var ok bool
			err := p.QueryRow(ctx, `SELECT has_column_privilege(current_user,$1,$2,$3)`, "news."+grant.object, strings.Trim(strings.TrimSpace(column), `"`), grant.privilege).Scan(&ok)
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	default:
		return false, errors.New("unsupported grant object")
	}
}
