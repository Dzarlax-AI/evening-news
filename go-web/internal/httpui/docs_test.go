package httpui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/schedules"
)

type inventoryReader struct{}

func (inventoryReader) Feed(context.Context, repositories.FeedQuery) (repositories.FeedResponse, error) {
	return repositories.FeedResponse{Articles: []map[string]any{}}, nil
}
func (inventoryReader) Article(context.Context, int64, bool) (map[string]any, error) {
	return map[string]any{"id": 1}, nil
}
func (inventoryReader) CategoriesConfig(context.Context) (map[string]any, error) {
	return map[string]any{}, nil
}
func (inventoryReader) Categories(context.Context, repositories.StatsQuery) (map[string]any, error) {
	return map[string]any{"categories": []any{}}, nil
}
func (inventoryReader) Sources(context.Context, repositories.StatsQuery) (map[string]any, error) {
	return map[string]any{"sources": []any{}}, nil
}
func (inventoryReader) Search(context.Context, repositories.SearchQuery) (map[string]any, error) {
	return map[string]any{"articles": []any{}}, nil
}
func (inventoryReader) Digest(context.Context, time.Time) (repositories.DigestPage, error) {
	return repositories.DigestPage{}, nil
}
func (inventoryReader) APIFeed(context.Context, repositories.APIFeedQuery) (repositories.APIFeedResponse, error) {
	return repositories.APIFeedResponse{}, nil
}
func (inventoryReader) RSS(context.Context, int) ([]repositories.RSSArticle, error) { return nil, nil }

type routeInventory struct {
	RegisteredRoutes []struct {
		Path    string   `json:"path"`
		Methods []string `json:"methods"`
	} `json:"registered_routes"`
}

// Keep this empty by default: every frozen baseline method must resolve to a
// concrete Go ServeMux pattern. Any approved gap must have a method/path key and
// a short reason here, and should be removed once its route is implemented.
var routeCoverageAllowlist = map[string]string{}

func assembleInventoryMux(t *testing.T) (*http.ServeMux, *fakeBackendOps) {
	t.Helper()
	service := auth.New(auth.Config{Username: "admin", Password: "test-only", JWTSecret: "test-only-signing-key", Development: true})
	ui := New(Config{Reader: inventoryReader{}, PublicAdmin: func(*http.Request) bool { return false }, TemplatesDir: "../../templates", StaticDir: "../../../web/static", StartedAt: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)})
	mux := http.NewServeMux()
	ui.Register(mux)
	service.Register(mux)
	RegisterAdmin(mux, service, ui)
	// The management service is nil because probes are anonymous and must stop
	// at its real auth middleware before any command/query adapter can run.
	RegisterManagement(mux, ManagementConfig{Service: (*management.Service)(nil), Auth: service, ScheduledJobsReceiptLimit: 100})
	backendOps := &fakeBackendOps{raw: json.RawMessage(`{}`)}
	RegisterBackend(mux, service, backendOps)
	schedules.NewHandler(nil).Register(mux, service)
	if err := RegisterDocs(mux, "../../contracts/openapi.json"); err != nil {
		t.Fatal(err)
	}
	return mux, backendOps
}

var routeParameter = regexp.MustCompile(`\{([^}]+)\}`)

func samplePath(pattern string) string {
	return routeParameter.ReplaceAllStringFunc(pattern, func(match string) string {
		name := strings.Trim(match, "{}")
		switch strings.ToLower(name) {
		case "date":
			return "2026-10-26"
		case "filename":
			return "fixture.tar.gz"
		default:
			return "1"
		}
	})
}

func normalizeMuxPattern(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	pattern = strings.ReplaceAll(pattern, "/{$}", "/")
	return routeParameter.ReplaceAllString(pattern, "{}")
}

// This inventory test reads the frozen FastAPI route registration, dispatches
// each path/method through the assembled Go mux, and records ServeMux's actual
// match. It uses anonymous requests and isolated fakes, so protected mutation
// handlers stop at auth and no provider, filesystem, database, or job runs.
func TestFrozenRouteInventoryMatchesAssembledMux(t *testing.T) {
	data, err := os.ReadFile("../../../tests/fixtures/web-contract-v1/registered-routes-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture routeInventory
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	methods := 0
	for _, route := range fixture.RegisteredRoutes {
		methods += len(route.Methods)
	}
	if len(fixture.RegisteredRoutes) != 95 || methods != 110 {
		t.Fatalf("frozen registration inventory changed: path records=%d methods=%d", len(fixture.RegisteredRoutes), methods)
	}
	mux, backendOps := assembleInventoryMux(t)
	var missing []string
	for _, route := range fixture.RegisteredRoutes {
		for _, method := range route.Methods {
			t.Run(method+" "+route.Path, func(t *testing.T) {
				path := samplePath(route.Path)
				req := httptest.NewRequest(method, path, nil)
				if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
					req.Header.Set("Content-Type", "application/json")
				}
				resp := httptest.NewRecorder()
				mux.ServeHTTP(resp, req)
				matched := normalizeMuxPattern(req.Pattern)
				want := normalizeMuxPattern(route.Path)
				if method == http.MethodHead {
					// Go's ServeMux correctly serves HEAD through the matching GET route.
					matchedMethod := req.Pattern
					if !strings.HasPrefix(matchedMethod, "GET ") {
						t.Errorf("HEAD request did not match a GET handler: pattern=%q status=%d", req.Pattern, resp.Code)
					}
				}
				if matched != want {
					key := method + " " + route.Path
					if reason, ok := routeCoverageAllowlist[key]; ok && strings.TrimSpace(reason) != "" {
						t.Logf("explicit route migration allowance: %s: %s", key, reason)
						return
					}
					missing = append(missing, key+" (matched "+req.Pattern+")")
					t.Errorf("no matching registered Go route: pattern=%q status=%d", req.Pattern, resp.Code)
				}
			})
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		t.Errorf("unregistered baseline routes:\n%s", strings.Join(missing, "\n"))
	}
	if backendOps.calls != 0 {
		t.Errorf("anonymous route inventory probe reached private backend %d times", backendOps.calls)
	}
}

func TestDocsRoutesServeFrozenOpenAPIWithoutRemoteAssets(t *testing.T) {
	mux := http.NewServeMux()
	if err := RegisterDocs(mux, "../../contracts/openapi.json"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/docs", "/redoc", "/docs/oauth2-redirect"} {
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
		if resp.Code != http.StatusOK || !strings.HasPrefix(resp.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("GET %s: status=%d content-type=%q", path, resp.Code, resp.Header().Get("Content-Type"))
		}
		if strings.Contains(strings.ToLower(resp.Body.String()), "cdn.") || strings.Contains(resp.Body.String(), "swagger-ui-bundle.js") {
			t.Fatalf("GET %s references a remote docs asset", path)
		}
	}
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if resp.Code != http.StatusOK || !json.Valid(resp.Body.Bytes()) {
		t.Fatalf("GET /openapi.json: status=%d body=%q", resp.Code, resp.Body.String())
	}
	var served map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &served); err != nil {
		t.Fatal(err)
	}
	if len(served["paths"].(map[string]any)) != 104 {
		t.Fatalf("unexpected frozen schema path count: %d", len(served["paths"].(map[string]any)))
	}
}

func TestCheckedInOpenAPIPreservesBaselineAndAddsAISettings(t *testing.T) {
	fixtureBytes, err := os.ReadFile("../../../tests/fixtures/web-contract-v1/registered-routes-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	contractBytes, err := os.ReadFile("../../contracts/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		OpenAPI json.RawMessage `json:"openapi"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal(fixture.OpenAPI, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(contractBytes, &got); err != nil {
		t.Fatal(err)
	}
	gotRoot := got.(map[string]any)
	paths := gotRoot["paths"].(map[string]any)
	components := gotRoot["components"].(map[string]any)
	schemas := components["schemas"].(map[string]any)
	for _, name := range []string{"OperationalSettings", "OperationalSettingsUpdate", "ViberConfiguration"} {
		if schemas[name] == nil {
			t.Fatalf("missing operational settings schema %s", name)
		}
		delete(schemas, name)
	}
	ai, ok := paths["/api/v1/ai/settings"].(map[string]any)
	if !ok || ai["get"] == nil || ai["put"] == nil {
		t.Fatal("AI settings GET and PUT must be documented")
	}
	put := ai["put"].(map[string]any)
	if put["requestBody"] == nil || put["responses"].(map[string]any)["409"] == nil || put["responses"].(map[string]any)["422"] == nil {
		t.Fatal("AI settings update contract is incomplete")
	}
	delete(paths, "/api/v1/ai/settings")
	for path, methods := range map[string][]string{"/api/v1/configuration/readiness": {"get"}, "/api/v1/configuration/telegram": {"get", "put"}, "/api/v1/configuration/viber": {"get", "put"}, "/api/v1/viber/verify": {"post"}, "/api/v1/viber/connect": {"post"}, "/api/v1/viber/test": {"post"}, "/api/v1/viber/preview": {"get"}, "/integrations/viber/webhook": {"post"}, "/api/v1/configuration/operational": {"get", "put"}, "/api/v1/setup/status": {"get"}, "/api/v1/setup/claim": {"post"}, "/api/v1/installation/progress": {"get", "put"}, "/api/v1/credentials/{provider}": {"get", "put", "delete"}} {
		item, ok := paths[path].(map[string]any)
		if !ok {
			t.Fatalf("missing onboarding path %s", path)
		}
		for _, method := range methods {
			if item[method] == nil {
				t.Fatalf("missing %s %s", method, path)
			}
		}
		delete(paths, path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("contracts/openapi.json differs from the frozen Python OpenAPI snapshot")
	}
	for _, disallowed := range []string{"test-only-signing-key", "fixture_admin", "fixture-token"} {
		if strings.Contains(string(contractBytes), disallowed) {
			t.Fatalf("OpenAPI contract contains a test credential marker: %s", disallowed)
		}
	}
}

func TestDocumentationShellLocalesAndFrozenData(t *testing.T) {
	mux := http.NewServeMux()
	if err := RegisterDocs(mux, "../../contracts/openapi.json"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/docs", "/redoc", "/docs/oauth2-redirect"} {
		ru := request(mux, "GET", path)
		sr := request(mux, "GET", path+"?locale=sr-Latn")
		if ru.Code != 200 || !strings.Contains(ru.Body.String(), `lang="ru"`) || sr.Code != 200 || !strings.Contains(sr.Body.String(), `lang="sr-Latn"`) {
			t.Fatalf("%s locales: ru=%d sr=%d", path, ru.Code, sr.Code)
		}
		if path != "/docs/oauth2-redirect" && !strings.Contains(sr.Body.String(), `href="/static/css/dzarlax.css"`) {
			t.Fatal("language switcher design-system styles missing")
		}
		if path != "/docs/oauth2-redirect" && (!strings.Contains(ru.Body.String(), "Операции") || !strings.Contains(sr.Body.String(), "Operacije") || ru.Header().Get("ETag") == sr.Header().Get("ETag")) {
			t.Fatalf("%s localized shell or cache mismatch", path)
		}
		if w := request(mux, "GET", path+"?locale=unknown"); w.Code != http.StatusNotFound {
			t.Fatalf("%s accepted unknown locale", path)
		}
	}
	raw := request(mux, "GET", "/openapi.json")
	fixture, err := os.ReadFile("../../contracts/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Body.Bytes(), fixture) {
		t.Fatal("frozen API contract bytes changed")
	}
}
