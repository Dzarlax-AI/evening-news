package httpui

import (
	"bytes"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"os"
	"sort"
	"strings"
)

type apiDocument struct {
	Info struct {
		Title       string `json:"title"`
		Version     string `json:"version"`
		Description string `json:"description"`
	} `json:"info"`
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		Schemas map[string]json.RawMessage `json:"schemas"`
	} `json:"components"`
}

type docsPage struct {
	Title, Version, Description string
	Operations                  []docsOperation
	Schemas                     []docsSchema
}
type docsOperation struct {
	Path, Method, Summary, OperationID string
	Details                            string
}
type docsSchema struct{ Name, Definition string }

var docsTemplate = template.Must(template.New("docs").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} API</title><style>
:root{color-scheme:light dark;--bg:#fcfaf7;--fg:#18181b;--muted:#666;--line:#dedbd6;--accent:#285c78;--panel:#fff}
@media(prefers-color-scheme:dark){:root{--bg:#171717;--fg:#eee;--muted:#aaa;--line:#444;--accent:#9bcbe1;--panel:#222}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif}
main{max-width:1100px;margin:0 auto;padding:32px 22px 72px}header{border-bottom:1px solid var(--line);padding-bottom:20px;margin-bottom:24px}
h1,h2,h3{font-family:Georgia,serif;line-height:1.2}h1{font-size:2.2rem;margin:0 0 8px}h2{margin-top:34px}a{color:var(--accent)}code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.meta,.muted{color:var(--muted)}.actions{display:flex;gap:18px;flex-wrap:wrap;margin-top:14px}.operation{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:16px;margin:12px 0}
.route{font-size:1.05rem;overflow-wrap:anywhere}.method{display:inline-block;min-width:4.6em;margin-right:10px;padding:2px 7px;border-radius:4px;background:var(--accent);color:var(--bg);font:bold .75rem/1.6 system-ui;text-align:center}
details{margin-top:12px}summary{cursor:pointer;color:var(--accent)}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:var(--bg);border:1px solid var(--line);border-radius:5px;padding:12px;font-size:.84rem}
@media(max-width:600px){main{padding:20px 14px 48px}h1{font-size:1.8rem}}
</style></head><body><main><header><h1>{{.Title}} API</h1><p class="meta">Version {{.Version}}</p><p>{{.Description}}</p><nav class="actions"><a href="#operations">Operations</a><a href="#schemas">Schemas</a><a href="/openapi.json">OpenAPI JSON</a></nav></header>
<section id="operations"><h2>Operations</h2>{{range .Operations}}<article class="operation"><div class="route"><span class="method">{{.Method}}</span><code>{{.Path}}</code></div><p>{{if .Summary}}{{.Summary}}{{else}}<span class="muted">{{.OperationID}}</span>{{end}}</p><details><summary>Request and response details</summary><pre>{{.Details}}</pre></details></article>{{end}}</section>
<section id="schemas"><h2>Data schemas</h2>{{range .Schemas}}<details class="operation"><summary><code>{{.Name}}</code></summary><pre>{{.Definition}}</pre></details>{{end}}</section>
</main></body></html>`))

const oauthRedirectHTML = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Authorization response</title></head><body><p>Authorization response received. You can close this window.</p></body></html>`

// RegisterDocs serves the checked-in, frozen API contract and an offline HTML
// reference. It uses no remote assets, OAuth providers, or test credentials.
func RegisterDocs(mux *http.ServeMux, specPath string) error {
	if specPath == "" {
		specPath = "contracts/openapi.json"
	}
	data, err := os.ReadFile(specPath)
	if err != nil {
		return errors.New("cannot read frozen OpenAPI contract")
	}
	if len(data) == 0 || len(data) > 2<<20 || !json.Valid(data) {
		return errors.New("frozen OpenAPI contract is invalid")
	}
	var spec apiDocument
	if err := json.Unmarshal(data, &spec); err != nil || len(spec.Paths) == 0 || spec.Info.Title == "" {
		return errors.New("frozen OpenAPI contract is incomplete")
	}
	page := docsPage{Title: spec.Info.Title, Version: spec.Info.Version, Description: spec.Info.Description}
	paths := make([]string, 0, len(spec.Paths))
	for path := range spec.Paths {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		methods := make([]string, 0, len(spec.Paths[path]))
		for method := range spec.Paths[path] {
			methods = append(methods, method)
		}
		sort.Strings(methods)
		for _, method := range methods {
			op := map[string]any{}
			if err := json.Unmarshal(spec.Paths[path][method], &op); err != nil {
				return errors.New("frozen OpenAPI operation is invalid")
			}
			details, err := json.MarshalIndent(op, "", "  ")
			if err != nil {
				return errors.New("frozen OpenAPI operation cannot be rendered")
			}
			summary, _ := op["summary"].(string)
			operationID, _ := op["operationId"].(string)
			page.Operations = append(page.Operations, docsOperation{Path: path, Method: strings.ToUpper(method), Summary: summary, OperationID: operationID, Details: string(details)})
		}
	}
	schemaNames := make([]string, 0, len(spec.Components.Schemas))
	for name := range spec.Components.Schemas {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)
	for _, name := range schemaNames {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, spec.Components.Schemas[name], "", "  "); err != nil {
			return errors.New("frozen OpenAPI schema is invalid")
		}
		page.Schemas = append(page.Schemas, docsSchema{Name: name, Definition: pretty.String()})
	}
	mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	servePage := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=300")
		if err := docsTemplate.Execute(w, page); err != nil {
			return
		}
	}
	mux.HandleFunc("GET /docs", servePage)
	mux.HandleFunc("GET /redoc", servePage)
	mux.HandleFunc("GET /docs/oauth2-redirect", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(oauthRedirectHTML))
	})
	return nil
}
