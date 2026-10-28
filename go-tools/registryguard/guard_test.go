package registryguard

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeResponse struct {
	status  int
	body    string
	headers http.Header
}
type fakeClient struct {
	responses []fakeResponse
	requests  []*http.Request
	fail      bool
}

func (f *fakeClient) Do(r *http.Request) (*http.Response, error) {
	f.requests = append(f.requests, r)
	if f.fail {
		return nil, errors.New("private detail")
	}
	v := f.responses[len(f.requests)-1]
	return &http.Response{StatusCode: v.status, Header: v.headers, Body: io.NopCloser(strings.NewReader(v.body))}, nil
}
func TestAuthenticatedAbsenceAndScopes(t *testing.T) {
	for repo := range allowed {
		t.Run(repo, func(t *testing.T) {
			scope := "repository:" + repo + ":pull"
			f := &fakeClient{responses: []fakeResponse{{401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="` + scope + `"`}}}, {200, `{"token":"fixture-token"}`, nil}, {404, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, nil}}}
			tag := "ghcr.io/" + repo + ":sha-" + strings.Repeat("a", 40)
			exists, e := Exists(f, tag, "fixture-user", "secret", repo)
			if e != nil || exists {
				t.Fatalf("exists=%v error=%v", exists, e)
			}
			if f.requests[1].URL.Query().Get("scope") != scope {
				t.Fatal("wrong token scope")
			}
			if f.requests[2].Header.Get("Authorization") != "Bearer fixture-token" {
				t.Fatal("missing token")
			}
		})
	}
}
func TestFailsClosed(t *testing.T) {
	repo := DefaultRepository
	tag := "ghcr.io/" + repo + ":sha-" + strings.Repeat("a", 40)
	tests := []struct {
		name      string
		responses []fakeResponse
		fail      bool
		want      string
	}{{"redirect", []fakeResponse{{302, "", nil}}, false, "registry_manifest_check_failed"}, {"unknown_404", []fakeResponse{{404, `{"errors":[{"code":"DENIED"}]}`, nil}}, false, "registry_manifest_check_failed"}, {"bad_challenge", []fakeResponse{{401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://attacker.invalid/token",service="ghcr.io",scope="repository:dzarlax-ai/rss-scheduler:pull"`}}}}, false, "registry_auth_challenge_invalid"}, {"transport", nil, true, "registry_transport_error"}, {"auth", []fakeResponse{{404, `{"errors":[{"code":"NAME_UNKNOWN"}]}`, nil}, {403, "", nil}}, false, "registry_auth_error"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeClient{responses: tt.responses, fail: tt.fail}
			_, e := Exists(f, tag, "user", "secret", repo)
			if e == nil || e.Error() != tt.want {
				t.Fatalf("got %v", e)
			}
		})
	}
}
func TestUnauthenticated404IsRechecked(t *testing.T) {
	repo := DefaultRepository
	tag := "ghcr.io/" + repo + ":sha-" + strings.Repeat("a", 40)
	f := &fakeClient{responses: []fakeResponse{{404, `{"errors":[{"code":"NAME_UNKNOWN"}]}`, nil}, {200, `{"access_token":"test-token"}`, nil}, {200, "{}", nil}}}
	exists, e := Exists(f, tag, "user", "secret", repo)
	if e != nil || !exists || len(f.requests) != 3 {
		t.Fatalf("exists=%v error=%v requests=%d", exists, e, len(f.requests))
	}
}
func TestRedirectClient(t *testing.T) {
	if e := Client().CheckRedirect(nil, nil); !errors.Is(e, http.ErrUseLastResponse) {
		t.Fatal(e)
	}
}
