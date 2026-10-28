package extractorsmoke

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type testClient struct {
	t      *testing.T
	count  int
	failAt int
}

func (c *testClient) Do(r *http.Request) (*http.Response, error) {
	c.count++
	status := 200
	var body string
	switch c.count {
	case 1:
		body = `{"capabilities":{"http":true}}`
	case 2:
		body = `{"contract_version":"1","build_version":"fixture-build"}`
	case 3:
		status = 401
	case 4:
		status = 400
	case 5:
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			c.t.Fatal("missing authorization")
		}
		var p map[string]any
		if json.NewDecoder(r.Body).Decode(&p) != nil || p["html"] != "<p>anchor</p>" {
			c.t.Fatal("missing fixture")
		}
		body = `{"contract_version":"1","build_version":"fixture-build","request_id":"smoke-fixture","original_url":"https://fixture.test/story","outcome":"success","result":{"content":"anchor"}}`
	case 6:
		body = `{"contract_version":"1","request_id":"smoke-denied","original_url":"http://127.0.0.1:9/private","outcome":"rejected","error":{"code":"public_url_denied","retryable":false}}`
	default:
		c.t.Fatal("too many requests")
	}
	if c.count == c.failAt {
		status = 500
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
}
func TestSmoke(t *testing.T) {
	fixture := []byte(`{"cases":[{"id":"test","input":{"strategy":"readability","url":"https://fixture.test/story","html":"<p>anchor</p>"},"expected":{"state":"extracted","contains":["anchor"],"not_contains":["noise"]}}]}`)
	c := &testClient{t: t}
	id, e := Run(c, "http://fixture", "fixture-token", "fixture-build", fixture)
	if e != nil || id != "test" || c.count != 6 {
		t.Fatalf("id=%s err=%v requests=%d", id, e, c.count)
	}
	for i := 1; i <= 6; i++ {
		c = &testClient{t: t, failAt: i}
		if _, e = Run(c, "http://fixture", "fixture-token", "fixture-build", fixture); e == nil {
			t.Fatalf("accepted failure at %d", i)
		}
	}
}
