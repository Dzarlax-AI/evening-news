// Package registryguard checks immutable GHCR tags without following redirects.
package registryguard

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const registry = "ghcr.io"
const DefaultRepository = "dzarlax-ai/rss-scheduler"
const maxBytes = 64 * 1024
const accept = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"

var allowed = map[string]bool{"dzarlax-ai/rss-scheduler": true, "dzarlax-ai/rss-job-worker": true, "dzarlax-ai/rss-article-ingestion": true, "dzarlax-ai/rss-article-processing": true, "dzarlax-ai/rss-web": true, "dzarlax-ai/rss-background": true, "dzarlax-ai/rss-summarizer": true}
var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,256}$`)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)
var challengePattern = regexp.MustCompile(`([A-Za-z]+)="([^"\\]*)"`)

type doer interface {
	Do(*http.Request) (*http.Response, error)
}

func Client() *http.Client {
	return &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func request(client doer, req *http.Request) (int, http.Header, []byte, error) {
	resp, e := client.Do(req)
	if e != nil {
		return 0, nil, nil, errors.New("registry_transport_error")
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if e != nil {
		return 0, nil, nil, errors.New("registry_transport_error")
	}
	return resp.StatusCode, resp.Header, b, nil
}
func unknown(b []byte) (bool, error) {
	if len(b) > maxBytes {
		return false, errors.New("registry_protocol_error")
	}
	var payload any
	if json.Unmarshal(b, &payload) != nil {
		return false, errors.New("registry_protocol_error")
	}
	obj, ok := payload.(map[string]any)
	if !ok {
		return false, nil
	}
	entries, ok := obj["errors"].([]any)
	if !ok {
		return false, nil
	}
	for _, item := range entries {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		code, _ := row["code"].(string)
		if code == "MANIFEST_UNKNOWN" || code == "NAME_UNKNOWN" {
			return true, nil
		}
	}
	return false, nil
}

func Exists(client doer, tag, user, password, repo string) (bool, error) {
	fail := func(code string) (bool, error) { return false, errors.New(code) }
	prefix := "ghcr.io/" + repo + ":sha-"
	if !allowed[repo] || !strings.HasPrefix(tag, prefix) || !shaPattern.MatchString(strings.TrimPrefix(tag, prefix)) || !usernamePattern.MatchString(user) || password == "" {
		return fail("registry_check_configuration_invalid")
	}
	manifest := "https://ghcr.io/v2/" + repo + "/manifests/sha-" + strings.TrimPrefix(tag, prefix)
	req, _ := http.NewRequest("GET", manifest, nil)
	req.Header.Set("Accept", accept)
	status, h, b, e := request(client, req)
	if e != nil {
		return false, e
	}
	if status == 200 {
		return true, nil
	}
	scope := "repository:" + repo + ":pull"
	if status == 404 {
		yes, e := unknown(b)
		if e != nil {
			return false, e
		}
		if !yes {
			return fail("registry_manifest_check_failed")
		}
	} else if status == 401 {
		value := h.Get("WWW-Authenticate")
		if !strings.HasPrefix(value, "Bearer ") {
			return fail("registry_auth_challenge_invalid")
		}
		pairs := map[string]string{}
		for _, m := range challengePattern.FindAllStringSubmatch(strings.TrimPrefix(value, "Bearer "), -1) {
			pairs[m[1]] = m[2]
		}
		if pairs["realm"] != "https://ghcr.io/token" || pairs["service"] != registry || pairs["scope"] != scope {
			return fail("registry_auth_challenge_invalid")
		}
	} else {
		return fail("registry_manifest_check_failed")
	}
	query := url.Values{"service": {registry}, "scope": {scope}}
	tokenReq, _ := http.NewRequest("GET", "https://ghcr.io/token?"+query.Encode(), nil)
	tokenReq.SetBasicAuth(user, password)
	tokenReq.Header.Set("Accept", "application/json")
	status, _, b, e = request(client, tokenReq)
	if e != nil {
		return false, e
	}
	if status != 200 || len(b) > maxBytes {
		return fail("registry_auth_error")
	}
	var tokens struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(b, &tokens) != nil {
		return fail("registry_auth_error")
	}
	token := tokens.Token
	if token == "" {
		token = tokens.AccessToken
	}
	if token == "" || len(token) > maxBytes || !tokenPattern.MatchString(token) {
		return fail("registry_auth_error")
	}
	req, _ = http.NewRequest("GET", manifest, nil)
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+token)
	status, _, b, e = request(client, req)
	if e != nil {
		return false, e
	}
	if status == 200 {
		return true, nil
	}
	if status == 404 {
		yes, e := unknown(b)
		if e != nil {
			return false, e
		}
		if yes {
			return false, nil
		}
	}
	if status == 401 || status == 403 {
		return fail("registry_auth_error")
	}
	return fail("registry_manifest_check_failed")
}
