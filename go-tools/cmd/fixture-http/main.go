// fixture-http makes bounded requests to the web container on the isolated gate network.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type command struct {
	Target, Method, Path, Authorization string
	Body                                []byte
	Headers                             map[string]string
}
type reply struct {
	Status    int    `json:"status"`
	Body      []byte `json:"body"`
	HasCookie bool   `json:"has_cookie"`
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func run(in io.Reader, out io.Writer) error {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DialContext = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
	return runWithClient(in, out, &http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
}
func runWithClient(in io.Reader, out io.Writer, client httpDoer) error {
	var c command
	if json.NewDecoder(io.LimitReader(in, 1024*1024)).Decode(&c) != nil {
		return errors.New("invalid probe input")
	}
	u, e := url.Parse(c.Path)
	if e != nil || !strings.HasPrefix(c.Path, "/") || strings.HasPrefix(c.Path, "//") || u.IsAbs() || u.Host != "" || u.Fragment != "" || strings.Contains(u.Path, "..") || (c.Method != "GET" && c.Method != "POST" && c.Method != "PUT" && c.Method != "PATCH" && c.Method != "DELETE") {
		return errors.New("invalid probe target")
	}
	base := "http://web:8000"
	if c.Target == "scheduler" {
		base = "http://background:8080"
	} else if c.Target == "domain" {
		base = "http://executor:8100"
	} else if c.Target != "" && c.Target != "web" {
		return errors.New("invalid probe target")
	}
	r, e := http.NewRequest(c.Method, base+c.Path, bytes.NewReader(c.Body))
	if e != nil {
		return e
	}
	if c.Authorization != "" {
		r.Header.Set("Authorization", c.Authorization)
	}
	if c.Body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range c.Headers {
		allowed := k == "Idempotency-Key"
		// Managed admin acceptance models TLS terminated by a trusted reverse
		// proxy. Permit only fixed https scheme assertions for login and the
		// synthetic protected article read; never forward identity headers.
		if k == "X-Forwarded-Proto" && v == "https" && secureWebFixturePath(c) {
			allowed = true
		}
		if !allowed {
			return errors.New("invalid probe header")
		}
		r.Header.Set(k, v)
	}

	resp, e := client.Do(r)
	if e != nil {
		return errors.New("probe request failed")
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
	if e != nil || len(body) > 1024*1024 {
		return errors.New("probe response invalid")
	}
	return json.NewEncoder(out).Encode(reply{resp.StatusCode, body, len(resp.Header.Values("Set-Cookie")) != 0})
}

func secureWebFixturePath(c command) bool {
	if c.Target != "web" {
		return false
	}
	if c.Method == "POST" && c.Path == "/auth/login" {
		return true
	}
	const prefix = "/api/v1/articles/"
	if c.Method != "GET" || !strings.HasPrefix(c.Path, prefix) {
		return false
	}
	id := strings.TrimPrefix(c.Path, prefix)
	if id == "" {
		return false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
func main() {
	if run(os.Stdin, os.Stdout) != nil {
		os.Exit(1)
	}
}
