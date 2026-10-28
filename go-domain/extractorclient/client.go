// Package extractorclient provides bounded admission for the private extractor.
package extractorclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("extractor unavailable")

type Client struct {
	base, token string
	http        *http.Client
	slot        chan struct{}
	mu          sync.Mutex
	waiters     int
}

func New(base, token string) (*Client, error) {
	u, e := url.Parse(base)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || token == "" {
		return nil, ErrUnavailable
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &Client{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slot: make(chan struct{}, 1)}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
func (c *Client) acquire(ctx context.Context) error {
	c.mu.Lock()
	if c.waiters >= 8 {
		c.mu.Unlock()
		return ErrUnavailable
	}
	c.waiters++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.waiters--; c.mu.Unlock() }()
	select {
	case c.slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) Post(ctx context.Context, path string, payload any, out any) error {
	body, e := json.Marshal(payload)
	if e != nil || len(body) > 8*1024*1024 {
		return ErrUnavailable
	}
	for retry := 0; retry <= 8; retry++ {
		if e = c.acquire(ctx); e != nil {
			return e
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
		if e != nil {
			<-c.slot
			return ErrUnavailable
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "identity")
		resp, e := c.http.Do(req)
		if e != nil {
			<-c.slot
			return ErrUnavailable
		}
		data, e := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024+1))
		resp.Body.Close()
		<-c.slot
		if e != nil || len(data) > 8*1024*1024 {
			return ErrUnavailable
		}
		if resp.StatusCode == 429 && retry < 8 {
			seconds, e := strconv.Atoi(resp.Header.Get("Retry-After"))
			if e != nil || seconds < 0 || seconds > 86400 {
				return ErrUnavailable
			}
			timer := time.NewTimer(time.Duration(seconds) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
			continue
		}
		if resp.StatusCode != 200 || !strings.EqualFold(resp.Header.Get("Content-Encoding"), "identity") && resp.Header.Get("Content-Encoding") != "" {
			return ErrUnavailable
		}
		if json.Unmarshal(data, out) != nil {
			return ErrUnavailable
		}
		return nil
	}
	return ErrUnavailable
}
