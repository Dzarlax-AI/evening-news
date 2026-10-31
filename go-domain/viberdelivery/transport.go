// Package viberdelivery implements the Viber channel transport.
package viberdelivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type State string

const (
	Sent     State = "sent"
	Rejected State = "rejected"
	Unknown  State = "unknown"
)

type Result struct {
	State                 State
	MessageToken          string
	StatusCode, APIStatus int
	RetryAfter            time.Duration
}
type Superadmin struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type ChannelInfo struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Superadmins []Superadmin `json:"superadmins"`
}

// Transport always connects to the fixed Viber endpoint. Client may supply a test RoundTripper.
type Transport struct {
	Client *http.Client
	Token  string
}

func VerifySignature(body []byte, signature, token string) bool {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write(body)
	provided, err := hex.DecodeString(signature)
	return token != "" && err == nil && hmac.Equal(mac.Sum(nil), provided)
}
func (t Transport) request(ctx context.Context, method string, payload any) (int, http.Header, []byte, bool) {
	if t.Token == "" || (method != "post" && method != "get_account_info" && method != "set_webhook") {
		return 0, nil, nil, false
	}
	b, err := json.Marshal(payload)
	if err != nil || len(b) > 1<<20 {
		return 0, nil, nil, false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(b, &object) != nil || object == nil {
		return 0, nil, nil, false
	}
	tokenJSON, _ := json.Marshal(t.Token)
	object["auth_token"] = tokenJSON
	b, err = json.Marshal(object)
	if err != nil || len(b) > 1<<20 {
		return 0, nil, nil, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://chatapi.viber.com/pa/"+method, bytes.NewReader(b))
	if err != nil {
		return 0, nil, nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Viber-Auth-Token", t.Token)
	client := http.Client{Timeout: 30 * time.Second}
	if t.Client != nil {
		client = *t.Client
		if client.Timeout <= 0 || client.Timeout > 30*time.Second {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, false
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return resp.StatusCode, resp.Header, nil, false
	}
	return resp.StatusCode, resp.Header, data, true
}
func retryAfter(h http.Header) time.Duration {
	raw := strings.TrimSpace(h.Get("Retry-After"))
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		if len(raw) > 0 && strings.Trim(raw, "0123456789") == "" {
			return time.Hour
		}
		return 0
	}
	if n >= 3600 {
		return time.Hour
	}
	return time.Duration(n) * time.Second
}
func (t Transport) Send(ctx context.Context, payload any) Result {
	code, h, b, ok := t.request(ctx, "post", payload)
	r := Result{State: Unknown, StatusCode: code}
	if !ok {
		return r
	}
	if code == http.StatusTooManyRequests {
		r.State = Rejected
		r.RetryAfter = retryAfter(h)
		return r
	}
	if code >= 400 && code < 500 && code != http.StatusRequestTimeout {
		r.State = Rejected
		return r
	}
	if code >= 500 || code < 200 || code >= 300 {
		return r
	}
	var raw struct {
		Status       *int            `json:"status"`
		MessageToken json.RawMessage `json:"message_token"`
	}
	if json.Unmarshal(b, &raw) != nil || raw.Status == nil {
		return r
	}
	r.APIStatus = *raw.Status
	if *raw.Status != 0 {
		r.State = Rejected
		if *raw.Status == 12 {
			r.RetryAfter = 30 * time.Second
		}
		return r
	}
	var token string
	if len(raw.MessageToken) > 0 {
		if raw.MessageToken[0] == '"' {
			_ = json.Unmarshal(raw.MessageToken, &token)
		} else {
			var n json.Number
			if json.Unmarshal(raw.MessageToken, &n) == nil {
				token = n.String()
			}
		}
	}
	if token == "" || strings.Trim(token, "0123456789") != "" || strings.Trim(token, "0") == "" {
		return r
	}
	r.State = Sent
	r.MessageToken = token
	return r
}
func (t Transport) AccountInfo(ctx context.Context) (ChannelInfo, error) {
	code, _, b, ok := t.request(ctx, "get_account_info", struct{}{})
	if !ok || code != 200 {
		return ChannelInfo{}, ErrTransport
	}
	var r struct {
		Status       *int   `json:"status"`
		ID           string `json:"id"`
		Name         string `json:"name"`
		ChatHostname string `json:"chat_hostname"`
		Members      []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Role string `json:"role"`
		} `json:"members"`
	}
	if json.Unmarshal(b, &r) != nil || r.Status == nil || *r.Status != 0 || r.ID == "" {
		return ChannelInfo{}, ErrTransport
	}
	if r.Name == "" {
		r.Name = r.ChatHostname
	}
	out := ChannelInfo{ID: r.ID, Name: r.Name}
	for _, member := range r.Members {
		if member.Role == "superadmin" && member.ID != "" {
			out.Superadmins = append(out.Superadmins, Superadmin{ID: member.ID, Name: member.Name})
		}
	}
	return out, nil
}
func (t Transport) SetWebhook(ctx context.Context, url string) error {
	code, _, b, ok := t.request(ctx, "set_webhook", struct {
		URL string `json:"url"`
	}{url})
	if !ok || code != 200 {
		return ErrTransport
	}
	var r struct {
		Status *int `json:"status"`
	}
	if json.Unmarshal(b, &r) != nil || r.Status == nil || *r.Status != 0 {
		return ErrTransport
	}
	return nil
}
