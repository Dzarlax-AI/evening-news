package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

type ViberOperations interface {
	Viber(context.Context, string, string, json.RawMessage, string) (json.RawMessage, error)
	ViberWebhook(context.Context, []byte, string) (json.RawMessage, error)
}

func (c *Client) Viber(ctx context.Context, method, operation string, body json.RawMessage, editionID string) (json.RawMessage, error) {
	if !((method == http.MethodPost && (operation == "verify" || operation == "connect" || operation == "test")) || (method == http.MethodGet && operation == "preview")) {
		return nil, &APIError{Status: 422, Detail: "Invalid Viber operation"}
	}
	endpoint := "/viber/" + operation
	if operation == "preview" {
		endpoint += "?edition_id=" + url.QueryEscape(editionID)
	}
	return c.rawJSON(ctx, method, endpoint, bytes.NewReader(body), "application/json")
}
func (c *Client) ViberWebhook(ctx context.Context, body []byte, signature string) (json.RawMessage, error) {
	return c.json(ctx, http.MethodPost, "/viber/webhook", struct {
		Body      []byte `json:"body"`
		Signature string `json:"signature"`
	}{body, signature})
}
