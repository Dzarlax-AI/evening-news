package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// The control-plane snapshot contains no integration secrets. This callback
// runs before worker recovery, outside its lease mutex and write transactions.
func runtimeConfigurationGate(base, token string) func(context.Context) error {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+"/internal/v1/runtime-configuration", nil)
		if err != nil {
			return errors.New("runtime configuration unavailable")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := client.Do(req)
		if err != nil {
			return errors.New("runtime configuration unavailable")
		}
		defer res.Body.Close()
		var snapshot struct {
			Maintenance *bool `json:"maintenance_mode"`
		}
		if res.StatusCode != 200 || json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&snapshot) != nil || snapshot.Maintenance == nil {
			return errors.New("runtime configuration unavailable")
		}
		if *snapshot.Maintenance {
			return errors.New("installation recovery requires operator resume")
		}
		return nil
	}
}
