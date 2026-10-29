package main

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// healthcheck asks the same authenticated readiness endpoint used by the worker.
func healthcheck() error {
	return checkHealth(&http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
}

type healthClient interface {
	Do(*http.Request) (*http.Response, error)
}

func checkHealth(client healthClient) error {
	source, err := loadSource()
	if err != nil {
		return errors.New("runtime configuration unavailable")
	}
	token := source.get("JOB_EXECUTOR_TOKEN")
	if !tokenOK(token) {
		return errors.New("invalid private service token")
	}
	port := source.get("JOB_EXECUTOR_PORT")
	if port == "" {
		port = "8100"
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return errors.New("invalid listen port")
	}
	request, err := http.NewRequest(http.MethodGet, "http://"+net.JoinHostPort("127.0.0.1", port)+"/internal/v1/status", nil)
	if err != nil {
		return errors.New("healthcheck failed")
	}
	request.Header.Set("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		return errors.New("healthcheck failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("healthcheck failed")
	}
	var result struct {
		Ready bool `json:"ready"`
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 || json.Unmarshal(body, &result) != nil || !result.Ready {
		return errors.New("healthcheck failed")
	}
	return nil
}
