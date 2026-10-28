package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
)

const maxInput = 5 << 20

func main() {
	input, err := io.ReadAll(io.LimitReader(os.Stdin, maxInput+1))
	if err != nil || len(input) > maxInput {
		write(failure("resource_limit"), 1)
	}
	dec := json.NewDecoder(strings.NewReader(string(input)))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	var request analysis.Request
	if err := dec.Decode(&request); err != nil || ensureEOF(dec) != nil {
		write(failure("invalid_input"), 1)
	}
	key := os.Getenv("GEMINI_API_KEY")
	endpoint := request.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("GEMINI_ENDPOINT")
	}
	model := request.Model
	if model == "" {
		model = os.Getenv("SUMMARIZATION_MODEL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	result, usage, err := analysis.Analyze(ctx, request, analysis.ProviderConfig{
		APIKey: key, Model: model, Endpoint: endpoint, Client: &http.Client{Timeout: 40 * time.Second},
	})
	if err != nil {
		write(analysis.ErrorResponse(err), 1)
	}
	write(analysis.Response{Version: 1, OK: true, Result: result, Usage: usage}, 0)
}

func ensureEOF(dec *json.Decoder) error {
	var v any
	if err := dec.Decode(&v); err != io.EOF {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func failure(code string) analysis.Response {
	return analysis.Response{Version: 1, OK: false, Error: &analysis.Error{Code: code}}
}
func write(response analysis.Response, status int) {
	_ = json.NewEncoder(os.Stdout).Encode(response)
	if status != 0 {
		os.Exit(status)
	}
}
