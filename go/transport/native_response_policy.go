package transport

import (
	"errors"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const nativeWorkerPolicy = "worker-src 'none'; connect-src https:"

type nativeResponseHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type nativeFetchPaused struct {
	NetworkID string `json:"networkId"`
	ID        string `json:"requestId"`
	Request   struct {
		URL string `json:"url"`
	} `json:"request"`
	ResourceType        string                 `json:"resourceType"`
	ResponseStatusCode  *int                   `json:"responseStatusCode"`
	ResponseStatusText  string                 `json:"responseStatusText"`
	ResponseHeaders     []nativeResponseHeader `json:"responseHeaders"`
	ResponseErrorReason string                 `json:"responseErrorReason"`
}

func nativeFetchPatterns() []any {
	return []any{
		map[string]any{"urlPattern": "*", "requestStage": "Request"},
		map[string]any{"urlPattern": "*", "resourceType": "Document", "requestStage": "Response"},
	}
}

// Validate publisher metadata before bounded body replay. The additional CSP
// composes with publisher policies; replay normalizes decoded-body framing.
func nativeWorkerPolicyParams(event nativeFetchPaused) (map[string]any, error) {
	failure := func() (map[string]any, error) {
		return nil, &nativeEventFailure{"response_policy_invalid", errors.Join(ErrNativePolicy, ErrNativeOwnershipLost)}
	}
	if event.ID == "" || len(event.ID) > 256 || event.ResourceType != "Document" || event.ResponseStatusCode == nil || *event.ResponseStatusCode < 200 || *event.ResponseStatusCode > 599 || event.ResponseErrorReason != "" || event.ResponseHeaders == nil || len(event.ResponseHeaders) > 255 || len(event.ResponseStatusText) > 256 || strings.ContainsAny(event.ResponseStatusText, "\r\n\x00") {
		return failure()
	}
	size := len("Content-Security-Policy") + len(nativeWorkerPolicy)
	for _, header := range event.ResponseHeaders {
		size += len(header.Name) + len(header.Value)
		if size > 64*1024 || !httpguts.ValidHeaderFieldName(header.Name) || !httpguts.ValidHeaderFieldValue(header.Value) {
			return failure()
		}
	}
	headers := make([]nativeResponseHeader, len(event.ResponseHeaders), len(event.ResponseHeaders)+1)
	copy(headers, event.ResponseHeaders)
	headers = append(headers, nativeResponseHeader{"Content-Security-Policy", nativeWorkerPolicy})
	params := map[string]any{"requestId": event.ID, "responseCode": *event.ResponseStatusCode, "responseHeaders": headers}
	// HTTP/2 has no reason phrase. Supplying an explicit empty phrase is invalid
	// in CDP; omitting it lets the browser use its status-code default.
	if event.ResponseStatusText != "" {
		params["responsePhrase"] = event.ResponseStatusText
	}
	return params, nil
}
