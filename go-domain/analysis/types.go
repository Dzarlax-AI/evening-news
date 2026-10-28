package analysis

// Request is the bounded, versioned stdin contract for one article analysis.
type Request struct {
	Version                 int                `json:"version"`
	Title                   string             `json:"title"`
	Content                 string             `json:"content"`
	URL                     string             `json:"url"`
	OriginalContext         *string            `json:"original_context,omitempty"`
	Categories              []CategoryMetadata `json:"categories"`
	LocalCategory           string             `json:"local_category"`
	ConfiguredLocalCategory string             `json:"configured_local_category"`
	Model                   string             `json:"model"`
	Endpoint                string             `json:"endpoint"`
	MaxTextChars            int                `json:"max_text_chars"`
}

type CategoryMetadata struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"display_name"`
	Examples    []string `json:"examples"`
}

type Response struct {
	Version int            `json:"version"`
	OK      bool           `json:"ok"`
	Result  map[string]any `json:"result,omitempty"`
	Usage   *Usage         `json:"usage,omitempty"`
	Error   *Error         `json:"error,omitempty"`
}

type Usage struct {
	Model            string `json:"model"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	CachedTokens     int    `json:"cached_tokens"`
	TotalTokens      int    `json:"total_tokens"`
}

type GeminiUsage struct {
	PromptTokenCount        int  `json:"promptTokenCount"`
	CandidatesTokenCount    int  `json:"candidatesTokenCount"`
	CachedContentTokenCount int  `json:"cachedContentTokenCount"`
	TotalTokenCount         *int `json:"totalTokenCount"`
}

type Error struct {
	Code               string   `json:"code"`
	StatusCode         *int     `json:"status_code,omitempty"`
	RetryAfterSeconds  *float64 `json:"retry_after_seconds,omitempty"`
	RetryableTransport bool     `json:"retryable_transport,omitempty"`
}
