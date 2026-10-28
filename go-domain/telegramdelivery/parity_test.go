package telegramdelivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestLegacyPayloadCaptureAndStrictResponseDelta(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy-telegram.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Case          string `json:"case"`
		Mode          string `json:"mode"`
		Status        int    `json:"response_status"`
		Body          string `json:"response_body"`
		LegacySuccess bool   `json:"legacy_success"`
		Posts         []struct {
			Method  string          `json:"method"`
			Payload json.RawMessage `json:"payload"`
		} `json:"posts"`
	}
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Mode+"/"+tc.Case, func(t *testing.T) {
			if len(tc.Posts) != 1 {
				t.Fatal("expected one captured POST")
			}
			expected := tc.Posts[0]
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				body, e := io.ReadAll(r.Body)
				if e != nil {
					t.Fatal(e)
				}
				var actual, legacy any
				if json.Unmarshal(body, &actual) != nil || json.Unmarshal(expected.Payload, &legacy) != nil {
					t.Fatal("invalid payload JSON")
				}
				a, _ := json.Marshal(actual)
				b, _ := json.Marshal(legacy)
				if string(a) != string(b) {
					t.Fatal("payload differs from legacy capture")
				}
				if !strings.HasSuffix(r.URL.Path, "/"+expected.Method) {
					t.Fatal("method changed")
				}
				return &http.Response{StatusCode: tc.Status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.Body)), Request: r}, nil
			})}
			result := (Transport{Client: client, Endpoint: "https://fixture.invalid", Token: "fixture"}).Send(context.Background(), expected.Method, expected.Payload)
			want := Unknown
			switch tc.Case {
			case "ok":
				want = Sent
			case "reject400", "rate429":
				want = Rejected
			}
			if result.State != want || calls != 1 {
				t.Fatalf("case=%s state=%s calls=%d", tc.Case, result.State, calls)
			}
			if (tc.Case == "empty200" || tc.Case == "malformed200" || tc.Case == "missing_ok200") && !tc.LegacySuccess {
				t.Fatal("expected recorded legacy success delta")
			}
		})
	}
}
