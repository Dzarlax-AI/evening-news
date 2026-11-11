package egress

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testRenderBudget(t *testing.T, server, token string, limit int64, ttl int) (string, string) {
	t.Helper()
	body := fmt.Sprintf(`{"contract_version":"1","wire_limit_bytes":%d,"ttl_ms":%d}`, limit, ttl)
	req, _ := http.NewRequest("POST", server+"/v1/render-budgets", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("control request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatal("budget allocation rejected")
	}
	var value struct {
		ID    string `json:"budget_id"`
		Token string `json:"proxy_token"`
	}
	if json.NewDecoder(resp.Body).Decode(&value) != nil || len(value.ID) != 32 || len(value.Token) != 64 {
		t.Fatal("invalid control receipt")
	}
	return value.ID, value.Token
}
func testRevokeBudget(t *testing.T, server, id string) renderReceipt {
	t.Helper()
	req, _ := http.NewRequest("DELETE", server+"/v1/render-budgets/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("revoke failed")
	}
	defer resp.Body.Close()
	var value renderReceipt
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&value) != nil {
		t.Fatal("invalid revoke receipt")
	}
	return value
}
func testScopedConnect(t *testing.T, server, token string) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	c, err := net.Dial("tcp", strings.TrimPrefix(server, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	auth := base64.StdEncoding.EncodeToString([]byte("extractor:" + token))
	fmt.Fprintf(c, "CONNECT 1.1.1.1:443 HTTP/1.1\r\nHost: 1.1.1.1:443\r\nProxy-Authorization: Basic %s\r\n\r\n", auth)
	reader := bufio.NewReader(c)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil {
		c.Close()
		t.Fatal("CONNECT failed")
	}
	return c, reader, response.StatusCode
}
func TestRenderBudgetScopedBudgetAggregatesAndPreservesNeighbor(t *testing.T) {
	_, server := newRenderGateway(t, Config{Dialer: dialerFunc(func(_ context.Context, _, _ string) (net.Conn, error) {
		a, b := net.Pipe()
		go func() { defer b.Close(); io.Copy(b, b) }()
		return a, nil
	})})
	badID, bad := testRenderBudget(t, server.URL, testToken, 1024, 3000)
	goodID, good := testRenderBudget(t, server.URL, testToken, 4096, 3000)
	goodConn, goodReader, status := testScopedConnect(t, server.URL, good)
	defer goodConn.Close()
	if status != 200 {
		t.Fatal("neighbor rejected")
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, r, status := testScopedConnect(t, server.URL, bad)
			defer c.Close()
			if status != 200 {
				return
			}
			go c.Write(bytes.Repeat([]byte("x"), 2048))
			io.Copy(io.Discard, r)
		}()
	}
	wg.Wait()
	if _, err := goodConn.Write([]byte("ping")); err != nil {
		t.Fatal("neighbor closed by other budget")
	}
	data := make([]byte, 4)
	if _, err := io.ReadFull(goodReader, data); err != nil || string(data) != "ping" {
		t.Fatal("neighbor did not survive")
	}
	badReceipt := testRevokeBudget(t, server.URL, badID)
	if !badReceipt.Exhausted || badReceipt.Bytes > 1024 || badReceipt.Active != 0 {
		t.Fatal("shared byte bound failed")
	}
	goodReceipt := testRevokeBudget(t, server.URL, goodID)
	if goodReceipt.Exhausted || goodReceipt.Bytes != 8 || goodReceipt.Active != 0 {
		t.Fatal("neighbor charged incorrectly")
	}
	if again := testRevokeBudget(t, server.URL, badID); again != badReceipt {
		t.Fatal("revoke not idempotent")
	}
}
func TestRenderBudgetControlAndProxyCredentialsAreSeparate(t *testing.T) {
	_, server := newRenderGateway(t, Config{})
	c, _, status := testScopedConnect(t, server.URL, testToken)
	c.Close()
	if status != 407 {
		t.Fatal("master accepted as CONNECT credential")
	}
	id, token := testRenderBudget(t, server.URL, testToken, 1024, 1000)
	req, _ := http.NewRequest("GET", server.URL+"/v1/render-budgets/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("proxy credential accepted as control credential")
	}
	testRevokeBudget(t, server.URL, id)
	c, _, status = testScopedConnect(t, server.URL, token)
	c.Close()
	if status != 407 {
		t.Fatal("revoked token accepted")
	}
}
func TestRenderBudgetExpiredBudgetCannotOpenTunnel(t *testing.T) {
	_, server := newRenderGateway(t, Config{})
	id, token := testRenderBudget(t, server.URL, testToken, 1024, 10)
	time.Sleep(30 * time.Millisecond)
	c, _, status := testScopedConnect(t, server.URL, token)
	c.Close()
	if status != 407 {
		t.Fatal("expired token accepted")
	}
	receipt := testRevokeBudget(t, server.URL, id)
	if !receipt.Expired || !receipt.Revoked {
		t.Fatal("expiry not retained")
	}
}

func newRenderGateway(t *testing.T, cfg Config) (*Gateway, *httptest.Server) {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		g.Drain(ctx)
		server.Close()
	})
	return g, server
}

func TestRenderBudgetRejectsMalformedRegistrationAndDrain(t *testing.T) {
	g, server := newRenderGateway(t, Config{})
	valid := `{"contract_version":"1","wire_limit_bytes":1024,"ttl_ms":1000}`
	for _, body := range []string{
		`null`, `{}`, `{"contract_version":"1","wire_limit_bytes":1024}`,
		`{"contract_version":"1","wire_limit_bytes":1024,"ttl_ms":1000,"ttl_ms":1}`,
		`{"contract_version":"1","wire_limit_bytes":1024,"ttl_ms":null}`,
		`{"contract_version":"1","wire_limit_bytes":1024,"TTL_MS":1000}`,
		`{"contract_version":"2","wire_limit_bytes":1024,"ttl_ms":1000}`,
		`{"contract_version":"1","wire_limit_bytes":5242881,"ttl_ms":1000}`,
		valid + `{}`, valid + strings.Repeat(" ", 1024),
	} {
		req, _ := http.NewRequest("POST", server.URL+"/v1/render-budgets", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+testToken)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("malformed registration status=%d", response.StatusCode)
		}
	}
	if len(g.budgets) != 0 {
		t.Fatal("malformed registration allocated a budget")
	}
	if err := g.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", server.URL+"/v1/render-budgets", strings.NewReader(valid))
	req.Header.Set("Authorization", "Bearer "+testToken)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || len(g.budgets) != 0 {
		t.Fatal("registration crossed drain fence")
	}
}

type blockedBudgetWriter struct {
	started, release chan struct{}
}

func (w blockedBudgetWriter) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return 1, io.ErrShortWrite
}

func TestRenderBudgetReservationExhaustsBeforeBlockedShortWrite(t *testing.T) {
	g, _ := newRenderGateway(t, Config{})
	b := &renderBudget{limit: 4, remaining: 4, ops: map[*operation]struct{}{}}
	var tunnel atomic.Int64
	tunnel.Store(100)
	w := blockedBudgetWriter{make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.copyScopedTunnel(w, strings.NewReader("oversized"), &tunnel, b)
	}()
	<-w.started
	if receipt := b.receipt(); !receipt.Exhausted || receipt.Bytes != 0 {
		t.Error("blocked write concealed exhaustion")
	}
	close(w.release)
	<-done
	if receipt := b.receipt(); !receipt.Exhausted || receipt.Bytes != 1 || !receipt.Revoked {
		t.Fatal("short write refunded budget or misreported forwarded bytes")
	}
}

func TestRenderBudgetFinalReceiptSurvivesLateExpiry(t *testing.T) {
	b := &renderBudget{limit: 4, remaining: 4, ops: map[*operation]struct{}{}}
	b.stop(false, false)
	final := b.receipt()
	// A timer callback may already be running when revoke stops the timer.
	b.stop(false, true)
	var tunnel atomic.Int64
	tunnel.Store(100)
	var dst bytes.Buffer
	g, _ := newRenderGateway(t, Config{})
	g.copyScopedTunnel(&dst, strings.NewReader("late"), &tunnel, b)
	if b.receipt() != final || dst.Len() != 0 {
		t.Fatal("revoked budget changed after a late expiry or copy")
	}
}

func TestRenderBudgetRegistryBoundsAndReclaimsReceipts(t *testing.T) {
	g, server := newRenderGateway(t, Config{MaxConcurrent: 1})
	for i := 0; i < 128; i++ {
		testRenderBudget(t, server.URL, testToken, 1024, 1000)
	}
	req, _ := http.NewRequest("POST", server.URL+"/v1/render-budgets", strings.NewReader(`{"contract_version":"1","wire_limit_bytes":1024,"ttl_ms":1000}`))
	req.Header.Set("Authorization", "Bearer "+testToken)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests || len(g.budgets) != 128 {
		t.Fatal("budget registry exceeded its bound")
	}
	// Advance one stopped receipt past retention without a multi-second sleep.
	g.budgetMu.Lock()
	for _, budget := range g.budgets {
		budget.stop(false, false)
		budget.expires = time.Now().Add(-6 * time.Second)
		break
	}
	g.budgetMu.Unlock()
	testRenderBudget(t, server.URL, testToken, 1024, 1000)
	if len(g.budgets) != 128 {
		t.Fatal("expired receipt was not reclaimed")
	}
}
