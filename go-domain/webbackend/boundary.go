// Package webbackend serves the private backend used by the existing Go web UI.
package webbackend

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Boundary struct {
	Token           string
	Worker          bool
	Next            http.Handler
	timeoutOverride time.Duration
}

// deadlineWriter streams responses without buffering the backup archive. Once
// the deadline wins, late handler writes are rejected and cannot race a 504.
type deadlineWriter struct {
	mu               sync.Mutex
	w                http.ResponseWriter
	ctx              context.Context
	header           http.Header
	started, expired bool
}

func (w *deadlineWriter) Header() http.Header { return w.header }
func (w *deadlineWriter) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.expired || w.ctx.Err() != nil {
		return
	}
	for key, values := range w.header {
		w.w.Header()[key] = append([]string(nil), values...)
	}
	w.started = true
	w.w.WriteHeader(code)
}
func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.expired || w.ctx.Err() != nil {
		return 0, http.ErrHandlerTimeout
	}
	if !w.started {
		for key, values := range w.header {
			w.w.Header()[key] = append([]string(nil), values...)
		}
		w.started = true
		w.w.WriteHeader(http.StatusOK)
	}
	return w.w.Write(p)
}
func (w *deadlineWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.expired || w.ctx.Err() != nil {
		return
	}
	if !w.started {
		for key, values := range w.header {
			w.w.Header()[key] = append([]string(nil), values...)
		}
		w.started = true
		w.w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.w).Flush()
}
func (w *deadlineWriter) expire(detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.expired = true
	if !w.started {
		_ = http.NewResponseController(w.w).SetWriteDeadline(time.Now().Add(time.Second))
		body, _ := json.Marshal(map[string]string{"detail": detail})
		body = append(body, '\n')
		w.w.Header().Set("Content-Type", "application/json")
		w.w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.w.Write(body)
		_ = http.NewResponseController(w.w).Flush()
	}
}

func timeoutDetail(method string) string {
	if method == http.MethodGet || method == http.MethodHead {
		return "Operation timed out"
	}
	return "Operation timed out; outcome may be unknown. Verify before retrying."
}
func timedOut(err error, ctx context.Context) bool {
	var netErr net.Error
	return ctx.Err() == context.DeadlineExceeded || (errors.As(err, &netErr) && netErr.Timeout())
}

func reject(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"detail": detail})
}
func (b Boundary) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if b.Token == "" {
		reject(w, 503, "Web backend authentication is unavailable")
		return
	}
	values := r.Header.Values("Authorization")
	expected := "Bearer " + b.Token
	if len(values) != 1 || len(values[0]) != len(expected) || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
		reject(w, 401, "Unauthorized")
		return
	}
	if b.Worker && r.Method == http.MethodPost {
		switch r.URL.Path {
		case "/internal/web/v1/backups/upload":
			reject(w, 409, "Backup uploads are disabled in worker mode; use offline maintenance with the worker stopped.")
			return
		case "/internal/web/v1/backups/cleanup":
			reject(w, 409, "Backup cleanup is disabled in worker mode; use offline maintenance with the worker stopped.")
			return
		case "/internal/web/v1/backups/restore":
			reject(w, 409, "Restore is disabled in worker mode; stop the worker and use offline maintenance.")
			return
		}
	}
	upload := r.Method == http.MethodPost && r.URL.Path == "/internal/web/v1/backups/upload"
	limit := int64(64 * 1024)
	timeout := 70 * time.Second
	if upload {
		limit = 64*1024*1024 + 64*1024
		timeout = 120 * time.Second
	} else if r.Method == http.MethodGet {
		timeout = 120 * time.Second
	}
	if b.timeoutOverride > 0 {
		timeout = b.timeoutOverride
	}
	lengths := r.Header.Values("Content-Length")
	if len(lengths) > 1 {
		reject(w, 400, "Invalid Content-Length")
		return
	}
	if len(lengths) == 1 {
		n, e := strconv.ParseInt(lengths[0], 10, 64)
		if e != nil || n < 0 {
			reject(w, 400, "Invalid Content-Length")
			return
		}
		if n > limit {
			reject(w, 413, "Request body exceeds limit")
			return
		}
	}
	if r.ContentLength > limit {
		reject(w, 413, "Request body exceeds limit")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	r = r.WithContext(ctx)
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(timeout))
	if upload {
		temp, err := os.CreateTemp("", "news-private-upload-*")
		if err != nil {
			reject(w, 500, "Request body is unavailable")
			return
		}
		name := temp.Name()
		defer func() { temp.Close(); os.Remove(name) }()
		n, err := io.CopyBuffer(temp, io.LimitReader(r.Body, limit+1), make([]byte, 64*1024))
		if err != nil {
			if timedOut(err, ctx) {
				reject(w, 504, timeoutDetail(r.Method))
			}
			return
		}
		if n > limit {
			reject(w, 413, "Request body exceeds limit")
			return
		}
		if _, err = temp.Seek(0, io.SeekStart); err != nil {
			reject(w, 500, "Request body is unavailable")
			return
		}
		r.Body = io.NopCloser(temp)
	} else {
		data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
		if err != nil {
			if timedOut(err, ctx) {
				reject(w, 504, timeoutDetail(r.Method))
			}
			return
		}
		if int64(len(data)) > limit {
			reject(w, 413, "Request body exceeds limit")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
	}
	// The body has been fully captured. Leaving the socket read deadline in
	// place can cancel an otherwise active request while its handler runs.
	_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
	if ctx.Err() == context.DeadlineExceeded {
		reject(w, 504, timeoutDetail(r.Method))
		return
	}
	if ctx.Err() != nil {
		return
	}
	output := &deadlineWriter{w: w, ctx: ctx, header: make(http.Header)}
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		b.Next.ServeHTTP(output, r)
	}()
	select {
	case panicValue := <-done:
		if panicValue != nil {
			panic(panicValue)
		}
		if ctx.Err() == context.DeadlineExceeded {
			output.expire(timeoutDetail(r.Method))
		}
	case <-ctx.Done():
		// Unblock any in-progress socket write before serializing shutdown.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
		if ctx.Err() == context.DeadlineExceeded {
			output.expire(timeoutDetail(r.Method))
		} else {
			output.mu.Lock()
			output.expired = true
			output.mu.Unlock()
		}
		// Keep the outer request tracker and upload staging alive until the
		// handler actually exits, including when it ignores cancellation.
		<-done
	}
}
