// Package webbackend serves the private backend used by the existing Go web UI.
package webbackend

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Boundary struct {
	Token  string
	Worker bool
	Next   http.Handler
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
			if ctx.Err() == context.DeadlineExceeded {
				reject(w, 504, "Operation timed out; outcome may be unknown. Verify before retrying.")
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
			if ctx.Err() == context.DeadlineExceeded {
				detail := "Operation timed out; outcome may be unknown. Verify before retrying."
				if r.Method == http.MethodGet || r.Method == http.MethodHead {
					detail = "Operation timed out"
				}
				reject(w, 504, detail)
			}
			return
		}
		if int64(len(data)) > limit {
			reject(w, 413, "Request body exceeds limit")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
	}
	if ctx.Err() != nil {
		return
	}
	b.Next.ServeHTTP(w, r)
}
