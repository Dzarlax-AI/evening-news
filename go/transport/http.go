// Package transport contains isolated, loopback-only prototype adapters.
package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/gogs/chardet"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

const (
	MaxPageBytes    = 5 * 1024 * 1024
	ChromeUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	SafariUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

var ErrNonLoopback = errors.New("prototype_http_non_loopback")

// Options can tighten the fixed prototype bounds; zero values use the defaults.
// Client is optional and permits test TLS configuration. The adapter copies it,
// installs its own redirect policy, and preserves the caller's transport.
type Options struct {
	Client           *http.Client
	AlternateHeaders bool
	Timeout          time.Duration
	MaxWireBytes     int64
	MaxDecodedBytes  int64
}

// Document contains UTF-8 HTML and the observed response decoding metadata.
type Document struct {
	HTML       []byte
	StatusCode int
	Encoding   string
	URL        string
}

// Fetch requests only loopback HTTP(S) URLs. It performs one header profile per
// call, so callers explicitly control whether alternate HTTP is attempted.
func Fetch(ctx context.Context, rawURL string, opts Options) (Document, error) {
	var empty Document
	u, err := url.Parse(rawURL)
	if err != nil {
		return empty, fmt.Errorf("prototype_http_url: %w", err)
	}
	if err := validateLoopbackURL(u); err != nil {
		return empty, err
	}
	wireLimit, err := pageLimit(opts.MaxWireBytes)
	if err != nil {
		return empty, err
	}
	decodedLimit, err := pageLimit(opts.MaxDecodedBytes)
	if err != nil {
		return empty, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	if timeout < 0 || timeout > 30*time.Second {
		return empty, errors.New("prototype_http_invalid_timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := http.Client{Timeout: timeout}
	if opts.Client != nil {
		client = *opts.Client
	}
	if client.Timeout <= 0 || client.Timeout > timeout {
		client.Timeout = timeout
	}
	if client.Transport == nil {
		client.Transport = &http.Transport{
			Proxy:                 nil,
			DialContext:           dialLoopback,
			DisableCompression:    true,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: timeout,
			MaxIdleConns:          2,
			IdleConnTimeout:       time.Second,
		}
		defer client.CloseIdleConnections()
	}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := validateLoopbackURL(req.URL); err != nil {
			return err
		}
		if len(via) > 5 {
			return errors.New("prototype_http_redirect_limit")
		}
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return empty, err
	}
	setHeaders(req.Header, opts.AlternateHeaders)
	response, err := client.Do(req)
	if err != nil {
		return empty, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return empty, fmt.Errorf("prototype_http_status: %d", response.StatusCode)
	}
	// An injected transport must not silently decompress, otherwise wire bounds
	// and the advertised content encoding could no longer be verified.
	if response.Uncompressed {
		return empty, errors.New("prototype_http_auto_decompressed")
	}
	wire, err := readBounded(response.Body, wireLimit, extractor.ErrWireLimit)
	if err != nil {
		return empty, err
	}
	sharedLimit := max(wireLimit, decodedLimit)
	body, err := extractor.DecodePage(bytes.NewReader(wire), response.Header.Get("Content-Encoding"), int(sharedLimit))
	if err != nil {
		return empty, err
	}
	if int64(len(body)) > decodedLimit {
		return empty, extractor.ErrOutputLimit
	}
	html, encoding, err := decodeCharset(body, response.Header.Get("Content-Type"), decodedLimit)
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	finalURL := u.String()
	if response.Request != nil && response.Request.URL != nil {
		finalURL = response.Request.URL.String()
	}
	return Document{HTML: html, StatusCode: response.StatusCode, Encoding: encoding, URL: finalURL}, nil
}

func pageLimit(value int64) (int64, error) {
	if value == 0 {
		return MaxPageBytes, nil
	}
	if value < 0 || value > MaxPageBytes {
		return 0, errors.New("prototype_http_invalid_byte_limit")
	}
	return value, nil
}

func validateLoopbackURL(u *url.URL) error {
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Opaque != "" {
		return errors.New("prototype_http_invalid_url")
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return ErrNonLoopback
	}
	return nil
}

func dialLoopback(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, ErrNonLoopback
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
}

func setHeaders(headers http.Header, alternate bool) {
	ua := ChromeUserAgent
	if alternate {
		ua = SafariUserAgent
	}
	for name, value := range map[string]string{
		"User-Agent":      ua,
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
		"Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
		"Accept-Encoding": "gzip, deflate, br",
		"DNT":             "1", "Connection": "keep-alive", "Upgrade-Insecure-Requests": "1",
		"Cache-Control": "no-cache", "Pragma": "no-cache",
	} {
		headers.Set(name, value)
	}
}

func readBounded(reader io.Reader, limit int64, limitErr error) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(data)) > limit {
		return nil, limitErr
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func decodeCharset(body []byte, contentType string, limit int64) ([]byte, string, error) {
	_, params, _ := mime.ParseMediaType(contentType)
	label := params["charset"]
	if label != "" {
		if enc, name := charset.Lookup(label); enc != nil {
			if !strings.EqualFold(name, "utf-8") || utf8.Valid(body) {
				decoded, err := readBounded(transform.NewReader(bytes.NewReader(body), enc.NewDecoder()), limit, extractor.ErrOutputLimit)
				return decoded, name, err
			}
		}
	}
	enc, name, certain := charset.DetermineEncoding(body, "")
	if !certain && utf8.Valid(body) {
		return body, "utf-8", nil
	}
	if !certain {
		if detected, err := chardet.NewHtmlDetector().DetectBest(body); err == nil {
			if detectedEncoding, detectedName := charset.Lookup(detected.Charset); detectedEncoding != nil {
				enc, name = detectedEncoding, detectedName
			}
		}
	}
	decoded, err := readBounded(transform.NewReader(bytes.NewReader(body), enc.NewDecoder()), limit, extractor.ErrOutputLimit)
	return decoded, name, err
}
