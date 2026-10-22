package transport

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/andybalholm/brotli"
)

var ErrUnsafePublicURL = errors.New("public_url_denied")
var ErrPublicAdmissionUnavailable = errors.New("admission_unavailable")

type PublicHTTPStatusError struct {
	StatusCode        int
	RetryAfterSeconds *int
}

type PublicChallengeError struct{}

func (*PublicChallengeError) Error() string { return "upstream_challenge" }

// IsChallengePage recognizes explicit interstitial markup, rather than ordinary
// articles which mention Cloudflare or CAPTCHA in their prose.
func IsChallengePage(body []byte) bool {
	h := strings.ToLower(string(body))
	return strings.Contains(h, `id="challenge-form"`) || strings.Contains(h, `id='challenge-form'`) || strings.Contains(h, `id="cf-challenge-running"`) || strings.Contains(h, `id='cf-challenge-running'`) || (strings.Contains(h, "<title>just a moment") && strings.Contains(h, "cf_chl_"))
}

func parseRetryAfter(raw string, now time.Time) *int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var n int64
	if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if value < 0 {
			return nil
		}
		n = value
	} else {
		date, err := http.ParseTime(raw)
		if err != nil {
			return nil
		}
		delta := date.Sub(now)
		if delta > 0 {
			n = int64((delta + time.Second - 1) / time.Second)
		}
	}
	if n > 86400 {
		n = 86400
	}
	value := int(n)
	return &value
}

func (e *PublicHTTPStatusError) Error() string {
	return fmt.Sprintf("public_http_status: %d", e.StatusCode)
}

// Deny special-use ranges, including transition mechanisms which can tunnel to
// private IPv4 destinations. All DNS answers must be public before dialing.
var deniedPublicPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, s := range []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "::/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "fc00::/7", "fe80::/10", "ff00::/8"} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.Zone() != "" {
		return false
	}
	// Current global IPv6 allocation is 2000::/3. Do not treat deprecated
	// site-local addresses or unallocated ranges as public destinations.
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, p := range deniedPublicPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// ValidatePublicURL performs syntax and literal-address admission. DNS is
// validated again at the actual socket dial, never just before an unsafe dial.
func ValidatePublicURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Opaque != "" {
		return ErrUnsafePublicURL
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.Contains(host, "%") {
		return ErrUnsafePublicURL
	}
	if port := u.Port(); port != "" && port != "80" && port != "443" {
		return ErrUnsafePublicURL
	}
	if ip, e := netip.ParseAddr(host); e == nil && !publicIP(ip) {
		return ErrUnsafePublicURL
	}
	return nil
}

type publicResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func resolvePublic(ctx context.Context, resolver publicResolver, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicIP(ip) {
			return nil, ErrUnsafePublicURL
		}
		return []netip.Addr{ip}, nil
	}
	ips, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, ErrPublicAdmissionUnavailable
	}
	for _, ip := range ips {
		if !publicIP(ip) {
			return nil, ErrUnsafePublicURL
		}
	}
	return ips, nil
}

// AdmitPublicURL validates DNS before delegating a browser-only strategy to a
// different runtime. HTTP requests still revalidate and pin at the actual dial.
// An unresolved destination is not permission to bypass public URL admission.
func AdmitPublicURL(ctx context.Context, raw string) error {
	return admitPublicURLWithResolver(ctx, raw, net.DefaultResolver)
}

func admitPublicURLWithResolver(ctx context.Context, raw string, resolver publicResolver) error {
	if err := ValidatePublicURL(raw); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	u, _ := url.Parse(raw)
	_, err := resolvePublic(bounded, resolver, u.Hostname())
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrUnsafePublicURL) {
			return ErrUnsafePublicURL
		}
		return ErrPublicAdmissionUnavailable
	}
	return nil
}

func publicDial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := resolvePublic(ctx, net.DefaultResolver, host)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, ErrUnsafePublicURL) {
			return nil, err
		}
		return nil, ErrPublicAdmissionUnavailable
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	var last error
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, last
}

type publicResponse struct {
	body   []byte
	header http.Header
	status int
	url    string
	wire   int64
}

// publicGet owns the transport. No injected client, proxy environment, cookies,
// redirect credentials or alternate dial path can bypass socket admission.
func publicGet(ctx context.Context, raw string, opts Options) (publicResponse, error) {
	return publicGetMode(ctx, raw, opts, true)
}

func publicGetMode(ctx context.Context, raw string, opts Options, followRedirects bool) (publicResponse, error) {
	var out publicResponse
	if opts.Client != nil {
		return out, errors.New("public_custom_client_denied")
	}
	if err := ValidatePublicURL(raw); err != nil {
		return out, err
	}
	wireLimit, err := pageLimit(opts.MaxWireBytes)
	if err != nil {
		return out, err
	}
	decodedLimit, err := pageLimit(opts.MaxDecodedBytes)
	if err != nil {
		return out, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	if timeout < 0 || timeout > 30*time.Second {
		return out, errors.New("public_invalid_timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tr := &http.Transport{Proxy: nil, DialContext: publicDial, DisableCompression: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: timeout, MaxResponseHeaderBytes: 64 * 1024, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if err := ValidatePublicURL(req.URL.String()); err != nil {
			return err
		}
		if !followRedirects {
			return http.ErrUseLastResponse
		}
		if len(via) >= 5 {
			return errors.New("public_redirect_limit")
		}
		return nil
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return out, err
	}
	setHeaders(req.Header, opts.AlternateHeaders)
	resp, err := client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && (followRedirects || resp.StatusCode < 300 || resp.StatusCode >= 400) {
		return out, &PublicHTTPStatusError{StatusCode: resp.StatusCode, RetryAfterSeconds: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
	}
	wire, err := readBounded(resp.Body, wireLimit, extractor.ErrWireLimit)
	if err != nil {
		return out, err
	}
	body, err := publicDecode(wire, resp.Header.Get("Content-Encoding"), decodedLimit)
	if err != nil {
		return out, err
	}
	if int64(len(body)) > decodedLimit {
		return out, extractor.ErrOutputLimit
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return publicResponse{body: body, header: resp.Header.Clone(), status: resp.StatusCode, url: resp.Request.URL.String(), wire: int64(len(wire))}, nil
}

// The wire has already been bounded. Limit decompression itself to the output
// budget, including when the remaining output budget is smaller than wire.
func publicDecode(wire []byte, encoding string, outputLimit int64) ([]byte, error) {
	raw := bytes.NewReader(wire)
	var reader io.Reader = raw
	var closeReader io.Closer
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
	case "gzip":
		z, e := gzip.NewReader(raw)
		if e != nil {
			return nil, e
		}
		z.Multistream(false)
		reader = z
		closeReader = z
	case "deflate":
		z, e := zlib.NewReader(raw)
		if e != nil {
			return nil, e
		}
		reader = z
		closeReader = z
	case "br":
		reader = brotli.NewReader(raw)
	default:
		return nil, errors.New("unsupported_content_encoding")
	}
	if closeReader != nil {
		defer closeReader.Close()
	}
	body, err := readBounded(reader, outputLimit, extractor.ErrOutputLimit)
	if err != nil {
		return nil, err
	}
	if raw.Len() != 0 {
		return nil, errors.New("trailing_compressed_data")
	}
	return body, nil
}

// FetchPublic fetches public HTTP(S) content with independently bounded wire,
// decompressed and UTF-8 output and a validated, pinned destination socket.
func FetchPublic(ctx context.Context, raw string, opts Options) (Document, error) {
	r, err := publicGet(ctx, raw, opts)
	if err != nil {
		return Document{}, err
	}
	limit, _ := pageLimit(opts.MaxDecodedBytes)
	body, encoding, err := decodeCharset(r.body, r.header.Get("Content-Type"), limit)
	if err != nil {
		return Document{}, err
	}
	if IsChallengePage(body) {
		return Document{}, &PublicChallengeError{}
	}
	return Document{HTML: body, StatusCode: r.status, Encoding: encoding, URL: r.url}, nil
}
