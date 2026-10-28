// Package media refreshes media for a selected historical Telegram post.
package media

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/andybalholm/brotli"
	"github.com/jackc/pgx/v5"
	"golang.org/x/net/html/charset"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var channelRE = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)
var numberRE = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

type Service struct {
	HTTP     *http.Client
	MaxBytes int64
}

func New() *Service {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &Service{HTTP: &http.Client{Transport: tr, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, MaxBytes: 5 * 1024 * 1024}
}
func postParts(raw string) []string {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.User != nil || (!strings.EqualFold(u.Hostname(), "t.me") && !strings.EqualFold(u.Hostname(), "telegram.me")) || (u.Port() != "" && u.Port() != "443") || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || u.EscapedPath() != u.Path || strings.ContainsAny(raw, "?#") {
		return nil
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) > 0 && parts[0] == "s" {
		parts = parts[1:]
	}
	return parts
}
func PostTarget(article, source string) (string, string, bool) {
	post, channel := postParts(article), postParts(source)
	if len(post) != 2 || len(channel) != 1 || !channelRE.MatchString(post[0]) || !strings.EqualFold(post[0], channel[0]) || !numberRE.MatchString(post[1]) {
		return "", "", false
	}
	id := post[0] + "/" + post[1]
	return "https://t.me/s/" + id, id, true
}
func (s *Service) fetch(ctx context.Context, target string) (string, bool, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if e != nil {
		return "", false, e
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	resp, e := s.HTTP.Do(req)
	if e != nil {
		return "", false, e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", false, nil
	}
	limit := s.MaxBytes
	if limit < 1 {
		return "", false, errors.New("invalid media limit")
	}
	wire, e := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if e != nil || int64(len(wire)) > limit {
		return "", false, errors.New("media exceeds limit")
	}
	var reader io.Reader = bytes.NewReader(wire)
	switch strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		g, err := gzip.NewReader(reader)
		if err != nil {
			return "", false, err
		}
		defer g.Close()
		reader = g
	case "deflate":
		f, err := zlib.NewReader(reader)
		if err != nil {
			return "", false, err
		}
		defer f.Close()
		reader = f
	case "br":
		reader = brotli.NewReader(reader)
	default:
		return "", false, errors.New("unsupported content encoding")
	}
	data, e := io.ReadAll(io.LimitReader(reader, limit+1))
	if e != nil || int64(len(data)) > limit {
		return "", false, errors.New("media exceeds limit")
	}
	_, params, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if label := params["charset"]; label != "" && !strings.EqualFold(label, "utf-8") {
		if enc, _ := charset.Lookup(label); enc != nil {
			decoded, err := io.ReadAll(io.LimitReader(enc.NewDecoder().Reader(bytes.NewReader(data)), limit*4+1))
			if err != nil || int64(len(decoded)) > limit*4 {
				return "", false, errors.New("media exceeds limit")
			}
			data = decoded
		}
	}
	if !utf8.Valid(data) {
		data = []byte(strings.ToValidUTF8(string(data), "�"))
	}
	return string(data), true, nil
}
func (s *Service) Refresh(ctx context.Context, f authority.Fence, articleID int64) (bool, error) {
	tx, e := f.Pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if e = f.Validate(ctx, tx); e != nil {
		return false, e
	}
	var articleURL string
	var sourceID *int64
	e = tx.QueryRow(ctx, `SELECT url,source_id FROM articles WHERE id=$1`, articleID).Scan(&articleURL, &sourceID)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, errors.New("article missing")
	}
	if e != nil {
		return false, e
	}
	if sourceID == nil {
		return false, nil
	}
	var sourceURL, kind string
	e = tx.QueryRow(ctx, `SELECT url,source_type FROM sources WHERE id=$1`, *sourceID).Scan(&sourceURL, &kind)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if kind != "telegram" {
		return false, nil
	}
	if e = tx.Rollback(ctx); e != nil {
		return false, e
	}
	target, id, ok := PostTarget(articleURL, sourceURL)
	if !ok {
		return false, nil
	}
	html, ok, e := s.fetch(ctx, target)
	if e != nil {
		return false, e
	}
	if !ok || html == "" {
		return false, nil
	}
	files, found, e := extractor.ParseTelegramPostMediaBounded(html, target, id, int(s.MaxBytes*4))
	if e != nil {
		return false, e
	}
	if !found {
		return false, nil
	}
	encoded, e := json.Marshal(files)
	if e != nil {
		return false, e
	}
	var image any
	for _, file := range files {
		if file["type"] == "image" {
			if v, ok := file["url"].(string); ok {
				image = v
			}
			break
		}
	}
	tx2, e := f.Pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx2.Rollback(c)
	}()
	if e = f.Validate(ctx, tx2); e != nil {
		return false, e
	}
	var currentURL, currentKind string
	e = tx2.QueryRow(ctx, `SELECT s.url,s.source_type FROM articles a JOIN sources s ON s.id=a.source_id WHERE a.id=$1 AND a.source_id=$2 AND a.url=$3 FOR UPDATE OF a FOR SHARE OF s`, articleID, *sourceID, articleURL).Scan(&currentURL, &currentKind)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if currentURL != sourceURL || currentKind != "telegram" {
		return false, nil
	}
	if _, e = tx2.Exec(ctx, `UPDATE articles SET media_files=$2::jsonb,image_url=$3 WHERE id=$1`, articleID, encoded, image); e != nil {
		return false, e
	}
	if e = tx2.Commit(ctx); e != nil {
		return false, e
	}
	return true, nil
}
