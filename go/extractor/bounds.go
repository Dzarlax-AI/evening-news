package extractor

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"strings"

	"github.com/andybalholm/brotli"
)

var ErrWireLimit = errors.New("extraction_wire_limit")
var ErrOutputLimit = errors.New("extraction_http_limit")

// DecodePage enforces wire and decoded byte bounds without trusting Content-Length.
// It is an offline transport primitive: callers supply a captured response reader.
func DecodePage(wire io.Reader, encoding string, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid_byte_limit")
	}
	raw, err := io.ReadAll(io.LimitReader(wire, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, ErrWireLimit
	}
	r := bytes.NewReader(raw)
	var decoded io.Reader = r
	var closer io.Closer
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
	case "gzip":
		g, e := gzip.NewReader(r)
		if e != nil {
			return nil, e
		}
		g.Multistream(false)
		decoded = g
		closer = g
	case "deflate":
		z, e := zlib.NewReader(r)
		if e != nil {
			return nil, e
		}
		decoded = z
		closer = z
	case "br":
		decoded = brotli.NewReader(r)
	default:
		return nil, errors.New("unsupported_content_encoding")
	}
	if closer != nil {
		defer closer.Close()
	}
	out, err := io.ReadAll(io.LimitReader(decoded, int64(limit)+1))
	if len(out) > limit {
		return nil, ErrOutputLimit
	}
	if err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, errors.New("trailing_compressed_data")
	}
	return out, nil
}
