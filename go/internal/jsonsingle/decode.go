// Package jsonsingle decodes one bounded JSON value without asking encoding/json
// to buffer arbitrarily long leading or trailing whitespace.
package jsonsingle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var ErrTrailingData = errors.New("unexpected trailing JSON data")

// Decode reads at most maxBytes+1 bytes, decodes one value with unknown fields
// rejected, then drains the bounded remainder. The extra byte distinguishes an
// exact-size value from an oversized input, including malformed inputs.
func Decode(r io.Reader, maxBytes int64, dst any) (tooLarge bool, err error) {
	limited := &io.LimitedReader{R: r, N: maxBytes + 1}
	tracked := &errorTrackingReader{r: limited}
	buffered := bufio.NewReaderSize(tracked, 32*1024)

	// Consume only JSON's four ASCII whitespace bytes before constructing the
	// decoder. Decoder.Decode otherwise buffers a long prefix while searching
	// for the first value.
	var first []byte
	var prefixErr error
	for {
		b, readErr := buffered.ReadByte()
		if readErr != nil {
			if readErr != io.EOF {
				prefixErr = readErr
			}
			break
		}
		if !isJSONWhitespace(b) {
			first = []byte{b}
			break
		}
	}

	var input io.Reader = buffered
	if len(first) != 0 {
		input = io.MultiReader(bytes.NewReader(first), buffered)
	}
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(dst)

	trailing := false
	_, drainErr := drainJSONWhitespace(io.MultiReader(decoder.Buffered(), buffered), &trailing)

	// A non-EOF reader error can accompany bytes containing a complete JSON
	// value. Preserve it even when encoding/json accepted those bytes.
	if tracked.err != nil {
		err = tracked.err
	} else if prefixErr != nil {
		err = prefixErr
	} else if drainErr != nil && drainErr != io.EOF {
		err = drainErr
	} else if limited.N == 0 {
		tooLarge = true
	} else if decodeErr != nil {
		err = decodeErr
	} else if trailing {
		err = ErrTrailingData
	}
	return tooLarge, err
}

type errorTrackingReader struct {
	r   io.Reader
	err error
}

func (r *errorTrackingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF && r.err == nil {
		r.err = err
	}
	return n, err
}

func drainJSONWhitespace(r io.Reader, trailing *bool) (int64, error) {
	var total int64
	var emptyReads int
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		for _, b := range buf[:n] {
			if !isJSONWhitespace(b) {
				*trailing = true
			}
		}
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return total, io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
	}
}

func isJSONWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}
