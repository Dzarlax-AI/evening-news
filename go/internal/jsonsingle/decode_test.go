package jsonsingle

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type sample struct {
	Value string `json:"value"`
}

func TestDecodeSingleValueAndBounds(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    string
		limit    int64
		wantSize bool
		wantErr  bool
	}{
		{"leading_and_trailing_whitespace", " \t\r\n{\"value\":\"ok\"} \n", 64, false, false},
		{"exact_byte_limit", "{\"value\":\"ok\"}     ", 19, false, false},
		{"large_leading_whitespace", strings.Repeat(" ", 1<<20) + `{"value":"ok"}`, (1 << 20) + 64, false, false},
		{"whitespace_only", "   \n", 8, false, true},
		{"multiple_values", "{\"value\":\"ok\"} {}", 64, false, true},
		{"trailing_garbage", "{\"value\":\"ok\"} x", 64, false, true},
		{"unknown_field", "{\"value\":\"ok\",\"extra\":1}", 64, false, true},
		{"utf8_whitespace_is_invalid", "\u00a0{\"value\":\"ok\"}", 64, false, true},
		{"bom_is_invalid", "\ufeff{\"value\":\"ok\"}", 64, false, true},
		{"overflow_whitespace", "{\"value\":\"ok\"}           ", 16, true, false},
		{"overflow_garbage_after_malformed_prefix", "{bad               ", 8, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got sample
			tooLarge, err := Decode(strings.NewReader(tt.input), tt.limit, &got)
			if tooLarge != tt.wantSize || (err != nil) != tt.wantErr {
				t.Fatalf("tooLarge=%v err=%v, want %v/%v", tooLarge, err, tt.wantSize, tt.wantErr)
			}
			if !tooLarge && err == nil && got.Value != "ok" {
				t.Fatalf("decoded value=%q", got.Value)
			}
		})
	}
}

type bytesAndErrorReader struct {
	data []byte
	err  error
}

func (r *bytesAndErrorReader) Read(p []byte) (int, error) {
	if r.data == nil {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = nil
	return n, r.err
}

func TestDecodePropagatesReaderErrorAfterValidValue(t *testing.T) {
	want := errors.New("synthetic read failure")
	input := &bytesAndErrorReader{data: []byte(`{"value":"ok"}`), err: want}
	var got sample
	tooLarge, err := Decode(input, 64, &got)
	if tooLarge || !errors.Is(err, want) {
		t.Fatalf("tooLarge=%v err=%v", tooLarge, err)
	}
}

type emptyThenBytesReader struct {
	empty bool
	data  []byte
}

func (r *emptyThenBytesReader) Read(p []byte) (int, error) {
	if !r.empty {
		r.empty = true
		return 0, nil
	}
	if r.data == nil {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = nil
	return n, nil
}

func TestDrainContinuesAfterTransientEmptyRead(t *testing.T) {
	trailing := false
	if _, err := drainJSONWhitespace(&emptyThenBytesReader{data: []byte("x")}, &trailing); err != io.EOF {
		t.Fatalf("drain error=%v", err)
	}
	if !trailing {
		t.Fatal("trailing data after transient empty read was missed")
	}
}
