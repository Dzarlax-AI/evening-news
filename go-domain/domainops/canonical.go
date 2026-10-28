package domainops

import (
	"bytes"
	"encoding/json"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
)

// The domain-operation fingerprint uses Python ensure_ascii=False, including
// literal U+2028 and U+2029. Callback fingerprints deliberately escape them.
func canonicalDomainJSON(raw json.RawMessage) ([]byte, error) {
	encoded, err := callbacks.CanonicalJSON(raw)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	for i := 0; i < len(encoded); {
		if encoded[i] != '\\' {
			out.WriteByte(encoded[i])
			i++
			continue
		}
		start := i
		for i < len(encoded) && encoded[i] == '\\' {
			i++
		}
		count := i - start
		if count%2 == 1 && i+5 <= len(encoded) && (string(encoded[i:i+5]) == "u2028" || string(encoded[i:i+5]) == "u2029") {
			out.Write(encoded[start : i-1])
			if encoded[i+4] == '8' {
				out.WriteString("\u2028")
			} else {
				out.WriteString("\u2029")
			}
			i += 5
		} else {
			out.Write(encoded[start:i])
		}
	}
	return out.Bytes(), nil
}
