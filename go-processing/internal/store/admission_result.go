package store

import "encoding/json"

func articleProcessingItemResult(p Persisted) []byte {
	result := map[string]any{"state": p.State, "error": p.Error}
	if admission, ok := p.Fields["content_admission"].(map[string]any); ok {
		verdict, _ := admission["verdict"].(string)
		if verdict == "keep" || verdict == "exclude" || verdict == "uncertain" {
			result["content_admission"] = admission
		}
	}
	raw, _ := json.Marshal(result)
	return raw
}
