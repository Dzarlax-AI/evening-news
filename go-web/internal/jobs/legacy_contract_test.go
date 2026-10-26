package jobs

import (
	"reflect"
	"testing"
)

func TestManagementArticleCommandsAcceptTypedIDsAndPersistLegacyDefaults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload map[string]any
		want    map[string]any
	}{
		{
			name:    "single article reprocess defaults",
			payload: map[string]any{"article_ids": []int64{51}},
			want:    map[string]any{"article_ids": []int{51}, "force": true, "reextract_media": false},
		},
		{
			name:    "explicit reprocess options",
			payload: map[string]any{"article_ids": []int64{52}, "force": false, "reextract_media": true},
			want:    map[string]any{"article_ids": []int{52}, "force": false, "reextract_media": true},
		},
		{
			name:    "selected retry IDs and defaults",
			payload: map[string]any{"article_ids": []int64{13, 12}, "force": false, "reextract_media": false},
			want:    map[string]any{"article_ids": []int{12, 13}, "force": false, "reextract_media": false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ValidatePayload("article_reprocess", tc.payload)
			if err != nil {
				t.Fatalf("valid management payload rejected: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("normalized payload=%#v want=%#v", got, tc.want)
			}
		})
	}
}
