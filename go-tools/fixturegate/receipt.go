package fixturegate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// WriteReceipt records only images that completed the gate and cleanup.
// The destination must not already exist; no raw fixture data is included.
func WriteReceipt(dir string, images map[string]Image) error {
	if dir == "" || len(images) != 3 {
		return errors.New("invalid_acceptance")
	}
	refs := map[string]string{"web": "evening-news-web:consolidation-fixture", "background": "evening-news-background:consolidation-fixture", "executor": "evening-news-executor:consolidation-fixture"}
	ids, revisions := map[string]string{}, map[string]string{}
	seen := map[string]bool{}
	common := ""
	for _, target := range targets {
		v, ok := images[target]
		if !ok || !imageID.MatchString(v.ID) || !revision.MatchString(v.Revision) || seen[v.ID] || (common != "" && common != v.Revision) {
			return errors.New("invalid_acceptance")
		}
		common = v.Revision
		seen[v.ID] = true
		ids[target] = v.ID
		revisions[target] = v.Revision
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(map[string]any{"result": "passed", "image_ids": ids, "image_revisions": revisions, "image_tags": refs}, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "receipt.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(filepath.Join(dir, "receipt.json"))
		return err
	}
	return nil
}
