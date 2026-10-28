// extractor-candidate-metadata records a test-only image archive digest.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var imageIDPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var sourcePattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func record(ref, id, source, archive, output string) error {
	if ref == "" || !imageIDPattern.MatchString(id) || !sourcePattern.MatchString(source) || archive == "" || output == "" {
		return errors.New("invalid_metadata")
	}
	in, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("invalid_archive")
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, in); err != nil {
		return err
	}
	out, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	err = enc.Encode(map[string]string{"artifact_type": "ci_test_only_candidate", "image_ref": ref, "image_id": id, "source_sha": source, "archive": filepath.Base(archive), "archive_sha256": hex.EncodeToString(hash.Sum(nil))})
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(output)
	}
	return err
}
func main() {
	ref := flag.String("image-ref", "", "")
	id := flag.String("image-id", "", "")
	source := flag.String("source-sha", "", "")
	archive := flag.String("archive", "", "")
	output := flag.String("output", "", "")
	flag.Parse()
	if flag.NArg() != 0 || record(*ref, *id, *source, *archive, *output) != nil {
		fmt.Fprintln(os.Stderr, "candidate metadata failed")
		os.Exit(1)
	}
}
