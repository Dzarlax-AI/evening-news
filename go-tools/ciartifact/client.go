// Package ciartifact resolves and verifies artifacts from trusted GitHub Actions runs.
package ciartifact

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var sha = regexp.MustCompile(`^[0-9a-f]{40}$`)
var key = regexp.MustCompile(`^[0-9a-f]{64}$`)
var digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var repoPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
var artifactName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,200}$`)

const maxArchive int64 = 2 << 30
const maxExtract int64 = 4 << 30
const maxPages = 20
const mainWorkflow = ".github/workflows/docker-build-push.yml"
const gateWorkflow = ".github/workflows/core-integration.yml"

func fail(code string) error { return errors.New(code) }

type repository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
}
type workflowRun struct {
	ID             int64      `json:"id"`
	HeadSHA        string     `json:"head_sha"`
	HeadBranch     string     `json:"head_branch"`
	Event          string     `json:"event"`
	Status         string     `json:"status"`
	Conclusion     string     `json:"conclusion"`
	Path           string     `json:"path"`
	Repository     repository `json:"repository"`
	HeadRepository repository `json:"head_repository"`
}
type artifact struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Expired     bool      `json:"expired"`
	Digest      string    `json:"digest"`
	CreatedAt   time.Time `json:"created_at"`
	WorkflowRun struct {
		ID               int64  `json:"id"`
		HeadSHA          string `json:"head_sha"`
		HeadBranch       string `json:"head_branch"`
		RepositoryID     int64  `json:"repository_id"`
		HeadRepositoryID int64  `json:"head_repository_id"`
	} `json:"workflow_run"`
}
type Metadata struct {
	ID      int64  `json:"id"`
	RunID   int64  `json:"run_id"`
	Name    string `json:"name"`
	Digest  string `json:"digest"`
	HeadSHA string `json:"head_sha"`
	Found   bool   `json:"found"`
}
type Client struct {
	base          string
	token         string
	http          *http.Client
	allowLoopback bool
}

func New(token string) *Client { return newClient("https://api.github.com", token, false) }
func newClient(base, token string, loop bool) *Client {
	c := &Client{base: base, token: token, allowLoopback: loop}
	c.http = &http.Client{Timeout: 15 * time.Minute, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 5 || !c.safeURL(req.URL) {
			return fail("redirect_invalid")
		}
		if !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
			req.Header.Del("Authorization")
		}
		return nil
	}}
	return c
}
func (c *Client) safeURL(u *url.URL) bool {
	if u.Scheme == "https" {
		return true
	}
	if !c.allowLoopback || u.Scheme != "http" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}
func (c *Client) request(endpoint string) (*http.Response, error) {
	u, e := url.Parse(c.base + endpoint)
	if e != nil || !c.safeURL(u) {
		return nil, fail("request_invalid")
	}
	req, e := http.NewRequest("GET", u.String(), nil)
	if e != nil {
		return nil, fail("request_invalid")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, e := c.http.Do(req)
	if e != nil {
		return nil, fail("github_request_failed")
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fail("github_http_error")
	}
	return resp, nil
}
func (c *Client) json(endpoint string, out any) error {
	r, e := c.request(endpoint)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	if dec.Decode(out) != nil {
		return fail("github_response_invalid")
	}
	return nil
}
func (c *Client) list(repo, name string) ([]artifact, error) {
	var all []artifact
	for p := 1; p <= maxPages; p++ {
		var page struct {
			Total     int        `json:"total_count"`
			Artifacts []artifact `json:"artifacts"`
		}
		q := url.Values{"per_page": {"100"}, "page": {strconv.Itoa(p)}}
		if name != "" {
			q.Set("name", name)
		}
		if e := c.json("/repos/"+repo+"/actions/artifacts?"+q.Encode(), &page); e != nil {
			return nil, e
		}
		all = append(all, page.Artifacts...)
		if len(page.Artifacts) < 100 {
			if page.Total > len(all) {
				return nil, fail("artifact_history_truncated")
			}
			return all, nil
		}
		if p == maxPages {
			return nil, fail("artifact_history_truncated")
		}
	}
	return nil, fail("artifact_history_truncated")
}
func (c *Client) run(repo string, id int64) (workflowRun, error) {
	var r workflowRun
	e := c.json(fmt.Sprintf("/repos/%s/actions/runs/%d", repo, id), &r)
	return r, e
}
func validateArgs(repo, head string) error {
	if !repoPattern.MatchString(repo) || strings.Contains(repo, "..") || !sha.MatchString(head) {
		return fail("arguments_invalid")
	}
	return nil
}
func (c *Client) trusted(repo, kind, head string, a artifact, current int64, allowPR bool) (workflowRun, error) {
	if a.ID <= 0 || a.WorkflowRun.ID <= 0 || !digest.MatchString(a.Digest) {
		return workflowRun{}, fail("artifact_invalid")
	}
	r, e := c.run(repo, a.WorkflowRun.ID)
	if e != nil {
		return r, e
	}
	currentRun := current > 0 && current == r.ID && strconv.FormatInt(current, 10) == os.Getenv("GITHUB_RUN_ID")
	prException := allowPR && kind == "candidate" && currentRun && r.Event == "pull_request" && r.Status == "in_progress"
	if allowPR && !prException {
		return r, fail("current_pr_invalid")
	}
	if r.ID != a.WorkflowRun.ID || !sha.MatchString(r.HeadSHA) || a.WorkflowRun.HeadSHA != r.HeadSHA || r.Repository.ID <= 0 || r.HeadRepository.ID <= 0 || r.Repository.ID != a.WorkflowRun.RepositoryID || r.HeadRepository.ID != a.WorkflowRun.HeadRepositoryID || !strings.EqualFold(r.Repository.FullName, repo) || a.WorkflowRun.HeadBranch != r.HeadBranch {
		return r, fail("provenance_invalid")
	}
	if !prException && (r.Repository.ID != r.HeadRepository.ID || !strings.EqualFold(r.HeadRepository.FullName, repo) || r.HeadBranch != "main") {
		return r, fail("untrusted_run")
	}
	if kind != "fixture" && head != "" && r.HeadSHA != head && !(kind == "gate" && r.Path == gateWorkflow && r.Event == "workflow_dispatch") {
		return r, fail("head_mismatch")
	}
	if r.Path != mainWorkflow && !((kind == "gate" || kind == "fixture") && r.Path == gateWorkflow) {
		return r, fail("workflow_invalid")
	}
	if r.Event != "push" && r.Event != "workflow_dispatch" && !prException {
		return r, fail("event_invalid")
	}
	if kind == "baseline" && r.Event != "workflow_dispatch" {
		return r, fail("event_invalid")
	}
	if !(r.Status == "completed" && r.Conclusion == "success") && !(currentRun && r.Status == "in_progress" && kind != "baseline") {
		return r, fail("run_incomplete")
	}
	return r, nil
}
func isAncestor(root, base, head string) (bool, error) {
	if root == "" || !sha.MatchString(base) || !sha.MatchString(head) {
		return false, fail("ancestry_invalid")
	}
	err := exec.Command("git", "-C", root, "merge-base", "--is-ancestor", base, head).Run()
	if err == nil {
		return true, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, fail("ancestry_unproven")
}
func ancestor(root, base, head string) error {
	ok, e := isAncestor(root, base, head)
	if e != nil {
		return e
	}
	if !ok {
		return fail("ancestry_unproven")
	}
	return nil
}
func meta(a artifact, r workflowRun) Metadata {
	return Metadata{a.ID, r.ID, a.Name, a.Digest, r.HeadSHA, true}
}
func nameFor(kind, head, k string) (string, error) {
	switch kind {
	case "candidate":
		return "core-candidate-" + head, nil
	case "gate":
		if !key.MatchString(k) {
			break
		}
		return "core-acceptance-" + head + "-" + k, nil
	case "fixture":
		if !key.MatchString(k) {
			break
		}
		return "extractor-fixture-" + k, nil
	}
	return "", fail("arguments_invalid")
}
func (c *Client) Find(repo, kind, head, k, root string, current int64) (Metadata, error) {
	if e := validateArgs(repo, head); e != nil {
		return Metadata{}, e
	}
	n, e := nameFor(kind, head, k)
	if e != nil {
		return Metadata{}, e
	}
	all, e := c.list(repo, n)
	if e != nil {
		return Metadata{}, e
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID > all[j].ID
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})
	seen := map[int64]bool{}
	for _, a := range all {
		if a.Name != n {
			continue
		}
		if seen[a.WorkflowRun.ID] {
			return Metadata{}, fail("artifact_ambiguous")
		}
		seen[a.WorkflowRun.ID] = true
	}
	for _, a := range all {
		if a.Name != n {
			continue
		}
		if a.Expired {
			continue
		}
		r, e := c.trusted(repo, kind, head, a, current, false)
		if e != nil {
			switch e.Error() {
			case "run_incomplete", "untrusted_run", "workflow_invalid", "event_invalid":
				continue
			}
			return Metadata{}, e
		}
		if kind == "gate" && r.Path == gateWorkflow && r.Event == "workflow_dispatch" && r.HeadSHA != head {
			if e = ancestor(root, head, r.HeadSHA); e != nil {
				return Metadata{}, e
			}
		}
		if kind == "fixture" {
			ok, err := isAncestor(root, r.HeadSHA, head)
			if err != nil {
				return Metadata{}, err
			}
			if !ok {
				continue
			}
		}
		return meta(a, r), nil
	}
	return Metadata{Found: false}, nil
}
func (c *Client) Baseline(repo, root, head string) ([]byte, error) {
	if e := validateArgs(repo, head); e != nil {
		return nil, e
	}
	all, e := c.list(repo, "")
	if e != nil {
		return nil, e
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID > all[j].ID
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})
	for _, a := range all {
		if !strings.HasPrefix(a.Name, "release-images-") {
			continue
		}
		if a.Expired {
			return nil, fail("artifact_invalid")
		}
		r, e := c.trusted(repo, "baseline", "", a, 0, false)
		if e != nil { // Untrusted workflow runs are not publications; API failures remain errors.
			if e.Error() == "run_incomplete" {
				return nil, fail("baseline_publication_pending")
			}
			if e.Error() == "github_http_error" || e.Error() == "github_request_failed" || e.Error() == "github_response_invalid" || e.Error() == "artifact_invalid" {
				return nil, e
			}
			continue
		}
		if a.Name != "release-images-"+r.HeadSHA || a.CreatedAt.IsZero() {
			return nil, fail("artifact_invalid")
		}
		if e = ancestor(root, r.HeadSHA, head); e != nil {
			return nil, e
		}
		f, e := c.archive(repo, a, 128<<10)
		if e != nil {
			return nil, e
		}
		defer os.Remove(f)
		contents, e := singleManifest(f)
		if e != nil {
			return nil, e
		}
		if e = validateManifest(contents, r.HeadSHA); e != nil {
			return nil, e
		}
		return contents, nil
	}
	return nil, fail("baseline_not_found")
}
func (c *Client) archive(repo string, a artifact, limit int64) (string, error) {
	r, e := c.request(fmt.Sprintf("/repos/%s/actions/artifacts/%d/zip", repo, a.ID))
	if e != nil {
		return "", e
	}
	defer r.Body.Close()
	f, e := os.CreateTemp("", "ci-artifact-*.zip")
	if e != nil {
		return "", fail("archive_unavailable")
	}
	name := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(name)
		}
	}()
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(f, h), io.LimitReader(r.Body, limit+1))
	if e != nil || n > limit {
		return "", fail("archive_invalid")
	}
	if "sha256:"+hex.EncodeToString(h.Sum(nil)) != a.Digest {
		return "", fail("artifact_digest_mismatch")
	}
	if f.Close() != nil {
		return "", fail("archive_unavailable")
	}
	ok = true
	return name, nil
}
func safeEntries(z *zip.ReadCloser, maxSize uint64) ([]*zip.File, error) {
	if len(z.File) == 0 || len(z.File) > 128 {
		return nil, fail("archive_entries_invalid")
	}
	seen := map[string]bool{}
	var total uint64
	for _, f := range z.File {
		n := f.Name
		clean := path.Clean(n)
		if n == "" || strings.Contains(n, "\\") || strings.HasPrefix(n, "/") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != strings.TrimSuffix(n, "/") || f.Mode()&os.ModeSymlink != 0 || (!f.Mode().IsRegular() && !f.FileInfo().IsDir()) || seen[clean] {
			return nil, fail("archive_path_invalid")
		}
		seen[clean] = true
		if f.UncompressedSize64 > maxSize-total {
			return nil, fail("archive_size_invalid")
		}
		total += f.UncompressedSize64
	}
	return z.File, nil
}
func singleManifest(p string) ([]byte, error) {
	z, e := zip.OpenReader(p)
	if e != nil {
		return nil, fail("archive_invalid")
	}
	defer z.Close()
	files, e := safeEntries(z, 64<<10)
	if e != nil {
		return nil, e
	}
	if len(files) != 1 || files[0].Name != "release-images.json" || !files[0].Mode().IsRegular() {
		return nil, fail("manifest_archive_invalid")
	}
	r, e := files[0].Open()
	if e != nil {
		return nil, fail("archive_invalid")
	}
	defer r.Close()
	b, e := io.ReadAll(io.LimitReader(r, (64<<10)+1))
	if e != nil || len(b) > 64<<10 {
		return nil, fail("manifest_archive_invalid")
	}
	return b, nil
}
func validateManifest(b []byte, head string) error {
	var m struct {
		Version          int    `json:"schemaVersion"`
		Revision         string `json:"revision"`
		Platform         string `json:"platform"`
		BaselineRevision string `json:"baselineRevision"`
		Images           map[string]struct {
			Repository        string `json:"repository"`
			Revision          string `json:"revision"`
			Platform          string `json:"platform"`
			ImageID           string `json:"imageID"`
			Digest            string `json:"digest"`
			SourceFingerprint string `json:"sourceFingerprint"`
			Origin            string `json:"origin"`
		} `json:"images"`
	}
	if json.Unmarshal(b, &m) != nil || m.Version < 1 || m.Version > 2 || m.Revision != head || m.Platform != "linux/amd64" || len(m.Images) != 3 {
		return fail("manifest_invalid")
	}
	if m.Version == 2 && !sha.MatchString(m.BaselineRevision) {
		return fail("manifest_invalid")
	}
	repositories := map[string]string{"web": "dzarlax-ai/rss-web", "background": "dzarlax-ai/rss-background", "executor": "dzarlax-ai/rss-summarizer"}
	ids := map[string]bool{}
	for target, repo := range repositories {
		i, ok := m.Images[target]
		if !ok || i.Repository != repo || !sha.MatchString(i.Revision) || i.Platform != m.Platform || !digest.MatchString(i.ImageID) || !digest.MatchString(i.Digest) || ids[i.ImageID] || (m.Version == 1 && i.Revision != head) {
			return fail("manifest_invalid")
		}
		if m.Version == 2 && (!key.MatchString(i.SourceFingerprint) || (i.Origin != "built" && i.Origin != "reused") || (i.Origin == "built" && i.Revision != head)) {
			return fail("manifest_invalid")
		}
		ids[i.ImageID] = true
	}
	return nil
}
func (c *Client) Download(repo, kind, head, name, root string, runID, current int64, output string, allowPR bool) error {
	if allowPR && kind != "candidate" {
		return fail("arguments_invalid")
	}
	if e := validateArgs(repo, head); e != nil {
		return e
	}
	if runID <= 0 || !artifactName.MatchString(name) || output == "" {
		return fail("arguments_invalid")
	}
	switch kind {
	case "candidate":
		if name != "core-candidate-"+head {
			return fail("arguments_invalid")
		}
	case "gate":
		if !strings.HasPrefix(name, "core-acceptance-"+head+"-") || !key.MatchString(strings.TrimPrefix(name, "core-acceptance-"+head+"-")) {
			return fail("arguments_invalid")
		}
	case "fixture":
		if !strings.HasPrefix(name, "extractor-fixture-") || !key.MatchString(strings.TrimPrefix(name, "extractor-fixture-")) {
			return fail("arguments_invalid")
		}
	default:
		return fail("arguments_invalid")
	}
	all, e := c.list(repo, name)
	if e != nil {
		return e
	}
	var matches []artifact
	for _, a := range all {
		if a.Name == name && a.WorkflowRun.ID == runID {
			matches = append(matches, a)
		}
	}
	if len(matches) != 1 {
		return fail("artifact_missing_or_ambiguous")
	}
	a := matches[0]
	if a.Expired {
		return fail("artifact_invalid")
	}
	run, e := c.trusted(repo, kind, head, a, current, allowPR)
	if e != nil {
		return e
	}
	if kind == "gate" && run.Path == gateWorkflow && run.Event == "workflow_dispatch" && run.HeadSHA != head {
		if e = ancestor(root, head, run.HeadSHA); e != nil {
			return e
		}
	}
	if kind == "fixture" {
		if e = ancestor(root, run.HeadSHA, head); e != nil {
			return e
		}
	}
	f, e := c.archive(repo, a, maxArchive)
	if e != nil {
		return e
	}
	defer os.Remove(f)
	z, e := zip.OpenReader(f)
	if e != nil {
		return fail("archive_invalid")
	}
	defer z.Close()
	files, e := safeEntries(z, uint64(maxExtract))
	if e != nil {
		return e
	}
	if e = os.Mkdir(output, 0700); e != nil {
		return fail("destination_unavailable")
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(output)
		}
	}()
	for _, entry := range files {
		dest := filepath.Join(output, filepath.FromSlash(entry.Name))
		if entry.FileInfo().IsDir() {
			if os.MkdirAll(dest, 0700) != nil {
				return fail("extract_failed")
			}
			continue
		}
		if os.MkdirAll(filepath.Dir(dest), 0700) != nil {
			return fail("extract_failed")
		}
		w, e := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return fail("extract_failed")
		}
		r, e := entry.Open()
		if e != nil {
			w.Close()
			return fail("extract_failed")
		}
		n, copyErr := io.Copy(w, io.LimitReader(r, int64(entry.UncompressedSize64)+1))
		closeErr := r.Close()
		writeErr := w.Close()
		if copyErr != nil || closeErr != nil || writeErr != nil || n != int64(entry.UncompressedSize64) {
			return fail("extract_failed")
		}
	}
	success = true
	return nil
}
