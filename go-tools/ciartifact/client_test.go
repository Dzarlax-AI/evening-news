package ciartifact

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func archiveBytes(t *testing.T, name string) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, e := z.Create(name)
	if e != nil {
		t.Fatal(e)
	}
	w.Write([]byte("data"))
	if e = z.Close(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func testServer(t *testing.T, edit func(*artifact, *workflowRun), body []byte) (*Client, *httptest.Server) {
	t.Helper()
	h := sha256.Sum256(body)
	a := artifact{ID: 1, Name: "core-candidate-" + testHead, Digest: "sha256:" + hex.EncodeToString(h[:]), CreatedAt: time.Now()}
	a.WorkflowRun.ID = 2
	a.WorkflowRun.HeadSHA = testHead
	a.WorkflowRun.HeadBranch = "main"
	a.WorkflowRun.RepositoryID = 10
	a.WorkflowRun.HeadRepositoryID = 10
	r := workflowRun{ID: 2, HeadSHA: testHead, HeadBranch: "main", Event: "push", Status: "completed", Conclusion: "success", Path: mainWorkflow, Repository: repository{10, "owner/repo"}, HeadRepository: repository{10, "owner/repo"}}
	if edit != nil {
		edit(&a, &r)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/actions/artifacts"):
			json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "artifacts": []artifact{a}})
		case strings.HasSuffix(req.URL.Path, "/actions/runs/2"):
			json.NewEncoder(w).Encode(r)
		case strings.HasSuffix(req.URL.Path, "/actions/artifacts/1/zip"):
			w.Write(body)
		default:
			http.Error(w, "secret body", 500)
		}
	}))
	return newClient(srv.URL, "secret", true), srv
}
func TestProvenance(t *testing.T) {
	tests := []struct {
		name string
		edit func(*artifact, *workflowRun)
		want string
	}{{"valid", nil, ""}, {"expired", func(a *artifact, _ *workflowRun) { a.Expired = true }, "notfound"}, {"missing digest", func(a *artifact, _ *workflowRun) { a.Digest = "" }, "artifact_invalid"}, {"fork", func(a *artifact, r *workflowRun) {
		r.HeadRepository.ID = 11
		r.HeadRepository.FullName = "fork/repo"
		a.WorkflowRun.HeadRepositoryID = 11
	}, "notfound"}, {"pr", func(_ *artifact, r *workflowRun) { r.Event = "pull_request" }, "notfound"}, {"wrong workflow", func(_ *artifact, r *workflowRun) { r.Path = ".github/workflows/other.yml" }, "notfound"}, {"wrong head", func(a *artifact, r *workflowRun) {
		r.HeadSHA = strings.Repeat("b", 40)
		a.WorkflowRun.HeadSHA = r.HeadSHA
	}, "head_mismatch"}, {"failed", func(_ *artifact, r *workflowRun) { r.Conclusion = "failure" }, "notfound"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, s := testServer(t, tt.edit, []byte("x"))
			defer s.Close()
			m, e := c.Find("owner/repo", "candidate", testHead, "", "", 0)
			if tt.want == "" {
				if e != nil || !m.Found {
					t.Fatalf("%+v %v", m, e)
				}
			} else if tt.want == "notfound" {
				if e != nil || m.Found {
					t.Fatalf("expected absent, got %+v %v", m, e)
				}
			} else if e == nil || e.Error() != tt.want {
				t.Fatalf("got %v", e)
			}
		})
	}
}
func TestCurrentRunRestriction(t *testing.T) {
	c, s := testServer(t, func(_ *artifact, r *workflowRun) { r.Status = "in_progress"; r.Conclusion = "" }, []byte("x"))
	defer s.Close()
	t.Setenv("GITHUB_RUN_ID", "2")
	if _, e := c.Find("owner/repo", "candidate", testHead, "", "", 2); e != nil {
		t.Fatal(e)
	}
	if m, e := c.Find("owner/repo", "candidate", testHead, "", "", 3); e != nil || m.Found {
		t.Fatal("accepted other run")
	}
	t.Setenv("GITHUB_RUN_ID", "3")
	if m, e := c.Find("owner/repo", "candidate", testHead, "", "", 2); e != nil || m.Found {
		t.Fatal("accepted forged current run")
	}
}
func TestDownloadRejectsTraversalAndDigest(t *testing.T) {
	for _, name := range []string{"../escape", "safe"} {
		t.Run(name, func(t *testing.T) {
			b := archiveBytes(t, name)
			c, s := testServer(t, nil, b)
			defer s.Close()
			if name == "safe" {
				c, s = testServer(t, func(a *artifact, _ *workflowRun) { a.Digest = "sha256:" + strings.Repeat("0", 64) }, b)
				defer s.Close()
			}
			dir := filepath.Join(t.TempDir(), "out")
			e := c.Download("owner/repo", "candidate", testHead, "core-candidate-"+testHead, "", 2, 0, dir, false)
			if e == nil {
				t.Fatal("accepted invalid archive")
			}
			if _, e = os.Stat(dir); !os.IsNotExist(e) {
				t.Fatal("destination remains")
			}
		})
	}
}
func TestHTTPErrorNotAbsence(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "secret body", 500) }))
	defer s.Close()
	c := newClient(s.URL, "secret", true)
	_, e := c.Find("owner/repo", "candidate", testHead, "", "", 0)
	if e == nil || e.Error() != "github_http_error" {
		t.Fatalf("%v", e)
	}
}
func TestRedirectStripsToken(t *testing.T) {
	got := ""
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.Write([]byte("ok"))
	}))
	defer dst.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, dst.URL, http.StatusFound) }))
	defer src.Close()
	c := newClient(src.URL, "secret", true)
	r, e := c.request("/x")
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if got != "" {
		t.Fatal("token sent to redirected host")
	}
}
func TestAncestorRejectsNewer(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"init", "-q", dir}, {"-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "first"}, {"-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "second"}} {
		if b, e := exec.Command("git", args...).CombinedOutput(); e != nil {
			t.Fatalf("%s %v", b, e)
		}
	}
	get := func(ref string) string {
		b, e := exec.Command("git", "-C", dir, "rev-parse", ref).Output()
		if e != nil {
			t.Fatal(e)
		}
		return strings.TrimSpace(string(b))
	}
	if e := ancestor(dir, get("HEAD"), get("HEAD~1")); e == nil {
		t.Fatal("accepted newer baseline")
	}
}
func TestDuplicateAndPagination(t *testing.T) {
	for _, tc := range []struct {
		count     int
		duplicate bool
		want      string
	}{{2, true, "artifact_ambiguous"}, {101, false, "artifact_history_truncated"}} {
		t.Run(fmt.Sprint(tc.count), func(t *testing.T) {
			c, s := testServer(t, nil, []byte("x"))
			defer s.Close()
			up := c.base
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/actions/artifacts") {
					resp, e := http.Get(up + r.URL.RequestURI())
					if e != nil {
						t.Fatal(e)
					}
					defer resp.Body.Close()
					var v struct {
						Artifacts []artifact `json:"artifacts"`
					}
					json.NewDecoder(resp.Body).Decode(&v)
					if tc.duplicate {
						v.Artifacts = append(v.Artifacts, v.Artifacts[0])
					}
					json.NewEncoder(w).Encode(map[string]any{"total_count": tc.count, "artifacts": v.Artifacts})
					return
				}
				http.Redirect(w, r, up+r.URL.RequestURI(), http.StatusTemporaryRedirect)
			}))
			defer srv.Close()
			c = newClient(srv.URL, "secret", true)
			_, e := c.Find("owner/repo", "candidate", testHead, "", "", 0)
			if e == nil || e.Error() != tc.want {
				t.Fatalf("%v", e)
			}
		})
	}
}

func TestBaselineLatestCannotRollBack(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		b, e := exec.Command("git", args...).CombinedOutput()
		if e != nil {
			t.Fatalf("git failed: %v", e)
		}
		return strings.TrimSpace(string(b))
	}
	git("init", "-q", dir)
	git("-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "first")
	older := git("-C", dir, "rev-parse", "HEAD")
	git("-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "second")
	newer := git("-C", dir, "rev-parse", "HEAD")
	img := func(repo, revision, id string) map[string]string {
		return map[string]string{"repository": repo, "revision": revision, "platform": "linux/amd64", "imageID": "sha256:" + strings.Repeat(id, 64), "digest": "sha256:" + strings.Repeat(id, 64)}
	}
	manifest, _ := json.Marshal(map[string]any{"schemaVersion": 1, "revision": newer, "platform": "linux/amd64", "images": map[string]any{"web": img("dzarlax-ai/rss-web", newer, "1"), "background": img("dzarlax-ai/rss-background", newer, "2"), "executor": img("dzarlax-ai/rss-summarizer", newer, "3")}})
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	w, _ := z.Create("release-images.json")
	w.Write(manifest)
	z.Close()
	data := buf.Bytes()
	hash := sha256.Sum256(data)
	makeArtifact := func(id int64, rev string, when time.Time) artifact {
		a := artifact{ID: id, Name: "release-images-" + rev, Digest: "sha256:" + hex.EncodeToString(hash[:]), CreatedAt: when}
		a.WorkflowRun.ID = id
		a.WorkflowRun.HeadSHA = rev
		a.WorkflowRun.HeadBranch = "main"
		a.WorkflowRun.RepositoryID = 10
		a.WorkflowRun.HeadRepositoryID = 10
		return a
	}
	artifacts := []artifact{makeArtifact(1, older, time.Now().Add(-time.Hour)), makeArtifact(2, newer, time.Now())}
	pending := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/actions/artifacts"):
			json.NewEncoder(w).Encode(map[string]any{"total_count": 2, "artifacts": artifacts})
		case strings.Contains(r.URL.Path, "/actions/runs/"):
			id := int64(1)
			rev := older
			if strings.HasSuffix(r.URL.Path, "/2") {
				id = 2
				rev = newer
			}
			status, conclusion := "completed", "success"
			if id == 2 && pending {
				status, conclusion = "in_progress", ""
			}
			json.NewEncoder(w).Encode(workflowRun{ID: id, HeadSHA: rev, HeadBranch: "main", Event: "workflow_dispatch", Status: status, Conclusion: conclusion, Path: mainWorkflow, Repository: repository{10, "owner/repo"}, HeadRepository: repository{10, "owner/repo"}})
		case strings.HasSuffix(r.URL.Path, "/actions/artifacts/2/zip"):
			w.Write(data)
		default:
			http.Error(w, "failure", 500)
		}
	}))
	defer srv.Close()
	c := newClient(srv.URL, "secret", true)
	if _, e := c.Baseline("owner/repo", dir, older); e == nil || e.Error() != "ancestry_unproven" {
		t.Fatalf("latest rollback accepted: %v", e)
	}
	got, e := c.Baseline("owner/repo", dir, newer)
	if e != nil || !bytes.Equal(got, manifest) {
		t.Fatalf("valid baseline rejected: %v", e)
	}
	pending = true
	if _, e := c.Baseline("owner/repo", dir, newer); e == nil || e.Error() != "baseline_publication_pending" {
		t.Fatalf("pending newer publication must block fallback: %v", e)
	}
}

func TestPRExceptionOnlyCurrentCandidate(t *testing.T) {
	b := archiveBytes(t, "image.tar")
	c, s := testServer(t, func(a *artifact, r *workflowRun) {
		r.Event = "pull_request"
		r.Status = "in_progress"
		r.Conclusion = ""
		r.HeadBranch = "feature"
		a.WorkflowRun.HeadBranch = "feature"
		r.HeadRepository.ID = 11
		r.HeadRepository.FullName = "fork/repo"
		a.WorkflowRun.HeadRepositoryID = 11
	}, b)
	defer s.Close()
	t.Setenv("GITHUB_RUN_ID", "2")
	name := "core-candidate-" + testHead
	if e := c.Download("owner/repo", "candidate", testHead, name, "", 2, 2, filepath.Join(t.TempDir(), "out"), true); e != nil {
		t.Fatalf("current PR failed: %v", e)
	}
	if e := c.Download("owner/repo", "candidate", testHead, name, "", 2, 0, filepath.Join(t.TempDir(), "out"), true); e == nil {
		t.Fatal("accepted non-current PR")
	}
	if m, e := c.Find("owner/repo", "candidate", testHead, "", "", 2); e != nil || m.Found {
		t.Fatal("PR candidate trusted by find")
	}
}

func TestFindSkipsFailedAndExpiredHistory(t *testing.T) {
	now := time.Now()
	mk := func(id int64, expired bool) artifact {
		a := artifact{ID: id, Name: "core-candidate-" + testHead, Digest: "sha256:" + strings.Repeat("a", 64), CreatedAt: now.Add(time.Duration(id) * time.Second), Expired: expired}
		a.WorkflowRun.ID = id
		a.WorkflowRun.HeadSHA = testHead
		a.WorkflowRun.HeadBranch = "main"
		a.WorkflowRun.RepositoryID = 10
		a.WorkflowRun.HeadRepositoryID = 10
		return a
	}
	all := []artifact{mk(3, true), mk(2, false), mk(1, false)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/actions/artifacts") {
			json.NewEncoder(w).Encode(map[string]any{"total_count": len(all), "artifacts": all})
			return
		}
		id := int64(1)
		conclusion := "success"
		if strings.HasSuffix(r.URL.Path, "/2") {
			id = 2
			conclusion = "failure"
		}
		json.NewEncoder(w).Encode(workflowRun{ID: id, HeadSHA: testHead, HeadBranch: "main", Event: "push", Status: "completed", Conclusion: conclusion, Path: mainWorkflow, Repository: repository{10, "owner/repo"}, HeadRepository: repository{10, "owner/repo"}})
	}))
	defer server.Close()
	c := newClient(server.URL, "secret", true)
	m, e := c.Find("owner/repo", "candidate", testHead, "", "", 0)
	if e != nil || !m.Found || m.RunID != 1 {
		t.Fatalf("expected older trusted run: %+v %v", m, e)
	}
	all = all[:2]
	m, e = c.Find("owner/repo", "candidate", testHead, "", "", 0)
	if e != nil || m.Found {
		t.Fatalf("expected absence: %+v %v", m, e)
	}
}
func TestStandaloneGateEarlierHead(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		b, e := exec.Command("git", args...).CombinedOutput()
		if e != nil {
			t.Fatalf("git failed: %v", e)
		}
		return strings.TrimSpace(string(b))
	}
	git("init", "-q", dir)
	git("-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "first")
	old := git("-C", dir, "rev-parse", "HEAD")
	git("-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "second")
	newer := git("-C", dir, "rev-parse", "HEAD")
	k := strings.Repeat("a", 64)
	name := "core-acceptance-" + old + "-" + k
	b := archiveBytes(t, "receipt.json")
	c, s := testServer(t, func(a *artifact, r *workflowRun) {
		a.Name = name
		a.WorkflowRun.HeadSHA = newer
		r.HeadSHA = newer
		r.Event = "workflow_dispatch"
		r.Path = gateWorkflow
	}, b)
	defer s.Close()
	m, e := c.Find("owner/repo", "gate", old, k, dir, 0)
	if e != nil || !m.Found || m.HeadSHA != newer {
		t.Fatalf("gate rejected: %+v %v", m, e)
	}
	if e = c.Download("owner/repo", "gate", old, name, dir, 2, 0, filepath.Join(t.TempDir(), "gate"), false); e != nil {
		t.Fatalf("gate download rejected: %v", e)
	}
	if _, e = c.Find("owner/repo", "gate", old, k, t.TempDir(), 0); e == nil {
		t.Fatal("unprovable ancestry accepted")
	}
	// The tested revision may not be newer than the workflow's producer revision.
	wrongName := "core-acceptance-" + newer + "-" + k
	c2, s3 := testServer(t, func(a *artifact, r *workflowRun) {
		a.Name = wrongName
		a.WorkflowRun.HeadSHA = old
		r.HeadSHA = old
		r.Event = "workflow_dispatch"
		r.Path = gateWorkflow
	}, b)
	defer s3.Close()
	if _, err := c2.Find("owner/repo", "gate", newer, k, dir, 0); err == nil {
		t.Fatal("non-ancestor tested SHA accepted")
	}
	c, s2 := testServer(t, func(a *artifact, r *workflowRun) {
		a.Name = name
		a.WorkflowRun.HeadSHA = newer
		r.HeadSHA = newer
		r.Event = "workflow_dispatch"
		r.Path = mainWorkflow
	}, b)
	defer s2.Close()
	if _, e = c.Find("owner/repo", "gate", old, k, dir, 0); e == nil {
		t.Fatal("main workflow wrong head accepted")
	}
}
