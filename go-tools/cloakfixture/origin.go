// Package cloakfixture contains isolated fixtures for native browser acceptance.
package cloakfixture

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var fixtureID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

const fixtureBody = "The synthetic harbor team compared a new water sensor with a marked staff gauge during several morning inspections. The replacement records short surges that the older hourly readings missed, stores observations while its radio link is down, and keeps each original timestamp when it uploads delayed measurements. Engineers will review calibration notes and maintenance time before deciding whether to repeat the trial near the ferry terminal. This fixture contains no publisher content."

type arrival struct {
	Case string  `json:"case"`
	Job  string  `json:"job"`
	At   float64 `json:"at"`
}
type snapshot struct {
	Version   int       `json:"schema_version"`
	Active    int       `json:"active"`
	MaxActive int       `json:"max_active"`
	Arrivals  []arrival `json:"arrivals"`
	Completed int       `json:"completed"`
	Errors    int       `json:"errors"`
}

// Origin exposes only synthetic article routes and persists bounded barrier state.
type Origin struct {
	dir     string
	mu      sync.Mutex
	state   snapshot
	started time.Time
}

func NewOrigin(dir string) (*Origin, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("origin_state_unavailable")
	}
	o := &Origin{dir: dir, started: time.Now(), state: snapshot{Version: 1, Arrivals: []arrival{}}}
	if err := o.persist(); err != nil {
		return nil, err
	}
	return o, nil
}
func (o *Origin) persist() error {
	b, err := json.Marshal(o.state)
	if err != nil {
		return fmt.Errorf("origin_state_unavailable")
	}
	f, err := os.CreateTemp(o.dir, "origin-state.tmp-")
	if err != nil {
		return fmt.Errorf("origin_state_unavailable")
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err == nil {
		err = os.Rename(name, filepath.Join(o.dir, "origin-state.json"))
	}
	if err != nil {
		return fmt.Errorf("origin_state_unavailable")
	}
	return nil
}
func (o *Origin) arrive(c, j string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.state.Arrivals) >= 128 {
		return fmt.Errorf("origin_capacity")
	}
	previousMax := o.state.MaxActive
	o.state.Active++
	if o.state.Active > o.state.MaxActive {
		o.state.MaxActive = o.state.Active
	}
	o.state.Arrivals = append(o.state.Arrivals, arrival{c, j, time.Since(o.started).Seconds()})
	if err := o.persist(); err != nil {
		o.state.Active--
		o.state.MaxActive = previousMax
		o.state.Arrivals = o.state.Arrivals[:len(o.state.Arrivals)-1]
		return err
	}
	return nil
}
func (o *Origin) finish(failed bool) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.state.Active--
	o.state.Completed++
	if failed {
		o.state.Errors++
	}
	return o.persist()
}
func (o *Origin) released(c string) bool {
	p := filepath.Join(o.dir, "release", c)
	i, e := os.Lstat(p)
	return e == nil && i.Mode().IsRegular() && i.Size() <= 32
}
func (o *Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "close")
	if r.Method != http.MethodGet {
		http.Error(w, "fixture route rejected", 404)
		return
	}
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, "ok")
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) == 2 && parts[0] == "error" && fixtureID.MatchString(parts[1]) {
		if o.arrive(parts[1], "error") != nil {
			http.Error(w, "fixture state unavailable", 503)
			return
		}
		if o.finish(true) != nil {
			http.Error(w, "fixture state unavailable", 503)
			return
		}
		o.article(w, 503, "Synthetic upstream error")
		return
	}
	if len(parts) != 3 || parts[0] != "hold" || !fixtureID.MatchString(parts[1]) || !fixtureID.MatchString(parts[2]) {
		http.Error(w, "fixture route rejected", 404)
		return
	}
	c, j := parts[1], parts[2]
	if o.arrive(c, j) != nil {
		http.Error(w, "fixture state unavailable", 503)
		return
	}
	timer := time.NewTimer(90 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	released := o.released(c)
	for !released {
		select {
		case <-r.Context().Done():
			_ = o.finish(true)
			return
		case <-timer.C:
			_ = o.finish(true)
			http.Error(w, "fixture barrier timed out", 504)
			return
		case <-ticker.C:
			released = o.released(c)
		}
	}
	if o.finish(false) != nil {
		http.Error(w, "fixture state unavailable", 503)
		return
	}
	o.article(w, 200, "Fixture "+c+" "+j)
}
func (*Origin) article(w http.ResponseWriter, status int, title string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	title = html.EscapeString(title)
	fmt.Fprintf(w, `<!doctype html><html><head><title>%s</title></head><body><main data-ready="true"><article><h1>%s</h1><p>%s</p></article></main></body></html>`, title, title, fixtureBody)
}
