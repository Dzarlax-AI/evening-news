package main

import (
	"golang.org/x/text/encoding/charmap"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSyntheticOrigins(t *testing.T) {
	o := &origin{}
	for _, p := range []string{"/healthz", "/news", "/s/fixture_channel", "/rss.xml?edition=one", "/active-sigterm/0123456789ab"} {
		w := httptest.NewRecorder()
		o.ServeHTTP(w, httptest.NewRequest("GET", p, nil))
		if w.Code != 200 {
			t.Fatalf("%s: %d", p, w.Code)
		}
	}
	if o.pageFetches.Load() != 1 {
		t.Fatal("page count")
	}
	w := httptest.NewRecorder()
	o.ServeHTTP(w, httptest.NewRequest("GET", "/rss.xml?edition=two", nil))
	raw, e := charmap.Windows1251.NewDecoder().Bytes(w.Body.Bytes())
	if e != nil || !strings.Contains(string(raw), "Новости: выпуск два") {
		t.Fatal("windows-1251 fixture changed")
	}
	w = httptest.NewRecorder()
	o.ServeHTTP(w, httptest.NewRequest("GET", "/active-sigterm/invalid", nil))
	if w.Code != 404 {
		t.Fatal("invalid marker accepted")
	}
}
