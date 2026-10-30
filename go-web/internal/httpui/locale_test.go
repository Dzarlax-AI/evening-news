package httpui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestCatalogSubstitutionsPreservePlaceholders(t *testing.T) {
	placeholder := regexp.MustCompile(`\$\{[^}]+\}|%[a-zA-Z]`)
	for ru, sr := range uiCatalog {
		if strings.TrimSpace(ru) == "" || strings.TrimSpace(sr) == "" {
			t.Fatal("empty catalog entry")
		}
		a, b := placeholder.FindAllString(ru, -1), placeholder.FindAllString(sr, -1)
		if strings.Join(a, "\x00") != strings.Join(b, "\x00") {
			t.Errorf("placeholder mismatch for %q", ru)
		}
	}
}
func TestSerbianOnboardingSourceInventory(t *testing.T) {
	cyrillic := regexp.MustCompile(`[А-Яа-яЁё]`)
	for _, path := range []string{"../../templates/admin/login.html", "../../templates/admin/setup.html", "../../../web/static/js/onboarding.js"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if cyrillic.MatchString(translateUISource(string(b), "sr-Latn")) {
			t.Errorf("untranslated onboarding source: %s", path)
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/login?locale=sr-Latn", nil)
	serveLocalizedSetup(w, r, "../../templates/admin/login.html")
	if !strings.Contains(w.Body.String(), `lang="sr-Latn"`) || !strings.Contains(w.Body.String(), "Dobro došli nazad") || !strings.Contains(w.Body.String(), "onboarding.js?locale=sr-Latn") {
		t.Fatalf("localized login missing expected output: %s", w.Body.String())
	}
}

func TestSourceTranslationDoesNotChangeRenderedUserData(t *testing.T) {
	reader := &localizedFakeReader{&fakeReader{article: map[string]any{"title": "Категории", "summary": "Новости", "is_advertisement": false}}}
	mux := http.NewServeMux()
	testHandler(reader).Register(mux)
	w := request(mux, "GET", "/article/1?locale=sr-Latn")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Категории") {
		t.Fatalf("user data changed: status=%d body=%s", w.Code, w.Body.String())
	}
}
func TestLocalizedETagAndCanonical(t *testing.T) {
	reader := &localizedFakeReader{&fakeReader{}}
	mux := http.NewServeMux()
	testHandler(reader).Register(mux)
	ru := request(mux, "GET", "/search?q=cloud&locale=ru")
	sr := request(mux, "GET", "/search?q=cloud&locale=sr-Latn")
	if ru.Header().Get("ETag") == "" || sr.Header().Get("ETag") == "" || ru.Header().Get("ETag") == sr.Header().Get("ETag") {
		t.Fatal("locale variants share an ETag")
	}
	if !strings.Contains(sr.Body.String(), `hreflang="sr-Latn"`) || !strings.Contains(sr.Body.String(), `rel="canonical"`) {
		t.Fatal("locale metadata missing")
	}
	r := httptest.NewRequest("GET", "/search?q=cloud&locale=sr-Latn", nil)
	r.Header.Set("If-None-Match", sr.Header().Get("ETag"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusNotModified {
		t.Fatalf("conditional request status=%d", w.Code)
	}
}

func TestSerbianAdminCatalogInventory(t *testing.T) {
	cyrillic := regexp.MustCompile(`[А-Яа-яЁё]+`)
	for _, name := range []string{"admin_base.html", "categories.html", "sources.html", "summaries.html", "settings.html", "backup.html", "stats.html", "dashboard.html"} {
		path := "../../templates/admin/" + name
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := translateUISource(string(b), "sr-Latn")
		if matches := cyrillic.FindAllString(text, -1); len(matches) > 0 {
			t.Errorf("%s has untranslated source: %v", name, matches)
		}
	}
}

func TestLocaleClientBehavior(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is unavailable")
	}
	cmd := exec.Command(node, "../../test/locale_behavior.cjs")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("locale behavior: %v\n%s", err, output)
	}
}

func TestSerbianModalLabelsOriginalText(t *testing.T) {
	b, err := os.ReadFile("../../templates/public/fragments/article_modal.html")
	if err != nil {
		t.Fatal(err)
	}
	source := translateUISource(string(b), "sr-Latn")
	for _, want := range []string{`id="modalOriginalTextHeading" hidden>Izvorni tekst</h2>`, "Prikaži izvorni tekst", "Sakrij izvorni tekst", "contentEl.setAttribute('lang', 'und')", "contentEl.removeAttribute('lang')"} {
		if !strings.Contains(source, want) {
			t.Errorf("modal is missing %q", want)
		}
	}
}
