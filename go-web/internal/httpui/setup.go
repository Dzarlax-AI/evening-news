package httpui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterSetup(mux *http.ServeMux, service *auth.Service, pool *pgxpool.Pool, installationID string) {
	mux.HandleFunc("GET /setup", func(w http.ResponseWriter, r *http.Request) {
		var configured bool
		err := pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM managed_admin WHERE id=1)`).Scan(&configured)
		if err != nil {
			http.Error(w, "Setup unavailable", 503)
			return
		}
		if configured {
			if _, err = service.AdminPage(r); err != nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
		}
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFile(w, r, "templates/admin/setup.html")
	})
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFile(w, r, "templates/admin/login.html")
	})
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			var step string
			var complete bool
			var revision int64
			err := pool.QueryRow(r.Context(), `SELECT onboarding_step,onboarding_completed,revision FROM installation_state WHERE id=1 AND installation_id=$1`, installationID).Scan(&step, &complete, &revision)
			if err != nil {
				writeProblem(w, 503, "Installation unavailable")
				return
			}
			writeJSON(w, 200, map[string]any{"step": step, "completed": complete, "revision": revision})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		var in struct {
			Step             string `json:"step"`
			Completed        bool   `json:"completed"`
			ExpectedRevision int64  `json:"expected_revision"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&in) != nil || dec.Decode(new(any)) != io.EOF || in.ExpectedRevision < 1 || !validSetupStep(in.Step) {
			writeProblem(w, 422, "Invalid progress")
			return
		}
		var revision int64
		err := pool.QueryRow(r.Context(), `UPDATE installation_state SET onboarding_step=$1,onboarding_completed=$2,revision=revision+1,updated_at=now() WHERE id=1 AND installation_id=$3 AND revision=$4 RETURNING revision`, in.Step, in.Completed, installationID, in.ExpectedRevision).Scan(&revision)
		if errors.Is(err, pgx.ErrNoRows) {
			writeProblem(w, 409, "Progress changed; reload")
			return
		}
		if err != nil {
			writeProblem(w, 503, "Installation unavailable")
			return
		}
		writeJSON(w, 200, map[string]any{"step": in.Step, "completed": in.Completed, "revision": revision})
	}
	mux.Handle("GET /api/v1/installation/progress", auth.RequireAPIAdmin(service, http.HandlerFunc(handler)))
	mux.Handle("PUT /api/v1/installation/progress", auth.RequireAPIAdmin(service, http.HandlerFunc(handler)))
}
func validSetupStep(step string) bool {
	switch step {
	case "storage", "ai", "telegram", "sources", "categories", "schedule", "finish":
		return true
	default:
		return false
	}
}
