package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/webbackend"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type jobConfigurationHandler struct {
	pool  *pgxpool.Pool
	guard webbackend.Guard
	token string
}

func (h jobConfigurationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	expected := "Bearer " + h.token
	got := r.Header.Get("Authorization")
	if h.token == "" || len(got) != len(expected) || subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if h.guard == nil || h.guard.Check(r.Context()) != nil {
		http.Error(w, "Unavailable", http.StatusServiceUnavailable)
		return
	}
	store := appsettings.Store{Pool: h.pool}
	var v appsettings.JobOperational
	var err error
	switch r.Method {
	case http.MethodPost:
		var in struct {
			JobID             string `json:"job_id"`
			AttemptGeneration int64  `json:"attempt_generation"`
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		d.DisallowUnknownFields()
		if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF {
			http.Error(w, "Invalid request", http.StatusUnprocessableEntity)
			return
		}
		v, err = store.PinOperational(r.Context(), in.JobID, in.AttemptGeneration)
	case http.MethodGet:
		query := r.URL.Query()
		if len(query) != 2 || len(query["job_id"]) != 1 || query.Get("job_id") == "" || len(query["attempt_generation"]) != 1 {
			http.Error(w, "Invalid request", http.StatusUnprocessableEntity)
			return
		}
		attempt, parseErr := strconv.ParseInt(query.Get("attempt_generation"), 10, 64)
		if parseErr != nil || attempt < 1 {
			http.Error(w, "Invalid request", http.StatusUnprocessableEntity)
			return
		}
		v, err = store.JobOperational(r.Context(), query.Get("job_id"), attempt)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if errors.Is(err, appsettings.ErrInvalid) {
		http.Error(w, "Invalid request", http.StatusUnprocessableEntity)
		return
	}
	if errors.Is(err, appsettings.ErrConflict) {
		http.Error(w, "Job attempt is no longer current", http.StatusConflict)
		return
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		if r.Method == http.MethodGet && errors.Is(err, pgx.ErrNoRows) {
			status = http.StatusNotFound
		}
		http.Error(w, "Job configuration unavailable", status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
