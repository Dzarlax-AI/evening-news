package schedules

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
)

type Handler struct {
	root           ScheduleSettingsRoot
	createDisabled bool
}

func NewHandler(root ScheduleSettingsRoot) *Handler {
	if root == nil {
		root = NewRoot(nil, nil, nil)
	}
	return &Handler{root: root}
}

func NewManagedHandler(root ScheduleSettingsRoot) *Handler {
	h := NewHandler(root)
	h.createDisabled = true
	return h
}

func (h *Handler) Register(mux *http.ServeMux, service *auth.Service) {
	mux.Handle("GET /api/v1/schedule/settings", auth.RequireAPIAdmin(service, http.HandlerFunc(h.getSettings)))
	mux.Handle("POST /api/v1/schedule/settings", auth.RequireAPIAdmin(service, http.HandlerFunc(h.createSetting)))
	mux.Handle("PUT /api/v1/schedule/settings/{task_name}", auth.RequireAPIAdmin(service, http.HandlerFunc(h.updateSetting)))
	mux.Handle("DELETE /api/v1/schedule/settings/{task_name}", auth.RequireAPIAdmin(service, http.HandlerFunc(h.deleteSetting)))
	mux.Handle("GET /api/v1/schedule/status", auth.RequireAPIAdmin(service, http.HandlerFunc(h.getStatus)))
}

func (h *Handler) getSettings(w http.ResponseWriter, r *http.Request) {
	out, err := h.root.Settings(r.Context())
	writeResult(w, out, err)
}
func (h *Handler) createSetting(w http.ResponseWriter, r *http.Request) {
	request, err := decodeCreateRequest(r)
	if err != nil {
		writeValidation(w, err)
		return
	}
	if h.createDisabled {
		request.Enabled = false
	}
	out, rootErr := h.root.Create(r.Context(), request)
	writeResult(w, out, rootErr)
}
func (h *Handler) updateSetting(w http.ResponseWriter, r *http.Request) {
	request, err := decodeUpdateRequest(r)
	if err != nil {
		writeValidation(w, err)
		return
	}
	out, rootErr := h.root.Update(r.Context(), r.PathValue("task_name"), request)
	writeResult(w, out, rootErr)
}
func (h *Handler) deleteSetting(w http.ResponseWriter, r *http.Request) {
	out, err := h.root.Delete(r.Context(), r.PathValue("task_name"))
	writeResult(w, out, err)
}
func (h *Handler) getStatus(w http.ResponseWriter, r *http.Request) {
	out, err := h.root.Status(r.Context())
	writeResult(w, out, err)
}

func writeResult(w http.ResponseWriter, out map[string]any, err error) {
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			writeJSON(w, apiErr.Status, map[string]string{"detail": apiErr.Detail})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "Scheduler service is unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type requestValidationError struct {
	field, msg, kind string
	input            any
}

func (e *requestValidationError) Error() string { return e.msg }

func decodeCreateRequest(r *http.Request) (ScheduleCreateRequest, *requestValidationError) {
	fields, err := readObject(r)
	if err != nil {
		return ScheduleCreateRequest{}, err
	}
	request := ScheduleCreateRequest{Enabled: true, ScheduleType: "daily", Hour: 0, Minute: 0, Weekdays: []int{1, 2, 3, 4, 5, 6, 7}, Timezone: "Europe/Belgrade"}
	if raw, ok := fields["task_name"]; !ok {
		return request, &requestValidationError{field: "task_name", msg: "Field required", kind: "missing"}
	} else if err := json.Unmarshal(raw, &request.TaskName); err != nil {
		return request, &requestValidationError{field: "task_name", msg: "Input should be a valid string", kind: "string_type"}
	}
	if err := decodeOptional(fields, "enabled", &request.Enabled); err != nil {
		return request, err
	}
	if err := decodeOptional(fields, "schedule_type", &request.ScheduleType); err != nil {
		return request, err
	}
	if err := decodeOptional(fields, "hour", &request.Hour); err != nil {
		return request, err
	}
	if err := decodeOptional(fields, "minute", &request.Minute); err != nil {
		return request, err
	}
	if err := decodeOptional(fields, "weekdays", &request.Weekdays); err != nil {
		return request, err
	}
	if err := decodeOptional(fields, "timezone", &request.Timezone); err != nil {
		return request, err
	}
	if raw, ok := fields["task_config"]; ok && string(raw) != "null" {
		config, decodeErr := decodeMap(raw)
		if decodeErr != nil {
			return request, &requestValidationError{field: "task_config", msg: "Input should be a valid dictionary", kind: "dict_type"}
		}
		request.TaskConfig = config
	}
	return request, nil
}

func decodeUpdateRequest(r *http.Request) (ScheduleUpdateRequest, *requestValidationError) {
	fields, err := readObject(r)
	if err != nil {
		return ScheduleUpdateRequest{}, err
	}
	request := ScheduleUpdateRequest{}
	if raw, ok := fields["enabled"]; ok && string(raw) != "null" {
		var value bool
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "enabled", msg: "Input should be a valid boolean", kind: "bool_type"}
		}
		request.Enabled = &value
	}
	if raw, ok := fields["schedule_type"]; ok && string(raw) != "null" {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "schedule_type", msg: "Input should be a valid string", kind: "string_type"}
		}
		request.ScheduleType = &value
	}
	if raw, ok := fields["hour"]; ok && string(raw) != "null" {
		var value int
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "hour", msg: "Input should be a valid integer", kind: "int_type"}
		}
		request.Hour = &value
	}
	if raw, ok := fields["minute"]; ok && string(raw) != "null" {
		var value int
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "minute", msg: "Input should be a valid integer", kind: "int_type"}
		}
		request.Minute = &value
	}
	if raw, ok := fields["weekdays"]; ok && string(raw) != "null" {
		var value []int
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "weekdays", msg: "Input should be a valid list", kind: "list_type"}
		}
		request.Weekdays = &value
	}
	if raw, ok := fields["timezone"]; ok && string(raw) != "null" {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return request, &requestValidationError{field: "timezone", msg: "Input should be a valid string", kind: "string_type"}
		}
		request.Timezone = &value
	}
	if raw, ok := fields["task_config"]; ok && string(raw) != "null" {
		config, decodeErr := decodeMap(raw)
		if decodeErr != nil {
			return request, &requestValidationError{field: "task_config", msg: "Input should be a valid dictionary", kind: "dict_type"}
		}
		request.TaskConfig = config
		request.TaskConfigSet = true
	}
	return request, nil
}

func readObject(r *http.Request) (map[string]json.RawMessage, *requestValidationError) {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16*1024+1))
	if err != nil || len(raw) > 16*1024 {
		return nil, &requestValidationError{field: "body", msg: "Request body is invalid or too large", kind: "json_invalid"}
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, &requestValidationError{field: "body", msg: "Input should be a valid dictionary", kind: "dict_type"}
	}
	return object, nil
}

func decodeOptional(fields map[string]json.RawMessage, key string, dest any) *requestValidationError {
	raw, ok := fields[key]
	if !ok {
		return nil
	}
	if string(raw) == "null" {
		return &requestValidationError{field: key, msg: "Input should not be null", kind: "none_required"}
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return &requestValidationError{field: key, msg: "Input has an invalid type", kind: "type_error"}
	}
	return nil
}
func decodeMap(raw json.RawMessage) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var result map[string]any
	err := decoder.Decode(&result)
	if result == nil && err == nil {
		err = errors.New("not an object")
	}
	return result, err
}
func writeValidation(w http.ResponseWriter, err *requestValidationError) {
	status := http.StatusUnprocessableEntity
	detail := []map[string]any{{"type": err.kind, "loc": []any{"body", err.field}, "msg": err.msg, "input": err.input}}
	writeJSON(w, status, map[string]any{"detail": detail})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
