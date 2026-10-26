package schedules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

var serviceOperations = map[string]struct{}{
	"news_processing": {}, "news_digest": {}, "telegram_digest": {}, "backup": {}, "reprocess_failed": {},
}

type APIError struct {
	Status int
	Detail string
}

func (e *APIError) Error() string { return e.Detail }

type JobStatus struct {
	CreatedAt  *time.Time
	FinishedAt *time.Time
	State      string
	Result     map[string]any
	ErrorCode  any
}

type JobsSnapshot struct {
	Latest      map[string]JobStatus
	Running     map[string]bool
	LastSuccess map[string]JobStatus
}

type JobsSnapshotReader interface {
	Snapshot(ctx context.Context, scheduleKeys []string) (JobsSnapshot, error)
}

type ScheduleCreateRequest struct {
	TaskName     string
	Enabled      bool
	ScheduleType string
	Hour         int
	Minute       int
	Weekdays     []int
	Timezone     string
	TaskConfig   map[string]any
}

type ScheduleUpdateRequest struct {
	Enabled       *bool
	ScheduleType  *string
	Hour          *int
	Minute        *int
	Weekdays      *[]int
	Timezone      *string
	TaskConfig    map[string]any
	TaskConfigSet bool
}

type ScheduleSettingsRoot interface {
	Settings(context.Context) (map[string]any, error)
	Create(context.Context, ScheduleCreateRequest) (map[string]any, error)
	Update(context.Context, string, ScheduleUpdateRequest) (map[string]any, error)
	Delete(context.Context, string) (map[string]any, error)
	Status(context.Context) (map[string]any, error)
}

type Root struct {
	client *Client
	jobs   JobsSnapshotReader
	now    func() time.Time
}

func NewRoot(client *Client, jobs JobsSnapshotReader, clock func() time.Time) *Root {
	if clock == nil {
		clock = time.Now
	}
	return &Root{client: client, jobs: jobs, now: clock}
}

func (root *Root) Settings(ctx context.Context) (map[string]any, error) {
	client, err := root.requireClient()
	if err != nil {
		return nil, err
	}
	resources, err := client.ListSchedules(ctx)
	if err != nil {
		return nil, mapSchedulerError(err)
	}
	resources = ownedResources(resources)
	settings, err := root.snapshotSettings(ctx, resources)
	if err != nil {
		return nil, err
	}
	return map[string]any{"settings": settings, "timestamp": formatNaive(root.now().UTC())}, nil
}

func (root *Root) Create(ctx context.Context, request ScheduleCreateRequest) (map[string]any, error) {
	if !supportedOperation(request.TaskName) {
		return nil, &APIError{Status: 422, Detail: "Unsupported scheduled task"}
	}
	client, err := root.requireClient()
	if err != nil {
		return nil, err
	}
	current, err := findServiceSchedule(ctx, client, request.TaskName)
	if err != nil {
		return nil, err
	}
	if current != nil {
		return nil, &APIError{Status: 400, Detail: fmt.Sprintf("Schedule for task '%s' already exists", request.TaskName)}
	}
	key := "news:" + request.TaskName
	body, err := serviceResourceBody(request, nil, request.TaskName)
	if err != nil {
		return nil, err
	}
	resource, err := client.PutSchedule(ctx, key, body)
	if err != nil {
		return nil, mapSchedulerError(err)
	}
	setting, err := serviceScheduleToConfig(resource)
	if err != nil {
		return nil, &APIError{Status: 502, Detail: "Scheduler service returned an invalid response"}
	}
	return map[string]any{"success": true, "message": "Schedule created successfully", "setting": setting}, nil
}

func (root *Root) Update(ctx context.Context, taskName string, request ScheduleUpdateRequest) (map[string]any, error) {
	if !supportedOperation(taskName) {
		return nil, &APIError{Status: 404, Detail: "Schedule setting not found"}
	}
	client, err := root.requireClient()
	if err != nil {
		return nil, err
	}
	current, err := findServiceSchedule(ctx, client, taskName)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, &APIError{Status: 404, Detail: "Schedule setting not found"}
	}
	body, err := serviceResourceBody(request, current, taskName)
	if err != nil {
		return nil, err
	}
	resource, err := client.PutSchedule(ctx, current["schedule_key"].(string), body)
	if err != nil {
		return nil, mapSchedulerError(err)
	}
	setting, err := serviceScheduleToConfig(resource)
	if err != nil {
		return nil, &APIError{Status: 502, Detail: "Scheduler service returned an invalid response"}
	}
	return map[string]any{"success": true, "message": "Schedule updated successfully", "setting": setting}, nil
}

func (root *Root) Delete(ctx context.Context, taskName string) (map[string]any, error) {
	if !supportedOperation(taskName) {
		return nil, &APIError{Status: 404, Detail: fmt.Sprintf("Schedule for task '%s' not found", taskName)}
	}
	client, err := root.requireClient()
	if err != nil {
		return nil, err
	}
	current, err := findServiceSchedule(ctx, client, taskName)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, &APIError{Status: 404, Detail: fmt.Sprintf("Schedule for task '%s' not found", taskName)}
	}
	key := current["schedule_key"].(string)
	revision, _ := integer(current["revision"])
	if err = client.DeleteSchedule(ctx, key, revision); err != nil {
		return nil, mapSchedulerError(err)
	}
	return map[string]any{"success": true, "message": fmt.Sprintf("Schedule for task '%s' deleted successfully", taskName)}, nil
}

func (root *Root) Status(ctx context.Context) (map[string]any, error) {
	settings, err := root.Settings(ctx)
	if err != nil {
		return nil, err
	}
	client, err := root.requireClient()
	if err != nil {
		return nil, err
	}
	serviceStatus, err := client.GetStatus(ctx)
	if err != nil {
		return nil, mapSchedulerError(err)
	}
	rows := settings["settings"].([]map[string]any)
	enabled, running := 0, 0
	tasks := make([]map[string]any, 0)
	fields := []string{"task_name", "enabled", "is_running", "last_run", "last_finished_at", "last_success_at", "last_status", "last_error", "last_duration_seconds", "next_run"}
	for _, item := range rows {
		if item["enabled"] == true {
			enabled++
			task := map[string]any{}
			for _, key := range fields {
				task[key] = item[key]
			}
			tasks = append(tasks, task)
		}
		if item["is_running"] == true {
			running++
		}
	}
	return map[string]any{"enabled_tasks": enabled, "running_tasks": running, "tasks": tasks, "timestamp": formatNaive(root.now().UTC()), "scheduler_service": map[string]any{"paused": serviceStatus["paused"], "pending_outbox": serviceStatus["pending_outbox"], "control_revision": serviceStatus["control_revision"], "last_tick_at": serviceStatus["last_tick_at"], "last_dispatch_at": serviceStatus["last_dispatch_at"]}}, nil
}

func (root *Root) requireClient() (*Client, error) {
	if root == nil || root.client == nil {
		return nil, &APIError{Status: 503, Detail: "Scheduler service is not configured"}
	}
	return root.client, nil
}

func (root *Root) snapshotSettings(ctx context.Context, resources []map[string]any) ([]map[string]any, error) {
	if len(resources) == 0 {
		return []map[string]any{}, nil
	}
	if root.jobs == nil {
		return nil, &APIError{Status: 503, Detail: "News execution status is unavailable"}
	}
	keys := make([]string, 0, len(resources))
	for _, resource := range resources {
		keys = append(keys, resource["schedule_key"].(string))
	}
	sort.Strings(keys)
	snapshot, err := root.jobs.Snapshot(ctx, keys)
	if err != nil {
		return nil, &APIError{Status: 503, Detail: "News execution status is unavailable"}
	}
	settings := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		setting, err := serviceScheduleToConfig(resource)
		if err != nil {
			return nil, &APIError{Status: 502, Detail: "Scheduler service returned an invalid response"}
		}
		key := resource["schedule_key"].(string)
		job, hasJob := snapshot.Latest[key]
		if hasJob {
			setting["last_run"] = timeString(job.CreatedAt)
			setting["last_finished_at"] = timeString(job.FinishedAt)
			if job.State == "succeeded" {
				setting["last_status"] = job.Result["status"]
			} else {
				setting["last_status"] = job.State
			}
			setting["last_error"] = job.ErrorCode
			setting["last_duration_seconds"] = job.Result["duration_seconds"]
			setting["is_running"] = snapshot.Running[key]
		} else {
			setting["is_running"] = snapshot.Running[key]
		}
		if success, ok := snapshot.LastSuccess[key]; ok {
			setting["last_success_at"] = timeString(success.FinishedAt)
		}
		if hasJob && job.State == "succeeded" && job.Result["status"] != "warning" && job.Result["status"] != "deferred" {
			if _, ok := snapshot.LastSuccess[key]; ok {
				setting["last_status"] = "success"
			}
		}
		settings = append(settings, setting)
	}
	return settings, nil
}

func ownedResources(resources []map[string]any) []map[string]any {
	out := make([]map[string]any, 0)
	for _, r := range resources {
		if isNewsServiceResource(r) {
			out = append(out, r)
		}
	}
	return out
}
func isNewsServiceResource(r map[string]any) bool {
	if r == nil || r["target_alias"] != "news-worker" {
		return false
	}
	operation, ok := r["operation"].(string)
	if !ok || !supportedOperation(operation) {
		return false
	}
	key, ok := r["schedule_key"].(string)
	if !ok || !strings.HasPrefix(key, "news:") {
		return false
	}
	suffix := strings.TrimPrefix(key, "news:")
	if suffix == operation {
		return true
	}
	if suffix == "" {
		return false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	n, err := strconv.ParseUint(suffix, 10, 64)
	return err == nil && n > 0
}
func supportedOperation(value string) bool { _, ok := serviceOperations[value]; return ok }

func findServiceSchedule(ctx context.Context, client *Client, task string) (map[string]any, error) {
	resources, err := client.ListSchedules(ctx)
	if err != nil {
		return nil, mapSchedulerError(err)
	}
	matches := []map[string]any{}
	for _, resource := range resources {
		if isNewsServiceResource(resource) && resource["operation"] == task {
			matches = append(matches, resource)
		}
	}
	if len(matches) > 1 {
		return nil, &APIError{Status: 409, Detail: "Multiple scheduler resources map to this task"}
	}
	if len(matches) == 0 {
		return nil, nil
	}
	return matches[0], nil
}

func mapSchedulerError(err error) error {
	var schedulerErr *SchedulerError
	if !errors.As(err, &schedulerErr) {
		return &APIError{Status: 503, Detail: "Scheduler service is unavailable"}
	}
	if schedulerErr.Code == "invalid_response" {
		return &APIError{Status: 502, Detail: "Scheduler service returned an invalid response"}
	}
	status := schedulerErr.Status
	if status != 401 && status != 403 && status != 404 && status != 409 && status != 422 && status != 429 && status != 503 {
		status = 503
	}
	detail := map[int]string{401: "Scheduler service authentication failed", 403: "Scheduler service denied the request", 404: "Schedule not found", 409: "Schedule changed concurrently", 422: "Invalid schedule configuration", 429: "Scheduler service is rate limited", 503: "Scheduler service is unavailable"}[status]
	return &APIError{Status: status, Detail: detail}
}

func serviceScheduleToConfig(resource map[string]any) (map[string]any, error) {
	if ValidateScheduleResource(resource) != nil {
		return nil, errors.New("invalid scheduler resource")
	}
	calendar := resource["calendar"].(map[string]any)
	typ := calendar["type"].(string)
	payload := cloneMap(resource["payload"].(map[string]any))
	if typ == "interval" {
		payload["interval_minutes"] = calendar["interval_minutes"]
	}
	key := resource["schedule_key"].(string)
	suffix := strings.TrimPrefix(key, "news:")
	var id any = key
	if number, err := strconv.ParseInt(suffix, 10, 64); err == nil && number >= 0 {
		id = number
	}
	weekdays := []int{}
	if raw, ok := calendar["weekdays"].([]any); ok {
		for _, v := range raw {
			n, ok := integer(v)
			if !ok {
				return nil, errors.New("invalid weekdays")
			}
			weekdays = append(weekdays, int(n))
		}
	}
	hour, _ := integer(calendar["hour"])
	minute, _ := integer(calendar["minute"])
	var next any
	if value := resource["next_due_at"]; value != nil {
		parsed, err := time.Parse(time.RFC3339Nano, value.(string))
		if err != nil {
			return nil, err
		}
		next = formatNaive(parsed.UTC())
	}
	return map[string]any{"id": id, "task_name": resource["operation"], "enabled": resource["enabled"], "schedule_type": typ, "hour": hour, "minute": minute, "weekdays": weekdays, "timezone": calendar["timezone"], "task_config": payload, "last_run": nil, "last_finished_at": nil, "last_success_at": nil, "last_status": nil, "last_error": nil, "last_duration_seconds": nil, "next_run": next, "is_running": false, "created_at": resource["created_at"], "updated_at": resource["updated_at"], "schedule_key": key, "schedule_revision": resource["revision"]}, nil
}

type scheduleCreateValues struct {
	ScheduleCreateRequest
	scheduleTypeSet bool
}

func serviceResourceBody(request any, current map[string]any, taskName string) (map[string]any, error) {
	var typ, timezone string
	var enabled bool
	var hour, minute int
	var weekdays []int
	var taskConfig map[string]any
	var revision int64
	var targetAlias, operation string
	oldCalendar := map[string]any{}
	if current != nil {
		oldCalendar = current["calendar"].(map[string]any)
		revision, _ = integer(current["revision"])
		targetAlias = current["target_alias"].(string)
		operation = current["operation"].(string)
		taskConfig = cloneMap(current["payload"].(map[string]any))
		enabled = current["enabled"].(bool)
	}
	switch req := request.(type) {
	case ScheduleCreateRequest:
		typ = req.ScheduleType
		timezone = req.Timezone
		enabled = req.Enabled
		hour = req.Hour
		minute = req.Minute
		weekdays = req.Weekdays
		taskConfig = req.TaskConfig
		operation = taskName
		targetAlias = "news-worker"
	case ScheduleUpdateRequest:
		typ = oldCalendar["type"].(string)
		timezone = oldCalendar["timezone"].(string)
		if req.ScheduleType != nil {
			typ = *req.ScheduleType
		}
		if req.Timezone != nil {
			timezone = *req.Timezone
		}
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		oldHour, _ := integer(oldCalendar["hour"])
		oldMinute, _ := integer(oldCalendar["minute"])
		hour = int(oldHour)
		minute = int(oldMinute)
		if req.Hour != nil {
			hour = *req.Hour
		}
		if req.Minute != nil {
			minute = *req.Minute
		}
		if req.Weekdays != nil {
			weekdays = *req.Weekdays
		} else if days, ok := oldCalendar["weekdays"].([]any); ok {
			for _, d := range days {
				n, _ := integer(d)
				weekdays = append(weekdays, int(n))
			}
		}
		if req.TaskConfigSet {
			taskConfig = req.TaskConfig
		}
	default:
		return nil, &APIError{Status: 422, Detail: "Invalid schedule configuration"}
	}
	if taskConfig == nil {
		taskConfig = map[string]any{}
	}
	calendar := map[string]any{"type": typ, "timezone": timezone}
	switch typ {
	case "daily", "weekly":
		calendar["hour"] = hour
		calendar["minute"] = minute
		if weekdays != nil {
			days := make([]any, len(weekdays))
			for i, v := range weekdays {
				days[i] = v
			}
			calendar["weekdays"] = days
		}
	case "hourly":
		calendar["minute"] = minute
		if len(weekdays) > 0 {
			days := make([]any, len(weekdays))
			for i, v := range weekdays {
				days[i] = v
			}
			calendar["weekdays"] = days
		}
	case "interval":
		interval, ok := taskConfig["interval_minutes"]
		if !ok {
			interval = oldCalendar["interval_minutes"]
		}
		if interval == nil {
			interval = 30
		}
		if _, ok := integer(interval); !ok {
			return nil, &APIError{Status: 422, Detail: "Invalid scheduled task configuration"}
		}
		calendar["interval_minutes"] = interval
	default:
		return nil, &APIError{Status: 422, Detail: "Invalid schedule type"}
	}
	payload := cloneMap(taskConfig)
	delete(payload, "interval_minutes")
	validated, err := validatePayload(operation, payload)
	if err != nil {
		return nil, &APIError{Status: 422, Detail: "Invalid scheduled task configuration"}
	}
	return map[string]any{"expected_revision": revision, "enabled": enabled, "calendar": calendar, "target_alias": targetAlias, "operation": operation, "payload": validated}, nil
}

func validatePayload(kind string, payload map[string]any) (map[string]any, error) {
	allowed := map[string]map[string]bool{"news_processing": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "news_digest": {"schedule_id": true, "scheduled_for": true, "run_processing": true, "send_telegram": true, "timeout_seconds": true}, "telegram_digest": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "backup": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "reprocess_failed": {"schedule_id": true, "scheduled_for": true, "limit": true, "dry_run": true, "timeout_seconds": true}}
	fields, ok := allowed[kind]
	if !ok {
		return nil, errors.New("unsupported operation")
	}
	for key := range payload {
		if !fields[key] {
			return nil, errors.New("unsupported payload field")
		}
	}
	validated := map[string]any{}
	_, hasID := payload["schedule_id"]
	_, hasDue := payload["scheduled_for"]
	if hasID != hasDue {
		return nil, errors.New("schedule fields must be paired")
	}
	if hasID {
		id, ok := integer(payload["schedule_id"])
		if !ok || id <= 0 {
			return nil, errors.New("invalid schedule id")
		}
		due, ok := payload["scheduled_for"].(string)
		if !ok {
			return nil, errors.New("invalid schedule due")
		}
		parsed, err := time.Parse(time.RFC3339Nano, due)
		if err != nil || formatNaive(parsed) != due {
			return nil, errors.New("invalid schedule due")
		}
		validated["schedule_id"] = id
		validated["scheduled_for"] = due
	}
	if value, exists := payload["timeout_seconds"]; exists {
		n, ok := integer(value)
		if !ok || n < 1 || n > 86400 {
			return nil, errors.New("invalid timeout")
		}
		validated["timeout_seconds"] = n
	}
	if kind == "news_digest" {
		for _, key := range []string{"run_processing", "send_telegram"} {
			if value, ok := payload[key]; ok {
				if _, yes := value.(bool); !yes {
					return nil, errors.New("invalid flag")
				}
				validated[key] = value
			}
		}
	}
	if kind == "reprocess_failed" {
		if value, ok := payload["limit"]; ok {
			n, yes := integer(value)
			if !yes || n < 1 || n > 100 {
				return nil, errors.New("invalid limit")
			}
			validated["limit"] = n
		}
		if value, ok := payload["dry_run"]; ok {
			if _, yes := value.(bool); !yes {
				return nil, errors.New("invalid dry run")
			}
			validated["dry_run"] = value
		}
	}
	encoded, err := json.Marshal(validated)
	if err != nil || len(encoded) > 4096 {
		return nil, errors.New("payload too large")
	}
	return validated, nil
}

func cloneMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
func timeString(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatNaive(*value)
}
func formatNaive(value time.Time) string {
	text := value.UTC().Format("2006-01-02T15:04:05.999999999")
	if dot := strings.IndexByte(text, '.'); dot >= 0 {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return text
}
