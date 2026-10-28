package terminalevents

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var sourceNames = map[string]string{"rss": "RSS", "telegram": "Telegram", "custom": "Page Monitor"}

type sourceState struct {
	Config   string  `json:"config"`
	Failed   int     `json:"failed_streak"`
	Healthy  int     `json:"healthy_streak"`
	Last     string  `json:"last_state"`
	Incident string  `json:"incident"`
	EventJob *string `json:"event_job"`
}
type sourceLedger struct {
	Version      int                     `json:"v"`
	LastPosition int64                   `json:"last_position"`
	Sources      map[string]*sourceState `json:"sources"`
}
type sourceObservation struct {
	ID                        int64
	Type, Config, State, Code string
}
type sourceEvent struct {
	ID         int64
	Type, Code string
	Recovery   bool
}
type sourceNotice struct {
	Title, Message string
	Events         []sourceEvent
}

func settingKey(identity string) string {
	v := sha256.Sum256([]byte(identity))
	return "scheduled_source_alerts:v1:" + hex.EncodeToString(v[:])
}
func validSourceLedger(raw []byte) (sourceLedger, bool) {
	var v sourceLedger
	if len(raw) > 12*1024 || json.Unmarshal(raw, &v) != nil || v.Version != 1 || v.LastPosition < 0 || v.Sources == nil || len(v.Sources) > 64 {
		return v, false
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil || top["last_position"] == nil || string(top["last_position"]) == "null" {
		return v, false
	}
	var rawSources map[string]map[string]json.RawMessage
	if json.Unmarshal(top["sources"], &rawSources) != nil {
		return v, false
	}
	for _, fields := range rawSources {
		for _, name := range []string{"config", "failed_streak", "healthy_streak", "last_state", "incident"} {
			if fields[name] == nil || string(fields[name]) == "null" {
				return v, false
			}
		}
	}
	for k, s := range v.Sources {
		if s == nil || k == "" {
			return v, false
		}
		for _, r := range k {
			if r < '0' || r > '9' {
				return v, false
			}
		}
		if !hashPattern.MatchString(s.Config) || s.Failed < 0 || s.Failed > 2 || s.Healthy < 0 || s.Healthy > 2 || !oneOf(s.Last, "new", "warning", "healthy", "missing") || !oneOf(s.Incident, "none", "active", "attempting", "recovering", "unknown", "retired") {
			return v, false
		}
	}
	return v, true
}
func oneOf(v string, items ...string) bool {
	for _, x := range items {
		if v == x {
			return true
		}
	}
	return false
}
func reconcileSourceLedger(v *sourceLedger, active map[string]bool) {
	for _, s := range v.Sources {
		if (s.Incident == "attempting" || s.Incident == "recovering") && (s.EventJob == nil || !active[*s.EventJob]) {
			s.Incident = "unknown"
			s.EventJob = nil
		}
	}
}
func breakSourceStreaks(v *sourceLedger, position int64, active map[string]bool) {
	reconcileSourceLedger(v, active)
	if position <= v.LastPosition {
		return
	}
	v.LastPosition = position
	for _, s := range v.Sources {
		s.Failed = 0
		s.Healthy = 0
		s.Last = "missing"
	}
}
func prepareSourceNotice(v *sourceLedger, position int64, job string, items []sourceObservation, previous *int64, active map[string]bool) (*sourceNotice, bool) {
	reconcileSourceLedger(v, active)
	if position <= v.LastPosition {
		return nil, true
	}
	if v.LastPosition != 0 && (previous == nil || *previous != v.LastPosition) {
		for _, s := range v.Sources {
			s.Failed = 0
			s.Healthy = 0
			s.Last = "missing"
		}
	}
	current := map[string]bool{}
	for _, item := range items {
		current[strconv.FormatInt(item.ID, 10)] = true
	}
	for id, s := range v.Sources {
		if !current[id] {
			if s.Incident == "none" && s.EventJob == nil {
				delete(v.Sources, id)
			} else {
				s.Failed = 0
				s.Healthy = 0
				s.Last = "missing"
			}
		}
	}
	events := []sourceEvent{}
	healthy := 0
	for _, item := range items {
		id := strconv.FormatInt(item.ID, 10)
		s := v.Sources[id]
		if s == nil || s.Config != item.Config {
			s = &sourceState{Config: item.Config, Last: "new", Incident: "none"}
			v.Sources[id] = s
		}
		if item.State == "warning" {
			s.Healthy = 0
			if s.Last == "warning" {
				s.Failed = min(2, s.Failed+1)
			} else {
				s.Failed = 1
			}
			s.Last = "warning"
			if s.Failed >= 2 && s.Incident == "none" {
				s.Incident = "attempting"
				j := job
				s.EventJob = &j
				events = append(events, sourceEvent{ID: item.ID, Type: item.Type, Code: item.Code})
			}
		} else {
			healthy++
			s.Failed = 0
			if s.Last == "healthy" {
				s.Healthy = min(2, s.Healthy+1)
			} else {
				s.Healthy = 1
			}
			s.Last = "healthy"
			if s.Healthy >= 2 && s.Incident == "active" {
				s.Incident = "recovering"
				j := job
				s.EventJob = &j
				events = append(events, sourceEvent{ID: item.ID, Type: item.Type, Recovery: true})
			} else if s.Healthy >= 2 && s.Incident == "unknown" {
				s.Incident = "none"
				s.EventJob = nil
			}
		}
	}
	v.LastPosition = position
	raw, err := json.Marshal(v)
	if err != nil || len(v.Sources) > 64 || len(raw) > 12*1024 {
		return nil, false
	}
	if len(events) == 0 {
		return nil, true
	}
	descriptions := []string{}
	recoveryCount := 0
	for _, e := range events {
		d := fmt.Sprintf("Source #%d (%s)", e.ID, sourceNames[e.Type])
		if e.Recovery {
			d += " recovered"
			recoveryCount++
		} else {
			d += " reported " + e.Code
		}
		descriptions = append(descriptions, d)
	}
	title := "Scheduled source updates"
	if recoveryCount == len(events) {
		title = "Scheduled source recovered"
	} else if recoveryCount == 0 {
		title = "Scheduled source warning"
	}
	message := truncate(strings.Join(descriptions, "; "), 350) + fmt.Sprintf(". Sources successful: %d/%d.", healthy, len(items))
	return &sourceNotice{Title: title, Message: truncate(message, 500), Events: events}, true
}
func finishSourceNotice(v *sourceLedger, events []sourceEvent, job string, delivered bool) {
	for _, event := range events {
		s := v.Sources[strconv.FormatInt(event.ID, 10)]
		if s == nil || s.EventJob == nil || *s.EventJob != job {
			continue
		}
		expected := "attempting"
		if event.Recovery {
			expected = "recovering"
		}
		if s.Incident != expected {
			continue
		}
		if delivered {
			if event.Recovery {
				s.Incident = "none"
			} else {
				s.Incident = "active"
			}
		} else {
			s.Incident = "unknown"
		}
		s.EventJob = nil
		if event.Recovery {
			s.Healthy = 0
		}
	}
}
