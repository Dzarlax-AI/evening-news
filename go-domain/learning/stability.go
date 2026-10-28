package learning

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type domainStats struct {
	Domain         string             `json:"domain"`
	Total          int                `json:"total_attempts"`
	Success        int                `json:"successful_attempts"`
	Failed         int                `json:"failed_attempts"`
	LastSuccess    *float64           `json:"last_success_timestamp"`
	LastFailure    *float64           `json:"last_failure_timestamp"`
	AverageTime    float64            `json:"average_extraction_time_ms"`
	SuccessRate    float64            `json:"success_rate"`
	MethodSuccess  map[string]int     `json:"method_success_counts"`
	MethodFailure  map[string]int     `json:"method_failure_counts"`
	MethodAverage  map[string]float64 `json:"method_avg_times"`
	AverageLength  float64            `json:"average_content_length"`
	AverageQuality float64            `json:"average_quality_score"`
	AllFailures    int                `json:"consecutive_all_methods_failures"`
	LastAllFailure *float64           `json:"last_all_methods_failure_timestamp"`
}
type stabilityData struct {
	Domains      map[string]*domainStats `json:"domain_stats"`
	CreditsUsed  float64                 `json:"ai_credits_used"`
	CreditsSaved float64                 `json:"ai_credits_saved"`
	LastCleanup  float64                 `json:"last_cleanup_time"`
}
type stability struct {
	mu         sync.Mutex
	path       string
	loaded     bool
	unwritable bool
	data       stabilityData
}

func newStability(path string) *stability {
	return &stability{path: path, data: stabilityData{Domains: make(map[string]*domainStats), LastCleanup: float64(time.Now().UnixNano()) / 1e9}}
}
func (s *stability) load() {
	if s.loaded {
		return
	}
	s.loaded = true
	file, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		s.unwritable = true
		return
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 5*1024*1024+1))
	if err != nil || len(raw) > 5*1024*1024 {
		s.unwritable = true
		return
	}
	var data stabilityData
	if json.Unmarshal(raw, &data) != nil || data.Domains == nil {
		s.unwritable = true
		return
	}
	s.data = data
}
func (s *stability) success(domain, method string, elapsed, length int, quality float64, publish ...func(string, string) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.load()
	if s.unwritable {
		return errors.New("stability state unavailable")
	}
	before, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			var old stabilityData
			if json.Unmarshal(before, &old) == nil {
				s.data = old
			}
		}
	}()
	p := s.data.Domains[domain]
	if p == nil {
		p = &domainStats{Domain: domain}
		s.data.Domains[domain] = p
	}
	if p.MethodSuccess == nil {
		p.MethodSuccess = map[string]int{}
	}
	if p.MethodFailure == nil {
		p.MethodFailure = map[string]int{}
	}
	if p.MethodAverage == nil {
		p.MethodAverage = map[string]float64{}
	}
	now := float64(time.Now().UnixNano()) / 1e9
	p.Total++
	p.Success++
	p.LastSuccess = &now
	p.MethodSuccess[method]++
	count := p.MethodSuccess[method]
	p.MethodAverage[method] = (p.MethodAverage[method]*float64(count-1) + float64(elapsed)) / float64(count)
	p.SuccessRate = float64(p.Success) / float64(p.Total)
	p.AverageTime = (p.AverageTime*float64(p.Total-1) + float64(elapsed)) / float64(p.Total)
	if length > 0 {
		p.AverageLength = (p.AverageLength*float64(p.Success-1) + float64(length)) / float64(p.Success)
		p.AverageQuality = (p.AverageQuality*float64(p.Success-1) + quality) / float64(p.Success)
	}
	p.AllFailures = 0
	p.LastAllFailure = nil
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".domain-stats-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if len(publish) > 0 {
		err = publish[0](name, s.path)
	} else {
		err = os.Rename(name, s.path)
	}
	if err != nil {
		return err
	}
	if now-s.data.LastCleanup > 24*3600 {
		for domain, item := range s.data.Domains {
			last := 0.0
			if item.LastSuccess != nil && *item.LastSuccess > last {
				last = *item.LastSuccess
			}
			if item.LastFailure != nil && *item.LastFailure > last {
				last = *item.LastFailure
			}
			if last < now-30*24*3600 {
				delete(s.data.Domains, domain)
			}
		}
		s.data.LastCleanup = now
	}
	return nil
}
