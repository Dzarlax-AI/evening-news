package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

type memoryMetrics struct {
	RSSBytes        *uint64 `json:"rss_bytes"`
	RSSSource       string  `json:"rss_source"`
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64  `json:"heap_inuse_bytes"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	HeapObjects     uint64  `json:"heap_objects"`
	NumGC           uint32  `json:"num_gc"`
	Goroutines      int     `json:"goroutines"`
}

func readMemory() memoryMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	r := memoryMetrics{RSSSource: "unavailable", HeapAllocBytes: m.HeapAlloc, HeapInuseBytes: m.HeapInuse, TotalAllocBytes: m.TotalAlloc, HeapObjects: m.HeapObjects, NumGC: m.NumGC, Goroutines: runtime.NumGoroutine()}
	if runtime.GOOS == "linux" {
		if b, e := os.ReadFile("/proc/self/statm"); e == nil {
			f := strings.Fields(string(b))
			if len(f) > 1 {
				pages, e := strconv.ParseUint(f[1], 10, 64)
				if e == nil {
					v := pages * uint64(os.Getpagesize())
					r.RSSBytes = &v
					r.RSSSource = "proc_self_statm"
				}
			}
		}
	}
	return r
}
