//go:build linux

package webbackend

import (
	"os"
	"syscall"
	"time"
)

func changeTime(info os.FileInfo) time.Time {
	if s, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(s.Ctim.Sec, s.Ctim.Nsec)
	}
	return info.ModTime()
}
