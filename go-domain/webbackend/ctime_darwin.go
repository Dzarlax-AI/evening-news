//go:build darwin

package webbackend

import (
	"os"
	"syscall"
	"time"
)

func changeTime(info os.FileInfo) time.Time {
	if s, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(s.Ctimespec.Sec, s.Ctimespec.Nsec)
	}
	return info.ModTime()
}
