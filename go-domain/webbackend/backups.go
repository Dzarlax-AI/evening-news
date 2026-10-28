package webbackend

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sys/unix"
	"math"
	"mime"

	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type cancelReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r cancelReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

var backupName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.tar\.gz$`)

type backupItem struct {
	Filename  string  `json:"filename"`
	Filepath  string  `json:"filepath"`
	Size      int64   `json:"size"`
	SizeMB    float64 `json:"size_mb"`
	CreatedAt string  `json:"created_at"`
}

func (s *Server) backupRoot() string {
	if s.BackupRoot != "" {
		return s.BackupRoot
	}
	return "backups"
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	root := s.backupRoot()
	if e := os.MkdirAll(root, 0755); e != nil {
		reject(w, 500, "Backup directory unavailable")
		return
	}
	files, e := os.ReadDir(root)
	if e != nil {
		reject(w, 500, "Backup directory unavailable")
		return
	}
	items := []backupItem{}
	for _, entry := range files {
		if !strings.HasSuffix(entry.Name(), ".tar.gz") || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, e := entry.Info()
		if e != nil || !info.Mode().IsRegular() {
			continue
		}
		created := changeTime(info).In(time.Local)
		items = append(items, backupItem{Filename: entry.Name(), Filepath: "backups/" + entry.Name(), Size: info.Size(), SizeMB: math.RoundToEven(float64(info.Size())/(1024*1024)*100) / 100, CreatedAt: created.Format("2006-01-02T15:04:05.000000")})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].CreatedAt > items[j].CreatedAt })
	write(w, 200, map[string]any{"backups": items, "total": len(items), "timestamp": timestamp()})
}
func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("filename")
	if !backupName.MatchString(name) {
		reject(w, 400, "Invalid filename")
		return
	}
	root, e := filepath.Abs(s.backupRoot())
	if e != nil {
		reject(w, 400, "Invalid filename")
		return
	}
	dir, e := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		reject(w, 404, "Backup file not found")
		return
	}
	defer unix.Close(dir)
	fd, e := unix.Openat(dir, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		if e == unix.ELOOP {
			reject(w, 400, "Invalid filename")
		} else {
			reject(w, 404, "Backup file not found")
		}
		return
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	info, e := file.Stat()
	if e != nil || !info.Mode().IsRegular() {
		reject(w, 404, "Backup file not found")
		return
	}
	if info.Size() > 64*1024*1024 {
		reject(w, 413, "Backup exceeds download limit")
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, name))
	http.ServeContent(w, r, name, info.ModTime(), file)
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filename string `json:"filename"`
	}
	if decodeObject(readAll(r), &req) != nil || len(strings.TrimSpace(req.Filename)) == 0 || len(req.Filename) > 190 {
		reject(w, 422, "Invalid request")
		return
	}
	name := strings.TrimSpace(req.Filename)
	if !backupName.MatchString(name) {
		reject(w, 400, "Invalid filename")
		return
	}
	info, err := os.Lstat(filepath.Join(s.backupRoot(), name))
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		reject(w, 400, "Invalid filename")
		return
	}
	if err != nil || !info.Mode().IsRegular() {
		reject(w, 404, "Backup file not found")
		return
	}
	reject(w, 501, "Backup restore is not implemented")
}

func (s *Server) cleanup(w http.ResponseWriter, r *http.Request) {
	req := struct {
		KeepCount *int `json:"keep_count"`
	}{}
	data := readAll(r)
	if decodeObject(data, &req) != nil || hasNullField(data) {
		reject(w, 422, "Invalid request")
		return
	}
	keep := 5
	if req.KeepCount != nil {
		keep = *req.KeepCount
	}
	if keep < 0 || keep > 100 {
		reject(w, 422, "Invalid request")
		return
	}
	root := s.backupRoot()
	if err := os.MkdirAll(root, 0755); err != nil {
		reject(w, 500, "Backup directory unavailable")
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		reject(w, 500, "Backup directory unavailable")
		return
	}
	type item struct {
		name     string
		modified time.Time
	}
	files := []item{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".tar.gz") || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := e.Info()
		if err == nil && info.Mode().IsRegular() {
			files = append(files, item{e.Name(), info.ModTime()})
		}
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].modified.After(files[j].modified) })
	removed := 0
	for i := keep; i < len(files); i++ {
		if s.Guard.Check(r.Context()) != nil {
			reject(w, 503, "Executor instance is not the database owner")
			return
		}
		if err := os.Remove(filepath.Join(root, files[i].name)); err != nil {
			reject(w, 500, "Backup cleanup failed")
			return
		}
		removed++
	}
	remaining := len(files) - removed
	write(w, 200, map[string]any{"message": fmt.Sprintf("Cleanup completed. Kept %d backups.", remaining), "deleted": removed, "remaining": remaining, "timestamp": timestamp()})
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	reader, err := r.MultipartReader()
	if err != nil {
		reject(w, 422, "A backup file is required")
		return
	}
	part, err := reader.NextPart()
	if err != nil || part.FormName() != "file" || part.FileName() == "" {
		reject(w, 422, "A backup file is required")
		return
	}
	_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil {
		reject(w, 422, "A backup file is required")
		return
	}
	name := params["filename"]
	if !backupName.MatchString(name) {
		reject(w, 400, "invalid backup filename")
		return
	}
	root := s.backupRoot()
	if err := os.MkdirAll(root, 0755); err != nil {
		reject(w, 500, "Backup directory unavailable")
		return
	}
	path := filepath.Join(root, name)
	if info, e := os.Lstat(path); e == nil && info.Mode()&os.ModeSymlink != 0 {
		reject(w, 400, "invalid backup path")
		return
	}
	file, err := os.CreateTemp(root, ".upload-*.tmp")
	if err != nil {
		reject(w, 500, "Backup upload failed")
		return
	}
	temp := file.Name()
	defer os.Remove(temp)
	defer file.Close()
	size, err := io.CopyBuffer(file, io.LimitReader(cancelReader{ctx: r.Context(), reader: part}, 64*1024*1024+1), make([]byte, 64*1024))
	if err != nil {
		reject(w, 500, "Backup upload failed")
		return
	}
	if size > 64*1024*1024 {
		reject(w, 413, "Backup exceeds upload limit")
		return
	}
	part.Close()
	if next, e := reader.NextPart(); e != io.EOF {
		if e == nil {
			next.Close()
		}
		reject(w, 422, "Invalid request")
		return
	}
	if err = file.Sync(); err != nil {
		reject(w, 500, "Backup upload failed")
		return
	}
	if err = file.Close(); err != nil {
		reject(w, 500, "Backup upload failed")
		return
	}
	if s.Guard.Check(r.Context()) != nil {
		reject(w, 503, "Executor instance is not the database owner")
		return
	}
	if info, e := os.Lstat(path); e == nil && info.Mode()&os.ModeSymlink != 0 {
		reject(w, 400, "invalid backup path")
		return
	}
	if err = os.Rename(temp, path); err != nil {
		reject(w, 500, "Backup upload failed")
		return
	}
	write(w, 200, map[string]any{"message": "Backup uploaded successfully", "filename": name, "size": size, "timestamp": timestamp()})
}
