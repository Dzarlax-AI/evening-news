// Package backup creates fenced, streamed database and application archives.
package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/jackc/pgx/v5"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Service struct{ Root, DatabaseURL, DumpBinary string }

func New(root, dsn string) *Service {
	return &Service{Root: root, DatabaseURL: dsn, DumpBinary: "pg_dump"}
}
func (s *Service) Create(ctx context.Context, f authority.Fence) (int64, error) {
	cfg, err := pgx.ParseConfig(strings.Replace(s.DatabaseURL, "postgresql+asyncpg://", "postgres://", 1))
	if err != nil {
		return 0, errors.New("invalid backup database configuration")
	}
	dumpBinary := s.DumpBinary
	if os.Getenv("NEWS_BACKUP_MATCH_PG_MAJOR") == "1" && dumpBinary == "pg_dump" {
		var serverVersion int
		if err = f.Pool.QueryRow(ctx, "SHOW server_version_num").Scan(&serverVersion); err != nil {
			return 0, errors.New("database backup version unavailable")
		}
		switch serverVersion / 10000 {
		case 16, 17:
			dumpBinary = filepath.Join("/usr/libexec", "postgresql"+strconv.Itoa(serverVersion/10000), "pg_dump")
		default:
			return 0, errors.New("unsupported backup database version")
		}
	}
	suffix := "_" + strings.ReplaceAll(f.JobID, "-", "") + "_a" + strconv.Itoa(f.AttemptGeneration)
	stamp := time.Now().Format("20060102_150405")
	base := "backup_" + stamp + suffix
	name := "news_aggregator_backup_" + stamp + suffix + ".tar.gz"
	root := filepath.Join(s.Root, "backups")
	if err = os.MkdirAll(root, 0750); err != nil {
		return 0, err
	}
	dir := filepath.Join(root, "."+base+".working")
	if err = os.Mkdir(dir, 0700); err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)
	dump, err := os.OpenFile(filepath.Join(dir, "database.sql"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}

	defer dump.Close()
	cmd := exec.CommandContext(ctx, dumpBinary, "-h", cfg.Host, "-p", strconv.Itoa(int(cfg.Port)), "-U", cfg.User, "--no-owner", "--no-acl", cfg.Database)
	cmd.Env = withoutEnv(os.Environ(), "PGPASSWORD")
	cmd.Env = append(cmd.Env, "PGPASSWORD="+cfg.Password)
	cmd.Stdout = dump
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	err = cmd.Run()
	closeErr := dump.Close()
	if err != nil {
		return 0, errors.New("database backup failed")
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	for _, item := range []string{".env", "docker-compose.yml", "data", "logs"} {
		source := filepath.Join(s.Root, item)
		if _, e := os.Lstat(source); errors.Is(e, os.ErrNotExist) {
			continue
		} else if e != nil {
			return 0, e
		}
		if err = copyTree(ctx, source, filepath.Join(dir, item)); err != nil {
			return 0, err
		}
	}
	metadata := map[string]any{"backup_date": time.Now().UTC().Format("2006-01-02T15:04:05.999999"), "database": cfg.Database, "database_type": "PostgreSQL", "version": "v2.0", "host": "internal_hostname", "contents": []string{"database.sql - Full PostgreSQL dump", ".env - Environment configuration", "docker-compose.yml - Docker configuration", "data/ - Application data files", "logs/ - Application logs"}}
	raw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return 0, err
	}
	if err = os.WriteFile(filepath.Join(dir, "backup_info.json"), raw, 0600); err != nil {
		return 0, err
	}
	staged := filepath.Join(root, "."+name+".working")
	archive, err := os.OpenFile(staged, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	defer os.Remove(staged)
	gz := gzip.NewWriter(archive)
	tarWriter := tar.NewWriter(gz)
	err = addArchive(ctx, tarWriter, dir, base)
	if e := tarWriter.Close(); err == nil {
		err = e
	}
	if e := gz.Close(); err == nil {
		err = e
	}
	if e := archive.Close(); err == nil {
		err = e
	}
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(staged)
	if err != nil {
		return 0, err
	}
	final := filepath.Join(root, name)
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if err = f.Validate(ctx, tx); err != nil {
		return 0, err
	}
	if err = os.Link(staged, final); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	if err = s.updateConfig(ctx, f); err != nil {
		return 0, err
	}
	return info.Size(), nil
}
func withoutEnv(env []string, key string) []string {
	out := make([]string, 0, len(env))
	prefix := key + "="
	for _, v := range env {
		if !strings.HasPrefix(v, prefix) {
			out = append(out, v)
		}
	}
	return out
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
func copyTree(ctx context.Context, src, dst string) error {
	abs, err := filepath.Abs(src)
	if err != nil {
		return err
	}
	root := abs
	entry, err := os.Lstat(abs)
	if err != nil {
		return err
	}
	if entry.Mode()&os.ModeSymlink != 0 && (filepath.Base(abs) == "data" || filepath.Base(abs) == "logs") {
		return errors.New("backup input root is a symlink")
	}
	if !entry.IsDir() {
		root = filepath.Dir(abs)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	return copyNode(ctx, abs, dst, root, map[string]bool{})
}

// Follow links only within the relevant input tree. Reject cycles and special
// files rather than silently omitting them from the backup.
func copyNode(ctx context.Context, src, dst, root string, active map[string]bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(src)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, real)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return errors.New("backup source escapes input root")
	}
	info, err := os.Stat(real)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if active[real] {
			return errors.New("backup directory cycle")
		}
		active[real] = true
		defer delete(active, real)
		if err = os.Mkdir(dst, info.Mode().Perm()); err != nil {
			return err
		}
		entries, err := os.ReadDir(real)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = copyNode(ctx, filepath.Join(real, entry.Name()), filepath.Join(dst, entry.Name()), root, active); err != nil {
				return err
			}
		}
		return os.Chtimes(dst, info.ModTime(), info.ModTime())
	}
	if !info.Mode().IsRegular() {
		return errors.New("unsupported backup source")
	}
	in, err := os.Open(real)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	_, copyErr := io.CopyBuffer(out, contextReader{ctx: ctx, reader: in}, make([]byte, 32*1024))
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Chtimes(dst, info.ModTime(), info.ModTime())
}
func addArchive(ctx context.Context, tw *tar.Writer, root, base string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, e := os.Lstat(path)
		if e != nil {
			return e
		}
		if !info.IsDir() && !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			return nil
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, e = os.Readlink(path)
			if e != nil {
				return e
			}
		}
		header, e := tar.FileInfoHeader(info, link)
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		header.Name = filepath.ToSlash(filepath.Join(base, rel))
		if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if e = tw.WriteHeader(header); e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, e := os.Open(path)
		if e != nil {
			return e
		}
		defer file.Close()
		_, e = io.CopyBuffer(tw, contextReader{ctx: ctx, reader: file}, make([]byte, 32*1024))
		return e
	})
}
func (s *Service) updateConfig(ctx context.Context, f authority.Fence) error {
	target := filepath.Join(s.Root, "backup_schedule.json")
	config := map[string]any{"enabled": false, "schedule_time": "03:00", "keep_days": 30, "last_backup": nil, "created_at": time.Now().UTC().Format("2006-01-02T15:04:05.999999")}
	if in, err := os.Open(target); err == nil {
		raw, e := io.ReadAll(io.LimitReader(in, 1024*1024+1))
		_ = in.Close()
		if e != nil {
			return e
		}
		if len(raw) > 1024*1024 {
			return errors.New("backup schedule exceeds limit")
		}
		if json.Unmarshal(raw, &config) != nil || config == nil {
			return errors.New("backup schedule invalid")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	config["last_backup"] = time.Now().UTC().Format("2006-01-02T15:04:05.999999")
	raw, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	temp := filepath.Join(s.Root, ".backup_schedule.json."+strings.ReplaceAll(f.JobID, "-", "")+".a"+strconv.Itoa(f.AttemptGeneration)+".working")
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(temp)
	if _, err = file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if err = f.Validate(ctx, tx); err != nil {
		return err
	}
	if err = os.Rename(temp, target); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
