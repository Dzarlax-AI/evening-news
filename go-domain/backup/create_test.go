package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCopyTreeDereferencesSymlinksAndRejectsCycles(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("file", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "copy")
	if err := copyTree(context.Background(), source, dst); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(dst, "link"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("link not dereferenced: %v %v", info, err)
	}
	data, err := os.ReadFile(filepath.Join(dst, "link"))
	if err != nil || string(data) != "fixture" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if err = os.WriteFile(filepath.Join(root, "outside"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(filepath.Join(root, "outside"), filepath.Join(source, "escape")); err != nil {
		t.Fatal(err)
	}
	if err = copyTree(context.Background(), source, filepath.Join(root, "escaped")); err == nil {
		t.Fatal("external symlink accepted")
	}
	if err = os.Remove(filepath.Join(source, "escape")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(".", filepath.Join(source, "cycle")); err != nil {
		t.Fatal(err)
	}
	if err = copyTree(context.Background(), source, filepath.Join(root, "cyclic")); err == nil {
		t.Fatal("directory cycle accepted")
	}
}

func TestBackupVersionProbeFailureDoesNotCreateStaging(t *testing.T) {
	t.Setenv("NEWS_BACKUP_MATCH_PG_MAJOR", "1")
	root := t.TempDir()
	pool, err := pgxpool.New(context.Background(), "postgres://fixture:fixture@127.0.0.1:1/gate_db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	svc := New(root, "postgres://fixture:fixture@127.0.0.1:1/gate_db?sslmode=disable")
	_, err = svc.Create(context.Background(), authority.Fence{Pool: pool})
	if err == nil || err.Error() != "database backup version unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging created on version error: %v %v", entries, err)
	}
}

func TestCancelledCopyLeavesNoCompletedArchive(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("synthetic fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "output")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := copyTree(ctx, source, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy error=%v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("canceled copy created output: %v", err)
	}
}

func TestServerVersionNumberPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost" && cfg.ConnConfig.Host != "::1" {
		t.Fatal("loopback fixture required")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	version, err := serverVersionNumber(context.Background(), pool)
	if err != nil || version/10000 < 16 || version/10000 > 17 {
		t.Fatalf("server version probe failed: version=%d error=%v", version, err)
	}
}
