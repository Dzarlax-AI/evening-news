package appsettings

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestViberSettingsCAS(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL unavailable")
	}
	u, e := url.Parse(dsn)
	if e != nil || !isOperationalLoopback(u.Hostname()) {
		t.Fatal("loopback required")
	}
	for k := range u.Query() {
		switch strings.ToLower(k) {
		case "host", "hostaddr", "service", "servicefile":
			t.Fatal("invalid routing")
		}
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("viber_settings_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+q); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+q+" CASCADE")
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	_, e = pool.Exec(ctx, `CREATE TABLE settings(key text PRIMARY KEY,value jsonb,updated_at timestamptz);CREATE TABLE integration_credentials(provider text primary key,revision bigint,ciphertext bytea);CREATE TABLE viber_digest_deliveries(state text,updated_at timestamptz);INSERT INTO integration_credentials VALUES('viber',1,'\\x01');INSERT INTO viber_digest_deliveries VALUES('ready',now()),('completed',now()),('delivery_unknown',now());`)
	if e != nil {
		t.Fatal(e)
	}
	s := Store{Pool: pool}
	v, e := s.Viber(ctx)
	if e != nil || v.DeliveryEnabled || v.Revision != 0 {
		t.Fatal("bad defaults", e)
	}
	now := time.Now().UTC()
	in := ViberUpdate{ChannelID: "channel", ChannelName: "name", SenderID: "sender", CredentialRevision: 1, VerifiedCredentialRevision: 1, WebhookReady: true, VerifiedAt: &now, DeliveryEnabled: true}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := s.UpdateViber(ctx, in); results <- e }()
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for e := range results {
		if e == nil {
			success++
		} else if errors.Is(e, ErrConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("CAS failed")
	}
	in.ExpectedRevision = 1
	in.DeliveryEnabled = false
	if _, e = s.UpdateViber(ctx, in); e != nil {
		t.Fatal(e)
	}
	var cancelled, terminal int
	if e = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='cancelled'),count(*) FILTER (WHERE state IN ('completed','delivery_unknown')) FROM viber_digest_deliveries`).Scan(&cancelled, &terminal); e != nil || cancelled != 1 || terminal != 2 {
		t.Fatal("cancel failed", e)
	}
	// Exercise the real global settings API, not an equivalent direct SQL toggle.
	in.ExpectedRevision = 2
	in.DeliveryEnabled = true
	reenabled, e := s.UpdateViber(ctx, in)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO viber_digest_deliveries VALUES('waiting_edition',now()),('ready',now()),('retry_wait',now())`); e != nil {
		t.Fatal(e)
	}
	if reenabled.EnabledFrom == nil || reenabled.EnabledFrom.Nanosecond()%1000 != 0 {
		t.Fatal("activation epoch is not PostgreSQL microsecond precision")
	}
	global := operationalUpdate(0, 50)
	global.DeliveryEnabled = false
	if _, e = s.UpdateOperational(ctx, global); e != nil {
		t.Fatal("global off", e)
	}
	afterOff, e := s.Viber(ctx)
	if e != nil || !afterOff.DeliveryEnabled || afterOff.EnabledFrom == nil || !afterOff.EnabledFrom.After(*reenabled.EnabledFrom) || afterOff.Revision != reenabled.Revision+1 {
		t.Fatal("global off did not rotate Viber activation epoch", e)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_digest_deliveries WHERE state IN ('waiting_edition','ready','retry_wait')`).Scan(&cancelled); e != nil || cancelled != 0 {
		t.Fatal("global off left an old backlog", e)
	}
	global.ExpectedRevision = 1
	global.DeliveryEnabled = true
	if _, e = s.UpdateOperational(ctx, global); e != nil {
		t.Fatal("global on", e)
	}
	afterOn, e := s.Viber(ctx)
	if e != nil || afterOn.EnabledFrom == nil || !afterOn.EnabledFrom.After(*afterOff.EnabledFrom) || afterOn.Revision != afterOff.Revision+1 {
		t.Fatal("global on reused old activation epoch", e)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_digest_deliveries WHERE state IN ('completed','delivery_unknown')`).Scan(&terminal); e != nil || terminal != 2 {
		t.Fatal("global toggle changed terminal receipts", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE integration_credentials SET revision=2`); e != nil {
		t.Fatal(e)
	}
	in.ExpectedRevision = afterOn.Revision
	in.DeliveryEnabled = true
	if _, e = s.UpdateViber(ctx, in); !errors.Is(e, ErrInvalid) {
		t.Fatal("stale verification accepted", e)
	}
}
