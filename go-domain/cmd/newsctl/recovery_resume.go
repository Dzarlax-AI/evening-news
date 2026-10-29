package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
)

// resumeRecoveryProvision continues only the exact authenticated, staged bundle.
// SQL imports are single transactions; an existing schema is never overwritten.
type recoveryRestorer func(context.Context, managedsetup.State, string, string, string) error

func resumeRecoveryProvision(ctx context.Context, l managedsetup.Layout, s managedsetup.State, endpoint string, pending *pendingRecovery) error {
	return resumeRecoveryProvisionWithRestorer(ctx, l, s, endpoint, pending, restoreDatabase)
}
func resumeRecoveryProvisionWithRestorer(ctx context.Context, l managedsetup.Layout, s managedsetup.State, endpoint string, pending *pendingRecovery, restore recoveryRestorer) error {
	if pending == nil || pending.InstallationID != s.InstallationID {
		return errors.New("recovery identity mismatch")
	}
	owner, err := connectAt(ctx, s, "postgres", endpoint)
	if err != nil {
		return err
	}
	defer owner.Close()
	var ready bool
	if owner.QueryRow(ctx, `SELECT count(*)=2 AND bool_and(pg_get_userbyid(datdba)=current_user) FROM pg_database WHERE datname IN ('newsdb','schedulerdb')`).Scan(&ready) != nil || !ready {
		return errors.New("recovery databases unavailable or owned by another identity")
	}
	var active int
	names := []string{}
	for _, name := range roleNames {
		names = append(names, name)
	}
	if owner.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename=ANY($1)`, names).Scan(&active) != nil || active != 0 {
		return errors.New("stop runtime services before continuing recovery")
	}
	var rolesOK bool
	if owner.QueryRow(ctx, `SELECT count(*)=7 AND bool_and(rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls AND NOT rolreplication AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=pg_roles.oid OR m.roleid=pg_roles.oid)) FROM pg_roles WHERE rolname=ANY($1)`, names).Scan(&rolesOK) != nil || !rolesOK {
		return errors.New("recovery runtime role state changed")
	}
	if owner.QueryRow(ctx, `SELECT count(*)=2 AND bool_and(NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls AND NOT rolreplication AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=pg_roles.oid OR m.roleid=pg_roles.oid)) FROM pg_roles WHERE rolname=ANY($1)`, []string{"news_ingestion_fence_owner", "news_processing_fence_owner"}).Scan(&rolesOK) != nil || !rolesOK {
		return errors.New("recovery fence owner state changed")
	}
	news, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		return err
	}
	defer news.Close()
	var schema bool
	if news.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='news')`).Scan(&schema) != nil {
		return errors.New("recovery schema inspection failed")
	}
	if !schema {
		if err = restore(ctx, s, "newsdb", endpoint, filepath.Join(l.Setup, "recovery", "news.sql")); err != nil {
			return err
		}
	}
	var id string
	if news.QueryRow(ctx, `SELECT installation_id FROM news.installation_state WHERE id=1`).Scan(&id) != nil || id != s.InstallationID {
		return errors.New("existing recovery database identity does not match; no overwrite performed")
	}
	result, err := migration.Run(ctx, news, true, false)
	if err != nil || len(result.Errors) > 0 {
		return errors.New("restored schema migration failed")
	}
	if err = prepareRecoveredNewsOnce(ctx, news, s, pending.NewsSHA); err != nil {
		return err
	}
	if _, err = news.Exec(ctx, managedsetup.GrantPolicy); err != nil {
		return errors.New("recovery privilege provisioning failed")
	}
	scheduler, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		return err
	}
	defer scheduler.Close()
	if scheduler.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='scheduler_service')`).Scan(&schema) != nil {
		return errors.New("scheduler recovery inspection failed")
	}
	if !schema {
		if err = restore(ctx, s, "schedulerdb", endpoint, filepath.Join(l.Setup, "recovery", "scheduler.sql")); err != nil {
			return err
		}
	} else {
		var valid bool
		if scheduler.QueryRow(ctx, `SELECT count(*)=4 FROM information_schema.tables WHERE table_schema='scheduler_service' AND table_name=ANY($1)`, []string{"schedules", "occurrences", "control", "service_state"}).Scan(&valid) != nil || !valid {
			return errors.New("existing scheduler recovery schema is incomplete; no overwrite performed")
		}
	}
	var schedulerID, schedulerHash string
	if scheduler.QueryRow(ctx, `SELECT installation_id,bundle_sha FROM scheduler_service.news_recovery_identity WHERE id=1`).Scan(&schedulerID, &schedulerHash) != nil || schedulerID != s.InstallationID || schedulerHash != pending.SchedulerSHA {
		return errors.New("scheduler recovery identity does not match staged bundle; no mutation performed")
	}
	if err = pauseRecoveredScheduler(ctx, scheduler); err != nil {
		return err
	}
	if _, err = news.Exec(ctx, `UPDATE news.installation_state SET provisioning_complete=true WHERE id=1 AND installation_id=$1`, s.InstallationID); err != nil {
		return errors.New("recovery finalization failed")
	}
	if err = finishRecoveryStaging(l); err != nil {
		return err
	}
	fmt.Println("Recovery provisioning completed; runtime and scheduler remain paused")
	return nil
}
