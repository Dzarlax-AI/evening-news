package cloakfixture

import (
	"context"
	"errors"
	"time"
)

// BudgetIsolation keeps a normal page waiting while a different context
// exhausts its wire budget, then verifies the waiting page and a fresh fetch.
func (p *Profiles) BudgetIsolation(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	good := p.start(ctx, "neighbor", "budget-neighbor", 20000)
	defer p.release("budget-neighbor")
	if _, err := p.wait(ctx, "budget-neighbor", 1); err != nil {
		return out, err
	}
	out["neighbor_arrival_confirmed"] = true
	bad, err := p.Worker.pageMonitorRejected(ctx, "browser-events/wire-gzip-fixed")
	out["oversized_outcome"] = bad.Outcome
	out["oversized_error"] = bad.Error
	if err != nil || bad.Error != "resource_limit" {
		return out, errors.New("budget_exhaustion_unconfirmed")
	}
	select {
	case <-good:
		return out, errors.New("neighbor_interrupted_by_other_budget")
	default:
		out["neighbor_pending_after_exhaustion"] = true
	}
	if err = p.release("budget-neighbor"); err != nil {
		return out, err
	}
	normal, err := receive(ctx, good)
	if err != nil || ensureSuccess(normal) != nil {
		return out, errors.New("neighbor_failed_after_other_budget")
	}
	out["neighbor_outcome"] = normal.Outcome
	fresh, err := p.Worker.PageMonitorRequest(ctx)
	if err != nil {
		return out, err
	}
	out["fresh_fetch_outcome"] = fresh.Outcome
	if err = p.Peer.AssertClean(ctx); err != nil {
		return out, err
	}
	out["cleanup_confirmed"] = true
	return out, nil
}
