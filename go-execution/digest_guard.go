package execution

import "context"

// A non-NULL phase is positive evidence that this attempt participated in the
// delivery guard. Older executors leave it NULL and are never presumed safe.
func (s *Server) prepareDigestDeliveryGuard(ctx context.Context, key attemptKey, x ExecutionContext) (func(context.Context) error, error) {
	q, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	var available bool
	err := s.pool.QueryRow(q, `SELECT EXISTS (SELECT 1 FROM pg_attribute
		WHERE attrelid='background_job_executions'::regclass AND attname='digest_delivery_started' AND NOT attisdropped)`).Scan(&available)
	if err != nil || !available {
		return nil, err
	}
	var scheduled bool
	err = s.pool.QueryRow(q, `SELECT occurrence_id IS NOT NULL AND scheduled_for IS NOT NULL
		FROM background_jobs WHERE id=$1`, key.job).Scan(&scheduled)
	if err != nil || !scheduled {
		return nil, err
	}
	err = s.fencedUpdate(ctx, key, x, `UPDATE background_job_executions SET digest_delivery_started=false
		WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3 AND state='running'
		AND digest_delivery_started IS NULL`)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return s.fencedUpdate(ctx, key, x, `UPDATE background_job_executions SET digest_delivery_started=true
			WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3 AND state='running'`)
	}, nil
}
