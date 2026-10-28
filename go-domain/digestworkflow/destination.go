package digestworkflow

import (
	"context"
	"errors"
)

var ErrDestination = errors.New("telegram_configuration_incomplete")

// Destinations mirrors TelegramService's DB override and settings fallback,
// including get_legacy_config's service-to-environment-main fallback.
func Destinations(mainOverride, serviceOverride, mainDefault, serviceDefault string) (string, string, error) {
	main := mainOverride
	if main == "" {
		main = mainDefault
	}
	if main == "" {
		return "", "", ErrDestination
	}
	service := serviceOverride
	if service == "" {
		service = serviceDefault
	}
	if service == "" {
		service = mainDefault
	}
	if service == "" {
		service = main
	}
	return main, service, nil
}
func (w Workflow) Destination(ctx context.Context, mainDefault, serviceDefault string) (string, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx, `SELECT key, CASE WHEN jsonb_typeof(value::jsonb)='string' THEN value::jsonb #>> '{}' ELSE NULL END FROM settings WHERE key IN ('telegram_chat_id','telegram_service_chat_id')`)
	if err != nil {
		return "", err
	}
	var main, service string
	for rows.Next() {
		var key string
		var val *string
		if err = rows.Scan(&key, &val); err != nil {
			rows.Close()
			return "", err
		}
		if val != nil {
			if key == "telegram_chat_id" {
				main = *val
			} else {
				service = *val
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	main, _, err = Destinations(main, service, mainDefault, serviceDefault)
	return main, err
}
