# Consolidated background fixture helpers

These helpers are mounted read-only into the disposable executor fixture only.
`lock_worker_status.py` holds one PostgreSQL row lock in the fixture database so
the real background worker heartbeat path can exercise lease loss and recovery.
It accepts only a bounded duration, prints no SQL or database values, and is not
part of any product image or runtime entrypoint.
