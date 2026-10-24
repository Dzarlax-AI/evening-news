"""Retired Python queue-worker command kept for import compatibility."""

import sys

from .services.background_execution import worker_healthcheck


if __name__ == "__main__":
    print(
        "The Python queue worker has been retired. Run the Go job worker and "
        "the private Python executor instead.",
        file=sys.stderr,
    )
    raise SystemExit(2)
