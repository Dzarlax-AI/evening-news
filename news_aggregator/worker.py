"""Entrypoint for the separately deployed durable background worker."""

import argparse
import asyncio

from .services.background_execution import main as run_worker
from .services.background_execution import worker_healthcheck


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evening News durable background worker")
    parser.add_argument("--healthcheck", action="store_true",
                        help="Check the persisted worker heartbeat and exit")
    args = parser.parse_args()
    if args.healthcheck:
        raise SystemExit(asyncio.run(worker_healthcheck()))
    run_worker()
