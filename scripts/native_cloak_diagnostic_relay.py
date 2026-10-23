#!/usr/bin/env python3
"""Run the normal restricted CDP relay with bounded synthetic-gate counters."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType


def instrumented_relay(base_class, diagnostics_report: Path):
    class DiagnosticsRelay(base_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Keep `synthetic` false: counters observe normal forwarding only.
            self.diagnostics_report = diagnostics_report

    return DiagnosticsRelay


def load_relay_module(source: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("native_cloak_relay_source", source)
    if spec is None or spec.loader is None:
        raise RuntimeError("relay_source_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--diagnostics-report", type=Path, required=True)
    parser.add_argument("--controller-ip", action="append", required=True)
    args = parser.parse_args()
    relay_module = load_relay_module(args.source)
    relay_module.Relay = instrumented_relay(relay_module.Relay, args.diagnostics_report)
    controllers = relay_module.private_controller_ips(args.controller_ip)
    asyncio.run(relay_module.serve(controllers, diagnostics_report=args.diagnostics_report))


if __name__ == "__main__":
    main()
