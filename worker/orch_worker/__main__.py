"""orch-worker command line.

Exit codes: 0 after SIGTERM/SIGINT, 1 on an unexpected error, 2 on bad
configuration, 3 when the coordinator rejects us (close 1008), 4 when another
worker took over our node id (close 4000). Never restart on 2, 3 or 4.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import socket
import sys
from pathlib import Path

from .agent import EXIT_CONFIG, Config, FatalError, run
from .capabilities import detect, load_profile
from .runtimes import make_runtime

log = logging.getLogger("orch_worker")


async def _run_until_signalled(config: Config, runtime) -> None:
    """Runs the agent; SIGTERM (docker stop) or SIGINT cancels it, which closes
    the WebSocket with a normal close frame instead of dropping it."""
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await run(config, runtime=runtime)
    except asyncio.CancelledError:
        log.info("signal received; disconnected")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="orch-worker",
        description="Connects this machine to the coordinator. The cluster token is read "
                    "from CLUSTER_TOKEN or --token-file, never from a flag.",
    )
    parser.add_argument("--coordinator", default="ws://localhost:8080/v1/workers",
                        help="coordinator WebSocket URL (default: %(default)s)")
    parser.add_argument("--node-id", default=socket.gethostname(),
                        help="unique node name (default: hostname)")
    parser.add_argument("--slots", type=int, default=1, help="concurrent jobs (default: 1)")
    parser.add_argument("--profile", help="JSON file overriding detected capabilities, "
                                          "e.g. profiles/sim-cuda.json")
    parser.add_argument("--token-file", help="file containing the cluster token")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        token = (Path(args.token_file).read_text().strip() if args.token_file
                 else os.environ.get("CLUSTER_TOKEN", ""))
    except OSError as e:
        log.error("--token-file: %s", e)
        return EXIT_CONFIG
    if not token:
        log.error("no cluster token: set CLUSTER_TOKEN or pass --token-file")
        return EXIT_CONFIG
    if args.slots < 1:
        log.error("--slots must be at least 1")
        return EXIT_CONFIG
    try:
        capabilities = detect(load_profile(args.profile) if args.profile else None)
    except (OSError, ValueError) as e:
        log.error("invalid capabilities: %s", e)
        return EXIT_CONFIG

    log.info("capabilities: %s", capabilities)
    runtime_name = capabilities["runtimes"][0]
    try:
        runtime = make_runtime(runtime_name)
    except Exception as e:  # e.g. no reachable Docker daemon
        log.error("cannot use the %s runtime: %s", runtime_name, e)
        return EXIT_CONFIG
    config = Config(url=args.coordinator, token=token, node_id=args.node_id,
                    slots=args.slots, capabilities=capabilities)
    try:
        asyncio.run(_run_until_signalled(config, runtime))
    except FatalError as e:
        log.error("%s; exiting", e)
        return e.exit_code
    return 0


if __name__ == "__main__":
    sys.exit(main())
