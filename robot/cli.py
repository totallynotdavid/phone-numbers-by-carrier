from __future__ import annotations

import logging

import robot.config as config

from robot.observability import configure_logging, kv, new_run_id
from robot.runtime.orchestrator import run


def main(argv: list[str] | None = None) -> None:
    cfg = config.load(argv)
    run_id = new_run_id()

    configure_logging(debug=cfg.debug)
    logging.getLogger(__name__).info("run_start %s", kv(run_id=run_id))

    run(cfg, run_id=run_id)
