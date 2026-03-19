from __future__ import annotations

import logging

from robot.config import load_config
from robot.observability import configure_logging, kv, new_run_id
from robot.runtime import run


def main(argv: list[str] | None = None) -> None:
    cfg = load_config(argv)
    run_id = new_run_id()

    configure_logging(debug=cfg.debug)
    logging.getLogger(__name__).info("run_start %s", kv(run_id=run_id))

    run(cfg, run_id=run_id)
