from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from robot.ingest.checkpoint import load_completed_rucs
from robot.observability import kv
from robot.runtime.scheduler import run_scheduler
from robot.sink.writer import OutputWriter


if TYPE_CHECKING:
    from robot.config.schema import RunConfig
    from robot.core.types import RunSummary


logger = logging.getLogger(__name__)


def run(cfg: RunConfig, *, run_id: str) -> RunSummary:
    checkpoint = load_completed_rucs(cfg.output_csv)
    with OutputWriter(cfg.output_csv) as writer:
        summary = run_scheduler(
            cfg,
            writer=writer,
            checkpoint=checkpoint,
            run_id=run_id,
        )

    logger.info(
        "run_summary %s",
        kv(
            run_id=run_id,
            rows_read=summary.rows_read,
            valid=summary.valid,
            ignored=summary.ignored,
            duplicates=summary.duplicates,
            skipped=summary.skipped,
            processed=summary.processed,
            succeeded=summary.succeeded,
            failed=summary.failed,
        ),
    )
    return summary
