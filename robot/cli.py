from __future__ import annotations

import logging
import sys

from robot import config
from robot.batch.reader import read_rucs
from robot.batch.runner import run
from robot.batch.writer import OutputWriter, load_checkpoint
from robot.observability import configure_logging, kv, new_run_id
from robot.osiptel.provider import OsiptelProvider
from robot.snapshot.provider import SnapshotProvider


def main(argv: list[str] | None = None) -> None:
    cfg = config.load(argv)
    run_id = new_run_id()

    configure_logging(debug=cfg.debug)
    logging.getLogger(__name__).info(
        "run_start %s", kv(run_id=run_id, snapshot=cfg.use_snapshot)
    )

    rucs, read_stats = read_rucs(cfg.input_csv, dedupe=cfg.dedupe)
    if not rucs:
        sys.exit("no valid RUCs in input")

    checkpoint = load_checkpoint(cfg.output_csv)
    provider = (
        SnapshotProvider(cfg.snapshot_json)
        if cfg.use_snapshot
        else OsiptelProvider.from_env(cfg.page_size)
    )

    with provider, OutputWriter(cfg.output_csv, cfg.output_mode) as writer:
        summary = run(
            rucs,
            checkpoint,
            provider,
            writer,
            cfg.concurrency,
            read_stats,
            run_id=run_id,
        )

    logging.getLogger(__name__).info(
        "run_done %s",
        kv(
            run_id=run_id,
            rows_read=summary.rows_read,
            valid=summary.valid,
            ignored=summary.ignored,
            duplicates=summary.duplicates,
            skipped=summary.skipped,
            processed=summary.processed,
            ok=summary.succeeded,
            failed=summary.failed,
        ),
    )
