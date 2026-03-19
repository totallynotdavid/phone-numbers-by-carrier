from __future__ import annotations

import logging
import os
import queue

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

from robot.domain import RUC, Result, Status
from robot.io.reader import read_rucs
from robot.io.writer import OutputWriter, load_checkpoint
from robot.observability import kv, timed
from robot.runtime.proxies import build_pool_from_env
from robot.runtime.worker import Worker, WorkerSettings


if TYPE_CHECKING:
    from robot.config import Config


logger = logging.getLogger(__name__)


@dataclass
class Summary:
    rows_read: int = 0
    valid: int = 0
    ignored: int = 0
    duplicates: int = 0
    skipped: int = 0
    processed: int = 0
    succeeded: int = 0
    failed: int = 0


def run(cfg: Config, *, run_id: str) -> Summary:
    rucs, read_stats = read_rucs(cfg.input_csv, dedupe=cfg.dedupe)
    if not rucs:
        msg = "no valid RUCs in input"
        raise RuntimeError(msg)

    checkpoint = load_checkpoint(cfg.output_csv)
    pending = [ruc for ruc in rucs if str(ruc) not in checkpoint]

    summary = Summary(
        rows_read=read_stats.rows_read,
        valid=read_stats.valid,
        ignored=read_stats.ignored,
        duplicates=read_stats.duplicates,
        skipped=len(rucs) - len(pending),
    )

    with OutputWriter(cfg.output_csv) as writer:
        if cfg.use_snapshot:
            snapshot_summary = _run_snapshot(cfg, pending, writer)
        else:
            snapshot_summary = _run_live(cfg, pending, writer, run_id=run_id)

    summary.processed = snapshot_summary.processed
    summary.succeeded = snapshot_summary.succeeded
    summary.failed = snapshot_summary.failed

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


def _run_snapshot(cfg: Config, pending: list[RUC], writer: OutputWriter) -> Summary:
    from robot.snapshot.provider import SnapshotProvider

    if cfg.snapshot_json is None:
        msg = "snapshot_json is required in snapshot mode"
        raise RuntimeError(msg)

    summary = Summary()
    provider = SnapshotProvider(cfg.snapshot_json)
    with provider:
        for ruc in pending:
            with timed() as timer:
                lines = provider.count_lines(ruc)
            writer.write(Result(ruc=ruc, total_lines=lines, status=Status.OK))
            summary.processed += 1
            summary.succeeded += 1
            logger.info(
                "snapshot_lookup_ok %s",
                kv(ruc=ruc, lines=lines, elapsed_ms=timer.elapsed_ms),
            )
    return summary


def _run_live(
    cfg: Config, pending: list[RUC], writer: OutputWriter, *, run_id: str
) -> Summary:
    task_queue: queue.Queue[RUC] = queue.Queue()
    for ruc in pending:
        task_queue.put(ruc)

    proxy_pool = build_pool_from_env(env_file=cfg.env_file)
    workers = min(cfg.workers, len(proxy_pool.proxies), len(pending)) if pending else 0
    if workers < 1:
        return Summary()

    settings = WorkerSettings(
        page_size=cfg.page_size,
        session_budget=cfg.session_budget,
        wait_min_s=cfg.wait_min_s,
        wait_max_s=cfg.wait_max_s,
        same_session_retries=cfg.same_session_retries,
        ban_cooldown_s=cfg.ban_cooldown_s,
        chrome_binary=os.getenv("CHROME_BINARY", ""),
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                Worker(
                    worker_id=idx,
                    run_id=run_id,
                    task_queue=task_queue,
                    proxy_pool=proxy_pool,
                    writer=writer,
                    settings=settings,
                ).run
            )
            for idx in range(1, workers + 1)
        ]

    summary = Summary()
    for future in futures:
        worker_summary = future.result()
        summary.processed += worker_summary.processed
        summary.succeeded += worker_summary.succeeded
        summary.failed += worker_summary.failed
    return summary
