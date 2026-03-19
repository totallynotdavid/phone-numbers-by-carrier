from __future__ import annotations

import os
import queue

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from robot.core.types import RUC, RunSummary
from robot.ingest.input_csv import ReadStats, enqueue_rucs
from robot.providers.geonode import StickyProxyPool, load_geonode_config
from robot.runtime.worker import Worker, WorkerSettings


if TYPE_CHECKING:
    from robot.config.schema import RunConfig
    from robot.sink.writer import OutputWriter


def run_scheduler(
    cfg: RunConfig,
    *,
    writer: OutputWriter,
    checkpoint: set[str],
    run_id: str,
) -> RunSummary:
    task_queue: queue.Queue[RUC | None] = queue.Queue(maxsize=cfg.workers * 100)

    def produce() -> ReadStats:
        try:
            return enqueue_rucs(
                cfg.input_csv,
                task_queue,
                dedupe=cfg.dedupe,
                checkpoint=checkpoint,
            )
        finally:
            for _ in range(cfg.workers):
                task_queue.put(None)

    geonode = load_geonode_config(env_file=cfg.env_file)
    pool = StickyProxyPool(config=geonode, capacity=cfg.workers)

    settings = WorkerSettings(
        page_size=cfg.page_size,
        session_budget=cfg.session_budget,
        wait_min_s=cfg.wait_min_s,
        wait_max_s=cfg.wait_max_s,
        same_session_retries=cfg.same_session_retries,
        ban_cooldown_s=cfg.ban_cooldown_s,
        chrome_binary=os.getenv("CHROME_BINARY", ""),
    )

    with ThreadPoolExecutor(max_workers=1) as producer_pool:
        producer_future = producer_pool.submit(produce)
        with ThreadPoolExecutor(max_workers=cfg.workers) as worker_pool:
            futures = [
                worker_pool.submit(
                    Worker(
                        worker_id=idx,
                        run_id=run_id,
                        task_queue=task_queue,
                        proxy_pool=pool,
                        writer=writer,
                        settings=settings,
                    ).run
                )
                for idx in range(1, cfg.workers + 1)
            ]
            task_queue.join()

        read_stats = producer_future.result()

    if read_stats.valid == 0:
        msg = "no valid RUCs in input"
        raise RuntimeError(msg)

    summary = RunSummary(
        rows_read=read_stats.rows_read,
        valid=read_stats.valid,
        ignored=read_stats.ignored,
        duplicates=read_stats.duplicates,
        skipped=read_stats.skipped,
    )
    for future in futures:
        worker_summary = future.result()
        summary.processed += worker_summary.processed
        summary.succeeded += worker_summary.succeeded
        summary.failed += worker_summary.failed
    return summary
