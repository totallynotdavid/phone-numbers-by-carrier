from __future__ import annotations

import argparse

from pathlib import Path

from robot.config.schema import RunConfig


def load_config(argv: list[str] | None = None) -> RunConfig:
    parser = argparse.ArgumentParser(prog="robot")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dedupe", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--session-budget", type=int, default=5)
    parser.add_argument("--wait-min-s", type=float, default=10.0)
    parser.add_argument("--wait-max-s", type=float, default=15.0)
    parser.add_argument("--same-session-retries", type=int, default=1)
    parser.add_argument("--ban-cooldown-s", type=float, default=180.0)
    parser.add_argument("--env-file", default=".env")
    ns = parser.parse_args(argv)

    errors: list[str] = []
    if ns.page_size < 1:
        errors.append("--page-size must be >= 1")
    if ns.workers < 1:
        errors.append("--workers must be >= 1")
    if ns.session_budget < 1:
        errors.append("--session-budget must be >= 1")
    if ns.wait_min_s < 0:
        errors.append("--wait-min-s must be >= 0")
    if ns.wait_max_s < ns.wait_min_s:
        errors.append("--wait-max-s must be >= --wait-min-s")
    if ns.same_session_retries < 0:
        errors.append("--same-session-retries must be >= 0")
    if ns.ban_cooldown_s < 0:
        errors.append("--ban-cooldown-s must be >= 0")
    if errors:
        parser.error("; ".join(errors))

    return RunConfig(
        input_csv=ns.input,
        output_csv=ns.output,
        page_size=ns.page_size,
        workers=ns.workers,
        dedupe=ns.dedupe,
        debug=ns.debug,
        session_budget=ns.session_budget,
        wait_min_s=ns.wait_min_s,
        wait_max_s=ns.wait_max_s,
        same_session_retries=ns.same_session_retries,
        ban_cooldown_s=ns.ban_cooldown_s,
        env_file=ns.env_file,
    )
