#!/usr/bin/env python3

"""
Autonomous agent optimization orchestrator — entrypoint.

Task-agnostic: parses CLI args, then either runs a single task's
iterate-until-converged loop, or runs a named linear chain of tasks.
All task-specific behavior lives in tasks/<task>/.

Usage:
    python main.py <task> [task args] [loop args]
    python main.py mongo-aggregation -a query.js -s schema.js -n 5
    python main.py chain mongo-full

See README.md for full documentation and how to add a new task.
"""

import sys

from src.cli import parse_args
from src.registry import get_plugin
from src.runner import AgentLoopRunner, LoopConfig


def _loop_kwargs(args) -> dict:
    """Shared loop settings common to tasks and chain steps."""
    return {
        "iterations": args.iterations,
        "model": args.model,
        "telemetry_dir": args.telemetry_dir,
        "engine": args.engine,
    }


def main() -> int:
    args = parse_args()

    # Chain mode: run a named linear chain of tasks.
    if args.task == "chain":
        from src.chain import ChainRunner, get_chain
        from src.cli import parse_seed

        steps = get_chain(args.chain_name)
        seed = parse_seed(args.seed)
        return ChainRunner(args.chain_name, steps, _loop_kwargs(args), seed=seed).run()

    # Single-task mode.
    plugin = get_plugin(args.task)
    task = plugin.build(args)
    config = LoopConfig(agent=args.agent, **_loop_kwargs(args))
    return AgentLoopRunner(task, config).run()


if __name__ == "__main__":
    sys.exit(main())
