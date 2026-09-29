"""
CLI argument parsing.

Builds a subcommand per registered task. Shared loop arguments
(iterations, agent, model, telemetry dir) are common to every task;
each task's plugin contributes its own task-specific arguments.

Usage:
    python main.py <task> [task args] [loop args]
    python main.py mongo-aggregation -a query.js -s schema.js -n 5
"""

import argparse
from pathlib import Path

from .registry import PLUGINS


DEFAULT_ITERATIONS = 10
DEFAULT_MODEL = "claude-opus-4.5"
DEFAULT_TELEMETRY_DIR = Path(__file__).resolve().parent.parent / "telemetry"


def _add_loop_arguments(parser: argparse.ArgumentParser, default_agent: str | None) -> None:
    """
    Arguments shared by every task's optimization loop. When default_agent is
    None (chain mode), the --agent flag is omitted because each chain step
    selects its own agent.
    """
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=DEFAULT_ITERATIONS,
        metavar="N",
        help=f"Maximum optimization iterations (default: {DEFAULT_ITERATIONS})",
    )
    if default_agent is not None:
        parser.add_argument(
            "--agent",
            default=default_agent,
            metavar="NAME",
            help=f"Kiro agent to use (default: {default_agent})",
        )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        metavar="MODEL",
        help=f"Model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--engine",
        default="v3",
        choices=["v2", "v3"],
        help="Kiro ACP agent engine (default: v3)",
    )
    parser.add_argument(
        "--telemetry-dir",
        type=Path,
        default=DEFAULT_TELEMETRY_DIR,
        metavar="DIR",
        help=f"Directory for telemetry JSONL files (default: {DEFAULT_TELEMETRY_DIR})",
    )


def parse_seed(seed_args: list[str]) -> dict:
    """
    Parse --seed ARTIFACT=PATH pairs into {artifact_name: Path}.
    Raises SystemExit with a clear message on malformed input.
    """
    seeds: dict = {}
    for item in seed_args or []:
        if "=" not in item:
            raise SystemExit(f"ERROR: --seed must be ARTIFACT=PATH, got: {item!r}")
        name, _, path = item.partition("=")
        name, path = name.strip(), path.strip()
        if not name or not path:
            raise SystemExit(f"ERROR: --seed must be ARTIFACT=PATH, got: {item!r}")
        seeds[name] = Path(path)
    return seeds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Autonomous agent optimization orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py mongo-aggregation -a query.js -s schema.js
  python main.py mongo-synthesis --sources data/ --target structure/PartyContact.json
  python main.py mongo-analyze
  python main.py chain mongo-full          # run analyze -> synthesis -> aggregation
  python main.py chain mongo-full --seed aggregation.js=baseline.js  # skip synthesis
        """,
    )

    subparsers = parser.add_subparsers(
        dest="task",
        metavar="TASK",
        required=True,
        help="The optimization task, or 'chain' to run a task chain",
    )

    # One subcommand per registered task plugin
    for plugin in PLUGINS.values():
        sub = subparsers.add_parser(
            plugin.name,
            help=plugin.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        plugin.add_arguments(sub)
        _add_loop_arguments(sub, plugin.default_agent)

    # A 'chain' subcommand that runs a named linear chain of tasks.
    from .chain import CHAINS
    chain_sub = subparsers.add_parser(
        "chain",
        help="Run a named linear chain of tasks (steps stop on first failure)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    chain_sub.add_argument(
        "chain_name",
        choices=list(CHAINS.keys()),
        metavar="CHAIN",
        help=f"Which chain to run. Available: {', '.join(CHAINS.keys())}",
    )
    chain_sub.add_argument(
        "--seed",
        action="append",
        default=[],
        metavar="ARTIFACT=PATH",
        help="Supply an artifact into best/ before the run, skipping the step "
             "that would produce it (e.g. --seed aggregation.js=baseline.js). "
             "Repeatable.",
    )
    _add_loop_arguments(chain_sub, default_agent=None)

    return parser.parse_args()
