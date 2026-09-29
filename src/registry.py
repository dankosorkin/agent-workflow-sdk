"""
Task registry.

Maps a task name to a TaskPlugin that knows how to register its own
CLI arguments and build a Task instance from parsed args. This is the
only place that imports concrete tasks, so the orchestrator core
(runner, acp, telemetry, task) stays free of task-specific imports.

To add a new autonomous agent:
  1. Create tasks/<your_task>/ with a Task subclass.
  2. Add a TaskPlugin entry to PLUGINS below.
Nothing else in src/ needs to change.
"""

import argparse
from dataclasses import dataclass
from typing import Callable

from .task import Task


@dataclass
class TaskPlugin:
    name: str
    help: str
    #: Default Kiro agent for this task (overridable via --agent).
    default_agent: str
    #: Adds task-specific CLI arguments to the given subparser.
    add_arguments: Callable[[argparse.ArgumentParser], None]
    #: Builds a Task instance from parsed args.
    build: Callable[[argparse.Namespace], Task]


# ---------------------------------------------------------------------------
# mongo-aggregation plugin
# ---------------------------------------------------------------------------

def _mongo_add_arguments(parser: argparse.ArgumentParser) -> None:
    from pathlib import Path

    parser.add_argument(
        "--aggregation", "-a",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to the aggregation pipeline JS file to optimize",
    )
    parser.add_argument(
        "--structure", "-s",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to the target schema/index structure JS file",
    )


def _mongo_build(args: argparse.Namespace) -> Task:
    from tasks.mongo_aggregation.task import (
        MongoAggregationConfig,
        MongoAggregationTask,
    )

    return MongoAggregationTask(
        MongoAggregationConfig(
            aggregation_file=args.aggregation,
            structure_file=args.structure,
        )
    )


# ---------------------------------------------------------------------------
# mongo-synthesis plugin
# ---------------------------------------------------------------------------

def _synthesis_add_arguments(parser: argparse.ArgumentParser) -> None:
    from pathlib import Path

    parser.add_argument(
        "--sources", "-S",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directory of source collection JSON files "
             "(default: tasks/mongo_synthesis/data)",
    )
    parser.add_argument(
        "--target", "-t",
        type=Path,
        default=None,
        metavar="FILE",
        help="Target structure JSON file "
             "(default: tasks/mongo_synthesis/structure/PartyContact.json)",
    )


def _synthesis_build(args: argparse.Namespace) -> Task:
    from tasks.mongo_synthesis.task import (
        DEFAULT_SOURCES_DIR,
        DEFAULT_TARGET_FILE,
        MongoSynthesisConfig,
        MongoSynthesisTask,
    )

    return MongoSynthesisTask(
        MongoSynthesisConfig(
            sources_dir=args.sources or DEFAULT_SOURCES_DIR,
            target_file=args.target or DEFAULT_TARGET_FILE,
        )
    )


# ---------------------------------------------------------------------------
# mongo-analyze plugin
# ---------------------------------------------------------------------------

def _analyze_add_arguments(parser: argparse.ArgumentParser) -> None:
    from pathlib import Path

    parser.add_argument(
        "--target", "-t",
        type=Path,
        default=None,
        metavar="FILE",
        help="Target structure JSON file, used for relationship hypotheses "
             "(default: tasks/mongo_synthesis/structure/PartyContact.json)",
    )


def _analyze_build(args: argparse.Namespace) -> Task:
    from tasks.mongo_analyze.task import (
        DEFAULT_TARGET_FILE,
        MongoAnalyzeConfig,
        MongoAnalyzeTask,
    )

    return MongoAnalyzeTask(
        MongoAnalyzeConfig(
            target_file=args.target or DEFAULT_TARGET_FILE,
        )
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

PLUGINS: dict[str, TaskPlugin] = {
    "mongo-aggregation": TaskPlugin(
        name="mongo-aggregation",
        help="Optimize a MongoDB aggregation pipeline",
        default_agent="aggregation-expert",
        add_arguments=_mongo_add_arguments,
        build=_mongo_build,
    ),
    "mongo-synthesis": TaskPlugin(
        name="mongo-synthesis",
        help="Synthesize an aggregation that maps sources to a target structure",
        default_agent="synthesis-expert",
        add_arguments=_synthesis_add_arguments,
        build=_synthesis_build,
    ),
    "mongo-analyze": TaskPlugin(
        name="mongo-analyze",
        help="Analyze a live MongoDB: ERD, cardinality, indexes, unique keys",
        default_agent="schema-analyst",
        add_arguments=_analyze_add_arguments,
        build=_analyze_build,
    ),
}


def get_plugin(name: str) -> TaskPlugin:
    if name not in PLUGINS:
        raise KeyError(name)
    return PLUGINS[name]
