"""The engine must not depend on the workflow layer (the dependency points one way)."""

from __future__ import annotations

import ast
from pathlib import Path

import agentflow

ROOT = Path(agentflow.__file__).parent


def test_engine_modules_never_import_the_workflow_layer():
    offenders = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT)
        if rel.parts[0] == "workflow":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
                # `from agentflow import workflow`
                if node.module == "agentflow":
                    names += [f"agentflow.{a.name}" for a in node.names]
            if any(n == "agentflow.workflow" or n.startswith("agentflow.workflow.") for n in names):
                offenders.append(str(rel))
    assert offenders == []


def test_the_layer_has_no_third_party_runtime_imports():
    import sys

    allowed = set(sys.stdlib_module_names) | {"agentflow", "__future__"}
    bad = []
    for path in (ROOT / "workflow").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                tops = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                tops = [(node.module or "").split(".")[0]]
            else:
                continue
            bad += [f"{path.name}: {t}" for t in tops if t not in allowed]
    assert bad == []
