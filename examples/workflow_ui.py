"""Run the workflow control plane (API + UI) on real agent CLIs.

    pip install -e '.[server]'
    python examples/workflow_ui.py --inventory examples/workflow_inventory.json

Then open http://127.0.0.1:8765/ . Nothing is called until you press "Generate plan":
the design roles run on Claude Code (inline agents, no tools, nothing written anywhere),
and approved plans run each step on the first backend listed for its agent in the
inventory. Binds 127.0.0.1 only; there is no auth.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from agentflow.server.workflow_app import serve
from agentflow.workflow import load_inventory
from agentflow.workflow.design import claude_designer
from agentflow.workflow.environment import Environment
from agentflow.workflow.runner import backend_runner
from agentflow.workflow.service import WorkflowService

REPO = Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--inventory", default=str(REPO / "examples" / "workflow_inventory.json"))
    p.add_argument(
        "--data", default=str(REPO / ".runs" / "workflow-ui"), help="records, store, checkpoints"
    )
    p.add_argument("--cwd", default=".", help="working directory the agents run in")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()

    inventory_path = Path(args.inventory).expanduser()
    cwd = Path(args.cwd).expanduser().resolve()
    service = WorkflowService(
        args.data,
        # Re-read on every call, so editing the file is picked up (and revokes approvals).
        inventory_source=lambda: load_inventory(inventory_path),
        designer=claude_designer(cwd),
        runner=backend_runner(cwd=cwd),
        environment=Environment(cwd, args.data),
    )
    print(f"inventory: {inventory_path}\ndata:      {args.data}\nagents cwd: {cwd}")
    print(f"open http://127.0.0.1:{args.port}/")
    serve(service, port=args.port, workspace=cwd)


if __name__ == "__main__":
    main()
