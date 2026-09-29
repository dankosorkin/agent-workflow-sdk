# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Codebase Q&A: a real multi-node workflow over Ollama with a real tool.

A three-node graph answers a question about THIS repository by actually
searching its files — no canned data, no ping-pong.

    plan  ──► search ──► answer ──► END
      ▲          │
      └──────────┘  (broaden once if the first search found nothing)

Nodes:
  plan    an LLM turns the question into a few search keywords (JSON list).
  search  the graph runs a real ripgrep/grep over the repo for those
          keywords and collects matching snippets. This is the tool step —
          the model asked for terms, the graph executed the search.
  answer  an LLM writes the final answer grounded ONLY in the snippets found.

A conditional edge loops plan->search once more with broadened terms if the
first pass found nothing, then gives up gracefully.

Run (needs a running Ollama with a chat model):
    python examples/codebase_qa_ollama.py "how does checkpointing work?"

Adds JSONL telemetry under .runs/ and prints per-node metrics.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Annotated

from agentflow import (
    END,
    START,
    Graph,
    JsonlTelemetry,
    MemoryCheckpointer,
    MultiHooks,
    RunMetrics,
    State,
    add,
    append,
    last,
)
from agentflow.backends.ollama import OllamaBackend
from agentflow.events import Message, TextChunk, ToolCall

REPO_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------


class QAState(State):
    question: Annotated[str, last]
    keywords: Annotated[list, last]  # current search terms
    snippets: Annotated[list, append]  # accumulated {file, line, text}
    attempts: Annotated[int, add]  # search passes made
    answer: Annotated[str, last]


# --------------------------------------------------------------------------
# The real tool: search the repository
# --------------------------------------------------------------------------


def search_files(keyword: str, *, max_hits: int = 8) -> list[dict]:
    """Search the repo for a keyword. Uses ripgrep if present, else Python."""
    hits: list[dict] = []
    rg = shutil.which("rg")
    if rg:
        try:
            out = subprocess.run(
                [
                    rg,
                    "-n",
                    "-i",
                    "--max-count",
                    str(max_hits),
                    "-g",
                    "*.py",
                    "-g",
                    "*.md",
                    keyword,
                    str(REPO_ROOT),
                ],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            ).stdout
            for line in out.splitlines()[: max_hits * 2]:
                m = re.match(r"^(.*?):(\d+):(.*)$", line)
                if m:
                    hits.append(
                        {
                            "file": _rel(m.group(1)),
                            "line": int(m.group(2)),
                            "text": m.group(3).strip()[:200],
                        }
                    )
        except (OSError, subprocess.TimeoutExpired):
            pass
        return hits[:max_hits]

    # Fallback: scan .py/.md files ourselves.
    needle = keyword.lower()
    for path in list(REPO_ROOT.rglob("*.py")) + list(REPO_ROOT.rglob("*.md")):
        if any(part in {".venv", "__pycache__", ".git", ".runs"} for part in path.parts):
            continue
        try:
            for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if needle in line.lower():
                    hits.append({"file": _rel(str(path)), "line": i, "text": line.strip()[:200]})
                    if len(hits) >= max_hits:
                        return hits
        except (OSError, UnicodeDecodeError):
            continue
    return hits


def _rel(p: str) -> str:
    try:
        return str(Path(p).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return p


# --------------------------------------------------------------------------
# Nodes
# --------------------------------------------------------------------------


def build_graph(llm: OllamaBackend):
    async def plan(state, ctx):
        broaden = state.get("attempts", 0) > 0
        instruction = (
            "You are planning a code search. Given the user's question about a "
            "Python library, output 2-4 short search keywords (single words or "
            "short phrases) most likely to appear in the source or docs. "
            + (
                "The first search found nothing — pick BROADER, more generic terms this time. "
                if broaden
                else ""
            )
            + "Reply with ONLY a JSON array of strings, nothing else."
        )
        messages = [
            Message(role="system", content=instruction),
            Message(role="user", content=state["question"]),
        ]
        text = await _complete(llm, messages, ctx)
        keywords = _parse_keywords(text) or _fallback_keywords(state["question"])
        return {"keywords": keywords}

    async def search(state, ctx):
        found: list[dict] = []
        seen = set()
        for kw in state.get("keywords", [])[:4]:
            ctx.emit(ToolCall(id=f"search:{kw}", name="search_files", args={"keyword": kw}))
            for hit in search_files(kw):
                key = (hit["file"], hit["line"])
                if key not in seen:
                    seen.add(key)
                    found.append(hit)
        return {"snippets": found, "attempts": 1}

    async def answer(state, ctx):
        snippets = state.get("snippets", [])
        if not snippets:
            return {"answer": "I couldn't find anything relevant in the repository."}
        context = "\n".join(f"- {s['file']}:{s['line']}: {s['text']}" for s in snippets[:20])
        messages = [
            Message(
                role="system",
                content=(
                    "Answer the user's question about this codebase using ONLY the "
                    "search results below. Cite files as path:line. Be concise. If "
                    "the results are insufficient, say so.\n\n"
                    f"Search results:\n{context}"
                ),
            ),
            Message(role="user", content=state["question"]),
        ]
        text = await _complete(llm, messages, ctx)
        return {"answer": text}

    def route_after_search(state):
        # Broaden once if nothing found on the first pass; otherwise answer.
        if not state.get("snippets") and state.get("attempts", 0) < 2:
            return "retry"
        return "answer"

    g = Graph(QAState)
    g.add_node("plan", plan)
    g.add_node("search", search)
    g.add_node("answer", answer)
    g.add_edge(START, "plan")
    g.add_edge("plan", "search")
    g.add_conditional_edges("search", route_after_search, {"retry": "plan", "answer": "answer"})
    g.add_edge("answer", END)
    return g


# --------------------------------------------------------------------------
# LLM helpers
# --------------------------------------------------------------------------


async def _complete(llm, messages, ctx) -> str:
    parts: list[str] = []
    async for ev in llm.chat(messages):
        if isinstance(ev, TextChunk):
            parts.append(ev.text)
            ctx.emit(ev)
    return "".join(parts).strip()


def _parse_keywords(text: str) -> list[str]:
    # Try to pull a JSON array out of the model's reply.
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if m:
        try:
            arr = json.loads(m.group(0))
            return [str(x).strip() for x in arr if str(x).strip()][:4]
        except json.JSONDecodeError:
            pass
    return []


def _fallback_keywords(question: str) -> list[str]:
    stop = {
        "how",
        "does",
        "the",
        "is",
        "a",
        "an",
        "what",
        "why",
        "in",
        "of",
        "and",
        "to",
        "do",
        "work",
        "works",
        "this",
        "that",
        "with",
        "for",
    }
    words = [w for w in re.findall(r"[a-zA-Z_]{3,}", question.lower()) if w not in stop]
    return words[:4] or ["def"]


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def _first_model() -> str | None:
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2) as resp:
            names = [m["name"] for m in json.load(resp).get("models", [])]
        return names[0] if names else None
    except Exception:  # noqa: BLE001
        return None


async def main() -> None:
    question = " ".join(sys.argv[1:]) or "How does checkpointing and resume work?"
    model = _first_model()
    if not model:
        print("Ollama not reachable or no models installed; skipping.")
        return

    llm = OllamaBackend(model)
    await llm.start()
    metrics = RunMetrics()
    telemetry = JsonlTelemetry(REPO_ROOT / ".runs")
    app = build_graph(llm).compile(
        checkpointer=MemoryCheckpointer(),
        hooks=MultiHooks(metrics, telemetry),
        step_limit=20,
    )

    print(f"model: {model}")
    print(f"question: {question}\n")
    try:
        # Stream so we can watch the workflow progress node by node.
        async for ev in app.stream({"question": question}, thread="qa"):
            if ev.kind == "node_start":
                print(f"[node] {ev.node}")
            elif ev.kind == "backend" and isinstance(ev.data, ToolCall):
                print(f"  [tool] search_files({ev.data.args.get('keyword')!r})")
        final = await app.get_state("qa")
    finally:
        await llm.close()

    state = final.state if final else {}
    print("\n=== keywords ===")
    print(state.get("keywords"))
    print("\n=== snippets found ===")
    for s in state.get("snippets", [])[:10]:
        print(f"  {s['file']}:{s['line']}")
    print("\n=== answer ===")
    print(state.get("answer"))
    print("\n=== metrics ===")
    print(json.dumps(metrics.summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
