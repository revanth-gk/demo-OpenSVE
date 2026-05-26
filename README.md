# AARE-X v2 Platform Vertical Slice Prototype

This repository contains the high-fidelity vertical slice prototype of the AARE-X v2 Platform. It demonstrates a dark-industrial console UI with interactive visual Directed Acyclic Graphs (DAG) mapping runtime query execution, alongside a robust FastAPI orchestrator routing queries through adaptive retrieval pipelines and cognitive verification guardrails.

## Architecture Outline
- **Backend (FastAPI Monolith)**: Implements query intent classification, NetworkX DAG topology mapping, and an asynchronous Server-Sent Events (SSE) stream simulating distributed retrieval nodes latency.
- **Frontend (Next.js 14 / Tailwind CSS)**: Renders a three-panel console layout in monospace design. Automatically parses incoming SSE states to transition visual graph states and metric gauges dynamically.

---

## Getting Started

### 1. Run the Backend Server
Prerequisites: Python 3.10+

```bash
cd aare-x-proto/backend
pip install -r requirements.txt
python main.py
```
The FastAPI documentation will be available at `http://localhost:8000/docs`.

### 2. Run the Frontend Console
Prerequisites: Node.js 18+

```bash
cd aare-x-proto/frontend
npm install
npm run dev
```
Open `http://localhost:3000` to interact with the console workbench.

---

## High-Fidelity Demo Triggers
1. **Adaptive Retrieval Paths**:
   - Querying `"FlashAttention key-value context pruning heuristics"` or `"Optimize HNSW vector indexes layer construction criteria"` routes the graph through Vector search.
   - Querying `"Show me speculative decoding failure scenarios and rollback logs"` triggers the `CODE` classifier and injects a `BM25 Keyword Retriever` node.
   - Querying `"Compare Raft vs Paxos optimization loops"` triggers the `MULTI_HOP` classifier and automatically overlays a `Knowledge Graph Traverse` node on the execution path.
2. **Self-Healing Guardrails Loop**:
   - Querying any phrase containing `"hallucination"`, `"conflict"`, or `"divergence"` causes the Verification Engine node to transition into an `error` state, log a conflict, run self-correction consensus rules, and transition to complete, adding a latency penalty to trace logs.
3. **Consensus Metric Inspection**:
   - Clicking document micro-chips (e.g. `[doc_001]`) inside the Synthesized output dynamically highlights and focus-scrolls to the document's reciprocal rank scores under the Trace tab.
