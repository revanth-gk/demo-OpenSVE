import asyncio
import json
import logging
import math
import os
import re
import uuid
from typing import Dict, List, Optional, Union
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import networkx as nx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aare-x-backend")

app = FastAPI(
    title="AARE-X v2 Platform Orchestrator API",
    description="Vertical Slice Prototype orchestrating adaptive retrieval, execution DAG routing, and verification pipelines.",
    version="2.0.0"
)

# Enable CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Local data cache
CORPUS: List[dict] = []
CORPUS_PATH = os.path.join(os.path.dirname(__file__), "corpus.json")

try:
    if os.path.exists(CORPUS_PATH):
        with open(CORPUS_PATH, "r", encoding="utf-8") as f:
            CORPUS = json.load(f)
        logger.info(f"Loaded {len(CORPUS)} documents from corpus.json")
    else:
        logger.warning("corpus.json not found! Initializing empty corpus.")
        CORPUS = []
except Exception as e:
    logger.error(f"Error loading corpus.json: {e}")
    CORPUS = []

# --- PYDANTIC MODELS (V2 COMPLIANT) ---

class QueryRequest(BaseModel):
    query: str = Field(..., min_length=3, description="The natural language engineering query.")
    token_budget: int = Field(2048, ge=512, le=8192, description="Token limit constraints.")
    latency_budget: float = Field(2.5, ge=0.5, le=10.0, description="Latency limit budget in seconds.")
    force_graph: bool = Field(False, description="Override planner to force knowledge graph retrieval.")
    force_cache: bool = Field(False, description="Override planner to bypass cache hit evaluation.")

class QueryIntent(BaseModel):
    query_type: str = Field(..., description="Classified type: FACTUAL, COMPARATIVE, RESEARCH, CODE, MULTI_HOP")
    detected_domain: str = Field(..., description="Primary engineering domain identified.")
    key_entities: List[str] = Field(default_factory=list, description="Extracted key technical entities.")
    confidence: float = Field(..., description="Classification model confidence score.")

class DAGNode(BaseModel):
    id: str
    label: str
    type: str = "executionNode"
    status: str = "pending" # pending, running, completed, error
    latency_ms: float = 0.0
    logs: str = ""

class DAGEdge(BaseModel):
    id: str
    source: str
    target: str
    animated: bool = False

class ExecutionDAG(BaseModel):
    nodes: List[dict]
    edges: List[dict]
    total_estimated_latency_ms: float

class RetrievalResult(BaseModel):
    doc_id: str
    title: str
    domain: str
    content: str
    scores: dict = Field(..., description="Scores for vector, bm25, and graph retrieval channels")
    rrf_score: float = Field(..., description="Reciprocal Rank Fusion score")

class VerifiedResponse(BaseModel):
    response: str
    references: List[str]
    citations: List[dict]
    hallucination_index: float
    verification_status: str # PASS, WARNING, RETRIED_PASS
    warnings: List[str]

# --- LOCAL SEMANTIC TEXT SEARCH & COGNITIVE LOGIC ---

def clean_and_tokenize(text: str) -> List[str]:
    # Extract alpha-numeric words for a simple TF-IDF fallback
    return re.findall(r"\b\w{3,}\b", text.lower())

def calculate_cosine_similarity(query: str, doc_text: str) -> float:
    # Quick TF-based cosine similarity calculation
    q_tokens = clean_and_tokenize(query)
    d_tokens = clean_and_tokenize(doc_text)
    if not q_tokens or not d_tokens:
        return 0.0
    
    # Term frequencies
    q_vocab = {}
    for t in q_tokens:
        q_vocab[t] = q_vocab.get(t, 0) + 1
        
    d_vocab = {}
    for t in d_tokens:
        d_vocab[t] = d_vocab.get(t, 0) + 1
        
    vocab = set(list(q_vocab.keys()) + list(d_vocab.keys()))
    
    # Compute dot product and norms
    dot_product = 0.0
    q_norm_sq = 0.0
    d_norm_sq = 0.0
    
    for word in vocab:
        q_val = q_vocab.get(word, 0)
        d_val = d_vocab.get(word, 0)
        dot_product += q_val * d_val
        q_norm_sq += q_val * q_val
        d_norm_sq += d_val * d_val
        
    if q_norm_sq == 0 or d_norm_sq == 0:
        return 0.0
    return dot_product / (math.sqrt(q_norm_sq) * math.sqrt(d_norm_sq))

def classify_query(query: str) -> QueryIntent:
    query_lower = query.lower()
    
    # Classify Query Type
    query_type = "FACTUAL"
    confidence = 0.82
    
    if "vs" in query_lower or "compare" in query_lower or "difference" in query_lower or "contrast" in query_lower:
        query_type = "COMPARATIVE"
        confidence = 0.94
    elif "code" in query_lower or "implement" in query_lower or "rust" in query_lower or "python" in query_lower or "function" in query_lower or "loop" in query_lower or "simd" in query_lower:
        query_type = "CODE"
        confidence = 0.91
    elif "research" in query_lower or "paper" in query_lower or "limits" in query_lower or "future" in query_lower or "architecture" in query_lower:
        query_type = "RESEARCH"
        confidence = 0.87
        
    # Check for multi-hop indications (complex terms from multiple domains)
    domains_mentioned = []
    if any(k in query_lower for k in ["raft", "paxos", "consensus", "replicated", "chubby", "spanner"]):
        domains_mentioned.append("distributed systems")
    if any(k in query_lower for k in ["hnsw", "hnsw", "vector", "ann", "quantization", "index", "ivf-pq", "lsm", "database"]):
        domains_mentioned.append("databases")
    if any(k in query_lower for k in ["llm", "context", "attention", "transformer", "flashattention", "decoding", "compile"]):
        domains_mentioned.append("AI compiler")
        
    if len(domains_mentioned) >= 2 or "multi-hop" in query_lower or "chain" in query_lower or "dependencies" in query_lower:
        query_type = "MULTI_HOP"
        confidence = 0.89

    # Default domain
    domain = "distributed systems" if not domains_mentioned else domains_mentioned[0]
    
    # Technical entity extraction
    entities = []
    entity_candidates = ["raft", "paxos", "hnsw", "ivf-pq", "crdt", "2pc", "spanner", "trueTime", "flashattention", "speculative decoding", "lsm-tree", "chubby"]
    for ent in entity_candidates:
        if ent in query_lower:
            entities.append(ent.upper())
            
    return QueryIntent(
        query_type=query_type,
        detected_domain=domain,
        key_entities=entities,
        confidence=confidence
    )

# --- ADAPTIVE RUNTIME PLANNER & DAG COMPILER ---

def build_execution_dag(request: QueryRequest, intent: QueryIntent) -> tuple[nx.DiGraph, ExecutionDAG]:
    G = nx.DiGraph()
    
    # Base nodes
    G.add_node("n_input", label="Ingest & Parse Query", type="inputNode", base_latency=50)
    G.add_node("n_classify", label="Query Intent Classifier", type="processNode", base_latency=120)
    
    # Semantic Cache check
    cache_latency = 80 if not request.force_cache else 10
    G.add_node("n_cache_check", label="Semantic Cache Evaluator", type="processNode", base_latency=cache_latency)
    G.add_edge("n_input", "n_classify")
    G.add_edge("n_classify", "n_cache_check")
    
    # Parallel retrieval nodes based on query classification & override flags
    retrievers = ["n_retrieve_vector"]
    G.add_node("n_retrieve_vector", label="Vector Store Retriever", type="retrieveNode", base_latency=350)
    G.add_edge("n_cache_check", "n_retrieve_vector")
    
    # If CODE, we explicitly require exact matching keywords using BM25
    if intent.query_type == "CODE" or "code" in request.query.lower():
        retrievers.append("n_retrieve_bm25")
        G.add_node("n_retrieve_bm25", label="BM25 Keyword Retriever", type="retrieveNode", base_latency=220)
        G.add_edge("n_cache_check", "n_retrieve_bm25")
        
    # If MULTI_HOP or force_graph override, inject Knowledge Graph traversal
    if intent.query_type == "MULTI_HOP" or request.force_graph:
        retrievers.append("n_retrieve_graph")
        G.add_node("n_retrieve_graph", label="Knowledge Graph Traverse", type="retrieveNode", base_latency=510)
        G.add_edge("n_cache_check", "n_retrieve_graph")
        
    # Join and Fuse Retrieval results
    G.add_node("n_ranker", label="RRF Rank Aggregator", type="processNode", base_latency=150)
    for ret in retrievers:
        G.add_edge(ret, "n_ranker")
        
    # Verification Pipeline
    G.add_node("n_verification", label="Cognitive Verification Engine", type="verifyNode", base_latency=450)
    G.add_edge("n_ranker", "n_verification")
    
    # Synthesis
    G.add_node("n_response_gen", label="Context Synthesizer Output", type="outputNode", base_latency=600)
    G.add_edge("n_verification", "n_response_gen")
    
    # Serialize to ReactFlow format for the workbench UI visualizer
    nodes = []
    edges = []
    
    # Simple layout math
    # We lay nodes out in levels (topological levels)
    topo_levels = list(nx.topological_generations(G))
    y_gap = 120
    x_gap = 220
    
    for level_idx, level in enumerate(topo_levels):
        y_pos = 50 + (level_idx * y_gap)
        level_len = len(level)
        x_start = -((level_len - 1) * x_gap) / 2
        
        for idx, node_id in enumerate(level):
            node_data = G.nodes[node_id]
            x_pos = x_start + (idx * x_gap)
            
            nodes.append({
                "id": node_id,
                "type": node_data.get("type", "executionNode"),
                "position": {"x": x_pos, "y": y_pos},
                "data": {
                    "label": node_data["label"],
                    "status": "pending",
                    "latency": 0.0,
                    "estimated": node_data["base_latency"],
                    "logs": f"Initialized stage node {node_id}."
                }
            })
            
    # Compile edges
    edge_idx = 0
    for u, v in G.edges():
        edges.append({
            "id": f"e_{u}_{v}_{edge_idx}",
            "source": u,
            "target": v,
            "animated": True if u in ["n_input", "n_classify"] else False
        })
        edge_idx += 1
        
    # Calculate estimated latency based on longest path (critical path)
    critical_path_latency = 0.0
    for node_id in G.nodes():
        critical_path_latency += G.nodes[node_id]["base_latency"]
        
    dag_model = ExecutionDAG(
        nodes=nodes,
        edges=edges,
        total_estimated_latency_ms=critical_path_latency
    )
    
    return G, dag_model

# --- IN MEMORY STATE MANAGERS FOR ACTIVE DEMO QUERIES ---

active_queries: Dict[str, dict] = {}

@app.post("/api/v1/query", response_model=Dict[str, Union[str, float, ExecutionDAG, QueryIntent]])
async def post_query(request: QueryRequest):
    query_id = str(uuid.uuid4())
    intent = classify_query(request.query)
    graph, dag = build_execution_dag(request, intent)
    
    # Store initial execution data
    active_queries[query_id] = {
        "request": request.model_dump(),
        "intent": intent.model_dump(),
        "dag": dag.model_dump(),
        "status": "initialized",
        "results": []
    }
    
    return {
        "query_id": query_id,
        "intent": intent,
        "dag": dag
    }

# --- SERVER-SENT EVENTS EXECUTION STREAM ---

@app.get("/api/v1/execution/{query_id}/stream")
async def stream_execution(query_id: str):
    if query_id not in active_queries:
        raise HTTPException(status_code=404, detail="Execution context not found.")
        
    context = active_queries[query_id]
    request_data = context["request"]
    intent_data = context["intent"]
    dag_data = context["dag"]
    query_str = request_data["query"]
    
    async def event_generator():
        # Read nodes topological order to run simulated distributed pipeline
        nodes_list = list(dag_data["nodes"])
        edges_list = list(dag_data["edges"])
        
        # Sort nodes topological-wise by finding topological layers
        id_to_node = {node["id"]: node for node in nodes_list}
        
        # Create execution layers using standard topological levels
        # n_input -> n_classify -> n_cache_check -> retrievers... -> ranker -> verification -> output
        execution_order = [
            ["n_input"],
            ["n_classify"],
            ["n_cache_check"],
            [n["id"] for n in nodes_list if n["id"] in ["n_retrieve_vector", "n_retrieve_bm25", "n_retrieve_graph"]],
            ["n_ranker"],
            ["n_verification"],
            ["n_response_gen"]
        ]
        
        # Keep track of active warnings, errors, and fetched logs
        warnings = []
        is_hallucination_prompt = any(word in query_str.lower() for word in ["hallucination", "conflict", "contradiction", "divergence"])
        
        # Run execution layers
        for layer in execution_order:
            if not layer:
                continue
                
            # Filter layout nodes active in this layer
            active_node_ids = [nid for nid in layer if nid in id_to_node]
            if not active_node_ids:
                continue
                
            # 1. Transition nodes to 'running'
            for nid in active_node_ids:
                node = id_to_node[nid]
                node["data"]["status"] = "running"
                node["data"]["logs"] = f"Evaluating operations inside distributed node cluster {nid}."
                
            # Broadcast state
            yield f"data: {json.dumps({'type': 'state_update', 'nodes': nodes_list})}\n\n"
            await asyncio.sleep(0.4) # Simulate distributed processing latency
            
            # 2. Complete processing, mock retrieval outputs, or verify data
            for nid in active_node_ids:
                node = id_to_node[nid]
                estimated = node["data"]["estimated"]
                
                # Add some simulated jitter / noise to processing speed
                actual_latency = round(estimated * (1.0 + (uuid.uuid4().int % 20 - 10) / 100.0), 1)
                node["data"]["latency"] = actual_latency
                node["data"]["status"] = "completed"
                
                # Custom Node Logs & Side Effects
                if nid == "n_input":
                    node["data"]["logs"] = f"Ingested query buffer size: {len(query_str)} bytes. Context payload bounded."
                elif nid == "n_classify":
                    node["data"]["logs"] = f"Classified Query Intent: {intent_data['query_type']} with {intent_data['confidence'] * 100:.1f}% confidence."
                elif nid == "n_cache_check":
                    if request_data["force_cache"]:
                        node["data"]["logs"] = "Cache lookup hit bypassed via override constraint. Re-routing."
                    else:
                        node["data"]["logs"] = "Cache miss. Query vector hash not found in redis-semantic-cache cluster."
                elif nid.startswith("n_retrieve_"):
                    # Simulating storage fetching
                    node["data"]["logs"] = f"Retrieval cluster processed vector searches in {actual_latency}ms. Partition shards queried: 8/8."
                elif nid == "n_ranker":
                    # Perform dynamic document matching & ranking against corpus.json
                    retrieved_docs = []
                    for doc in CORPUS:
                        sim = calculate_cosine_similarity(query_str, doc["content"])
                        # If domain matches, boost it
                        if doc["domain"] == intent_data["detected_domain"]:
                            sim += 0.15
                        if sim > 0.05:
                            # Fake individual search scores
                            v_score = round(sim * 0.95, 3)
                            b_score = round(sim * 0.85 + 0.05, 3)
                            g_score = round(sim * 0.75 + 0.10, 3) if "n_retrieve_graph" in id_to_node else 0.0
                            
                            # RRF Rank Fusion calculation
                            rrf = round((v_score + b_score + g_score) / 3.0, 3)
                            retrieved_docs.append({
                                "doc_id": doc["doc_id"],
                                "title": doc["title"],
                                "domain": doc["domain"],
                                "content": doc["content"],
                                "scores": {"vector": v_score, "bm25": b_score, "graph": g_score},
                                "rrf_score": rrf
                            })
                            
                    # Sort and take top 4
                    retrieved_docs = sorted(retrieved_docs, key=lambda x: x["rrf_score"], reverse=True)[:4]
                    context["results"] = retrieved_docs
                    
                    node["data"]["logs"] = f"Fused results from multiple indexes. Synthesized {len(retrieved_docs)} priority candidates."
                    
                    # Yield retrieved trace data immediately for the trace panel
                    yield f"data: {json.dumps({'type': 'trace_update', 'results': retrieved_docs})}\n\n"
                    
                elif nid == "n_verification":
                    if is_hallucination_prompt:
                        node["data"]["status"] = "error"
                        node["data"]["logs"] = "WARNING: Hallucination score exceeded threshold (0.68). Multi-hop path divergence detected!"
                        warnings.append("Consensus conflict flagged on cluster segment node.")
                        
                        yield f"data: {json.dumps({'type': 'state_update', 'nodes': nodes_list})}\n\n"
                        await asyncio.sleep(0.8) # Simulate retry loop delay
                        
                        # Automated recovery simulation
                        node["data"]["status"] = "completed"
                        node["data"]["latency"] += 350.0 # Add extra time for retry loop
                        node["data"]["logs"] = "Self-correction completed. Secondary verification parameters aligned. Consensus resolved."
                        warnings.append("Automated self-healing retry succeeded.")
                    else:
                        node["data"]["logs"] = "Passed standard semantic verification gate. All references aligned with consensus rules."
                        
                elif nid == "n_response_gen":
                    # Generate dynamic markdown content summarizing the technical answer
                    results = context.get("results", [])
                    ref_ids = [r["doc_id"] for r in results]
                    
                    if not results:
                        ans = "The orchestrator evaluated your query, but did not locate highly matching documents in the local technical corpus index. Please try queries focusing on HNSW, Raft, Paxos, or LLM KV optimizations."
                    else:
                        doc_refs_str = " ".join([f"[{r['doc_id']}]" for r in results])
                        ans = f"### System Orchestration Analysis\nBased on execution context analysis for your query, AARE-X platform queried the `{intent_data['detected_domain']}` domain and mapped key nodes. \n\n"
                        
                        ans += f"**Key Takeaways on {', '.join(intent_data['key_entities']) or 'Architectures'}:**\n"
                        for idx, doc in enumerate(results):
                            ans += f"- **{doc['title']}** (Confidence: `{doc['rrf_score']}`): {doc['content'][:220]}... [{doc['doc_id']}]\n"
                        
                        ans += f"\n\n**Cross-Cluster Verification Status:**\n"
                        if is_hallucination_prompt:
                            ans += f"The verification checks detected a transient consensus conflict. The platform triggered self-healing loops to resolve the mismatch. References verified: {', '.join(ref_ids)}."
                        else:
                            ans += f"All components are synchronized. The primary retrieved partitions [{ref_ids[0] if ref_ids else ''}] demonstrate valid replication parameters."
                            
                    # Verification metrics details
                    hallucination_index = 0.69 if is_hallucination_prompt else 0.08
                    v_status = "RETRIED_PASS" if is_hallucination_prompt else "PASS"
                    
                    resp_payload = VerifiedResponse(
                        response=ans,
                        references=ref_ids,
                        citations=[{"doc_id": r["doc_id"], "title": r["title"]} for r in results],
                        hallucination_index=hallucination_index,
                        verification_status=v_status,
                        warnings=warnings
                    )
                    
                    yield f"data: {json.dumps({'type': 'response_complete', 'data': resp_payload.model_dump()})}\n\n"
            
            # Yield completed state updates
            yield f"data: {json.dumps({'type': 'state_update', 'nodes': nodes_list})}\n\n"
            await asyncio.sleep(0.1)
            
        # Final connection end stream signal
        yield "data: [DONE]\n\n"
        
    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
